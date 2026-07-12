from __future__ import annotations
import functools
import json
import queue as _queue
import threading
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Optional, Union
import anyio
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from lc_router import AllCredentialsBusyError, LumoModel, LumoRouter, NoCredentialsError, REGISTRY
SERVER_NAME = "lucky-cat-api"
OWNER = "lucky-cat"
KEEPALIVE_SECONDS = 4.0
_KEEPALIVE = object()
class ChatMessage(BaseModel):
    role: str
    content: Union[str, List[Dict[str, Any]], None] = None
    name: Optional[str] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None
    tool_call_id: Optional[str] = None
class ChatCompletionRequest(BaseModel):
    model: Optional[str] = None
    messages: List[ChatMessage]
    stream: bool = False
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    max_tokens: Optional[int] = None
    tools: Optional[List[Any]] = None
    tool_choice: Optional[Any] = None
    reasoning_effort: Optional[str] = None
def _now() -> int:
    return int(time.time())
def _model_card(model: LumoModel) -> Dict[str, Any]:
    return {"id": model.id, "object": "model", "created": _now(), "owned_by": OWNER, "label": model.label, "context_window": model.context_window}
def _flatten_content(content: Union[str, List[Dict[str, Any]], None]) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    parts: List[str] = []
    for part in content:
        if isinstance(part, dict) and part.get("type") == "text":
            parts.append(str(part.get("text", "")))
        elif isinstance(part, str):
            parts.append(part)
    return "".join(parts)
def _messages_to_dicts(messages: List[ChatMessage]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for msg in messages:
        entry: Dict[str, Any] = {"role": msg.role, "content": msg.content}
        if msg.tool_calls:
            entry["tool_calls"] = msg.tool_calls
        if msg.tool_call_id:
            entry["tool_call_id"] = msg.tool_call_id
        out.append(entry)
    return out
def _wants_reasoning(req: ChatCompletionRequest) -> bool:
    return (req.reasoning_effort or "none").lower() not in {"none", "", "low"}
def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    router = LumoRouter()
    app.state.router = router
    print(f"[{SERVER_NAME}] pruning depleted credentials and ensuring a working pool...")
    await anyio.to_thread.run_sync(lambda: router.ensure_ready(block=True))
    print(f"[{SERVER_NAME}] working credentials: {router.pool.working()} (total {router.pool.total()})")
    yield
app = FastAPI(title=SERVER_NAME, version="2.0.0", lifespan=lifespan)
def get_router(request: Request) -> LumoRouter:
    return request.app.state.router
@app.get("/v1/models")
@app.get("/models")
async def list_models() -> Dict[str, Any]:
    return {"object": "list", "data": [_model_card(m) for m in REGISTRY.models]}
@app.get("/v1/models/{model_id:path}")
@app.get("/models/{model_id:path}")
async def retrieve_model(model_id: str) -> Dict[str, Any]:
    key = model_id.strip().lower()
    resolved = REGISTRY.resolve(model_id)
    known = {resolved.id.lower(), resolved.label.lower(), *[a.lower() for a in resolved.aliases]}
    if key not in known:
        raise HTTPException(status_code=404, detail=f"model '{model_id}' not found")
    return _model_card(resolved)
@app.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    router = get_router(request)
    return {"status": "ok", "server": SERVER_NAME, "models": [m.id for m in REGISTRY.models], "credentials": router.pool.stats()}
@app.post("/admin/reload")
async def reload_credentials(request: Request) -> Dict[str, Any]:
    router = get_router(request)
    count = router.pool.reload()
    return {"reloaded": count, "credentials": router.pool.stats()}
def _chunk(cid: str, created: int, model_id: str, delta: Dict[str, Any], finish_reason: Optional[str] = None) -> str:
    payload = {"id": cid, "object": "chat.completion.chunk", "created": created, "model": model_id, "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}]}
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
async def _stream_response(router: LumoRouter, messages: List[Dict[str, Any]], model: LumoModel, tools: Optional[List[Any]], tool_choice: Optional[Any], reasoning: bool) -> AsyncGenerator[str, None]:
    cid = f"chatcmpl-{uuid.uuid4().hex}"
    created = _now()
    finish_reason = "stop"
    started = False
    usage: Optional[Dict[str, Any]] = None
    tool_acc: Dict[int, Dict[str, Any]] = {}
    tool_order: List[int] = []
    tool_started: Dict[int, bool] = {}
    gen = router.stream(messages, model=model.id, tools=tools, tool_choice=tool_choice, reasoning=reasoning)
    events: "_queue.Queue[Any]" = _queue.Queue()
    _sentinel = object()
    def _pump() -> None:
        try:
            for item in gen:
                events.put(("ev", item))
        except BaseException as exc:
            events.put(("exc", exc))
        finally:
            events.put(_sentinel)
    worker = threading.Thread(target=_pump, daemon=True)
    worker.start()
    try:
        while True:
            try:
                item = await anyio.to_thread.run_sync(functools.partial(events.get, timeout=KEEPALIVE_SECONDS))
            except _queue.Empty:
                yield ": keepalive\n\n"
                continue
            if item is _sentinel:
                break
            kind, ev = item
            if kind == "exc":
                raise ev
            etype = ev.get("type")
            if etype == "content":
                text = ev.get("text", "")
                if not text:
                    continue
                if not started:
                    started = True
                    yield _chunk(cid, created, model.id, {"role": "assistant", "content": ""})
                yield _chunk(cid, created, model.id, {"content": text})
            elif etype == "reasoning":
                text = ev.get("text", "")
                if not text:
                    continue
                if not started:
                    started = True
                    yield _chunk(cid, created, model.id, {"role": "assistant", "content": ""})
                yield _chunk(cid, created, model.id, {"reasoning_content": text})
            elif etype == "tool_call_delta":
                if not started:
                    started = True
                    yield _chunk(cid, created, model.id, {"role": "assistant", "content": ""})
                index = ev.get("index", 0)
                if index not in tool_acc:
                    tool_acc[index] = {"id": ev.get("id") or f"call_{uuid.uuid4().hex[:24]}", "name": ev.get("name") or ""}
                    tool_order.append(index)
                if ev.get("id"):
                    tool_acc[index]["id"] = ev["id"]
                if ev.get("name"):
                    tool_acc[index]["name"] = ev["name"]
                fn_delta: Dict[str, Any] = {}
                if not tool_started.get(index):
                    tool_started[index] = True
                    fn_delta = {"id": tool_acc[index]["id"], "type": "function", "function": {"name": tool_acc[index]["name"], "arguments": ev.get("arguments") or ""}}
                else:
                    fn_delta = {"function": {"arguments": ev.get("arguments") or ""}}
                yield _chunk(cid, created, model.id, {"tool_calls": [{"index": list(tool_order).index(index), **fn_delta}]})
                finish_reason = "tool_calls"
            elif etype == "usage":
                usage = ev.get("usage")
            elif etype == "finish":
                if ev.get("finish_reason") == "content_filter":
                    finish_reason = "content_filter"
    except (NoCredentialsError, AllCredentialsBusyError) as exc:
        yield f"data: {json.dumps({'error': {'message': str(exc), 'type': 'service_unavailable'}})}\n\n"
        yield "data: [DONE]\n\n"
        return
    except Exception as exc:
        yield f"data: {json.dumps({'error': {'message': str(exc), 'type': 'upstream_error'}})}\n\n"
        yield "data: [DONE]\n\n"
        return
    if not started:
        yield _chunk(cid, created, model.id, {"role": "assistant", "content": ""})
    final_payload: Dict[str, Any] = {"id": cid, "object": "chat.completion.chunk", "created": created, "model": model.id, "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}]}
    if usage:
        final_payload["usage"] = {"completion_tokens": usage.get("completion_tokens", 0), "prompt_tokens": usage.get("prompt_tokens", 0), "total_tokens": usage.get("completion_tokens", 0) + usage.get("prompt_tokens", 0)}
    yield f"data: {json.dumps(final_payload, ensure_ascii=False)}\n\n"
    yield "data: [DONE]\n\n"
@app.post("/v1/chat/completions")
@app.post("/chat/completions")
@app.post("/api/v1/chat/completions")
async def chat_completions(request: Request, body: ChatCompletionRequest):
    router = get_router(request)
    model = REGISTRY.resolve(body.model)
    messages = _messages_to_dicts(body.messages)
    reasoning = _wants_reasoning(body)
    if body.stream:
        return StreamingResponse(_stream_response(router, messages, model, body.tools, body.tool_choice, reasoning), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
    try:
        result = await anyio.to_thread.run_sync(lambda: router.collect(messages, model=model.id, tools=body.tools, tool_choice=body.tool_choice, reasoning=reasoning))
    except NoCredentialsError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except AllCredentialsBusyError as exc:
        raise HTTPException(status_code=429, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"upstream error: {exc}")
    prompt_text = "".join(_flatten_content(m.content) for m in body.messages)
    completion_text = result["text"]
    usage = result.get("usage") or {}
    prompt_tokens = usage.get("prompt_tokens", _estimate_tokens(prompt_text))
    completion_tokens = usage.get("completion_tokens", _estimate_tokens(completion_text))
    message: Dict[str, Any] = {"role": "assistant", "content": completion_text or None}
    if result.get("reasoning"):
        message["reasoning_content"] = result["reasoning"]
    if result.get("tool_calls"):
        message["tool_calls"] = result["tool_calls"]
    return JSONResponse({"id": f"chatcmpl-{uuid.uuid4().hex}", "object": "chat.completion", "created": _now(), "model": model.id, "choices": [{"index": 0, "message": message, "finish_reason": result.get("finish_reason", "stop")}], "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens, "total_tokens": prompt_tokens + completion_tokens}})
def main() -> None:
    import uvicorn
    uvicorn.run("lc_server:app", host="127.0.0.1", port=8000, reload=False)
if __name__ == "__main__":
    main()
