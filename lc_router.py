from __future__ import annotations
import itertools
import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
import requests
from lc_credentials import Credential, CredentialFile, DEFAULT_OUTPUT, harvest_one, _select_browser
AUTO_REPLENISH = os.getenv("LC_AUTO_REPLENISH", "1") not in {"0", "false", "False", ""}
DEFAULT_ENDPOINT = "https://lumo.proton.me/api/ai/v1/chat/completions"
DEFAULT_REFERER = "https://lumo.proton.me/guest/"
DEFAULT_ORIGIN = "https://lumo.proton.me"
DEFAULT_APP_VERSION = "web-lumo@2.0.0.7"
DEFAULT_LOCALE = "en_US"
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36 Edg/150.0.0.0"
LUMO_TOOL_NAMES = {"proton_info", "web_search", "weather", "stock", "cryptocurrency", "generate_image", "describe_image", "edit_image", "web_extract"}
GOOD_THRESHOLD = 3
MIN_WORKING = 3
TARGET_WORKING = 5
MAX_TOTAL = 8
MAX_FAILURES = 3
COOLDOWN_BASE = 20.0
COOLDOWN_MAX = 300.0
HTTP_TIMEOUT = 90
MAX_ATTEMPTS = 3
@dataclass(frozen=True)
class LumoModel:
    id: str
    lumo_model: str
    label: str
    context_window: int
    aliases: tuple = ()
MODELS: List[LumoModel] = [
    LumoModel(id="qwen3.5-397b-a17b", lumo_model="lumo-lite", label="Qwen3.5 397B A17B", context_window=266000, aliases=("lumo-lite", "lumo-basic-v1", "qwen3.5", "qwen")),
    LumoModel(id="glm-5.2", lumo_model="lumo-max", label="GLM 5.2", context_window=200000, aliases=("lumo-max", "lumo-plus-v1", "glm5.2", "glm")),
]
class ModelRegistry:
    def __init__(self, models: List[LumoModel]) -> None:
        self._models = models
        self._by_key: Dict[str, LumoModel] = {}
        for model in models:
            self._by_key[model.id.lower()] = model
            self._by_key[model.label.lower()] = model
            for alias in model.aliases:
                self._by_key[alias.lower()] = model
    @property
    def models(self) -> List[LumoModel]:
        return list(self._models)
    def default(self) -> LumoModel:
        return self._models[0]
    def resolve(self, model_id: Optional[str]) -> LumoModel:
        if not model_id:
            return self.default()
        key = model_id.strip().lower()
        if key in {"auto", "lumo", "default"}:
            return self.default()
        return self._by_key.get(key, self.default())
REGISTRY = ModelRegistry(MODELS)
class _StreamError(RuntimeError):
    pass
class _LumoClient:
    def __init__(self, credential: Credential, timeout: int = HTTP_TIMEOUT) -> None:
        self.credential = credential
        self.timeout = timeout
        self.session = requests.Session()
    def _headers(self) -> Dict[str, str]:
        headers = {"accept": "application/vnd.protonmail.v1+json", "content-type": "application/json", "origin": DEFAULT_ORIGIN, "referer": DEFAULT_REFERER, "user-agent": self.credential.user_agent or DEFAULT_USER_AGENT, "x-pm-appversion": DEFAULT_APP_VERSION, "x-pm-locale": DEFAULT_LOCALE, "dnt": "1", "cookie": self.credential.cookie_header}
        if self.credential.x_pm_uid:
            headers["x-pm-uid"] = self.credential.x_pm_uid
        return headers
    def _payload(self, messages: List[Dict[str, Any]], lumo_model: str, tools: Optional[List[Dict[str, Any]]], tool_choice: Optional[Any], reasoning: bool) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"model": lumo_model, "messages": messages, "stream": True, "stream_options": {"include_usage": True}, "reasoning_effort": "high" if reasoning else "none", "lumo": {"client_type": "frontend", "target": "message"}}
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = tool_choice or "auto"
        return payload
    def stream(self, messages: List[Dict[str, Any]], lumo_model: str, tools: Optional[List[Dict[str, Any]]], tool_choice: Optional[Any], reasoning: bool) -> Generator[Dict[str, Any], None, None]:
        payload = self._payload(messages, lumo_model, tools, tool_choice, reasoning)
        with self.session.post(DEFAULT_ENDPOINT, headers=self._headers(), json=payload, stream=True, timeout=self.timeout) as resp:
            resp.raise_for_status()
            for line in resp.iter_lines(decode_unicode=True):
                if not line:
                    continue
                stripped = line.strip()
                if stripped.startswith(":"):
                    continue
                if stripped.startswith("data:"):
                    stripped = stripped[5:].strip()
                if not stripped:
                    continue
                if stripped == "[DONE]":
                    yield {"type": "done"}
                    return
                if not stripped.startswith("{"):
                    continue
                try:
                    obj = json.loads(stripped)
                except Exception:
                    continue
                for ev in self._events_from_obj(obj):
                    yield ev
    def _events_from_obj(self, obj: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        if obj.get("error"):
            yield {"type": "error", "message": str(obj.get("error"))}
            return
        marker = obj.get("object")
        if marker in {"lumo.image_data", "chat.tool_call", "chat.tool_result"}:
            return
        usage = obj.get("usage")
        if usage:
            yield {"type": "usage", "usage": usage}
        choices = obj.get("choices") or []
        if not choices:
            return
        choice = choices[0] or {}
        delta = choice.get("delta") or {}
        content = delta.get("content")
        if isinstance(content, str) and content:
            yield {"type": "content", "text": content}
        reasoning = delta.get("reasoning_content") or delta.get("reasoning")
        if isinstance(reasoning, str) and reasoning:
            yield {"type": "reasoning", "text": reasoning}
        for tc in delta.get("tool_calls") or []:
            fn = tc.get("function") or {}
            yield {"type": "tool_call_delta", "index": int(tc.get("index", 0) or 0), "id": tc.get("id"), "name": fn.get("name"), "arguments": fn.get("arguments") or ""}
        finish = choice.get("finish_reason")
        if finish == "content_filter":
            yield {"type": "finish", "finish_reason": "content_filter"}
        elif finish:
            yield {"type": "finish", "finish_reason": finish}
def _flatten_content(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    parts: List[str] = []
    if isinstance(content, list):
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                parts.append(str(part.get("text", "")))
            elif isinstance(part, str):
                parts.append(part)
    return "".join(parts)
def _parse_arguments(raw: Any) -> Any:
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return raw
    return {}
def to_lumo_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for msg in messages:
        role = msg.get("role")
        content = _flatten_content(msg.get("content"))
        tool_calls = msg.get("tool_calls")
        if role == "assistant" and tool_calls:
            if content:
                out.append({"role": "assistant", "content": content})
            for tc in tool_calls:
                fn = tc.get("function") or {}
                out.append({"role": "lumo_tool_call", "content": json.dumps({"name": fn.get("name"), "arguments": _parse_arguments(fn.get("arguments"))}, ensure_ascii=False)})
        elif role == "tool":
            out.append({"role": "tool", "content": content})
        else:
            out.append({"role": role or "user", "content": content})
    return out
def to_lumo_tools(tools: Optional[List[Any]]) -> Optional[List[Dict[str, Any]]]:
    if not tools:
        return None
    converted: List[Dict[str, Any]] = []
    for tool in tools:
        if isinstance(tool, str):
            converted.append({"name": tool})
            continue
        if not isinstance(tool, dict):
            continue
        fn = tool.get("function") if isinstance(tool.get("function"), dict) else None
        name = tool.get("name") or (fn.get("name") if fn else None)
        if not name:
            continue
        if name in LUMO_TOOL_NAMES and not fn:
            converted.append({"name": name})
        elif fn:
            entry: Dict[str, Any] = {"type": "function", "function": {"name": name}}
            if fn.get("description"):
                entry["function"]["description"] = fn["description"]
            if fn.get("parameters"):
                entry["function"]["parameters"] = fn["parameters"]
            converted.append(entry)
        else:
            converted.append({"name": name})
    return converted or None
@dataclass
class CredentialState:
    credential: Credential
    failures: int = 0
    successes: int = 0
    depleted: bool = False
    cooldown_until: float = 0.0
    last_used: float = 0.0
    last_error: Optional[str] = None
    def available(self, now: float) -> bool:
        return (not self.depleted) and now >= self.cooldown_until
class NoCredentialsError(RuntimeError):
    pass
class AllCredentialsBusyError(RuntimeError):
    pass
class CredentialPool:
    def __init__(self, path: Path = DEFAULT_OUTPUT) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self._states: List[CredentialState] = []
        self._cycle = itertools.cycle([])
        self._replenishing = False
        self.reload()
    def reload(self) -> int:
        creds = CredentialFile(self.path).load()
        with self._lock:
            existing = {s.credential.id: s for s in self._states}
            states: List[CredentialState] = []
            for cred in creds:
                if cred.id in existing:
                    state = existing[cred.id]
                    state.credential = cred
                    states.append(state)
                else:
                    states.append(CredentialState(credential=cred))
            self._states = states
            self._rebuild_cycle()
        return len(creds)
    def _rebuild_cycle(self) -> None:
        self._cycle = itertools.cycle(range(len(self._states))) if self._states else itertools.cycle([])
    def _save(self) -> None:
        CredentialFile(self.path).save([s.credential for s in self._states if not s.depleted])
    def total(self) -> int:
        with self._lock:
            return len(self._states)
    def working(self) -> int:
        with self._lock:
            return sum(1 for s in self._states if not s.depleted)
    def prune(self) -> int:
        with self._lock:
            before = len(self._states)
            self._states = [s for s in self._states if not s.depleted]
            self._rebuild_cycle()
            self._save()
            return before - len(self._states)
    def stats(self) -> Dict[str, Any]:
        now = time.time()
        with self._lock:
            return {"total": len(self._states), "working": sum(1 for s in self._states if not s.depleted), "available": sum(1 for s in self._states if s.available(now)), "credentials": [{"id": s.credential.id, "auth_cookie": s.credential.auth_cookie, "successes": s.successes, "failures": s.failures, "depleted": s.depleted, "cooldown_remaining": max(0.0, round(s.cooldown_until - now, 1)), "last_error": s.last_error} for s in self._states]}
    def acquire(self) -> Optional[CredentialState]:
        now = time.time()
        with self._lock:
            n = len(self._states)
            if n == 0:
                picked = None
            else:
                picked = None
                for _ in range(n):
                    idx = next(self._cycle)
                    state = self._states[idx]
                    if state.available(now):
                        state.last_used = now
                        picked = state
                        break
            low = sum(1 for s in self._states if not s.depleted) <= MIN_WORKING
        if low:
            self.maybe_replenish()
        return picked
    def report_result(self, state: CredentialState, ok: bool, depleted: bool = False, usage: Optional[Dict[str, Any]] = None, tier: Optional[str] = None, error: Optional[str] = None) -> None:
        changed = False
        with self._lock:
            if ok:
                state.successes += 1
                state.failures = 0
                state.cooldown_until = 0.0
                state.last_error = None
                remaining = (usage or {}).get("remaining_limits") or {}
                key = "lite" if tier == "lumo-lite" else "max"
                value = remaining.get(key)
                if value is not None and value <= 0:
                    state.depleted = True
                    changed = True
            else:
                state.failures += 1
                state.last_error = error
                if depleted or state.failures >= MAX_FAILURES:
                    state.depleted = True
                    changed = True
                else:
                    backoff = min(COOLDOWN_BASE * (2 ** (state.failures - 1)), COOLDOWN_MAX)
                    state.cooldown_until = time.time() + backoff
            if changed:
                self._states = [s for s in self._states if not s.depleted]
                self._rebuild_cycle()
                self._save()
            low = sum(1 for s in self._states if not s.depleted) <= MIN_WORKING
        if low:
            self.maybe_replenish()
    def _add(self, cred: Credential) -> None:
        with self._lock:
            self._states.append(CredentialState(credential=cred))
            self._rebuild_cycle()
            self._save()
    def maybe_replenish(self) -> None:
        if not AUTO_REPLENISH:
            return
        with self._lock:
            if self._replenishing:
                return
            working = sum(1 for s in self._states if not s.depleted)
            total = len(self._states)
            if working >= TARGET_WORKING or total >= MAX_TOTAL:
                return
            self._replenishing = True
        thread = threading.Thread(target=self._replenish_loop, daemon=True)
        thread.start()
    def _replenish_loop(self) -> None:
        try:
            browser = _select_browser()
            while True:
                if self.working() >= TARGET_WORKING or self.total() >= MAX_TOTAL:
                    break
                try:
                    cred = harvest_one(browser)
                except Exception:
                    break
                if cred.is_valid():
                    self._add(cred)
        finally:
            with self._lock:
                self._replenishing = False
    def ensure_ready(self, block: bool = True) -> None:
        self.prune()
        if self.working() > GOOD_THRESHOLD:
            self.maybe_replenish()
            return
        if not block:
            self.maybe_replenish()
            return
        browser = _select_browser()
        attempts = 0
        last_error = None
        while self.working() <= GOOD_THRESHOLD and self.total() < MAX_TOTAL and attempts < MAX_TOTAL:
            attempts += 1
            try:
                cred = harvest_one(browser)
            except Exception as exc:
                last_error = exc
                break
            if cred.is_valid():
                self._add(cred)
        if self.working() == 0 and last_error:
            import sys
            print(f"fetch error: {last_error}", file=sys.stderr)
        self.maybe_replenish()
class LumoRouter:
    def __init__(self, pool: Optional[CredentialPool] = None, registry: ModelRegistry = REGISTRY) -> None:
        self.pool = pool or CredentialPool()
        self.registry = registry
    def ensure_ready(self, block: bool = True) -> None:
        self.pool.ensure_ready(block=block)
    def stream(self, messages: List[Dict[str, Any]], model: Optional[str] = None, tools: Optional[List[Any]] = None, tool_choice: Optional[Any] = None, reasoning: bool = False) -> Generator[Dict[str, Any], None, None]:
        resolved = self.registry.resolve(model)
        lumo_messages = to_lumo_messages(messages)
        lumo_tools = to_lumo_tools(tools)
        if self.pool.total() == 0:
            self.pool.ensure_ready(block=True)
        if self.pool.total() == 0:
            raise NoCredentialsError(f"No credentials available and harvesting failed. Check the browser driver and network, then retry.")
        last_error: Optional[Exception] = None
        for _ in range(MAX_ATTEMPTS):
            state = self.pool.acquire()
            if state is None:
                raise AllCredentialsBusyError("All working credentials are cooling down; try again shortly.")
            client = _LumoClient(state.credential)
            emitted = False
            usage: Optional[Dict[str, Any]] = None
            try:
                for ev in client.stream(lumo_messages, resolved.lumo_model, lumo_tools, tool_choice, reasoning):
                    etype = ev.get("type")
                    if etype == "usage":
                        usage = ev.get("usage")
                    if etype == "error":
                        raise _StreamError(str(ev.get("message")))
                    if not emitted:
                        emitted = True
                        yield {"type": "route", "credential_id": state.credential.id, "model": resolved.id}
                    yield ev
                self.pool.report_result(state, ok=True, usage=usage, tier=resolved.lumo_model)
                return
            except requests.HTTPError as exc:
                code = exc.response.status_code if exc.response is not None else None
                depleted = code in (401, 403, 422, 429)
                self.pool.report_result(state, ok=False, depleted=depleted, error=f"http {code}")
                last_error = exc
                if emitted:
                    raise
                continue
            except _StreamError as exc:
                self.pool.report_result(state, ok=False, depleted=True, error=str(exc))
                last_error = exc
                if emitted:
                    raise
                continue
            except Exception as exc:
                self.pool.report_result(state, ok=False, depleted=False, error=str(exc))
                last_error = exc
                if emitted:
                    raise
                continue
        raise RuntimeError(f"All routing attempts failed. Last error: {last_error}")
    def collect(self, messages: List[Dict[str, Any]], model: Optional[str] = None, tools: Optional[List[Any]] = None, tool_choice: Optional[Any] = None, reasoning: bool = False) -> Dict[str, Any]:
        text_parts: List[str] = []
        reasoning_parts: List[str] = []
        tool_acc: Dict[int, Dict[str, Any]] = {}
        order: List[int] = []
        usage: Optional[Dict[str, Any]] = None
        credential_id: Optional[str] = None
        finish_reason = "stop"
        for ev in self.stream(messages, model=model, tools=tools, tool_choice=tool_choice, reasoning=reasoning):
            etype = ev.get("type")
            if etype == "route":
                credential_id = ev.get("credential_id")
            elif etype == "content":
                text_parts.append(ev.get("text", ""))
            elif etype == "reasoning":
                reasoning_parts.append(ev.get("text", ""))
            elif etype == "tool_call_delta":
                index = ev.get("index", 0)
                if index not in tool_acc:
                    tool_acc[index] = {"id": ev.get("id") or f"call_{uuid.uuid4().hex[:24]}", "name": ev.get("name") or "", "arguments": ""}
                    order.append(index)
                if ev.get("id"):
                    tool_acc[index]["id"] = ev["id"]
                if ev.get("name"):
                    tool_acc[index]["name"] = ev["name"]
                tool_acc[index]["arguments"] += ev.get("arguments") or ""
            elif etype == "usage":
                usage = ev.get("usage")
            elif etype == "finish":
                finish_reason = ev.get("finish_reason", finish_reason)
        tool_calls = [{"id": tool_acc[i]["id"], "type": "function", "function": {"name": tool_acc[i]["name"], "arguments": tool_acc[i]["arguments"]}} for i in order]
        if tool_calls:
            finish_reason = "tool_calls"
        return {"text": "".join(text_parts), "reasoning": "".join(reasoning_parts), "tool_calls": tool_calls, "usage": usage, "credential_id": credential_id, "model": self.registry.resolve(model), "finish_reason": finish_reason}
