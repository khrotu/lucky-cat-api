# lucky-cat-api

An OpenAI-compatible local server.

## Quick start

```powershell
pip install -r requirements.txt
python lc_server.py
```

Or with uvicorn:

```powershell
uvicorn lc_server:app --host 127.0.0.1 --port 8000
```

## Models

| ID | Context |
| --- | --- |
| `apertus-1.5` | 128k |
| `glm-5.3` | 128k |
| `qwen3.5-122b-a10b` | 128k |

## Endpoints

- `POST /v1/chat/completions` — chat completions. `stream: true` for SSE. Supports `tools`, `tool_choice`, and `reasoning_effort`.
- `GET  /v1/models` — list models.
- `GET  /v1/models/{id}` — retrieve one model.
- `GET  /health` — liveness plus live credential-pool stats.
- `POST /admin/reload` — reload `credentials.json` from disk without restarting.

`/chat/completions` and `/api/v1/chat/completions` are accepted as aliases of `/v1/chat/completions`.
