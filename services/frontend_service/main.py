from __future__ import annotations

import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Frontend Service", version="1.0.0")
ROOT = Path(__file__).resolve().parents[2]
STATIC_DIR = ROOT / "app" / "static"
ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://127.0.0.1:18010").rstrip("/")
PLANNER_SERVICE_URL = os.getenv("PLANNER_SERVICE_URL", "http://127.0.0.1:18011").rstrip("/")
EXECUTOR_SERVICE_URL = os.getenv("EXECUTOR_SERVICE_URL", "http://127.0.0.1:18012").rstrip("/")
TOOL_BACKEND_SERVICE_URL = os.getenv("TOOL_BACKEND_SERVICE_URL", "http://127.0.0.1:18013").rstrip("/")

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/")
def index() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/tools")
def tools() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "tools.html"))


@app.get("/manage")
def manage() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "manage.html"))


@app.get("/api-docs")
def api_docs() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "api_docs.html"))


@app.get("/api/docs/services")
def api_docs_services() -> dict:
    return {
        "services": [
            {"id": "orchestrator", "name": "orchestrator-service", "base_url": ORCHESTRATOR_URL, "docs_url": ORCHESTRATOR_URL + "/docs", "openapi_url": ORCHESTRATOR_URL + "/openapi.json"},
            {"id": "planner", "name": "planner-service", "base_url": PLANNER_SERVICE_URL, "docs_url": PLANNER_SERVICE_URL + "/docs", "openapi_url": PLANNER_SERVICE_URL + "/openapi.json"},
            {"id": "executor", "name": "executor-service", "base_url": EXECUTOR_SERVICE_URL, "docs_url": EXECUTOR_SERVICE_URL + "/docs", "openapi_url": EXECUTOR_SERVICE_URL + "/openapi.json"},
            {"id": "tool_backend", "name": "tool-backend-service", "base_url": TOOL_BACKEND_SERVICE_URL, "docs_url": TOOL_BACKEND_SERVICE_URL + "/docs", "openapi_url": TOOL_BACKEND_SERVICE_URL + "/openapi.json"},
        ]
    }


def _proxy(method: str, path: str, query: str, body: bytes, headers: dict) -> Response:
    url = ORCHESTRATOR_URL + path
    if query:
        url = url + "?" + query
    req_headers = {}
    for k, v in headers.items():
        lk = k.lower()
        if lk in {"host", "content-length", "connection"}:
            continue
        req_headers[k] = v
    req = urllib.request.Request(url=url, data=body if method in {"POST", "PUT", "PATCH", "DELETE"} else None, method=method, headers=req_headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as rsp:  # noqa: S310
            content = rsp.read()
            ctype = rsp.headers.get("Content-Type", "application/json")
            return Response(content=content, status_code=rsp.status, media_type=ctype.split(";")[0])
    except urllib.error.HTTPError as exc:
        content = exc.read()
        ctype = exc.headers.get("Content-Type", "application/json") if exc.headers else "application/json"
        return Response(content=content, status_code=exc.code, media_type=ctype.split(";")[0])
    except Exception:
        return Response(content=b'{"detail":"proxy_error"}', status_code=502, media_type="application/json")


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy_all(path: str, request: Request) -> Response:
    if path.startswith("static/"):
        return Response(status_code=404)
    body = await request.body()
    return _proxy(
        method=request.method.upper(),
        path="/" + path,
        query=request.url.query,
        body=body,
        headers=dict(request.headers),
    )
