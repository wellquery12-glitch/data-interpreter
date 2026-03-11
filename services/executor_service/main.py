from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.sandbox import SandboxExecutor

app = FastAPI(title="Executor Service", version="1.0.0")


class DataPayload(BaseModel):
    columns: List[str]
    rows: List[Dict[str, Any]]


class ExecutePayload(BaseModel):
    session_dir: str
    code: str
    data: DataPayload
    max_rows: int = 20


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/execute")
def execute(payload: ExecutePayload) -> dict:
    df = pd.DataFrame(payload.data.rows, columns=payload.data.columns)
    executor = SandboxExecutor(payload.session_dir)
    result = executor.run(code=payload.code, df=df, max_rows=payload.max_rows)
    return {
        "success": result.success,
        "answer": result.answer,
        "table": result.table,
        "plots": result.plots,
        "stdout": result.stdout,
        "error": result.error,
    }
