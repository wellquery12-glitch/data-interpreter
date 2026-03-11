from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

from fastapi import FastAPI
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.planner import CodePlanner, DatasetMeta

app = FastAPI(title="Planner Service", version="1.0.0")
planner = CodePlanner()


class MetaPayload(BaseModel):
    columns: List[str]
    dtypes: Dict[str, str]


class QuestionPayload(BaseModel):
    question: str
    meta: MetaPayload


class RepairPayload(BaseModel):
    question: str
    meta: MetaPayload
    error: str = ""


class BuildToolPayload(BaseModel):
    plan: dict
    meta: MetaPayload


def _meta(v: MetaPayload) -> DatasetMeta:
    return DatasetMeta(columns=v.columns, dtypes=v.dtypes)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/intent_key")
def intent_key(payload: dict) -> dict:
    return {"intent_key": planner.intent_key(str(payload.get("question", "")))}


@app.post("/generate")
def generate(payload: QuestionPayload) -> dict:
    return {"code": planner.generate(payload.question, _meta(payload.meta))}


@app.post("/generate_llm_only")
def generate_llm_only(payload: RepairPayload) -> dict:
    return {"code": planner.generate_llm_only(payload.question, _meta(payload.meta), payload.error) or ""}


@app.post("/plan_tool")
def plan_tool(payload: QuestionPayload) -> dict:
    return {"plan": planner.plan_tool(payload.question, _meta(payload.meta))}


@app.post("/build_tool_code")
def build_tool_code(payload: BuildToolPayload) -> dict:
    return {"code": planner.build_tool_code(payload.plan, _meta(payload.meta))}


@app.post("/is_fallback_code")
def is_fallback_code(payload: dict) -> dict:
    return {"is_fallback": planner.is_fallback_code(str(payload.get("code", "")))}


@app.post("/generate_auto_tool")
def generate_auto_tool(payload: QuestionPayload) -> dict:
    return {"code": planner.generate_auto_tool(payload.question, _meta(payload.meta)) or ""}


@app.post("/repair_code")
def repair_code(payload: RepairPayload) -> dict:
    return {"code": planner.repair_code(payload.question, _meta(payload.meta), payload.error)}


@app.post("/failure_solution")
def failure_solution(payload: RepairPayload) -> dict:
    return {"solution": planner.failure_solution(payload.question, _meta(payload.meta), payload.error)}


@app.post("/fallback_code")
def fallback_code() -> dict:
    return {"code": planner.fallback_code()}
