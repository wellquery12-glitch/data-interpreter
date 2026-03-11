from __future__ import annotations

import os
from typing import Any, Dict, List

from fastapi import FastAPI, Request
from pydantic import BaseModel

app = FastAPI(title="Tool Backend Service", version="1.0.0")
TOOL_BACKEND_PUBLIC_URL = os.getenv("TOOL_BACKEND_PUBLIC_URL", "").rstrip("/")

PACKAGES = {
    "finance_core_v1": {
        "package_id": "finance_core_v1",
        "name": "财务分析核心包",
        "description": "交易对象聚合、收支统计、TopN 排行",
        "version": "1.0.0",
        "tools": [
            {
                "tool_id": "tool_finance_counterparty_top",
                "name": "交易对象金额Top",
                "description": "按交易对象汇总金额并返回Top10",
                "endpoint_path": "/run/finance/counterparty_top",
                "method": "POST",
                "keywords": ["交易", "金额", "top", "对象"],
                "input_schema": {"question": "string", "data": "object"},
                "output_schema": {"answer": "string", "table": "array"},
            }
        ],
    },
    "ops_logistics_v1": {
        "package_id": "ops_logistics_v1",
        "name": "运力与履约分析包",
        "description": "司机绩效、运单时效、线路波动",
        "version": "1.0.0",
        "tools": [
            {
                "tool_id": "tool_ops_driver_top",
                "name": "司机运单Top",
                "description": "按司机统计运单次数",
                "endpoint_path": "/run/ops/driver_top",
                "method": "POST",
                "keywords": ["司机", "运单", "top", "最多"],
                "input_schema": {"question": "string", "data": "object"},
                "output_schema": {"answer": "string", "table": "array"},
            }
        ],
    },
}


class ToolRunPayload(BaseModel):
    question: str
    dataset_id: str = ""
    max_rows: int = 20
    data: Dict[str, Any]


def _public_base_url(request: Request) -> str:
    if TOOL_BACKEND_PUBLIC_URL:
        return TOOL_BACKEND_PUBLIC_URL
    return str(request.base_url).rstrip("/")


def _pkg_with_public_endpoints(pkg: Dict[str, Any], request: Request) -> Dict[str, Any]:
    base = _public_base_url(request)
    out = dict(pkg)
    tools = []
    for raw in pkg.get("tools", []):
        if not isinstance(raw, dict):
            continue
        tool = dict(raw)
        endpoint = str(tool.get("endpoint", "")).strip()
        endpoint_path = str(tool.get("endpoint_path", "")).strip()
        if endpoint_path and not endpoint:
            endpoint = base + endpoint_path
        tool["endpoint"] = endpoint
        tool.pop("endpoint_path", None)
        tools.append(tool)
    out["tools"] = tools
    return out


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/packages")
def packages() -> dict:
    rows = []
    for pkg in PACKAGES.values():
        rows.append(
            {
                "package_id": pkg["package_id"],
                "name": pkg["name"],
                "description": pkg["description"],
                "version": pkg["version"],
            }
        )
    return {"packages": rows}


@app.get("/packages/{package_id}")
def package_detail(package_id: str, request: Request) -> dict:
    pkg = PACKAGES.get(package_id)
    if not pkg:
        return {"package_id": package_id, "name": package_id, "description": "", "version": "", "tools": []}
    return _pkg_with_public_endpoints(pkg=pkg, request=request)


def _rows(payload: ToolRunPayload) -> List[Dict[str, Any]]:
    rows = payload.data.get("rows", [])
    return rows if isinstance(rows, list) else []


@app.post("/run/ops/driver_top")
def run_driver_top(payload: ToolRunPayload) -> dict:
    rows = _rows(payload)
    counter: Dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("driver", "")).strip()
        if not key:
            continue
        counter[key] = counter.get(key, 0) + 1
    table = [{"driver": k, "count": v} for k, v in sorted(counter.items(), key=lambda x: x[1], reverse=True)[: payload.max_rows]]
    return {
        "success": True,
        "answer": "远程工具：司机运单 Top 已生成",
        "table": table,
        "plots": [],
        "stdout": "",
        "usage": {"model": "tool-backend", "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "cost": 0.0},
    }


@app.post("/run/finance/counterparty_top")
def run_counterparty_top(payload: ToolRunPayload) -> dict:
    rows = _rows(payload)
    sums: Dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("交易对方", row.get("交易对象", row.get("counterparty", "")))).strip()
        if not key:
            continue
        amount = row.get("金额", row.get("amount", 0))
        try:
            val = float(amount)
        except Exception:  # noqa: BLE001
            val = 0.0
        sums[key] = sums.get(key, 0.0) + val
    table = [{"counterparty": k, "amount_sum": round(v, 4)} for k, v in sorted(sums.items(), key=lambda x: x[1], reverse=True)[: payload.max_rows]]
    return {
        "success": True,
        "answer": "远程工具：交易对象金额 Top 已生成",
        "table": table,
        "plots": [],
        "stdout": "",
        "usage": {"model": "tool-backend", "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "cost": 0.0},
    }
