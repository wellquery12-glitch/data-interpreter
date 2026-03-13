from __future__ import annotations

from pathlib import Path
from typing import List

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .agent import DataInterpreterAgent

app = FastAPI(title="Data Interpreter Agent", version="1.0.0")
BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "app" / "static"
SESSIONS_DIR = BASE_DIR / "sessions"
agent = DataInterpreterAgent(storage_dir=str(BASE_DIR / "storage"), sessions_dir=str(SESSIONS_DIR))

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/sessions", StaticFiles(directory=str(SESSIONS_DIR)), name="sessions")


class AskRequest(BaseModel):
    dataset_id: str = Field(..., description="upload 接口返回的 dataset_id")
    question: str = Field(..., description="自然语言问题")
    max_rows: int = Field(default=20, ge=1, le=200)
    mode: str = Field(default="auto", description="auto|smart|tool")
    selected_tools: List[str] = Field(default_factory=list, description="可选，指定本次优先使用的远程工具 ID")


class LLMConfigRequest(BaseModel):
    enabled: bool = True
    provider: str = "openai_compatible"
    api_key: str = ""
    base_url: str = ""
    model: str = "gpt-4o-mini"
    input_cost_per_million: float = Field(default=0.0, ge=0)
    output_cost_per_million: float = Field(default=0.0, ge=0)
    currency: str = "USD"
    max_total_tokens: int = Field(default=500000, ge=1000)
    warn_ratio: float = Field(default=0.8, ge=0.1, le=0.99)


class ToolPackageInstallRequest(BaseModel):
    package_id: str = ""
    manifest_url: str = ""
    source_url: str = ""


class ToolAddRequest(BaseModel):
    tool_id: str
    name: str
    description: str = ""
    package_id: str = "manual"
    endpoint: str
    method: str = "POST"
    input_schema: dict = Field(default_factory=dict)
    output_schema: dict = Field(default_factory=dict)
    keywords: List[str] = Field(default_factory=list)
    enabled: bool = True


class ToolEnableRequest(BaseModel):
    enabled: bool = True


class ToolSelectionRequest(BaseModel):
    selected_tool_ids: List[str] = Field(default_factory=list)


class DatasetAliasRequest(BaseModel):
    alias: str = ""


class DatasetMetadataRequest(BaseModel):
    alias: str = ""
    category: str = ""
    biz_description: str = ""
    analysis_notes: str = ""
    tags: List[str] = Field(default_factory=list)


class UciImportRequest(BaseModel):
    uci_id: int = Field(..., ge=1, description="UCI 数据集 ID")


class KaggleImportRequest(BaseModel):
    kaggle_ref: str = Field(..., description="Kaggle 数据集引用，格式 owner/dataset-slug")


class KaggleConfigRequest(BaseModel):
    username: str = ""
    api_key: str = ""


class TestRunRequest(BaseModel):
    include_pytest: bool = True


class InsightsAutoRequest(BaseModel):
    dataset_id: str = Field(..., description="目标数据集 ID")
    topic: str = Field(default="sales", description="sales|operations|finance|customer")
    requirement: str = Field(default="", description="用户的具体分析需求")
    max_rows: int = Field(default=10, ge=3, le=50)


class InsightsExportRequest(BaseModel):
    dataset_id: str = Field(..., description="目标数据集 ID")
    export_format: str = Field(default="markdown", description="markdown|pdf")
    topic: str = Field(default="sales", description="sales|operations|finance|customer")
    requirement: str = Field(default="", description="用户的具体分析需求")
    max_rows: int = Field(default=10, ge=3, le=50)
    report: dict = Field(default_factory=dict)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/architecture/services")
def architecture_services() -> dict:
    return agent.get_service_topology()


@app.get("/datasets")
def datasets(module: str = "", category: str = "", keyword: str = "") -> dict:
    return {"data": agent.list_datasets(module=module, category=category, keyword=keyword)}


@app.put("/datasets/{dataset_id}/alias")
def dataset_alias(dataset_id: str, req: DatasetAliasRequest) -> dict:
    try:
        return agent.set_dataset_alias(dataset_id=dataset_id, alias=req.alias)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/datasets/{dataset_id}")
def dataset_delete(dataset_id: str) -> dict:
    try:
        return agent.delete_dataset(dataset_id=dataset_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/datasets/{dataset_id}/preview")
def dataset_preview(dataset_id: str, rows: int = 8) -> dict:
    try:
        return {"data": agent.get_dataset_overview(dataset_id=dataset_id, max_rows=rows)}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/datasets/{dataset_id}/summary")
def dataset_summary(dataset_id: str) -> dict:
    try:
        return {"data": agent.get_dataset_summary(dataset_id=dataset_id)}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.put("/datasets/{dataset_id}/metadata")
def dataset_metadata(dataset_id: str, req: DatasetMetadataRequest) -> dict:
    try:
        return agent.update_dataset_metadata(dataset_id=dataset_id, payload=req.dict())
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/datasets/repository/uci")
def uci_datasets(keyword: str = "", page: int = 1, page_size: int = 20) -> dict:
    try:
        return agent.fetch_uci_datasets(keyword=keyword, page=page, page_size=page_size)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/datasets/repository/kaggle")
def kaggle_datasets(topic: str = "businessDataset", keyword: str = "", page: int = 1, page_size: int = 20) -> dict:
    try:
        return agent.fetch_kaggle_datasets(topic=topic, keyword=keyword, page=page, page_size=page_size)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/datasets/repository/kaggle/import")
def kaggle_import(req: KaggleImportRequest) -> dict:
    try:
        return agent.import_kaggle_dataset(kaggle_ref=req.kaggle_ref)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/datasets/repository/public/sources")
def public_sources() -> dict:
    return {"data": agent.list_public_sources()}


@app.post("/datasets/repository/uci/import")
def uci_import(req: UciImportRequest) -> dict:
    try:
        return agent.import_uci_dataset(uci_id=req.uci_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/records")
def records(page: int = 1, page_size: int = 5, dataset_id: str = "", keyword: str = "") -> dict:
    safe_page = max(1, page)
    safe_page_size = max(1, min(page_size, 100))
    return agent.list_records_paged(
        page=safe_page,
        page_size=safe_page_size,
        dataset_id=dataset_id,
        keyword=keyword,
    )


@app.get("/records/{session_id}")
def record_detail(session_id: str) -> dict:
    try:
        return {"data": agent.get_record_detail(session_id=session_id)}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/bugs/summary")
def bugs_summary(limit: int = 50) -> dict:
    safe_limit = max(1, min(limit, 200))
    return agent.get_bug_summary(limit=safe_limit)


@app.get("/")
def index() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/manage")
def manage() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "manage.html"))


@app.get("/repository")
def repository_page() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "repository.html"))


@app.get("/test-manage")
def test_manage_page() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "test_manage.html"))


@app.get("/tools")
def tools_page() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "tools.html"))


@app.get("/api-docs")
def api_docs_page() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "api_docs.html"))


@app.get("/api/docs/services")
def api_docs_services() -> dict:
    planner_url = agent.planner_service_url.rstrip("/") if agent.planner_service_url else ""
    executor_url = agent.executor_service_url.rstrip("/") if agent.executor_service_url else ""
    tool_backend_url = agent.tool_backend_service_url.rstrip("/") if agent.tool_backend_service_url else ""
    return {
        "services": [
            {
                "id": "orchestrator",
                "name": "orchestrator-service",
                "base_url": "",
                "docs_url": "/docs",
                "openapi_url": "/openapi.json",
            },
            {
                "id": "planner",
                "name": "planner-service",
                "base_url": planner_url,
                "docs_url": (planner_url + "/docs") if planner_url else "",
                "openapi_url": (planner_url + "/openapi.json") if planner_url else "",
            },
            {
                "id": "executor",
                "name": "executor-service",
                "base_url": executor_url,
                "docs_url": (executor_url + "/docs") if executor_url else "",
                "openapi_url": (executor_url + "/openapi.json") if executor_url else "",
            },
            {
                "id": "tool_backend",
                "name": "tool-backend-service",
                "base_url": tool_backend_url,
                "docs_url": (tool_backend_url + "/docs") if tool_backend_url else "",
                "openapi_url": (tool_backend_url + "/openapi.json") if tool_backend_url else "",
            },
        ]
    }


@app.post("/upload")
async def upload(file: UploadFile = File(...)) -> dict:
    try:
        content = await file.read()
        info = agent.save_dataset(filename=file.filename or "dataset.csv", content=content)
        return {
            "dataset_id": info.dataset_id,
            "path": info.path,
            "rows": info.rows,
            "cols": info.cols,
            "columns": info.columns,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/ask")
def ask(req: AskRequest) -> dict:
    try:
        return agent.ask(
            dataset_id=req.dataset_id,
            question=req.question,
            max_rows=req.max_rows,
            mode=req.mode,
            selected_tools=req.selected_tools,
        )
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail={
                "error_category": "dataset_not_found",
                "error_message": str(exc),
                "error_hint": "请先上传数据集或重新选择有效的 dataset_id。",
            },
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=500,
            detail={
                "error_category": "execution_error",
                "error_message": f"分析失败: {exc}",
                "error_hint": "服务端执行异常，请稍后重试或简化问题。",
            },
        ) from exc


@app.get("/llm/config")
def get_llm_config() -> dict:
    return agent.get_llm_settings()


@app.get("/kaggle/config")
def get_kaggle_config() -> dict:
    return agent.get_kaggle_settings()


@app.put("/llm/config")
def update_llm_config(req: LLMConfigRequest) -> dict:
    return agent.update_llm_settings(req.dict())


@app.put("/kaggle/config")
def update_kaggle_config(req: KaggleConfigRequest) -> dict:
    return agent.update_kaggle_settings(req.dict())


@app.post("/llm/reset")
def reset_llm_usage() -> dict:
    return agent.reset_llm_usage()


@app.get("/tests/runs")
def tests_runs(limit: int = 20) -> dict:
    return agent.list_test_runs(limit=limit)


@app.get("/tests/runs/{run_id}")
def tests_run_detail(run_id: str) -> dict:
    try:
        return {"data": agent.get_test_run(run_id=run_id)}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/tests/run")
def tests_run(req: TestRunRequest) -> dict:
    return agent.run_automated_tests(include_pytest=req.include_pytest)


@app.get("/tools/state")
def tools_state() -> dict:
    return agent.get_tools_hub()


@app.get("/tools/catalog")
def tools_catalog(source_url: str = "") -> dict:
    try:
        return agent.fetch_tools_catalog(source_url=source_url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tools/packages/install")
def tools_package_install(req: ToolPackageInstallRequest) -> dict:
    try:
        return agent.install_tool_package(package_id=req.package_id, manifest_url=req.manifest_url, source_url=req.source_url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tools/add")
def tools_add(req: ToolAddRequest) -> dict:
    try:
        return agent.add_tool(req.dict())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tools/select")
def tools_select(req: ToolSelectionRequest) -> dict:
    return agent.set_selected_tools(req.selected_tool_ids)


@app.post("/tools/{tool_id}/enabled")
def tools_enable(tool_id: str, req: ToolEnableRequest) -> dict:
    return agent.set_tool_enabled(tool_id=tool_id, enabled=req.enabled)


@app.get("/insights")
def insights_page() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "insights.html"))


@app.post("/insights/auto")
def insights_auto(req: InsightsAutoRequest) -> dict:
    try:
        return agent.auto_business_insights(
            dataset_id=req.dataset_id,
            topic=req.topic,
            requirement=req.requirement,
            max_rows=req.max_rows,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"自动分析失败: {exc}") from exc


@app.post("/insights/export")
def insights_export(req: InsightsExportRequest) -> dict:
    try:
        report = (
            req.report
            if isinstance(req.report, dict) and req.report
            else agent.auto_business_insights(
                dataset_id=req.dataset_id,
                topic=req.topic,
                requirement=req.requirement,
                max_rows=req.max_rows,
            )
        )
        return agent.export_insights_report(report=report, export_format=req.export_format)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"导出失败: {exc}") from exc
