from pathlib import Path

from app.agent import DataInterpreterAgent


def _prepare_agent(tmp_path: Path, dataset_id: str = "d1") -> DataInterpreterAgent:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    (storage / f"{dataset_id}.csv").write_text("driver,amount\nA,10\nB,20\nA,30\n", encoding="utf-8")
    return DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))


def test_install_tool_package_and_select(tmp_path: Path) -> None:
    agent = _prepare_agent(tmp_path)
    agent._http_json_get = lambda url: {  # type: ignore[assignment]
        "package_id": "pkg_demo",
        "name": "演示包",
        "description": "demo tools",
        "version": "1.0.0",
        "tools": [
            {
                "tool_id": "tool_counterparty_sum",
                "name": "交易对象聚合",
                "description": "按交易对象聚合金额",
                "endpoint": "http://127.0.0.1:9000/run",
                "method": "POST",
                "keywords": ["交易", "金额"],
                "input_schema": {"question": "string"},
                "output_schema": {"answer": "string"},
            }
        ],
    }

    state = agent.install_tool_package(package_id="pkg_demo", source_url="http://hub.local/api")

    assert len(state["packages"]) == 1
    assert len(state["tools"]) == 1
    assert state["tools"][0]["tool_id"] == "tool_counterparty_sum"
    assert "tool_counterparty_sum" in state["selected_tool_ids"]


def test_builtin_catalog_available_without_source_url(tmp_path: Path) -> None:
    agent = _prepare_agent(tmp_path)

    data = agent.fetch_tools_catalog(source_url="")

    assert len(data["packages"]) >= 1
    assert any(p.get("package_id") == "finance_core_v1" for p in data["packages"])


def test_catalog_uses_tool_backend_service_by_default(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TOOL_BACKEND_SERVICE_URL", "http://127.0.0.1:8013")
    agent = _prepare_agent(tmp_path)
    seen = {"url": ""}

    def fake_get(url: str) -> dict:
        seen["url"] = url
        return {"packages": [{"package_id": "pkg_remote", "name": "远程包", "description": "", "version": "1.0.0"}]}

    agent._http_json_get = fake_get  # type: ignore[assignment]
    data = agent.fetch_tools_catalog(source_url="")

    assert seen["url"].endswith("/packages")
    assert data["source_url"] == "http://127.0.0.1:8013"
    assert data["packages"][0]["package_id"] == "pkg_remote"


def test_service_topology_reports_status(tmp_path: Path) -> None:
    agent = _prepare_agent(tmp_path)
    agent.planner_service_url = "http://127.0.0.1:8011"
    agent.executor_service_url = "http://127.0.0.1:8012"
    agent.tool_backend_service_url = "http://127.0.0.1:8013"

    def fake_get(url: str) -> dict:
        if url.endswith("/health"):
            return {"status": "ok"}
        return {}

    agent._http_json_get = fake_get  # type: ignore[assignment]
    topo = agent.get_service_topology()
    names = {item["name"]: item for item in topo["services"]}

    assert names["orchestrator-service"]["status"] == "ok"
    assert names["planner-service"]["status"] == "ok"
    assert names["executor-service"]["status"] == "ok"
    assert names["tool-backend-service"]["status"] == "ok"


def test_tool_mode_uses_remote_selected_tool(tmp_path: Path) -> None:
    agent = _prepare_agent(tmp_path, dataset_id="d2")
    agent.add_tool(
        {
            "tool_id": "tool_driver_top",
            "name": "司机Top",
            "description": "统计司机运单top",
            "endpoint": "http://tool.local/run",
            "method": "POST",
            "keywords": ["司机", "运单"],
            "input_schema": {"question": "string"},
            "output_schema": {"answer": "string", "table": "array"},
            "enabled": True,
        }
    )
    agent._http_json_post = lambda url, payload: {  # type: ignore[assignment]
        "success": True,
        "answer": "远程工具已执行",
        "table": [{"driver": "A", "count": 2}],
        "plots": [],
        "stdout": "ok",
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15, "cost": 0.0001},
    }

    resp = agent.ask(
        dataset_id="d2",
        question="哪个司机运单最多？",
        max_rows=10,
        mode="tool",
        selected_tools=["tool_driver_top"],
    )

    assert resp["mode"] == "tool"
    assert resp["remote_tool_id"] == "tool_driver_top"
    assert resp["code_source"].startswith("remote_tool:")
    assert resp["answer"] == "远程工具已执行"
    assert resp["llm_usage_delta"]["total_tokens"] == 15


def test_selected_tools_embedded_into_planner_context(tmp_path: Path) -> None:
    agent = _prepare_agent(tmp_path, dataset_id="d3")
    agent.add_tool(
        {
            "tool_id": "tool_embed_demo",
            "name": "嵌入演示工具",
            "description": "用于验证代码生成上下文嵌入",
            "endpoint": "http://tool.local/embed",
            "method": "POST",
            "keywords": ["演示"],
            "input_schema": {"question": "string"},
            "output_schema": {"answer": "string"},
            "enabled": True,
        }
    )
    captured = []
    agent.planner.set_tool_context = lambda tools: captured.append(tools)  # type: ignore[assignment]

    resp = agent.ask(
        dataset_id="d3",
        question="哪个司机运单最多？",
        max_rows=5,
        mode="auto",
        selected_tools=["tool_embed_demo"],
    )

    assert resp["mode"] == "auto"
    assert captured
    assert captured[0][0]["tool_id"] == "tool_embed_demo"


def test_execution_graph_generated_after_ask(tmp_path: Path) -> None:
    agent = _prepare_agent(tmp_path, dataset_id="d4")

    resp = agent.ask(dataset_id="d4", question="哪个司机运单最多？", max_rows=10, mode="auto")

    assert resp["execution_graph_url"].endswith("execution_graph.png")
    assert any(str(p).endswith("execution_graph.png") for p in resp["plots"])
    assert isinstance(resp["execution_layers"], list)
    assert len(resp["execution_layers"]) >= 5
    assert any(layer.get("name") == "大模型调用层" for layer in resp["execution_layers"])
