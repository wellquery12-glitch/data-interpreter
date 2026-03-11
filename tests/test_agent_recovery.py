from pathlib import Path
import json

from app.agent import DataInterpreterAgent
from app.planner import CodePlanner, DatasetMeta


def _prepare_agent_with_dataset(tmp_path: Path, dataset_id: str = "d1") -> DataInterpreterAgent:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    (storage / f"{dataset_id}.csv").write_text(
        "driver,weight,amount\n张三,10,100\n李四,20,200\n张三,30,300\n", encoding="utf-8"
    )
    return DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))


def test_no_code_cache_reused_for_similar_question(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, dataset_id="d1")

    first = agent.ask(dataset_id="d1", question="哪个司机运单最多？", max_rows=10)
    second = agent.ask(dataset_id="d1", question="哪个司机运单最多？", max_rows=10)

    assert first["used_fallback"] is False
    assert second["code_source"] == "planner"


def test_fallback_after_two_failed_repairs(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, dataset_id="d2")
    agent.planner.generate = lambda question, meta: "result_df = df[['not_exists']]"  # type: ignore[assignment]
    agent.planner.repair_code = lambda question, meta, error: "result_df = df[['still_bad']]"  # type: ignore[assignment]

    resp = agent.ask(dataset_id="d2", question="测试失败重试", max_rows=10)

    assert resp["used_fallback"] is True
    assert resp["retries"] == 2
    assert resp["fallback_reason"] != ""
    assert resp["fallback_solution"] != ""
    assert len(resp["bug_logs"]) == 2
    assert "已触发兜底" in resp["answer"]
    assert len(resp["table"]) > 0
    assert resp["status"] == "degraded"
    assert resp["error_category"] == "field_missing"
    assert "列名" in resp["error_hint"]


def test_planner_auto_tool_generation() -> None:
    planner = CodePlanner()
    meta = DatasetMeta(columns=["amount", "driver"], dtypes={"amount": "float64", "driver": "object"})

    code = planner.generate_auto_tool("金额最大是多少", meta)

    assert code is not None
    assert "max" in code


def test_semantic_unsupported_is_classified(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, dataset_id="d5")

    resp = agent.ask(dataset_id="d5", question="帮我做城市维度的分位数热力图并给出回归系数", max_rows=10)

    assert resp["status"] == "degraded"
    assert resp["error_category"] == "semantic_unsupported"
    assert resp["error_message"] != ""
    assert resp["suggested_question"] != ""


def test_invalid_memory_tool_is_ignored(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, dataset_id="d3")

    agent.tools_file.write_text(
        json.dumps(
            {
                "counterparty_amount_top": {
                    "intent_key": "counterparty_amount_top",
                    "code": "result_df = df['交易时间'].astype(str).value_counts().reset_index()",
                    "success": True,
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    resp = agent.ask(dataset_id="d3", question="交易金额最多的交易对象", max_rows=10)

    assert resp["code_source"] != "memory_tool"
    assert "交易时间" not in resp["generated_code"]


def test_mismatched_success_code_not_saved_to_memory(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, dataset_id="d4")

    bad_code = "result_df = df['driver'].astype(str).value_counts().reset_index()"
    agent._upsert_tool(
        intent_key="counterparty_amount_top",
        question="交易金额最多的交易对象",
        code=bad_code,
        source="auto_repair",
        success=True,
    )

    tools = agent._load_tools()
    assert "counterparty_amount_top" not in tools


def test_dataset_overview_contains_columns_and_sample(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, dataset_id="d6")

    overview = agent.get_dataset_overview(dataset_id="d6", max_rows=2)

    assert overview["rows"] == 3
    assert overview["cols"] == 3
    assert "driver" in overview["columns"]
    assert len(overview["sample_rows"]) == 2


def test_dataset_overview_json_safe_for_nan_and_inf(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    (storage / "d7.csv").write_text("a,b\n1,2\n,3\n4,inf\n", encoding="utf-8")
    agent = DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))

    overview = agent.get_dataset_overview(dataset_id="d7", max_rows=5)

    assert overview["sample_rows"][1]["a"] is None
    assert overview["sample_rows"][2]["b"] is None


def test_plot_question_not_polluted_by_non_plot_memory(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    (storage / "d8.csv").write_text(
        "交易对方,金额\nA,10\nB,30\nA,15\nC,20\n",
        encoding="utf-8",
    )
    agent = DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))

    first = agent.ask(dataset_id="d8", question="交易金额最多的交易对象", max_rows=10)
    second = agent.ask(dataset_id="d8", question="请画图展示交易金额最多的交易对象", max_rows=10)

    assert "groupby" in first["generated_code"]
    assert "save_plot" in second["generated_code"]
    assert len(second["plots"]) > 0


def test_multi_question_split_and_execute(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    (storage / "d9.csv").write_text("driver,amount\nA,10\nB,30\nA,20\n", encoding="utf-8")
    agent = DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))

    resp = agent.ask(dataset_id="d9", question="哪个司机运单最多？并且 金额最大是多少", max_rows=10)

    assert resp["code_source"] == "multi"
    assert len(resp["sub_results"]) == 2
    assert resp["sub_results"][0]["question"] == "哪个司机运单最多"
    assert "最大值" in resp["sub_results"][1]["answer"]


def test_multi_question_split_with_then_connector(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    (storage / "d9b.csv").write_text("driver,amount\nA,10\nB,30\nA,20\n", encoding="utf-8")
    agent = DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))

    resp = agent.ask(dataset_id="d9b", question="哪个司机运单最多？然后 金额最大是多少？", max_rows=10)

    assert resp["code_source"] == "multi"
    assert len(resp["sub_results"]) == 2


def test_auto_rewrite_missing_field_and_reexecute(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    (storage / "d10.csv").write_text("金额(元)\n10\n20\n30\n", encoding="utf-8")
    agent = DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))

    agent.planner.generate = lambda question, meta: (
        "_tmp = pd.to_numeric(df['金额(元)'], errors='coerce')\nresult_df = pd.DataFrame([{'金额(元)_sum': float(_tmp.sum())}])\nanswer = '字段 金额(元) 的合计已计算'"
        if "金额(元)" in question
        else "_tmp = pd.to_numeric(df['金额'], errors='coerce')\nresult_df = pd.DataFrame([{'金额_sum': float(_tmp.sum())}])\nanswer = '字段 金额 的合计已计算'"
    )  # type: ignore[assignment]

    resp = agent.ask(dataset_id="d10", question="金额合计是多少", max_rows=10)

    assert resp["auto_rewrite_applied"] is True
    assert "金额(元)" in resp["auto_rewrite_question"]
    assert len(resp["table"]) == 1
    assert resp["table"][0]["金额(元)_sum"] == 60.0


def test_list_records_paged(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, dataset_id="d11")
    for i in range(7):
        agent._append_record(
            {
                "time": f"2026-01-01T00:00:0{i}",
                "session_id": f"s{i}",
                "dataset_id": "d11",
                "question": f"q{i}",
                "answer": f"a{i}",
            }
        )

    page1 = agent.list_records_paged(page=1, page_size=5, dataset_id="d11")
    page2 = agent.list_records_paged(page=2, page_size=5, dataset_id="d11")

    assert page1["total"] == 7
    assert page1["total_pages"] == 2
    assert len(page1["data"]) == 5
    assert len(page2["data"]) == 2


def test_tool_mode_runs_with_tool_code_source(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, dataset_id="d12")

    resp = agent.ask(dataset_id="d12", question="金额最大是多少", max_rows=10, mode="tool")

    assert resp["mode"] == "tool"
    assert resp["code_source"] == "tool_mode"
    assert len(resp["table"]) == 1


def test_record_detail_saved_and_readable(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, dataset_id="d13")

    resp = agent.ask(dataset_id="d13", question="哪个司机运单最多？", max_rows=10, mode="auto")
    detail = agent.get_record_detail(resp["session_id"])

    assert detail["session_id"] == resp["session_id"]
    assert detail["question"] == "哪个司机运单最多"


def test_smart_mode_degrades_to_auto_when_llm_unavailable(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, dataset_id="d14")
    agent.planner.generate_llm_only = lambda question, meta, error="": None  # type: ignore[assignment]

    resp = agent.ask(dataset_id="d14", question="哪个司机运单最多？", max_rows=10, mode="smart")

    assert resp["mode"] == "smart"
    assert resp["mode_degraded"] is True
    assert resp["code_source"] in {"smart_degraded_auto", "auto_tool", "auto_repair"}
    assert len(resp["table"]) > 0
    assert resp["execution_error"] == ""


def test_result_detail_json_safe_for_timestamp_payload(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    (storage / "d15.csv").write_text(
        "交易时间,金额\n2026-01-01,10\n2026-01-02,20\n2026-01-03,15\n",
        encoding="utf-8",
    )
    agent = DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))

    resp = agent.ask(dataset_id="d15", question="请画趋势图，按交易时间看金额变化", max_rows=10, mode="auto")

    detail_path = sessions / resp["session_id"] / "result.json"
    assert detail_path.exists()
    detail = json.loads(detail_path.read_text(encoding="utf-8"))
    assert detail["session_id"] == resp["session_id"]
    assert detail["execution_error"] == ""


def test_trend_question_without_date_column_falls_back_to_row_index(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    (storage / "d15b.csv").write_text(
        "driver,amount\nA,10\nB,30\nA,20\nC,25\n",
        encoding="utf-8",
    )
    agent = DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))

    resp = agent.ask(dataset_id="d15b", question="请画趋势图，按交易时间看金额变化", max_rows=10, mode="auto")

    assert resp["status"] == "ok"
    assert resp["execution_error"] == ""
    assert "行序号" in resp["answer"]
    assert len(resp["plots"]) > 0


def test_bug_summary_marks_fixed_when_later_success_exists(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, dataset_id="d16")
    agent._append_bug(
        {
            "time": "2026-03-10T10:00:00",
            "session_id": "s_bug",
            "dataset_id": "d16",
            "intent_key": "trend_plot",
            "question": "请画趋势图",
            "error": "当前问题暂不支持自动解析",
        }
    )
    agent._append_record(
        {
            "time": "2026-03-10T10:05:00",
            "session_id": "s_ok",
            "dataset_id": "d16",
            "intent_key": "trend_plot",
            "question": "请画趋势图",
            "answer": "已生成趋势图",
            "execution_error": "",
            "status": "ok",
        }
    )

    summary = agent.get_bug_summary(limit=20)

    assert summary["total"] >= 1
    row = summary["items"][0]
    assert row["intent_key"] == "trend_plot"
    assert row["fixed"] is True
    assert row["fixed_session_id"] == "s_ok"


def test_dataset_alias_can_be_set_and_used_for_preview(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, dataset_id="d17")

    out = agent.set_dataset_alias(dataset_id="d17", alias="司机单量")
    overview = agent.get_dataset_overview(dataset_id="司机单量", max_rows=2)

    assert out["alias"] == "司机单量"
    assert overview["dataset_id"] == "司机单量"
    datasets = agent.list_datasets()
    assert datasets[0]["alias"] == "司机单量"


def test_delete_dataset_removes_file_and_meta(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, dataset_id="d18")

    deleted = agent.delete_dataset(dataset_id="d18")

    assert deleted["deleted"] is True
    assert not (tmp_path / "storage" / "d18.csv").exists()
    listed = agent.list_datasets()
    assert all(row["dataset_id"] != "d18" for row in listed)


def test_delete_dataset_by_alias(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, dataset_id="d19")
    agent.set_dataset_alias(dataset_id="d19", alias="待删除别名")

    deleted = agent.delete_dataset(dataset_id="待删除别名")

    assert deleted["deleted"] is True
    assert deleted["dataset_id"] == "d19"
    assert not (tmp_path / "storage" / "d19.csv").exists()


def test_delete_dataset_when_only_meta_exists(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    agent = DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))
    agent._save_meta(
        {
            "ghost_id": {
                "original_filename": "ghost.csv",
                "saved_filename": "ghost_id.csv",
                "alias": "幽灵数据",
                "updated_at": "2026-03-10T00:00:00",
            }
        }
    )

    deleted = agent.delete_dataset(dataset_id="ghost_id")

    assert deleted["deleted"] is True
    assert deleted["removed_files"] == 0
    assert deleted["removed_meta"] is True
