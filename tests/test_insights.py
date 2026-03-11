from pathlib import Path

from app.agent import DataInterpreterAgent


def _prepare_agent(tmp_path: Path, dataset_id: str, content: str) -> DataInterpreterAgent:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    (storage / f"{dataset_id}.csv").write_text(content, encoding="utf-8")
    return DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))


def test_auto_sales_insights_generates_structured_output(tmp_path: Path) -> None:
    agent = _prepare_agent(
        tmp_path,
        dataset_id="sales1",
        content=(
            "订单日期,客户,金额\n"
            "2026-01-02,甲公司,100\n"
            "2026-01-03,乙公司,80\n"
            "2026-02-01,甲公司,120\n"
            "2026-02-10,丙公司,60\n"
        ),
    )

    report = agent.auto_business_insights(
        dataset_id="sales1",
        topic="sales",
        requirement="分析大客户贡献和月度波动",
        max_rows=10,
    )

    assert report["topic"] == "sales"
    assert report["requirement"] == "分析大客户贡献和月度波动"
    assert isinstance(report["insights"], list)
    assert len(report["insights"]) >= 3
    assert isinstance(report.get("plots", []), list)
    first = report["insights"][0]
    assert first["finding"]
    assert first["evidence"]
    assert first["suggestion"]
    assert first["priority"] in {"high", "medium", "low"}


def test_auto_sales_insights_without_amount_field(tmp_path: Path) -> None:
    agent = _prepare_agent(
        tmp_path,
        dataset_id="sales2",
        content=(
            "订单日期,客户\n"
            "2026-01-02,甲公司\n"
            "2026-01-03,乙公司\n"
        ),
    )

    report = agent.auto_business_insights(dataset_id="sales2", topic="sales", requirement="", max_rows=10)

    assert report["analysis_scope"] == "limited"
    assert report["confidence"] == "low"
    assert len(report["insights"]) >= 3


def test_auto_business_insights_supports_all_topics(tmp_path: Path) -> None:
    agent = _prepare_agent(
        tmp_path,
        dataset_id="sales_topic",
        content=(
            "订单日期,客户,金额\n"
            "2026-01-02,甲公司,100\n"
            "2026-01-03,乙公司,80\n"
            "2026-02-01,甲公司,120\n"
        ),
    )
    for topic in ("sales", "operations", "finance", "customer"):
        report = agent.auto_business_insights(
            dataset_id="sales_topic",
            topic=topic,
            requirement="关注收入稳定性",
            max_rows=10,
        )
        assert report["topic"] == topic
        assert report["requirement"] == "关注收入稳定性"
        assert len(report["insights"]) >= 3


def test_export_insights_markdown_and_pdf(tmp_path: Path) -> None:
    agent = _prepare_agent(
        tmp_path,
        dataset_id="sales3",
        content=(
            "订单日期,客户,金额\n"
            "2026-01-02,甲公司,100\n"
            "2026-02-03,乙公司,120\n"
        ),
    )
    report = agent.auto_business_insights(dataset_id="sales3", topic="sales", requirement="导出报告", max_rows=10)

    md = agent.export_insights_report(report=report, export_format="markdown")
    pdf = agent.export_insights_report(report=report, export_format="pdf")

    assert md["download_url"].endswith(".md")
    assert pdf["download_url"].endswith(".pdf")

    md_path = tmp_path / "sessions" / md["session_id"] / md["filename"]
    pdf_path = tmp_path / "sessions" / pdf["session_id"] / pdf["filename"]
    assert md_path.exists()
    assert pdf_path.exists()
    assert b"STSong-Light" in pdf_path.read_bytes()
