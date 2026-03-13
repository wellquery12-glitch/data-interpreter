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


def test_auto_business_insights_topic_specific_semantics(tmp_path: Path) -> None:
    agent = _prepare_agent(
        tmp_path,
        dataset_id="biz_topics",
        content=(
            "日期,客户,金额,收入,支出,利润,状态,时长,是否流失,客户分层\n"
            "2026-01-02,甲公司,100,100,60,40,完成,3,否,A\n"
            "2026-01-03,乙公司,80,80,70,10,处理中,8,是,B\n"
            "2026-02-01,甲公司,120,120,75,45,完成,4,否,A\n"
            "2026-02-10,丙公司,60,60,50,10,延迟,10,是,C\n"
        ),
    )

    operations = agent.auto_business_insights(
        dataset_id="biz_topics",
        topic="operations",
        requirement="关注履约效率",
        max_rows=10,
    )
    assert operations["topic"] == "operations"
    assert "运营" in operations["summary"]
    assert any("履约" in str(x.get("finding", "")) for x in operations["insights"])
    assert any(str(t.get("name", "")).startswith("operations_") for t in operations["tables"])

    finance = agent.auto_business_insights(
        dataset_id="biz_topics",
        topic="finance",
        requirement="关注收支结构",
        max_rows=10,
    )
    assert finance["topic"] == "finance"
    assert "财务" in finance["summary"]
    assert any("财务" in str(x.get("finding", "")) or "利润" in str(x.get("finding", "")) for x in finance["insights"])
    assert any(str(t.get("name", "")).startswith("finance_") for t in finance["tables"])

    customer = agent.auto_business_insights(
        dataset_id="biz_topics",
        topic="customer",
        requirement="关注客户留存",
        max_rows=10,
    )
    assert customer["topic"] == "customer"
    assert "客户" in customer["summary"]
    assert any("客户" in str(x.get("finding", "")) for x in customer["insights"])
    assert any(str(t.get("name", "")).startswith("customer_") for t in customer["tables"])


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
    md_text = md_path.read_text(encoding="utf-8")
    assert "![图表1](data:image/png;base64," in md_text
    pdf_dir = tmp_path / "sessions" / pdf["session_id"]
    assert any(p.name.startswith("plot_") and p.suffix.lower() == ".png" for p in pdf_dir.iterdir())
    data = pdf_path.read_bytes()
    assert data.startswith(b"%PDF")
    assert len(data) > 1024
