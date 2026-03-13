from pathlib import Path

from configs.template_registry import load_registry
from pipelines import UciAutoPipeline


def test_template_registry_counts() -> None:
    reg = load_registry()
    assert len(reg.chart_templates) >= 10
    assert len(reg.report_templates) >= 20


def test_pipeline_generates_reports(tmp_path: Path) -> None:
    datasets = tmp_path / "datasets"
    outputs = tmp_path / "outputs"
    datasets.mkdir()
    outputs.mkdir()
    src = tmp_path / "demo.csv"
    src.write_text("date,city,amount\\n2026-01-01,SH,10\\n2026-01-02,BJ,20\\n", encoding="utf-8")

    pipe = UciAutoPipeline(datasets_dir=str(datasets), outputs_dir=str(outputs))
    out = pipe.run(source=str(src), run_name="t1")

    assert out["analysis_count"] >= 3
    assert out["chart_count"] >= 1

    reports = out["reports"]
    assert Path(reports["executive_summary"]).exists()
    assert Path(reports["full_report"]).exists()
    assert Path(reports["machine_readable_summary"]).exists()
    assert Path(reports["full_report_html"]).exists()
