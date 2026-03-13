from pathlib import Path

from app.agent import DataInterpreterAgent


def _new_agent(tmp_path: Path) -> DataInterpreterAgent:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    return DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))


def test_update_metadata_and_summary(tmp_path: Path) -> None:
    agent = _new_agent(tmp_path)
    saved = agent.save_dataset("sales.csv", b"date,city,amount\n2026-01-01,SH,10\n2026-01-02,BJ,20\n")

    out = agent.update_dataset_metadata(
        dataset_id=saved.dataset_id,
        payload={
            "alias": "销售数据",
            "category": "销售",
            "biz_description": "区域销售流水",
            "analysis_notes": "重点关注按城市和时间趋势",
            "tags": ["sales", "daily"],
        },
    )
    assert out["alias"] == "销售数据"
    assert out["category"] == "销售"

    listed = agent.list_datasets(module="private", category="销售")
    assert listed and listed[0]["dataset_id"] == saved.dataset_id
    assert listed[0]["biz_description"] == "区域销售流水"

    summary = agent.get_dataset_summary(saved.dataset_id)
    assert summary["category"] == "销售"
    assert "区域销售流水" in summary["biz_description"]
    assert any("趋势" in s for s in summary["recommended_analyses"])
