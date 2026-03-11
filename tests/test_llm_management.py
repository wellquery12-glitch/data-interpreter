from pathlib import Path

from app.agent import DataInterpreterAgent


def _prepare_agent_with_dataset(tmp_path: Path, dataset_id: str = "d1") -> DataInterpreterAgent:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    (storage / f"{dataset_id}.csv").write_text("driver,amount\nA,10\nB,30\nA,20\n", encoding="utf-8")
    return DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))


def test_smart_mode_blocked_when_quota_exhausted(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, "d1")
    agent.update_llm_settings(
        {
            "enabled": True,
            "api_key": "test-key",
            "model": "gpt-4o-mini",
            "max_total_tokens": 1000,
            "warn_ratio": 0.8,
            "input_cost_per_million": 1.0,
            "output_cost_per_million": 1.0,
        }
    )
    agent._save_llm_usage(
        {
            "total_prompt_tokens": 800,
            "total_completion_tokens": 200,
            "total_tokens": 1000,
            "total_cost": 0.01,
            "events": [],
            "updated_at": "",
        }
    )

    resp = agent.ask(dataset_id="d1", question="哪个司机运单最多？", max_rows=10, mode="smart")

    assert resp["mode"] == "smart"
    assert resp["mode_degraded"] is True
    assert "已耗尽" in resp["mode_degraded_reason"]
    assert resp["llm_quota"]["exhausted"] is True


def test_llm_trace_accumulates_usage_and_cost(tmp_path: Path) -> None:
    agent = _prepare_agent_with_dataset(tmp_path, "d2")
    agent.update_llm_settings(
        {
            "enabled": True,
            "api_key": "test-key",
            "model": "gpt-4o-mini",
            "max_total_tokens": 100000,
            "warn_ratio": 0.8,
            "input_cost_per_million": 1.0,
            "output_cost_per_million": 2.0,
        }
    )
    agent.planner.begin_trace = lambda: None  # type: ignore[assignment]
    agent.planner.consume_trace = lambda: [  # type: ignore[assignment]
        {
            "time": "2026-03-10T00:00:00",
            "stage": "generate_code",
            "model": "gpt-4o-mini",
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "total_tokens": 150,
            "cost": 0.0002,
        }
    ]

    resp = agent.ask(dataset_id="d2", question="哪个司机运单最多？", max_rows=10, mode="auto")

    assert resp["llm_usage_delta"]["total_tokens"] == 150
    assert resp["llm_usage_delta"]["cost"] == 0.0002
    settings = agent.get_llm_settings()
    assert settings["usage"]["total_tokens"] == 150
    assert settings["usage"]["total_prompt_tokens"] == 100
    assert settings["usage"]["total_completion_tokens"] == 50
