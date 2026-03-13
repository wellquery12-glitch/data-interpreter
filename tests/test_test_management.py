from pathlib import Path

from app.agent import DataInterpreterAgent


def _new_agent(tmp_path: Path) -> DataInterpreterAgent:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    return DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))


def test_run_automated_tests_records_result(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    agent = _new_agent(tmp_path)

    seq = [
        {"cmd": "c1", "returncode": 0, "status": "pass", "duration_ms": 1, "output": "ok"},
        {"cmd": "c2", "returncode": 127, "status": "skipped", "duration_ms": 1, "output": "skip"},
        {"cmd": "c3", "returncode": 0, "status": "pass", "duration_ms": 1, "output": "ok"},
    ]

    def fake_exec(cmd, timeout=300):  # noqa: ANN001, ARG001
        return seq.pop(0)

    monkeypatch.setattr(agent, "_execute_test_command", fake_exec)

    out = agent.run_automated_tests(include_pytest=False)
    assert out["status"] == "pass"
    assert out["pass_count"] == 2
    assert out["skip_count"] == 1
    assert out["fail_count"] == 0
    assert out["run_id"]

    listed = agent.list_test_runs(limit=10)
    assert listed["total"] == 1
    rid = listed["items"][0]["run_id"]

    detail = agent.get_test_run(rid)
    assert detail["run_id"] == rid
    assert len(detail["items"]) == 3
