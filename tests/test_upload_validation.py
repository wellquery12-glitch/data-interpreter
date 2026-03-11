from pathlib import Path

from app.agent import DataInterpreterAgent


def _new_agent(tmp_path: Path) -> DataInterpreterAgent:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    return DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))


def test_reject_empty_upload(tmp_path: Path) -> None:
    agent = _new_agent(tmp_path)

    try:
        agent.save_dataset("empty.csv", b"")
    except ValueError as exc:
        assert "为空" in str(exc)
    else:
        raise AssertionError("empty upload should be rejected")


def test_reject_too_large_upload(tmp_path: Path) -> None:
    agent = _new_agent(tmp_path)
    payload = b"a" * (DataInterpreterAgent.MAX_UPLOAD_BYTES + 1)

    try:
        agent.save_dataset("big.csv", payload)
    except ValueError as exc:
        assert "文件过大" in str(exc)
    else:
        raise AssertionError("oversized upload should be rejected")


def test_cleanup_file_when_parse_failed(tmp_path: Path) -> None:
    agent = _new_agent(tmp_path)
    storage = Path(agent.storage)
    before = set(storage.glob("*"))

    try:
        agent.save_dataset("bad.xlsx", b"not-a-valid-xlsx")
    except ValueError as exc:
        assert "解析失败" in str(exc)
    else:
        raise AssertionError("invalid xlsx upload should be rejected")

    after = set(storage.glob("*"))
    assert before == after
