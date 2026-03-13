import json
import io
import zipfile
from pathlib import Path

from app.agent import DataInterpreterAgent


class _FakeResp:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return self.payload

    def __enter__(self) -> "_FakeResp":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


def _new_agent(tmp_path: Path) -> DataInterpreterAgent:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    return DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))


def test_fetch_kaggle_datasets(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    agent = _new_agent(tmp_path)

    def fake_urlopen(req, timeout=30):  # noqa: ANN001, ARG001
        url = req.full_url
        if "kaggle.com/api/v1/datasets/list" not in url:
            raise AssertionError(f"unexpected url: {url}")
        payload = [
            {
                "ref": "team/demo-business-dataset",
                "title": "Demo Business Dataset",
                "subtitle": "Demo subtitle",
                "lastUpdated": "2025-01-02T00:00:00Z",
                "usabilityRating": 0.9,
            }
        ]
        return _FakeResp(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    out = agent.fetch_kaggle_datasets(topic="businessDataset", keyword="demo", page=1, page_size=20)
    assert out["data"]
    row = out["data"][0]
    assert row["kaggle_ref"] == "team/demo-business-dataset"
    assert row["name"] == "Demo Business Dataset"
    assert row["dataset_page_url"].endswith("/team/demo-business-dataset")


def test_import_kaggle_dataset_with_credentials(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    agent = _new_agent(tmp_path)
    agent.update_kaggle_settings({"username": "u1", "api_key": "k1"})
    ref = "team/demo-business-dataset"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("data/demo.csv", "city,amount\nSH,10\nBJ,20\n")
    archive = buf.getvalue()
    calls = {"download": 0}

    def fake_urlopen(req, timeout=30):  # noqa: ANN001, ARG001
        url = req.full_url
        if "/datasets/download/team/demo-business-dataset" in url:
            calls["download"] += 1
            assert "Authorization" in req.headers
            return _FakeResp(archive)
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    first = agent.import_kaggle_dataset(ref)
    assert first["downloaded"] is True
    assert first["skipped"] is False
    assert first["dataset_id"]
    assert calls["download"] == 1

    second = agent.import_kaggle_dataset(ref)
    assert second["skipped"] is True
    assert second["dataset_id"] == first["dataset_id"]
    assert calls["download"] == 1
