import json
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


def test_import_uci_dataset_and_deduplicate(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    agent = _new_agent(tmp_path)
    uci_id = 222
    csv_url = "https://archive.ics.uci.edu/static/public/222/demo.csv"
    calls = {"csv": 0}

    def fake_urlopen(req, timeout=30):  # noqa: ANN001, ARG001
        url = req.full_url
        if "api/datasets/list" in url:
            payload = {
                "data": [
                    {
                        "ID": uci_id,
                        "Name": "Demo UCI",
                        "Abstract": "test description",
                        "Task": "Classification",
                        "Instances": 3,
                        "Features": 2,
                    }
                ]
            }
            return _FakeResp(json.dumps(payload).encode("utf-8"))
        if f"api/dataset?id={uci_id}" in url:
            payload = {
                "data": {
                    "name": "Demo UCI",
                    "abstract": "test description",
                    "data_url": csv_url,
                    "repository_url": f"https://archive.ics.uci.edu/dataset/{uci_id}/demo",
                }
            }
            return _FakeResp(json.dumps(payload).encode("utf-8"))
        if url == csv_url:
            calls["csv"] += 1
            return _FakeResp(b"driver,amount\nA,10\nB,20\n")
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    listed = agent.fetch_uci_datasets(keyword="", page=1, page_size=20)
    assert listed["data"][0]["downloaded"] is False

    first = agent.import_uci_dataset(uci_id=uci_id)
    assert first["downloaded"] is True
    assert first["skipped"] is False
    assert calls["csv"] == 1

    listed2 = agent.fetch_uci_datasets(keyword="", page=1, page_size=20)
    assert listed2["data"][0]["downloaded"] is True
    assert listed2["data"][0]["dataset_id"] == first["dataset_id"]

    second = agent.import_uci_dataset(uci_id=uci_id)
    assert second["skipped"] is True
    assert second["dataset_id"] == first["dataset_id"]
    assert calls["csv"] == 1

    datasets = agent.list_datasets()
    assert datasets and datasets[0]["source"] == "uci"
    assert datasets[0]["source_id"] == str(uci_id)
    assert datasets[0]["source_name"] == "Demo UCI"
    assert "test description" in datasets[0]["source_description"]
