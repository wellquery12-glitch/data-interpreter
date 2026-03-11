from pathlib import Path

import pandas as pd

from app.agent import DataInterpreterAgent
from app.sandbox import SandboxExecutor


def test_agent_df_cache_reuses_dataframe_without_reloading(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    sessions = tmp_path / "sessions"
    storage.mkdir()
    sessions.mkdir()
    csv_path = storage / "demo.csv"
    csv_path.write_text("driver,weight\n张三,10\n李四,20\n", encoding="utf-8")

    agent = DataInterpreterAgent(storage_dir=str(storage), sessions_dir=str(sessions))

    first = agent._load_df_cached(csv_path)
    agent._load_df = lambda _: (_ for _ in ()).throw(RuntimeError("cache miss"))  # type: ignore[assignment]
    second = agent._load_df_cached(csv_path)

    assert len(first) == len(second) == 2


def test_sandbox_blocks_import_statements(tmp_path: Path) -> None:
    executor = SandboxExecutor(str(tmp_path / "s"))
    df = pd.DataFrame({"a": [1, 2, 3]})

    result = executor.run("import os\nanswer = 'ok'", df=df, max_rows=10)

    assert result.success is False
    assert "import" in result.error
