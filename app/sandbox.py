from __future__ import annotations

import ast
import contextlib
import hashlib
import io
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import matplotlib
import pandas as pd

matplotlib.use("Agg")
from matplotlib import font_manager
import matplotlib.pyplot as plt


def _configure_matplotlib_fonts() -> None:
    preferred = ["PingFang SC", "Hiragino Sans GB", "Noto Sans CJK SC", "Microsoft YaHei", "SimHei", "Arial Unicode MS"]
    installed = {f.name for f in font_manager.fontManager.ttflist}
    chosen = [name for name in preferred if name in installed]
    if chosen:
        matplotlib.rcParams["font.sans-serif"] = chosen + list(matplotlib.rcParams.get("font.sans-serif", []))
    matplotlib.rcParams["axes.unicode_minus"] = False


_configure_matplotlib_fonts()


@dataclass
class ExecResult:
    success: bool
    answer: str
    table: List[Dict[str, Any]]
    plots: List[str]
    stdout: str
    error: str


class SandboxExecutor:
    def __init__(self, session_dir: str) -> None:
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)

    def run(self, code: str, df: pd.DataFrame, max_rows: int = 20) -> ExecResult:
        stdout_io = io.StringIO()
        plots: List[str] = []
        blocked_reason = self._validate_code(code)
        if blocked_reason:
            return ExecResult(
                success=False,
                answer=f"执行失败: {blocked_reason}",
                table=[],
                plots=[],
                stdout="",
                error=blocked_reason,
            )

        def save_plot(filename: str) -> str:
            safe = os.path.basename(filename)
            out = self.session_dir / safe
            plt.savefig(out)
            return str(out)

        def hash_text(value: Any) -> str:
            text = str(value)
            digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
            return digest[:12]

        def safe_import(name: str, globals=None, locals=None, fromlist=(), level=0):  # type: ignore[no-untyped-def]
            root = str(name).split(".")[0]
            blocked = {"os", "sys", "subprocess", "socket", "requests", "http", "urllib", "ctypes", "pathlib", "shutil"}
            if root in blocked:
                raise ImportError(f"禁止导入模块: {root}")
            return __import__(name, globals, locals, fromlist, level)

        safe_globals = {
            "__builtins__": {
                "__import__": safe_import,
                "len": len,
                "min": min,
                "max": max,
                "sum": sum,
                "range": range,
                "print": print,
                "str": str,
                "int": int,
                "float": float,
                "bool": bool,
                "list": list,
                "dict": dict,
                "set": set,
                "tuple": tuple,
                "abs": abs,
                "sorted": sorted,
                "enumerate": enumerate,
                "zip": zip,
                "any": any,
                "all": all,
            },
            "pd": pd,
            "plt": plt,
            "hash_text": hash_text,
        }

        exec_env: Dict[str, Any] = {
            **safe_globals,
            "df": df.copy(),
            "result_df": pd.DataFrame(),
            "answer": "",
            "plots": plots,
            "save_plot": save_plot,
            "hash_text": hash_text,
        }

        try:
            with contextlib.redirect_stdout(stdout_io):
                exec(code, exec_env, exec_env)
        except Exception as exc:  # noqa: BLE001
            return ExecResult(
                success=False,
                answer=f"执行失败: {exc}",
                table=[],
                plots=[],
                stdout=stdout_io.getvalue(),
                error=str(exc),
            )

        result_df = exec_env.get("result_df")
        if not isinstance(result_df, pd.DataFrame):
            result_df = pd.DataFrame()

        table = result_df.head(max_rows).to_dict(orient="records")
        answer = str(exec_env.get("answer", "分析已完成"))
        return ExecResult(
            success=True,
            answer=answer,
            table=table,
            plots=plots,
            stdout=stdout_io.getvalue(),
            error="",
        )

    @staticmethod
    def _validate_code(code: str) -> str:
        try:
            tree = ast.parse(code, mode="exec")
        except SyntaxError as exc:
            return f"代码语法错误: {exc}"

        blocked_calls = {"open", "eval", "exec", "compile", "input", "__import__"}
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                return "不允许在分析代码中使用 import"
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id in blocked_calls:
                    return f"不允许调用危险函数: {node.func.id}"
        return ""
