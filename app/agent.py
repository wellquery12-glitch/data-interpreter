from __future__ import annotations

import datetime as dt
import difflib
import json
import os
import re
import uuid
import urllib.error
import urllib.request
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib

matplotlib.use("Agg")
import pandas as pd
from matplotlib import pyplot as plt

from .planner import CodePlanner, DatasetMeta
from .sandbox import ExecResult, SandboxExecutor


@dataclass
class DatasetInfo:
    dataset_id: str
    path: str
    rows: int
    cols: int
    columns: List[str]


class DataInterpreterAgent:
    MAX_UPLOAD_BYTES = 20 * 1024 * 1024

    def __init__(self, storage_dir: str, sessions_dir: str) -> None:
        self.storage = Path(storage_dir)
        self.sessions = Path(sessions_dir)
        self.meta_file = self.storage / ".meta.json"
        self.records_file = self.sessions / "ask_records.jsonl"
        self.tools_file = self.sessions / "tools_memory.json"
        self.bugs_file = self.sessions / "bug_records.jsonl"
        self.llm_config_file = self.sessions / "llm_config.json"
        self.llm_usage_file = self.sessions / "llm_usage.json"
        self.tools_hub_file = self.sessions / "tools_hub.json"
        self._df_cache: "OrderedDict[str, Tuple[int, pd.DataFrame]]" = OrderedDict()
        self._df_cache_limit = 8
        self.storage.mkdir(parents=True, exist_ok=True)
        self.sessions.mkdir(parents=True, exist_ok=True)
        self.planner = CodePlanner()
        self.planner_service_url = os.getenv("PLANNER_SERVICE_URL", "").strip()
        self.executor_service_url = os.getenv("EXECUTOR_SERVICE_URL", "").strip()
        self.tool_backend_service_url = os.getenv("TOOL_BACKEND_SERVICE_URL", "").strip()

    def save_dataset(self, filename: str, content: bytes) -> DatasetInfo:
        if not content:
            raise ValueError("上传文件为空")
        if len(content) > self.MAX_UPLOAD_BYTES:
            raise ValueError(f"文件过大，最大支持 {self.MAX_UPLOAD_BYTES // (1024 * 1024)}MB")

        dataset_id = uuid.uuid4().hex
        ext = Path(filename).suffix.lower()
        if ext not in {".csv", ".xlsx"}:
            raise ValueError("仅支持 CSV/XLSX")

        path = self.storage / f"{dataset_id}{ext}"
        path.write_bytes(content)
        try:
            df = self._load_df(path)
        except Exception as exc:  # noqa: BLE001
            if path.exists():
                path.unlink()
            raise ValueError("文件解析失败，请确认内容是有效的 CSV/XLSX") from exc

        meta = self._load_meta()
        meta[dataset_id] = {
            "original_filename": filename,
            "saved_filename": path.name,
            "alias": "",
            "updated_at": dt.datetime.utcnow().isoformat(),
        }
        self._save_meta(meta)
        return DatasetInfo(
            dataset_id=dataset_id,
            path=str(path),
            rows=int(df.shape[0]),
            cols=int(df.shape[1]),
            columns=[str(c) for c in df.columns],
        )

    def list_datasets(self) -> List[Dict[str, Any]]:
        meta = self._load_meta()
        items: List[Dict[str, Any]] = []
        for p in sorted(self.storage.glob("*")):
            if not p.is_file():
                continue
            if p.suffix.lower() not in {".csv", ".xlsx"}:
                continue
            stat = p.stat()
            m = meta.get(p.stem, {})
            items.append(
                {
                    "dataset_id": p.stem,
                    "filename": p.name,
                    "original_filename": m.get("original_filename", p.name),
                    "alias": str(m.get("alias", "")),
                    "ext": p.suffix.lower(),
                    "size_bytes": int(stat.st_size),
                    "updated_at": dt.datetime.fromtimestamp(stat.st_mtime).isoformat(),
                }
            )
        items.sort(key=lambda x: x["updated_at"], reverse=True)
        return items

    def set_dataset_alias(self, dataset_id: str, alias: str) -> Dict[str, Any]:
        real_id = self._resolve_dataset_id(dataset_id)
        if not real_id:
            raise FileNotFoundError("dataset_id 不存在")
        alias_norm = str(alias or "").strip()
        if alias_norm:
            if not re.match(r"^[A-Za-z0-9_\-\u4e00-\u9fff]{1,64}$", alias_norm):
                raise ValueError("别名仅支持中英文、数字、下划线、中划线，长度 1-64")
        meta = self._load_meta()
        for did, info in meta.items():
            if did == real_id:
                continue
            if str(info.get("alias", "")).strip() == alias_norm and alias_norm:
                raise ValueError("别名已被其他数据集占用")
        row = meta.get(real_id, {})
        row["alias"] = alias_norm
        row["updated_at"] = dt.datetime.utcnow().isoformat()
        meta[real_id] = row
        self._save_meta(meta)
        return {"dataset_id": real_id, "alias": alias_norm}

    def delete_dataset(self, dataset_id: str) -> Dict[str, Any]:
        real_id = self._resolve_dataset_id(dataset_id)
        if not real_id:
            raise FileNotFoundError("dataset_id 不存在")
        removed_file_count = 0
        for ext in (".csv", ".xlsx"):
            p = self.storage / f"{real_id}{ext}"
            if p.exists():
                p.unlink()
                removed_file_count += 1
        meta = self._load_meta()
        removed_meta = False
        if real_id in meta:
            meta.pop(real_id, None)
            self._save_meta(meta)
            removed_meta = True
        # Clear cache for removed dataset file paths.
        keys = [k for k in self._df_cache.keys() if f"/{real_id}." in k]
        for k in keys:
            self._df_cache.pop(k, None)
        if removed_file_count == 0 and not removed_meta:
            raise FileNotFoundError("dataset 文件不存在")
        return {
            "deleted": True,
            "dataset_id": real_id,
            "removed_files": removed_file_count,
            "removed_meta": removed_meta,
        }

    def list_records(self, limit: int = 50, dataset_id: str = "") -> List[Dict[str, Any]]:
        if not self.records_file.exists():
            return []
        lines = self.records_file.read_text(encoding="utf-8").splitlines()
        rows: List[Dict[str, Any]] = []
        for line in reversed(lines):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            if dataset_id and row.get("dataset_id") != dataset_id:
                continue
            rows.append(row)
            if len(rows) >= limit:
                break
        return rows

    def get_bug_summary(self, limit: int = 50) -> Dict[str, Any]:
        bugs = self._read_jsonl(self.bugs_file)
        records = self._read_jsonl(self.records_file)
        if not bugs:
            return {"items": [], "total": 0}

        success_by_intent: Dict[str, List[Dict[str, Any]]] = {}
        for rec in records:
            intent = str(rec.get("intent_key", "")).strip()
            if not intent:
                continue
            status = str(rec.get("status", ""))
            exec_err = str(rec.get("execution_error", "")).strip()
            if status == "ok" and not exec_err:
                success_by_intent.setdefault(intent, []).append(rec)

        groups: Dict[str, Dict[str, Any]] = {}
        for bug in bugs:
            intent = str(bug.get("intent_key", "")).strip()
            err = str(bug.get("error", "")).strip()
            key = f"{intent}||{err}"
            row = groups.get(key)
            if row is None:
                row = {
                    "intent_key": intent,
                    "error": err,
                    "error_sample": err[:140],
                    "count": 0,
                    "first_time": str(bug.get("time", "")),
                    "last_time": str(bug.get("time", "")),
                    "last_session_id": str(bug.get("session_id", "")),
                    "last_question": str(bug.get("question", "")),
                }
                groups[key] = row
            row["count"] = int(row.get("count", 0)) + 1
            t = str(bug.get("time", ""))
            if t and (not row.get("last_time") or t > str(row.get("last_time"))):
                row["last_time"] = t
                row["last_session_id"] = str(bug.get("session_id", ""))
                row["last_question"] = str(bug.get("question", ""))
            if t and (not row.get("first_time") or t < str(row.get("first_time"))):
                row["first_time"] = t

        items: List[Dict[str, Any]] = []
        for row in groups.values():
            intent = str(row.get("intent_key", ""))
            last_time = str(row.get("last_time", ""))
            fixed = False
            fixed_time = ""
            fixed_session_id = ""
            for rec in success_by_intent.get(intent, []):
                t = str(rec.get("time", ""))
                if t and last_time and t > last_time:
                    fixed = True
                    fixed_time = t
                    fixed_session_id = str(rec.get("session_id", ""))
                    break
            row["fixed"] = fixed
            row["fixed_time"] = fixed_time
            row["fixed_session_id"] = fixed_session_id
            items.append(row)

        items.sort(key=lambda x: str(x.get("last_time", "")), reverse=True)
        safe_limit = max(1, min(int(limit or 50), 200))
        return {"items": items[:safe_limit], "total": len(items)}

    def list_records_paged(
        self,
        page: int = 1,
        page_size: int = 5,
        dataset_id: str = "",
        keyword: str = "",
    ) -> Dict[str, Any]:
        if not self.records_file.exists():
            return {"data": [], "total": 0, "page": page, "page_size": page_size, "total_pages": 0}

        page = max(1, int(page))
        page_size = max(1, int(page_size))
        kw = (keyword or "").strip().lower()

        lines = self.records_file.read_text(encoding="utf-8").splitlines()
        all_rows: List[Dict[str, Any]] = []
        for line in reversed(lines):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            if dataset_id and row.get("dataset_id") != dataset_id:
                continue
            if kw:
                text = f"{row.get('question', '')}\n{row.get('answer', '')}".lower()
                if kw not in text:
                    continue
            all_rows.append(row)

        total = len(all_rows)
        total_pages = (total + page_size - 1) // page_size if total else 0
        if total_pages > 0:
            page = min(page, total_pages)
        start = (page - 1) * page_size
        end = start + page_size
        return {
            "data": all_rows[start:end],
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

    def get_dataset_overview(self, dataset_id: str, max_rows: int = 8) -> Dict[str, Any]:
        path = self._find_dataset(dataset_id)
        if path is None:
            raise FileNotFoundError("dataset_id 不存在")
        df = self._load_df_cached(path)
        safe_rows = max(1, min(max_rows, 50))
        sample = self._to_json_safe_records(df.head(safe_rows))
        return {
            "dataset_id": dataset_id,
            "rows": int(df.shape[0]),
            "cols": int(df.shape[1]),
            "columns": [str(c) for c in df.columns],
            "dtypes": {str(c): str(t) for c, t in df.dtypes.items()},
            "sample_rows": sample,
        }

    def get_record_detail(self, session_id: str) -> Dict[str, Any]:
        path = self.sessions / session_id / "result.json"
        if not path.exists():
            # Backward compatibility for old records written before result.json was introduced.
            for row in self.list_records(limit=5000):
                if row.get("session_id") == session_id:
                    return {
                        "session_id": session_id,
                        "dataset_id": row.get("dataset_id", ""),
                        "question": row.get("question", ""),
                        "answer": row.get("answer", ""),
                        "generated_code": "",
                        "table": [],
                        "plots": [],
                        "stdout": "",
                        "execution_error": row.get("execution_error", ""),
                        "used_fallback": bool(row.get("used_fallback", False)),
                        "retries": int(row.get("retries", 0)),
                        "status": row.get("status", "degraded"),
                        "error_category": row.get("error_category", "execution_error"),
                        "error_message": "历史详情文件缺失，仅展示摘要信息",
                        "error_hint": "该记录创建于旧版本，未保存完整执行详情。",
                        "sub_results": [],
                        "mode": row.get("mode", "auto"),
                    }
            raise FileNotFoundError("session_id 不存在或详情已丢失")
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise ValueError("结果详情文件损坏") from exc

    def get_llm_settings(self) -> Dict[str, Any]:
        cfg = self._load_llm_config()
        usage = self._load_llm_usage()
        quota = self._quota_status(cfg=cfg, usage=usage)
        return {
            "config": cfg,
            "usage": usage,
            "quota": quota,
        }

    def update_llm_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        old = self._load_llm_config()
        cfg = self._sanitize_llm_config(payload=payload, base=old)
        cfg["updated_at"] = dt.datetime.utcnow().isoformat()
        self._save_llm_config(cfg)
        usage = self._load_llm_usage()
        quota = self._quota_status(cfg=cfg, usage=usage)
        return {"config": cfg, "usage": usage, "quota": quota}

    def reset_llm_usage(self) -> Dict[str, Any]:
        usage = self._default_llm_usage()
        usage["updated_at"] = dt.datetime.utcnow().isoformat()
        self._save_llm_usage(usage)
        cfg = self._load_llm_config()
        quota = self._quota_status(cfg=cfg, usage=usage)
        return {"config": cfg, "usage": usage, "quota": quota}

    def get_tools_hub(self) -> Dict[str, Any]:
        hub = self._load_tools_hub()
        return self._format_tools_hub(hub)

    def install_tool_package(self, package_id: str = "", manifest_url: str = "", source_url: str = "") -> Dict[str, Any]:
        manifest = self._fetch_package_manifest(package_id=package_id, manifest_url=manifest_url, source_url=source_url)
        hub = self._load_tools_hub()
        package_id_norm = str(manifest.get("package_id", "")).strip() or str(package_id).strip() or "pkg_" + uuid.uuid4().hex[:8]
        pkg = {
            "package_id": package_id_norm,
            "name": str(manifest.get("name", package_id_norm)),
            "description": str(manifest.get("description", "")),
            "version": str(manifest.get("version", "")),
            "installed_at": dt.datetime.utcnow().isoformat(),
        }
        hub.setdefault("packages", {})[package_id_norm] = pkg
        tools = manifest.get("tools", [])
        if not isinstance(tools, list):
            tools = []
        for raw in tools:
            if not isinstance(raw, dict):
                continue
            tool = self._sanitize_tool(raw=raw, default_package_id=package_id_norm)
            if not tool:
                continue
            hub.setdefault("tools", {})[tool["tool_id"]] = tool
            selected = hub.setdefault("selected_tool_ids", [])
            if tool["tool_id"] not in selected:
                selected.append(tool["tool_id"])
        hub["updated_at"] = dt.datetime.utcnow().isoformat()
        self._save_tools_hub(hub)
        return self._format_tools_hub(hub)

    def add_tool(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        hub = self._load_tools_hub()
        tool = self._sanitize_tool(raw=payload, default_package_id=str(payload.get("package_id", "manual") or "manual"))
        if not tool:
            raise ValueError("工具定义不完整，至少需要 tool_id/name/endpoint")
        hub.setdefault("tools", {})[tool["tool_id"]] = tool
        pkg_id = str(tool.get("package_id", "manual"))
        hub.setdefault("packages", {}).setdefault(
            pkg_id,
            {
                "package_id": pkg_id,
                "name": pkg_id,
                "description": "手动添加",
                "version": "",
                "installed_at": dt.datetime.utcnow().isoformat(),
            },
        )
        selected = hub.setdefault("selected_tool_ids", [])
        if tool["tool_id"] not in selected:
            selected.append(tool["tool_id"])
        hub["updated_at"] = dt.datetime.utcnow().isoformat()
        self._save_tools_hub(hub)
        return self._format_tools_hub(hub)

    def set_tool_enabled(self, tool_id: str, enabled: bool) -> Dict[str, Any]:
        hub = self._load_tools_hub()
        tools = hub.get("tools", {})
        if tool_id in tools and isinstance(tools[tool_id], dict):
            tools[tool_id]["enabled"] = bool(enabled)
        hub["updated_at"] = dt.datetime.utcnow().isoformat()
        self._save_tools_hub(hub)
        return self._format_tools_hub(hub)

    def set_selected_tools(self, tool_ids: List[str]) -> Dict[str, Any]:
        hub = self._load_tools_hub()
        known = {str(k) for k in hub.get("tools", {}).keys()}
        picked = [str(v) for v in tool_ids if str(v) in known]
        hub["selected_tool_ids"] = picked
        hub["updated_at"] = dt.datetime.utcnow().isoformat()
        self._save_tools_hub(hub)
        return self._format_tools_hub(hub)

    def fetch_tools_catalog(self, source_url: str) -> Dict[str, Any]:
        if not str(source_url or "").strip() and self.tool_backend_service_url:
            source = self.tool_backend_service_url.rstrip("/")
            data = self._http_json_get(url=source + "/packages")
            packages = data.get("packages", []) if isinstance(data, dict) else []
            if not isinstance(packages, list):
                packages = []
            return {"source_url": source, "packages": packages}
        if not str(source_url or "").strip():
            return {"source_url": "", "packages": self._builtin_tool_packages()}
        url = source_url.rstrip("/") + "/packages"
        data = self._http_json_get(url=url)
        packages = data.get("packages", []) if isinstance(data, dict) else []
        if not isinstance(packages, list):
            packages = []
        return {"source_url": source_url, "packages": packages}

    def get_service_topology(self) -> Dict[str, Any]:
        planner_url = self.planner_service_url.rstrip("/")
        executor_url = self.executor_service_url.rstrip("/")
        tool_backend_url = self.tool_backend_service_url.rstrip("/")
        services = [
            self._service_health(
                name="orchestrator-service",
                role="核心执行链路编排",
                url="local",
                enabled=True,
                is_local=True,
            ),
            self._service_health(
                name="planner-service",
                role="任务规划/代码规划",
                url=planner_url,
                enabled=bool(planner_url),
            ),
            self._service_health(
                name="executor-service",
                role="代码沙箱执行",
                url=executor_url,
                enabled=bool(executor_url),
            ),
            self._service_health(
                name="tool-backend-service",
                role="工具包目录/工具运行",
                url=tool_backend_url,
                enabled=bool(tool_backend_url),
            ),
        ]
        return {"services": services}

    def ask(
        self,
        dataset_id: str,
        question: str,
        max_rows: int = 20,
        mode: str = "auto",
        selected_tools: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        normalized_questions = self._split_questions(question)
        if len(normalized_questions) > 1:
            return self._ask_multi(
                dataset_id=dataset_id,
                questions=normalized_questions,
                max_rows=max_rows,
                mode=mode,
                selected_tools=selected_tools,
            )
        question = normalized_questions[0]

        path = self._find_dataset(dataset_id)
        if path is None:
            raise FileNotFoundError("dataset_id 不存在")

        df = self._load_df_cached(path)
        meta = DatasetMeta(
            columns=[str(c) for c in df.columns],
            dtypes={str(c): str(t) for c, t in df.dtypes.items()},
        )
        intent_key = self._planner_intent_key(question)
        code_source = "planner"
        mode_norm = (mode or "auto").strip().lower()
        if mode_norm not in {"auto", "smart", "tool"}:
            mode_norm = "auto"
        llm_cfg = self._load_llm_config()
        llm_usage_before = self._load_llm_usage()
        quota_before = self._quota_status(cfg=llm_cfg, usage=llm_usage_before)
        self.planner.begin_trace()
        self.planner.set_llm_runtime(
            config=llm_cfg,
            allow_llm=bool(quota_before.get("allow_llm", True)),
            deny_reason=str(quota_before.get("message", "")),
        )
        selected_tool_context = self._selected_tool_context(selected_tools=selected_tools or [])
        self.planner.set_tool_context(selected_tool_context)
        smart_degraded = False
        smart_degraded_reason = ""
        remote_trace: List[Dict[str, Any]] = []
        remote_tool_id = ""

        if mode_norm == "smart":
            llm_code = self._planner_generate_llm_only(question=question, meta=meta, error="")
            if llm_code:
                code = llm_code
                code_source = "llm_direct"
            else:
                # Smart mode degrades to auto planner when LLM is unavailable.
                smart_degraded = True
                if not quota_before.get("allow_llm", True):
                    smart_degraded_reason = str(quota_before.get("message", "LLM 资源已耗尽"))
                elif not llm_cfg.get("api_key"):
                    smart_degraded_reason = "未配置 API Key，已自动降级为本地规则分析"
                else:
                    smart_degraded_reason = "LLM 当前不可用，已自动降级为本地规则分析"
                code = self._planner_generate(question=question, meta=meta)
                code_source = "smart_degraded_auto"
        elif mode_norm == "tool":
            remote = self._run_remote_tools(
                question=question,
                dataset_id=dataset_id,
                df=df,
                max_rows=max_rows,
                selected_tools=selected_tools or [],
            )
            if remote:
                code = "# remote tool invocation"
                code_source = f"remote_tool:{remote['tool_id']}"
                remote_tool_id = str(remote["tool_id"])
                remote_trace = list(remote.get("llm_trace", []))
                result = ExecResult(
                    success=True,
                    answer=str(remote.get("answer", "远程工具执行完成")),
                    table=remote.get("table", []),
                    plots=remote.get("plots", []),
                    stdout=str(remote.get("stdout", "")),
                    error="",
                )
            else:
                plan = self._planner_plan_tool(question=question, meta=meta)
                code = self._planner_build_tool_code(plan=plan, meta=meta)
                code_source = "tool_mode"
        else:
            code = self._planner_generate(question=question, meta=meta)

        if self._planner_is_fallback_code(code):
            auto_code = self._planner_generate_auto_tool(question=question, meta=meta)
            if auto_code:
                code = auto_code
                code_source = "auto_tool"

        session_id = uuid.uuid4().hex
        if remote_tool_id:
            pass
        else:
            result = self._executor_run(session_id=session_id, code=code, df=df, max_rows=max_rows)
        if code_source in {"planner"} and self._planner_is_fallback_code(code):
            result.success = False
            result.error = "当前问题暂不支持自动解析"
        execution_error = ""
        used_fallback = False
        bug_logs: List[Dict[str, Any]] = []
        auto_rewrite_applied = False
        auto_rewrite_question = ""
        retries = 0
        max_retries = 2
        while not result.success and retries < max_retries:
            retries += 1
            execution_error = result.error
            bug_row = {
                "time": dt.datetime.utcnow().isoformat(),
                "session_id": session_id,
                "dataset_id": dataset_id,
                "intent_key": intent_key,
                "question": question,
                "retry": retries,
                "error": execution_error,
                "code_source": code_source,
            }
            bug_logs.append(bug_row)
            self._append_bug(bug_row)
            print(f"[BUG][retry={retries}] {execution_error}")

            if not auto_rewrite_applied:
                errv = self._classify_error(
                    execution_error=execution_error,
                    used_fallback=False,
                    retries=1,
                    code_source=code_source,
                )
                if errv.get("error_category") == "field_missing":
                    rewritten = self._rewrite_question_from_error(
                        question=question,
                        meta=meta,
                        error_message=execution_error,
                    )
                    if rewritten and rewritten != question:
                        question = rewritten
                        auto_rewrite_applied = True
                        auto_rewrite_question = rewritten
                        intent_key = self._planner_intent_key(question)
                        code = self._planner_generate(question=question, meta=meta)
                        if self._planner_is_fallback_code(code):
                            auto_code = self._planner_generate_auto_tool(question=question, meta=meta)
                            if auto_code:
                                code = auto_code
                                code_source = "auto_rewrite"
                        else:
                            code_source = "auto_rewrite"
                        result = self._executor_run(session_id=session_id, code=code, df=df, max_rows=max_rows)
                        if result.success:
                            break

            if mode_norm == "smart" and not smart_degraded:
                code = self._planner_generate_llm_only(question=question, meta=meta, error=execution_error) or ""
                if not code:
                    break
                code_source = "llm_repair"
                result = self._executor_run(session_id=session_id, code=code, df=df, max_rows=max_rows)
            else:
                code = self._planner_repair_code(question=question, meta=meta, error=execution_error)
                code_source = "auto_repair"
                result = self._executor_run(session_id=session_id, code=code, df=df, max_rows=max_rows)

        fallback_reason = ""
        fallback_solution = ""
        if not result.success:
            execution_error = result.error
            used_fallback = True
            fallback_reason = execution_error
            fallback_solution = self._planner_failure_solution(question=question, meta=meta, error=execution_error)
            self._append_bug(
                {
                    "time": dt.datetime.utcnow().isoformat(),
                    "session_id": session_id,
                    "dataset_id": dataset_id,
                    "intent_key": intent_key,
                    "question": question,
                    "retry": retries + 1,
                    "error": execution_error,
                    "code_source": code_source,
                    "final_fallback": True,
                }
            )
            code = self._planner_fallback_code()
            code_source = "fallback"
            result = self._executor_run(session_id=session_id, code=code, df=df, max_rows=max_rows)
            result.answer = f"{result.answer}（已触发兜底）"
        if remote_tool_id:
            plot_urls = [str(p) for p in result.plots]
        else:
            plot_urls = ["/sessions/{}/{}".format(session_id, Path(p).name) for p in result.plots]
        error_view = self._classify_error(
            execution_error=execution_error,
            used_fallback=used_fallback,
            retries=retries,
            code_source=code_source,
        )
        suggestion = self._suggest_rewritten_question(
            question=question,
            meta=meta,
            error_category=error_view["error_category"],
            error_message=error_view["error_message"],
        )
        llm_trace = self.planner.consume_trace() + remote_trace
        usage_delta = self._sum_llm_trace(trace=llm_trace)
        execution_layers = self._build_execution_layers(
            question=question,
            mode=mode_norm,
            intent_key=intent_key,
            code_source=code_source,
            retries=retries,
            used_fallback=used_fallback,
            status=error_view["status"],
            remote_tool_id=remote_tool_id,
            llm_trace=llm_trace,
            selected_tools=selected_tool_context,
            result=result,
            plot_count=len(plot_urls),
        )
        plan_graph = self._build_layered_plan_graph(execution_layers=execution_layers)
        execution_graph_url = self._render_execution_graph(session_id=session_id, graph=plan_graph)
        if execution_graph_url and execution_graph_url not in plot_urls:
            plot_urls.append(execution_graph_url)
        llm_usage_after, quota_after = self._apply_llm_trace(
            cfg=llm_cfg,
            usage_before=llm_usage_before,
            trace=llm_trace,
            session_id=session_id,
            dataset_id=dataset_id,
            question=question,
            mode=mode_norm,
        )
        self._append_record(
            {
                "time": dt.datetime.utcnow().isoformat(),
                "session_id": session_id,
                "dataset_id": dataset_id,
                "question": question,
                "answer": result.answer,
                "used_fallback": used_fallback,
                "execution_error": execution_error,
                "plots_count": len(plot_urls),
                "retries": retries,
                "intent_key": intent_key,
                "code_source": code_source,
                "status": error_view["status"],
                "error_category": error_view["error_category"],
                "suggested_question": suggestion["suggested_question"],
                "llm_tokens": usage_delta["total_tokens"],
                "llm_cost": usage_delta["cost"],
                "quota_warning": bool(quota_after.get("warning")),
                "quota_exhausted": bool(quota_after.get("exhausted")),
                "execution_graph_url": execution_graph_url,
            }
        )

        response = {
            "session_id": session_id,
            "dataset_id": dataset_id,
            "question": question,
            "answer": result.answer,
            "generated_code": code,
            "table": result.table,
            "plots": plot_urls,
            "stdout": result.stdout,
            "execution_error": execution_error,
            "used_fallback": used_fallback,
            "retries": retries,
            "intent_key": intent_key,
            "code_source": code_source,
            "bug_logs": bug_logs,
            "fallback_reason": fallback_reason,
            "fallback_solution": fallback_solution,
            "status": error_view["status"],
            "error_category": error_view["error_category"],
            "error_message": error_view["error_message"],
            "error_hint": error_view["error_hint"],
            "suggested_question": suggestion["suggested_question"],
            "suggestion_reason": suggestion["suggestion_reason"],
            "auto_rewrite_applied": auto_rewrite_applied,
            "auto_rewrite_question": auto_rewrite_question,
            "mode": mode_norm,
            "mode_degraded": smart_degraded,
            "mode_degraded_reason": smart_degraded_reason if smart_degraded else "",
            "remote_tool_id": remote_tool_id,
            "planning_graph": plan_graph,
            "execution_graph_url": execution_graph_url,
            "execution_layers": execution_layers,
            "llm_trace": llm_trace,
            "llm_usage_delta": usage_delta,
            "llm_quota": quota_after,
            "llm_usage_total": {
                "prompt_tokens": int(llm_usage_after.get("total_prompt_tokens", 0)),
                "completion_tokens": int(llm_usage_after.get("total_completion_tokens", 0)),
                "total_tokens": int(llm_usage_after.get("total_tokens", 0)),
                "total_cost": float(llm_usage_after.get("total_cost", 0.0)),
            },
        }
        self._save_result_detail(session_id=session_id, payload=response)
        return response

    def _ask_multi(
        self,
        dataset_id: str,
        questions: List[str],
        max_rows: int,
        mode: str,
        selected_tools: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        sub_results: List[Dict[str, Any]] = []
        all_plots: List[str] = []
        stdout_parts: List[str] = []
        code_parts: List[str] = []
        errors: List[str] = []
        any_fallback = False
        any_degraded = False
        first_table: List[Dict[str, Any]] = []

        for q in questions:
            sub = self.ask(
                dataset_id=dataset_id,
                question=q,
                max_rows=max_rows,
                mode=mode,
                selected_tools=selected_tools,
            )
            sub_results.append(sub)
            all_plots.extend(sub.get("plots", []))
            if sub.get("stdout"):
                stdout_parts.append(f"[{q}]\n{sub['stdout']}")
            if sub.get("generated_code"):
                code_parts.append(f"# 问题: {q}\n{sub['generated_code']}")
            if sub.get("execution_error"):
                errors.append(f"[{q}] {sub['execution_error']}")
            any_fallback = any_fallback or bool(sub.get("used_fallback", False))
            any_degraded = any_degraded or str(sub.get("status", "ok")) != "ok"
            if not first_table and isinstance(sub.get("table"), list) and sub.get("table"):
                first_table = sub["table"]

        answer = "\n".join([f"{idx + 1}. {r['question']} -> {r['answer']}" for idx, r in enumerate(sub_results)])
        status = "degraded" if any_degraded else "ok"
        category = "partial_degraded" if any_degraded else "none"

        response = {
            "session_id": uuid.uuid4().hex,
            "dataset_id": dataset_id,
            "question": "；".join(questions),
            "answer": answer,
            "generated_code": "\n\n# ----------------\n\n".join(code_parts),
            "table": first_table,
            "plots": all_plots,
            "stdout": "\n\n".join(stdout_parts),
            "execution_error": "\n".join(errors),
            "used_fallback": any_fallback,
            "retries": int(sum(int(r.get("retries", 0)) for r in sub_results)),
            "intent_key": "batch_multi_question",
            "code_source": "multi",
            "bug_logs": [],
            "fallback_reason": "",
            "fallback_solution": "",
            "status": status,
            "error_category": category,
            "error_message": "部分子问题发生降级" if any_degraded else "",
            "error_hint": "可查看分项结果并分别重试问题。" if any_degraded else "",
            "suggested_question": "",
            "suggestion_reason": "",
            "sub_results": sub_results,
            "mode": mode,
            "planning_graph": {},
            "execution_graph_url": "",
            "execution_layers": [],
        }
        self._save_result_detail(session_id=response["session_id"], payload=response)
        return response

    def _find_dataset(self, dataset_id: str) -> Optional[Path]:
        resolved = self._resolve_dataset_id(dataset_id)
        if resolved:
            dataset_id = resolved
        for ext in (".csv", ".xlsx"):
            path = self.storage / f"{dataset_id}{ext}"
            if path.exists():
                return path
        # Backward compatible matching against original filename stem.
        meta = self._load_meta()
        candidates = []
        for did, info in meta.items():
            original = str(info.get("original_filename", ""))
            if Path(original).stem == dataset_id:
                candidates.append((str(info.get("updated_at", "")), did))
        if candidates:
            candidates.sort(reverse=True)
            best_did = candidates[0][1]
            for ext in (".csv", ".xlsx"):
                path = self.storage / f"{best_did}{ext}"
                if path.exists():
                    return path
        return None

    def _resolve_dataset_id(self, dataset_id: str) -> str:
        value = str(dataset_id or "").strip()
        if not value:
            return ""
        meta = self._load_meta()
        # Exact dataset_id hit in metadata should be resolved first.
        if value in meta:
            return value
        for ext in (".csv", ".xlsx"):
            if (self.storage / f"{value}{ext}").exists():
                return value
        # Prefer explicit alias hit.
        for did, info in meta.items():
            if str(info.get("alias", "")).strip() == value:
                return did
        # Fallback: original filename stem alias, choose newest.
        candidates: List[Tuple[str, str]] = []
        for did, info in meta.items():
            original = str(info.get("original_filename", ""))
            if Path(original).stem == value:
                candidates.append((str(info.get("updated_at", "")), did))
        if candidates:
            candidates.sort(reverse=True)
            return candidates[0][1]
        return ""

    def _load_df_cached(self, path: Path) -> pd.DataFrame:
        key = str(path.resolve())
        mtime_ns = path.stat().st_mtime_ns
        cached = self._df_cache.get(key)
        if cached and cached[0] == mtime_ns:
            self._df_cache.move_to_end(key)
            return cached[1].copy()

        df = self._load_df(path)
        self._df_cache[key] = (mtime_ns, df)
        self._df_cache.move_to_end(key)
        while len(self._df_cache) > self._df_cache_limit:
            self._df_cache.popitem(last=False)
        return df.copy()

    @staticmethod
    def _load_df(path: Path) -> pd.DataFrame:
        if path.suffix.lower() == ".csv":
            df = pd.read_csv(path)
            return DataInterpreterAgent._normalize_df(df)

        # Handle generic xlsx and WeChat bill export format (header in middle rows).
        raw = pd.read_excel(path, header=None)
        header_idx = DataInterpreterAgent._find_wechat_header_row(raw)
        if header_idx is not None:
            headers = [str(v).strip() for v in raw.iloc[header_idx].tolist()]
            body = raw.iloc[header_idx + 1 :].copy()
            body.columns = headers
            body = body.dropna(how="all")
            body = body.loc[:, [c for c in body.columns if str(c).strip() and not str(c).startswith("Unnamed:")]]
            return DataInterpreterAgent._normalize_df(body.reset_index(drop=True))

        df = pd.read_excel(path)
        return DataInterpreterAgent._normalize_df(df)

    @staticmethod
    def _find_wechat_header_row(raw: pd.DataFrame) -> Optional[int]:
        max_scan = min(len(raw), 80)
        for i in range(max_scan):
            row = {str(v).strip() for v in raw.iloc[i].tolist() if str(v) != "nan"}
            if "交易时间" in row and ("交易对方" in row or "交易对象" in row):
                return i
        return None

    @staticmethod
    def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out.columns = [str(c).strip() for c in out.columns]
        for col in out.columns:
            s = out[col]
            if str(s.dtype) != "object":
                continue
            cleaned = s.astype(str).str.strip()
            # Normalize currency strings like "¥23.00".
            if cleaned.str.contains(r"^¥?\s*-?\d[\d,]*(?:\.\d+)?$", regex=True, na=False).mean() > 0.7:
                out[col] = pd.to_numeric(cleaned.str.replace("¥", "", regex=False).str.replace(",", "", regex=False), errors="coerce")
        return out

    def _load_meta(self) -> Dict[str, Dict[str, Any]]:
        if not self.meta_file.exists():
            return {}
        try:
            return json.loads(self.meta_file.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return {}

    def _save_meta(self, data: Dict[str, Dict[str, Any]]) -> None:
        self.meta_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _default_llm_config() -> Dict[str, Any]:
        return {
            "enabled": True,
            "provider": "openai_compatible",
            "api_key": "",
            "base_url": "",
            "model": "gpt-4o-mini",
            "input_cost_per_million": 0.0,
            "output_cost_per_million": 0.0,
            "currency": "USD",
            "max_total_tokens": 500000,
            "warn_ratio": 0.8,
            "updated_at": "",
        }

    @staticmethod
    def _default_llm_usage() -> Dict[str, Any]:
        return {
            "total_prompt_tokens": 0,
            "total_completion_tokens": 0,
            "total_tokens": 0,
            "total_cost": 0.0,
            "events": [],
            "updated_at": "",
        }

    def _load_llm_config(self) -> Dict[str, Any]:
        default = self._default_llm_config()
        if not self.llm_config_file.exists():
            return self._sanitize_llm_config(payload=default, base=default)
        try:
            raw = json.loads(self.llm_config_file.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return default
            return self._sanitize_llm_config(payload=raw, base=default)
        except Exception:  # noqa: BLE001
            return default

    def _save_llm_config(self, config: Dict[str, Any]) -> None:
        self.llm_config_file.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_llm_usage(self) -> Dict[str, Any]:
        default = self._default_llm_usage()
        if not self.llm_usage_file.exists():
            return default
        try:
            raw = json.loads(self.llm_usage_file.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return default
            events = raw.get("events", [])
            if not isinstance(events, list):
                events = []
            return {
                "total_prompt_tokens": int(raw.get("total_prompt_tokens", 0) or 0),
                "total_completion_tokens": int(raw.get("total_completion_tokens", 0) or 0),
                "total_tokens": int(raw.get("total_tokens", 0) or 0),
                "total_cost": float(raw.get("total_cost", 0.0) or 0.0),
                "events": events[-200:],
                "updated_at": str(raw.get("updated_at", "")),
            }
        except Exception:  # noqa: BLE001
            return default

    def _save_llm_usage(self, usage: Dict[str, Any]) -> None:
        self.llm_usage_file.write_text(json.dumps(usage, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _sanitize_llm_config(payload: Dict[str, Any], base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        out = dict(base or DataInterpreterAgent._default_llm_config())
        out["enabled"] = bool(payload.get("enabled", out.get("enabled", True)))
        out["provider"] = str(payload.get("provider", out.get("provider", "openai_compatible"))).strip() or "openai_compatible"
        out["api_key"] = str(payload.get("api_key", out.get("api_key", ""))).strip()
        out["base_url"] = str(payload.get("base_url", out.get("base_url", ""))).strip()
        out["model"] = str(payload.get("model", out.get("model", "gpt-4o-mini"))).strip() or "gpt-4o-mini"
        out["currency"] = str(payload.get("currency", out.get("currency", "USD"))).strip() or "USD"
        out["updated_at"] = str(payload.get("updated_at", out.get("updated_at", "")))

        try:
            out["max_total_tokens"] = max(1000, int(payload.get("max_total_tokens", out.get("max_total_tokens", 500000))))
        except Exception:  # noqa: BLE001
            out["max_total_tokens"] = int(out.get("max_total_tokens", 500000))
        try:
            wr = float(payload.get("warn_ratio", out.get("warn_ratio", 0.8)))
        except Exception:  # noqa: BLE001
            wr = 0.8
        out["warn_ratio"] = min(0.99, max(0.1, wr))

        try:
            out["input_cost_per_million"] = max(0.0, float(payload.get("input_cost_per_million", out.get("input_cost_per_million", 0.0))))
        except Exception:  # noqa: BLE001
            out["input_cost_per_million"] = float(out.get("input_cost_per_million", 0.0))
        try:
            out["output_cost_per_million"] = max(0.0, float(payload.get("output_cost_per_million", out.get("output_cost_per_million", 0.0))))
        except Exception:  # noqa: BLE001
            out["output_cost_per_million"] = float(out.get("output_cost_per_million", 0.0))
        return out

    @staticmethod
    def _quota_status(cfg: Dict[str, Any], usage: Dict[str, Any]) -> Dict[str, Any]:
        total_used = int(usage.get("total_tokens", 0) or 0)
        total_cap = int(cfg.get("max_total_tokens", 0) or 0)
        warn_ratio = float(cfg.get("warn_ratio", 0.8) or 0.8)
        enabled = bool(cfg.get("enabled", True))
        exhausted = bool(total_cap > 0 and total_used >= total_cap)
        ratio = float(total_used / total_cap) if total_cap > 0 else 0.0
        warning = bool(not exhausted and ratio >= warn_ratio)
        remaining = max(0, total_cap - total_used) if total_cap > 0 else 0
        allow_llm = bool(enabled and not exhausted)
        message = ""
        if not enabled:
            message = "LLM 已被禁用"
        elif exhausted:
            message = "LLM token 额度已耗尽，请更新额度或重置统计后继续"
        elif warning:
            message = "LLM token 使用已达到阈值 80% 以上，请关注余额"
        return {
            "enabled": enabled,
            "allow_llm": allow_llm,
            "warning": warning,
            "exhausted": exhausted,
            "used_tokens": total_used,
            "max_total_tokens": total_cap,
            "remaining_tokens": remaining,
            "used_ratio": round(ratio, 6),
            "warn_ratio": warn_ratio,
            "message": message,
        }

    @staticmethod
    def _sum_llm_trace(trace: List[Dict[str, Any]]) -> Dict[str, Any]:
        p = int(sum(int(item.get("prompt_tokens", 0) or 0) for item in trace))
        c = int(sum(int(item.get("completion_tokens", 0) or 0) for item in trace))
        t = int(sum(int(item.get("total_tokens", 0) or 0) for item in trace))
        cost = float(sum(float(item.get("cost", 0.0) or 0.0) for item in trace))
        return {
            "prompt_tokens": p,
            "completion_tokens": c,
            "total_tokens": t if t > 0 else (p + c),
            "cost": float(round(cost, 8)),
            "calls": len(trace),
        }

    def _apply_llm_trace(
        self,
        cfg: Dict[str, Any],
        usage_before: Dict[str, Any],
        trace: List[Dict[str, Any]],
        session_id: str,
        dataset_id: str,
        question: str,
        mode: str,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        usage = dict(usage_before)
        events = usage.get("events", [])
        if not isinstance(events, list):
            events = []
        now = dt.datetime.utcnow().isoformat()
        for item in trace:
            event = {
                "time": str(item.get("time", now)),
                "session_id": session_id,
                "dataset_id": dataset_id,
                "question": question,
                "mode": mode,
                "stage": str(item.get("stage", "")),
                "model": str(item.get("model", "")),
                "prompt_tokens": int(item.get("prompt_tokens", 0) or 0),
                "completion_tokens": int(item.get("completion_tokens", 0) or 0),
                "total_tokens": int(item.get("total_tokens", 0) or 0),
                "cost": float(item.get("cost", 0.0) or 0.0),
            }
            events.append(event)
            usage["total_prompt_tokens"] = int(usage.get("total_prompt_tokens", 0) or 0) + event["prompt_tokens"]
            usage["total_completion_tokens"] = int(usage.get("total_completion_tokens", 0) or 0) + event["completion_tokens"]
            usage["total_tokens"] = int(usage.get("total_tokens", 0) or 0) + max(
                event["total_tokens"], event["prompt_tokens"] + event["completion_tokens"]
            )
            usage["total_cost"] = float(usage.get("total_cost", 0.0) or 0.0) + event["cost"]
        usage["events"] = events[-200:]
        usage["total_cost"] = float(round(float(usage.get("total_cost", 0.0) or 0.0), 8))
        usage["updated_at"] = now
        self._save_llm_usage(usage)
        quota = self._quota_status(cfg=cfg, usage=usage)
        return usage, quota

    @staticmethod
    def _default_tools_hub() -> Dict[str, Any]:
        return {
            "packages": {},
            "tools": {},
            "selected_tool_ids": [],
            "updated_at": "",
        }

    def _load_tools_hub(self) -> Dict[str, Any]:
        default = self._default_tools_hub()
        if not self.tools_hub_file.exists():
            return default
        try:
            raw = json.loads(self.tools_hub_file.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return default
            out = dict(default)
            out["packages"] = raw.get("packages", {}) if isinstance(raw.get("packages"), dict) else {}
            out["tools"] = raw.get("tools", {}) if isinstance(raw.get("tools"), dict) else {}
            selected = raw.get("selected_tool_ids", [])
            out["selected_tool_ids"] = [str(v) for v in selected] if isinstance(selected, list) else []
            out["updated_at"] = str(raw.get("updated_at", ""))
            return out
        except Exception:  # noqa: BLE001
            return default

    def _save_tools_hub(self, data: Dict[str, Any]) -> None:
        self.tools_hub_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _format_tools_hub(hub: Dict[str, Any]) -> Dict[str, Any]:
        packages = list(hub.get("packages", {}).values()) if isinstance(hub.get("packages"), dict) else []
        tools = list(hub.get("tools", {}).values()) if isinstance(hub.get("tools"), dict) else []
        tools.sort(key=lambda x: str(x.get("tool_id", "")))
        packages.sort(key=lambda x: str(x.get("package_id", "")))
        return {
            "packages": packages,
            "tools": tools,
            "selected_tool_ids": [str(v) for v in hub.get("selected_tool_ids", []) if str(v)],
            "updated_at": str(hub.get("updated_at", "")),
        }

    @staticmethod
    def _builtin_tool_packages() -> List[Dict[str, Any]]:
        return [
            {
                "package_id": "finance_core_v1",
                "name": "财务分析核心包",
                "description": "交易对象聚合、收支统计、TopN 排行等基础财务分析工具。",
                "version": "1.0.0",
            },
            {
                "package_id": "ops_logistics_v1",
                "name": "运力与履约分析包",
                "description": "司机绩效、线路时效、运单波动分析等运营类工具。",
                "version": "1.0.0",
            },
            {
                "package_id": "risk_compliance_v1",
                "name": "风控合规包",
                "description": "异常金额检测、交易频次异常、规则命中说明。",
                "version": "1.0.0",
            },
            {
                "package_id": "viz_advanced_v1",
                "name": "可视化增强包",
                "description": "主题图表生成、分布对比图、趋势组合图。",
                "version": "1.0.0",
            },
        ]

    def _selected_tool_context(self, selected_tools: List[str]) -> List[Dict[str, Any]]:
        hub = self._load_tools_hub()
        tools = hub.get("tools", {}) if isinstance(hub.get("tools"), dict) else {}
        chosen = [str(v) for v in selected_tools if str(v)] if selected_tools else []
        if not chosen:
            chosen = [str(v) for v in hub.get("selected_tool_ids", []) if str(v)]
        rows: List[Dict[str, Any]] = []
        for tid in chosen:
            tool = tools.get(tid)
            if not isinstance(tool, dict):
                continue
            if not bool(tool.get("enabled", True)):
                continue
            rows.append(
                {
                    "tool_id": str(tool.get("tool_id", "")),
                    "name": str(tool.get("name", "")),
                    "description": str(tool.get("description", "")),
                    "input_schema": tool.get("input_schema", {}) if isinstance(tool.get("input_schema"), dict) else {},
                    "output_schema": tool.get("output_schema", {}) if isinstance(tool.get("output_schema"), dict) else {},
                }
            )
        return rows

    @staticmethod
    def _sanitize_tool(raw: Dict[str, Any], default_package_id: str) -> Optional[Dict[str, Any]]:
        tool_id = str(raw.get("tool_id", "")).strip() or str(raw.get("id", "")).strip()
        name = str(raw.get("name", "")).strip()
        endpoint = str(raw.get("endpoint", "")).strip()
        if not tool_id or not name or not endpoint:
            return None
        method = str(raw.get("method", "POST")).strip().upper() or "POST"
        if method not in {"POST", "GET"}:
            method = "POST"
        input_schema = raw.get("input_schema", {})
        output_schema = raw.get("output_schema", {})
        if not isinstance(input_schema, dict):
            input_schema = {}
        if not isinstance(output_schema, dict):
            output_schema = {}
        keywords = raw.get("keywords", [])
        if not isinstance(keywords, list):
            keywords = []
        return {
            "tool_id": tool_id,
            "name": name,
            "description": str(raw.get("description", "")),
            "package_id": str(raw.get("package_id", default_package_id) or default_package_id),
            "endpoint": endpoint,
            "method": method,
            "input_schema": input_schema,
            "output_schema": output_schema,
            "keywords": [str(v).strip() for v in keywords if str(v).strip()],
            "enabled": bool(raw.get("enabled", True)),
            "updated_at": dt.datetime.utcnow().isoformat(),
        }

    def _fetch_package_manifest(self, package_id: str, manifest_url: str, source_url: str) -> Dict[str, Any]:
        if manifest_url.strip():
            data = self._http_json_get(manifest_url.strip())
            if not isinstance(data, dict):
                raise ValueError("包清单格式错误")
            return data
        if not source_url.strip() and self.tool_backend_service_url:
            source_url = self.tool_backend_service_url
        if source_url.strip() and package_id.strip():
            url = source_url.rstrip("/") + "/packages/" + package_id.strip()
            data = self._http_json_get(url)
            if not isinstance(data, dict):
                raise ValueError("包清单格式错误")
            return data
        raise ValueError("安装工具包需要 manifest_url 或 source_url + package_id")

    def _http_json_get(self, url: str) -> Dict[str, Any]:
        req = urllib.request.Request(url=url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=12) as rsp:  # noqa: S310
                text = rsp.read().decode("utf-8")
        except urllib.error.URLError as exc:
            raise ValueError(f"访问远程服务失败: {exc}") from exc
        try:
            data = json.loads(text)
        except Exception as exc:  # noqa: BLE001
            raise ValueError("远程服务返回非 JSON") from exc
        if not isinstance(data, dict):
            raise ValueError("远程服务返回结构错误")
        return data

    def _http_json_post(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as rsp:  # noqa: S310
                text = rsp.read().decode("utf-8")
        except urllib.error.URLError as exc:
            raise ValueError(f"访问远程工具失败: {exc}") from exc
        try:
            data = json.loads(text)
        except Exception as exc:  # noqa: BLE001
            raise ValueError("远程工具返回非 JSON") from exc
        if not isinstance(data, dict):
            raise ValueError("远程工具返回结构错误")
        return data

    def _service_health(self, name: str, role: str, url: str, enabled: bool, is_local: bool = False) -> Dict[str, Any]:
        if is_local:
            return {
                "name": name,
                "role": role,
                "url": url,
                "enabled": True,
                "status": "ok",
                "message": "in-process",
            }
        if not enabled:
            return {
                "name": name,
                "role": role,
                "url": url,
                "enabled": False,
                "status": "disabled",
                "message": "未配置服务地址，当前走本地兜底逻辑",
            }
        try:
            health = self._http_json_get(url=url.rstrip("/") + "/health")
            status = str(health.get("status", "unknown")) if isinstance(health, dict) else "unknown"
            return {
                "name": name,
                "role": role,
                "url": url,
                "enabled": True,
                "status": status,
                "message": "",
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "name": name,
                "role": role,
                "url": url,
                "enabled": True,
                "status": "down",
                "message": str(exc),
            }

    @staticmethod
    def _meta_payload(meta: DatasetMeta) -> Dict[str, Any]:
        return {
            "columns": [str(c) for c in meta.columns],
            "dtypes": {str(k): str(v) for k, v in meta.dtypes.items()},
        }

    def _planner_rpc(self, path: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.planner_service_url:
            return None
        url = self.planner_service_url.rstrip("/") + path
        try:
            return self._http_json_post(url=url, payload=payload)
        except Exception:  # noqa: BLE001
            return None

    def _planner_intent_key(self, question: str) -> str:
        rsp = self._planner_rpc("/intent_key", {"question": question})
        if rsp and isinstance(rsp.get("intent_key"), str):
            return str(rsp.get("intent_key"))
        return self.planner.intent_key(question)

    def _planner_generate(self, question: str, meta: DatasetMeta) -> str:
        rsp = self._planner_rpc("/generate", {"question": question, "meta": self._meta_payload(meta)})
        if rsp and isinstance(rsp.get("code"), str) and str(rsp.get("code")).strip():
            return str(rsp.get("code"))
        return self.planner.generate(question=question, meta=meta)

    def _planner_generate_llm_only(self, question: str, meta: DatasetMeta, error: str = "") -> Optional[str]:
        rsp = self._planner_rpc(
            "/generate_llm_only",
            {"question": question, "meta": self._meta_payload(meta), "error": error},
        )
        if rsp and isinstance(rsp.get("code"), str) and str(rsp.get("code")).strip():
            return str(rsp.get("code"))
        return self.planner.generate_llm_only(question=question, meta=meta, error=error)

    def _planner_plan_tool(self, question: str, meta: DatasetMeta) -> Dict[str, Any]:
        rsp = self._planner_rpc("/plan_tool", {"question": question, "meta": self._meta_payload(meta)})
        if rsp and isinstance(rsp.get("plan"), dict):
            return rsp.get("plan", {})
        return self.planner.plan_tool(question=question, meta=meta)

    def _planner_build_tool_code(self, plan: Dict[str, Any], meta: DatasetMeta) -> str:
        rsp = self._planner_rpc("/build_tool_code", {"plan": plan, "meta": self._meta_payload(meta)})
        if rsp and isinstance(rsp.get("code"), str) and str(rsp.get("code")).strip():
            return str(rsp.get("code"))
        return self.planner.build_tool_code(plan=plan, meta=meta)

    def _planner_is_fallback_code(self, code: str) -> bool:
        rsp = self._planner_rpc("/is_fallback_code", {"code": code})
        if rsp and "is_fallback" in rsp:
            return bool(rsp.get("is_fallback"))
        return self.planner.is_fallback_code(code)

    def _planner_generate_auto_tool(self, question: str, meta: DatasetMeta) -> Optional[str]:
        rsp = self._planner_rpc("/generate_auto_tool", {"question": question, "meta": self._meta_payload(meta)})
        if rsp and isinstance(rsp.get("code"), str) and str(rsp.get("code")).strip():
            return str(rsp.get("code"))
        return self.planner.generate_auto_tool(question=question, meta=meta)

    def _planner_repair_code(self, question: str, meta: DatasetMeta, error: str) -> str:
        rsp = self._planner_rpc("/repair_code", {"question": question, "meta": self._meta_payload(meta), "error": error})
        if rsp and isinstance(rsp.get("code"), str) and str(rsp.get("code")).strip():
            return str(rsp.get("code"))
        return self.planner.repair_code(question=question, meta=meta, error=error)

    def _planner_failure_solution(self, question: str, meta: DatasetMeta, error: str) -> str:
        rsp = self._planner_rpc(
            "/failure_solution",
            {"question": question, "meta": self._meta_payload(meta), "error": error},
        )
        if rsp and isinstance(rsp.get("solution"), str):
            return str(rsp.get("solution"))
        return self.planner.failure_solution(question=question, meta=meta, error=error)

    def _planner_fallback_code(self) -> str:
        rsp = self._planner_rpc("/fallback_code", {})
        if rsp and isinstance(rsp.get("code"), str) and str(rsp.get("code")).strip():
            return str(rsp.get("code"))
        return self.planner.fallback_code()

    def _executor_run(self, session_id: str, code: str, df: pd.DataFrame, max_rows: int) -> ExecResult:
        if self.executor_service_url:
            payload = {
                "session_dir": str(self.sessions / session_id),
                "code": code,
                "data": {
                    "columns": [str(c) for c in df.columns],
                    "rows": self._to_json_safe_records(df),
                },
                "max_rows": int(max_rows),
            }
            try:
                rsp = self._http_json_post(self.executor_service_url.rstrip("/") + "/execute", payload=payload)
                return ExecResult(
                    success=bool(rsp.get("success", False)),
                    answer=str(rsp.get("answer", "")),
                    table=rsp.get("table", []) if isinstance(rsp.get("table"), list) else [],
                    plots=rsp.get("plots", []) if isinstance(rsp.get("plots"), list) else [],
                    stdout=str(rsp.get("stdout", "")),
                    error=str(rsp.get("error", "")),
                )
            except Exception:  # noqa: BLE001
                pass
        executor = SandboxExecutor(str(self.sessions / session_id))
        return executor.run(code=code, df=df, max_rows=max_rows)

    def _pick_active_tools(self, selected_tools: List[str], question: str) -> List[Dict[str, Any]]:
        hub = self._load_tools_hub()
        all_tools = hub.get("tools", {}) if isinstance(hub.get("tools"), dict) else {}
        selected = [str(v) for v in selected_tools if str(v)] if selected_tools else []
        if not selected:
            selected = [str(v) for v in hub.get("selected_tool_ids", []) if str(v)]
        if not selected:
            selected = [str(k) for k, v in all_tools.items() if isinstance(v, dict) and bool(v.get("enabled", True))]
        candidates: List[Dict[str, Any]] = []
        qlow = question.lower()
        for tid in selected:
            tool = all_tools.get(tid)
            if not isinstance(tool, dict):
                continue
            if not bool(tool.get("enabled", True)):
                continue
            keywords = tool.get("keywords", [])
            if isinstance(keywords, list) and keywords:
                matched = any(str(k).lower() in qlow for k in keywords if str(k).strip())
                if not matched and selected_tools:
                    matched = True
                if not matched:
                    continue
            candidates.append(tool)
        return candidates

    def _run_remote_tools(
        self,
        question: str,
        dataset_id: str,
        df: pd.DataFrame,
        max_rows: int,
        selected_tools: List[str],
    ) -> Optional[Dict[str, Any]]:
        tools = self._pick_active_tools(selected_tools=selected_tools, question=question)
        if not tools:
            return None
        sample_rows = self._to_json_safe_records(df.head(300))
        for tool in tools:
            payload = {
                "question": question,
                "dataset_id": dataset_id,
                "max_rows": max_rows,
                "data": {
                    "columns": [str(c) for c in df.columns],
                    "rows": sample_rows,
                },
            }
            try:
                if str(tool.get("method", "POST")).upper() == "GET":
                    # GET tools are treated as metadata-only tools.
                    resp = self._http_json_get(str(tool.get("endpoint", "")))
                else:
                    resp = self._http_json_post(str(tool.get("endpoint", "")), payload=payload)
            except Exception:  # noqa: BLE001
                continue
            if not isinstance(resp, dict):
                continue
            ok = bool(resp.get("success", True))
            if not ok:
                continue
            trace = resp.get("llm_trace", [])
            if not isinstance(trace, list):
                trace = []
            if not trace and isinstance(resp.get("usage"), dict):
                usage = resp.get("usage", {})
                trace = [
                    {
                        "time": dt.datetime.utcnow().isoformat(),
                        "stage": "remote_tool",
                        "model": str(usage.get("model", tool.get("name", "remote_tool"))),
                        "prompt_tokens": int(usage.get("prompt_tokens", 0) or 0),
                        "completion_tokens": int(usage.get("completion_tokens", 0) or 0),
                        "total_tokens": int(usage.get("total_tokens", 0) or 0),
                        "cost": float(usage.get("cost", 0.0) or 0.0),
                    }
                ]
            return {
                "tool_id": str(tool.get("tool_id")),
                "answer": str(resp.get("answer", "")),
                "table": resp.get("table", []) if isinstance(resp.get("table"), list) else [],
                "plots": resp.get("plots", []) if isinstance(resp.get("plots"), list) else [],
                "stdout": str(resp.get("stdout", "")),
                "llm_trace": trace,
            }
        return None

    @staticmethod
    def _build_execution_layers(
        question: str,
        mode: str,
        intent_key: str,
        code_source: str,
        retries: int,
        used_fallback: bool,
        status: str,
        remote_tool_id: str,
        llm_trace: List[Dict[str, Any]],
        selected_tools: List[Dict[str, Any]],
        result: ExecResult,
        plot_count: int,
    ) -> List[Dict[str, Any]]:
        layers: List[Dict[str, Any]] = []
        tool_names = [str(t.get("name", "")) for t in selected_tools if str(t.get("name", "")).strip()]
        llm_calls = len(llm_trace)
        llm_tokens = int(sum(int(v.get("total_tokens", 0) or 0) for v in llm_trace))
        llm_cost = float(sum(float(v.get("cost", 0.0) or 0.0) for v in llm_trace))
        output_rows = len(result.table) if isinstance(result.table, list) else 0

        layers.append(
            {
                "id": "question",
                "name": "问题输入层",
                "layer": 0,
                "state": "ok",
                "details": [
                    "question=" + question[:80],
                    "mode=" + mode,
                ],
            }
        )
        layers.append(
            {
                "id": "intent",
                "name": "意图识别层",
                "layer": 1,
                "state": "ok",
                "details": [
                    "intent_key=" + intent_key,
                    "selected_tools=" + str(len(selected_tools)),
                    "selected_names=" + ",".join(tool_names[:4]) if tool_names else "selected_names=无",
                ],
            }
        )
        strategy_state = "warn" if mode == "smart" and code_source.startswith("smart_degraded") else "ok"
        layers.append(
            {
                "id": "strategy",
                "name": "策略规划层",
                "layer": 2,
                "state": strategy_state,
                "details": [
                    "code_source=" + code_source,
                    "remote_tool=" + (remote_tool_id or "none"),
                    "fallback=" + ("yes" if used_fallback else "no"),
                ],
            }
        )
        layers.append(
            {
                "id": "llm",
                "name": "大模型调用层",
                "layer": 3,
                "state": "ok" if llm_calls > 0 else "warn",
                "details": [
                    "calls=" + str(llm_calls),
                    "tokens=" + str(llm_tokens),
                    "cost=" + format(llm_cost, ".6f"),
                ],
            }
        )
        layers.append(
            {
                "id": "tool_exec",
                "name": "工具执行层",
                "layer": 4,
                "state": "ok" if remote_tool_id else "warn",
                "details": [
                    "remote_tool_id=" + (remote_tool_id or "none"),
                    "retries=" + str(retries),
                    "stdout_size=" + str(len(result.stdout or "")),
                ],
            }
        )
        result_state = "ok" if status == "ok" else "warn"
        layers.append(
            {
                "id": "summary",
                "name": "汇总结果层",
                "layer": 5,
                "state": result_state,
                "details": [
                    "status=" + status,
                    "table_rows=" + str(output_rows),
                    "plots=" + str(plot_count + 1),
                ],
            }
        )
        return layers

    @staticmethod
    def _build_layered_plan_graph(execution_layers: List[Dict[str, Any]]) -> Dict[str, Any]:
        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, str]] = []
        ordered = sorted(execution_layers, key=lambda x: int(x.get("layer", 0)))
        for idx, layer in enumerate(ordered):
            lid = str(layer.get("id", "layer_" + str(idx)))
            details = layer.get("details", [])
            detail_text = ""
            if isinstance(details, list) and details:
                detail_text = "\n" + "\n".join(str(v) for v in details[:4])
            nodes.append(
                {
                    "id": lid,
                    "label": str(layer.get("name", lid)) + detail_text,
                    "layer": int(layer.get("layer", idx)),
                    "state": str(layer.get("state", "ok")),
                }
            )
            if idx > 0:
                prev = str(ordered[idx - 1].get("id", "layer_" + str(idx - 1)))
                edges.append({"src": prev, "dst": lid})
        return {"nodes": nodes, "edges": edges}

    def _render_execution_graph(self, session_id: str, graph: Dict[str, Any]) -> str:
        session_dir = self.sessions / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        out = session_dir / "execution_graph.png"
        nodes = graph.get("nodes", []) if isinstance(graph, dict) else []
        edges = graph.get("edges", []) if isinstance(graph, dict) else []
        if not isinstance(nodes, list) or not nodes:
            return ""

        layers: Dict[int, List[Dict[str, Any]]] = {}
        for node in nodes:
            if not isinstance(node, dict):
                continue
            layer = int(node.get("layer", 0) or 0)
            layers.setdefault(layer, []).append(node)
        for _, arr in layers.items():
            arr.sort(key=lambda x: str(x.get("id", "")))

        pos: Dict[str, Tuple[float, float]] = {}
        max_layer = max(layers.keys()) if layers else 0
        for layer, arr in layers.items():
            n = len(arr)
            for idx, node in enumerate(arr):
                y = (n - 1) / 2.0 - idx
                x = float(layer)
                pos[str(node.get("id", ""))] = (x, y)

        plt.figure(figsize=(max(10, max_layer * 1.8 + 2), 6))
        ax = plt.gca()
        color_map = {"ok": "#dcfce7", "warn": "#fef3c7", "error": "#fee2e2"}
        edge_color = "#64748b"

        for edge in edges:
            if not isinstance(edge, dict):
                continue
            src = str(edge.get("src", ""))
            dst = str(edge.get("dst", ""))
            if src not in pos or dst not in pos:
                continue
            x1, y1 = pos[src]
            x2, y2 = pos[dst]
            ax.annotate(
                "",
                xy=(x2 - 0.08, y2),
                xytext=(x1 + 0.08, y1),
                arrowprops={"arrowstyle": "->", "color": edge_color, "lw": 1.2},
            )

        for node in nodes:
            if not isinstance(node, dict):
                continue
            nid = str(node.get("id", ""))
            if nid not in pos:
                continue
            x, y = pos[nid]
            state = str(node.get("state", "ok"))
            face = color_map.get(state, "#e2e8f0")
            label = str(node.get("label", nid))
            ax.text(
                x,
                y,
                label,
                ha="center",
                va="center",
                fontsize=9,
                bbox={"boxstyle": "round,pad=0.4", "fc": face, "ec": "#475569", "lw": 1},
            )

        ax.set_title("本次执行分层图", fontsize=12)
        ax.set_xlim(-0.6, max_layer + 0.8)
        ax.set_ylim(-3.2, 3.2)
        ax.axis("off")
        plt.tight_layout()
        plt.savefig(out)
        plt.close()
        return "/sessions/{}/{}".format(session_id, out.name)

    def _append_record(self, row: Dict[str, Any]) -> None:
        with self.records_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _append_bug(self, row: Dict[str, Any]) -> None:
        with self.bugs_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    @staticmethod
    def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
        if not path.exists():
            return []
        out: List[Dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            if isinstance(row, dict):
                out.append(row)
        return out

    def _load_tools(self) -> Dict[str, Dict[str, Any]]:
        if not self.tools_file.exists():
            return {}
        try:
            raw = json.loads(self.tools_file.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return raw
            return {}
        except Exception:  # noqa: BLE001
            return {}

    def _save_tools(self, data: Dict[str, Dict[str, Any]]) -> None:
        self.tools_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def _save_result_detail(self, session_id: str, payload: Dict[str, Any]) -> None:
        session_dir = self.sessions / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        safe_payload = self._to_json_safe(payload)
        (session_dir / "result.json").write_text(json.dumps(safe_payload, ensure_ascii=False), encoding="utf-8")

    def _upsert_tool(self, intent_key: str, question: str, code: str, source: str, success: bool) -> None:
        if success and not self._code_matches_intent(intent_key=intent_key, code=code):
            return
        tools = self._load_tools()
        old = tools.get(intent_key, {})
        examples = old.get("examples") if isinstance(old.get("examples"), list) else []
        if question not in examples:
            examples.append(question)
        tools[intent_key] = {
            "intent_key": intent_key,
            "code": code,
            "examples": examples[-8:],
            "updated_at": dt.datetime.utcnow().isoformat(),
            "source": source,
            "success": bool(success),
            "success_count": int(old.get("success_count", 0)) + (1 if success else 0),
        }
        self._save_tools(tools)

    def _pick_memory_tool(self, tools_memory: Dict[str, Dict[str, Any]], intent_key: str) -> Optional[Dict[str, Any]]:
        tool = tools_memory.get(intent_key)
        if not tool:
            return None
        if not bool(tool.get("success", False)):
            return None
        code = str(tool.get("code", "")).strip()
        if not code:
            return None
        if not self._code_matches_intent(intent_key=intent_key, code=code):
            return None
        return tool

    @staticmethod
    def _code_matches_intent(intent_key: str, code: str) -> bool:
        c = code.lower()
        if intent_key == "driver_top_frequency":
            return "value_counts" in c
        if intent_key.startswith("counterparty_amount_top"):
            if "_plot" in intent_key and "save_plot" not in c:
                return False
            return "groupby" in c and ".sum(" in c
        if intent_key == "trend_plot":
            return "save_plot" in c and "plt.plot" in c
        if intent_key == "mean_aggregate":
            return ".mean(" in c
        if intent_key == "sum_aggregate":
            return ".sum(" in c
        if intent_key.startswith("q_"):
            # For long-tail intents, accept any non-empty safe snippet.
            return True
        # Conservative default for unknown keys.
        return bool(re.search(r"\bresult_df\b", c))

    @staticmethod
    def _classify_error(execution_error: str, used_fallback: bool, retries: int, code_source: str) -> Dict[str, str]:
        msg = (execution_error or "").strip()
        low = msg.lower()
        status = "ok"
        if msg or used_fallback or retries > 0:
            status = "degraded"
        if not msg and not used_fallback and retries == 0:
            return {
                "status": "ok",
                "error_category": "none",
                "error_message": "",
                "error_hint": "",
            }

        if any(k in low for k in ["keyerror", "not found in axis", "no column", "不存在", "are in the [columns]"]) or (
            msg.startswith("'") and msg.endswith("'")
        ):
            return {
                "status": status,
                "error_category": "field_missing",
                "error_message": msg,
                "error_hint": "问题里引用的字段可能不存在。请核对列名后重试。",
            }

        if any(k in msg for k in ["暂不支持自动解析", "暂不支持", "不支持"]) or code_source == "fallback":
            return {
                "status": status,
                "error_category": "semantic_unsupported",
                "error_message": msg or "当前问题暂不支持自动解析",
                "error_hint": "请把问题拆成更具体的统计步骤，或明确字段名后再试。",
            }

        return {
            "status": status,
            "error_category": "execution_error",
            "error_message": msg or "执行阶段出现异常",
            "error_hint": "执行阶段出现异常。可尝试缩小问题范围或降低 max_rows 后重试。",
        }

    def _suggest_rewritten_question(
        self,
        question: str,
        meta: DatasetMeta,
        error_category: str,
        error_message: str,
    ) -> Dict[str, str]:
        if error_category == "field_missing":
            missing = self._extract_missing_fields(error_message)
            rewritten = question
            mapping: List[str] = []
            for item in missing:
                matched = self._best_match_column(item, meta.columns)
                if matched and matched != item and item in rewritten:
                    rewritten = rewritten.replace(item, matched)
                    mapping.append(f"{item}->{matched}")
            if rewritten != question:
                return {
                    "suggested_question": rewritten,
                    "suggestion_reason": "检测到字段缺失，已按最相近字段改写：" + ", ".join(mapping),
                }
            template = self._build_supported_question(meta, question)
            if template and template != question:
                return {
                    "suggested_question": template,
                    "suggestion_reason": "未能定位到原字段，已改写为当前数据可执行的问题模板。",
                }
            return {"suggested_question": "", "suggestion_reason": ""}

        if error_category == "semantic_unsupported":
            template = self._build_supported_question(meta, question)
            if template and template != question:
                return {
                    "suggested_question": template,
                    "suggestion_reason": "当前问法暂不支持，已改写为可执行问句。",
                }
        return {"suggested_question": "", "suggestion_reason": ""}

    def _rewrite_question_from_error(self, question: str, meta: DatasetMeta, error_message: str) -> str:
        missing = self._extract_missing_fields(error_message)
        rewritten = question
        for item in missing:
            matched = self._best_match_column(item, meta.columns)
            if matched and matched != item and item in rewritten:
                rewritten = rewritten.replace(item, matched)
        return rewritten

    @staticmethod
    def _extract_missing_fields(error_message: str) -> List[str]:
        msg = error_message or ""
        values: List[str] = []
        # KeyError: 'xxx'
        values.extend(re.findall(r"'([^']+)'", msg))
        # Index(['a', 'b'], dtype='object')
        found = re.search(r"Index\(\[(.*?)\]", msg)
        if found:
            parts = [p.strip().strip("'").strip('"') for p in found.group(1).split(",")]
            values.extend([p for p in parts if p])
        ignored = {"columns", "index", "object"}
        out: List[str] = []
        for v in values:
            if v.lower() in ignored:
                continue
            if v not in out:
                out.append(v)
        return out

    @staticmethod
    def _norm_text(value: str) -> str:
        return re.sub(r"[^\w\u4e00-\u9fff]+", "", str(value).lower())

    def _best_match_column(self, text: str, columns: List[str]) -> str:
        if not columns:
            return ""
        norm = self._norm_text(text)
        if not norm:
            return ""
        candidates = {self._norm_text(c): c for c in columns}
        if norm in candidates:
            return candidates[norm]
        for nc, orig in candidates.items():
            if norm in nc or nc in norm:
                return orig
        matched = difflib.get_close_matches(norm, list(candidates.keys()), n=1, cutoff=0.45)
        return candidates[matched[0]] if matched else ""

    def _build_supported_question(self, meta: DatasetMeta, question: str) -> str:
        cols = meta.columns
        group_col = self._best_match_column("交易对方", cols) or self._best_match_column("交易对象", cols) or self._best_match_column("司机", cols)
        amount_col = (
            self._best_match_column("金额", cols)
            or self._best_match_column("amount", cols)
            or self._best_match_column("weight", cols)
        )
        date_col = self._best_match_column("交易时间", cols) or self._best_match_column("日期", cols) or self._best_match_column("时间", cols)
        if any(k in question for k in ["趋势", "折线", "line", "曲线"]) and date_col and amount_col:
            return f"请画趋势图，按{date_col}看{amount_col}变化"
        if group_col and amount_col:
            return f"按{group_col}汇总后，{amount_col}最多的是哪个？"
        if amount_col:
            return f"{amount_col}字段的最大值是多少？"
        if cols:
            return f"{cols[0]}字段有多少种不同取值？"
        return ""

    @staticmethod
    def _to_json_safe_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
        safe = df.copy()
        safe = safe.replace([float("inf"), float("-inf")], float("nan"))
        safe = safe.astype(object).where(pd.notna(safe), None)
        return safe.to_dict(orient="records")

    @staticmethod
    def _to_json_safe(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (str, int, bool)):
            return value
        if isinstance(value, float):
            if pd.isna(value):
                return None
            return value
        if isinstance(value, dict):
            return {str(k): DataInterpreterAgent._to_json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [DataInterpreterAgent._to_json_safe(v) for v in value]
        if hasattr(value, "tolist") and callable(getattr(value, "tolist")):
            try:
                return DataInterpreterAgent._to_json_safe(value.tolist())
            except Exception:  # noqa: BLE001
                pass
        if hasattr(value, "isoformat") and callable(getattr(value, "isoformat")):
            try:
                return str(value.isoformat())
            except Exception:  # noqa: BLE001
                pass
        if pd.isna(value):
            return None
        return str(value)

    @staticmethod
    def _split_questions(question: str) -> List[str]:
        raw = (question or "").strip()
        if not raw:
            return [""]
        parts = re.split(r"(?:[；;\n]+|[。？！?!]+|\s*(?:并且|同时|而且|另外|然后|此外)\s*)", raw)
        out = [p.strip(" ，,。；;") for p in parts if p and p.strip(" ，,。；;")]
        return out or [raw]
