from __future__ import annotations

import datetime as dt
import base64
import difflib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request
import zipfile
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
    UCI_LIST_API = "https://archive.ics.uci.edu/api/datasets/list"
    UCI_DETAIL_API = "https://archive.ics.uci.edu/api/dataset"
    KAGGLE_LIST_API = "https://www.kaggle.com/api/v1/datasets/list"

    def __init__(self, storage_dir: str, sessions_dir: str) -> None:
        self.storage = Path(storage_dir)
        self.sessions = Path(sessions_dir)
        self.base_dir = self.storage.parent
        self.meta_file = self.storage / ".meta.json"
        self.records_file = self.sessions / "ask_records.jsonl"
        self.tools_file = self.sessions / "tools_memory.json"
        self.bugs_file = self.sessions / "bug_records.jsonl"
        self.test_runs_file = self.sessions / "test_runs.jsonl"
        self.llm_config_file = self.sessions / "llm_config.json"
        self.llm_usage_file = self.sessions / "llm_usage.json"
        self.kaggle_config_file = self.sessions / "kaggle_config.json"
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
            "source": "upload",
            "module": "private",
            "source_id": "",
            "source_name": "",
            "source_description": "",
            "source_repo_url": "",
            "source_data_url": "",
            "category": "",
            "biz_description": "",
            "analysis_notes": "",
            "tags": [],
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

    def list_datasets(self, module: str = "", category: str = "", keyword: str = "") -> List[Dict[str, Any]]:
        meta = self._load_meta()
        items: List[Dict[str, Any]] = []
        mod = str(module or "").strip().lower()
        cat = str(category or "").strip().lower()
        kw = str(keyword or "").strip().lower()
        for p in sorted(self.storage.glob("*")):
            if not p.is_file():
                continue
            if p.suffix.lower() not in {".csv", ".xlsx"}:
                continue
            stat = p.stat()
            m = meta.get(p.stem, {})
            source = str(m.get("source", "upload") or "upload")
            module_value = str(m.get("module", "") or "").strip().lower()
            module_norm = module_value if module_value in {"public", "private"} else ("public" if source == "uci" else "private")
            row = {
                "dataset_id": p.stem,
                "filename": p.name,
                "original_filename": m.get("original_filename", p.name),
                "alias": str(m.get("alias", "")),
                "source": source,
                "module": module_norm,
                "source_id": str(m.get("source_id", "")),
                "source_name": str(m.get("source_name", "")),
                "source_description": str(m.get("source_description", "")),
                "source_repo_url": str(m.get("source_repo_url", "")),
                "source_data_url": str(m.get("source_data_url", "")),
                "category": str(m.get("category", "")),
                "biz_description": str(m.get("biz_description", "")),
                "analysis_notes": str(m.get("analysis_notes", "")),
                "tags": m.get("tags", []) if isinstance(m.get("tags", []), list) else [],
                "ext": p.suffix.lower(),
                "size_bytes": int(stat.st_size),
                "updated_at": dt.datetime.fromtimestamp(stat.st_mtime).isoformat(),
            }
            if mod and str(row.get("module", "")).lower() != mod:
                continue
            if cat and str(row.get("category", "")).strip().lower() != cat:
                continue
            if kw:
                text = "\n".join(
                    [
                        str(row.get("alias", "")),
                        str(row.get("original_filename", "")),
                        str(row.get("filename", "")),
                        str(row.get("dataset_id", "")),
                        str(row.get("source_name", "")),
                        str(row.get("source_description", "")),
                        str(row.get("category", "")),
                        str(row.get("biz_description", "")),
                        str(row.get("analysis_notes", "")),
                        ",".join([str(t) for t in row.get("tags", [])]),
                    ]
                ).lower()
                if kw not in text:
                    continue
            items.append(
                row
            )
        items.sort(key=lambda x: x["updated_at"], reverse=True)
        return items

    def fetch_uci_datasets(self, keyword: str = "", page: int = 1, page_size: int = 20) -> Dict[str, Any]:
        safe_page = max(1, int(page or 1))
        safe_page_size = max(1, min(int(page_size or 20), 100))
        query = str(keyword or "").strip()
        url = self.UCI_LIST_API
        if query:
            url = f"{url}?search={urllib.parse.quote(query)}"

        payload = self._http_json(url)
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            rows = []

        local_uci = self._local_uci_dataset_map()
        normalized: List[Dict[str, Any]] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            uci_id_raw = r.get("ID", r.get("id", ""))
            uci_id = str(uci_id_raw).strip()
            if not uci_id:
                continue
            existed = local_uci.get(uci_id, "")
            normalized.append(
                {
                    "uci_id": uci_id,
                    "name": str(r.get("Name", r.get("name", ""))),
                    "description": str(r.get("Abstract", r.get("abstract", ""))),
                    "task": str(r.get("Task", r.get("task", ""))),
                    "types": str(r.get("Types", r.get("types", ""))),
                    "num_instances": r.get("Instances", r.get("num_instances", "")),
                    "num_features": r.get("Features", r.get("num_features", "")),
                    "year": r.get("Year", r.get("year", "")),
                    "downloaded": bool(existed),
                    "dataset_id": existed,
                }
            )

        total = len(normalized)
        total_pages = (total + safe_page_size - 1) // safe_page_size if total else 0
        if total_pages > 0:
            safe_page = min(safe_page, total_pages)
        start = (safe_page - 1) * safe_page_size
        end = start + safe_page_size
        return {
            "data": normalized[start:end],
            "total": total,
            "page": safe_page,
            "page_size": safe_page_size,
            "total_pages": total_pages,
        }

    def fetch_kaggle_datasets(self, topic: str = "businessDataset", keyword: str = "", page: int = 1, page_size: int = 20) -> Dict[str, Any]:
        safe_page = max(1, int(page or 1))
        safe_page_size = max(1, min(int(page_size or 20), 100))
        query = str(keyword or "").strip()
        topic_norm = str(topic or "businessDataset").strip() or "businessDataset"
        params = urllib.parse.urlencode(
            {
                "page": safe_page,
                "pageSize": safe_page_size,
                "search": query,
                "topic": topic_norm,
            }
        )
        url = f"{self.KAGGLE_LIST_API}?{params}"
        cfg = self._load_kaggle_config()
        headers: Dict[str, str] = {"User-Agent": "DataInterpreterAgent/1.0"}
        auth = self._kaggle_auth_header(cfg)
        if auth:
            headers["Authorization"] = auth
        payload = self._http_json(url, headers=headers)
        rows = payload if isinstance(payload, list) else payload.get("datasets", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            rows = []

        local_kaggle = self._local_source_dataset_map("kaggle")
        normalized: List[Dict[str, Any]] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            ref = str(r.get("ref", "")).strip()
            title = str(r.get("title", "")).strip() or ref
            if not ref:
                continue
            existed = local_kaggle.get(ref, "")
            normalized.append(
                {
                    "kaggle_ref": ref,
                    "name": title,
                    "description": str(r.get("subtitle", "")).strip(),
                    "task": str(r.get("usabilityRating", "")),
                    "types": "kaggle",
                    "num_instances": "",
                    "num_features": "",
                    "year": str(r.get("lastUpdated", ""))[:4],
                    "dataset_page_url": f"https://www.kaggle.com/datasets/{ref}",
                    "downloaded": bool(existed),
                    "dataset_id": existed,
                }
            )

        # Kaggle API may not always return total count; use page-size heuristic.
        total = len(normalized) if safe_page == 1 and len(normalized) < safe_page_size else (safe_page * safe_page_size + (1 if len(normalized) == safe_page_size else 0))
        total_pages = safe_page if len(normalized) < safe_page_size else (safe_page + 1)
        return {
            "data": normalized,
            "total": total,
            "page": safe_page,
            "page_size": safe_page_size,
            "total_pages": total_pages,
            "topic": topic_norm,
        }

    @staticmethod
    def list_public_sources() -> List[Dict[str, Any]]:
        return [
            {
                "source_id": "uci",
                "name": "UCI Machine Learning Repository",
                "description": "公开机器学习数据集仓库，可按关键字检索并导入。",
                "enabled": True,
                "homepage": "https://archive.ics.uci.edu/datasets",
            },
            {
                "source_id": "kaggle",
                "name": "Kaggle Datasets (businessDataset)",
                "description": "Kaggle 商业主题公开数据集列表。",
                "enabled": True,
                "homepage": "https://www.kaggle.com/datasets?topic=businessDataset",
                "requires_credentials": True,
            }
        ]

    def import_uci_dataset(self, uci_id: int) -> Dict[str, Any]:
        safe_uci_id = int(uci_id or 0)
        if safe_uci_id <= 0:
            raise ValueError("uci_id 必须为正整数")

        existed_dataset_id = self._find_existing_uci_dataset(str(safe_uci_id))
        if existed_dataset_id:
            return {"dataset_id": existed_dataset_id, "uci_id": str(safe_uci_id), "downloaded": False, "skipped": True}

        detail_url = f"{self.UCI_DETAIL_API}?id={safe_uci_id}"
        payload = self._http_json(detail_url)
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        if not isinstance(data, dict) or not data:
            raise ValueError("UCI 数据集详情获取失败")

        name = str(data.get("name", data.get("Name", f"uci_{safe_uci_id}"))).strip() or f"uci_{safe_uci_id}"
        description = str(data.get("abstract", data.get("Abstract", ""))).strip()
        data_url = str(data.get("data_url", data.get("Data URL", ""))).strip()
        repo_url = str(data.get("repository_url", data.get("Repository URL", ""))).strip()
        if not data_url:
            raise ValueError("该数据集没有可直接下载的数据文件")

        filename = self._build_uci_filename(name=name, data_url=data_url, uci_id=safe_uci_id)
        content = self._http_bytes(data_url)
        info = self.save_dataset(filename=filename, content=content)

        meta = self._load_meta()
        row = meta.get(info.dataset_id, {})
        row["source"] = "uci"
        row["module"] = "public"
        row["source_id"] = str(safe_uci_id)
        row["source_name"] = name
        row["source_description"] = description
        row["source_repo_url"] = repo_url
        row["source_data_url"] = data_url
        row["category"] = str(row.get("category", "") or "")
        row["biz_description"] = str(row.get("biz_description", "") or "")
        row["analysis_notes"] = str(row.get("analysis_notes", "") or "")
        row["tags"] = row.get("tags", []) if isinstance(row.get("tags", []), list) else []
        row["updated_at"] = dt.datetime.utcnow().isoformat()
        meta[info.dataset_id] = row
        self._save_meta(meta)

        return {
            "dataset_id": info.dataset_id,
            "uci_id": str(safe_uci_id),
            "downloaded": True,
            "skipped": False,
            "rows": info.rows,
            "cols": info.cols,
            "columns": info.columns,
            "source_name": name,
            "source_description": description,
            "source_repo_url": repo_url,
            "source_data_url": data_url,
        }

    def import_kaggle_dataset(self, kaggle_ref: str) -> Dict[str, Any]:
        ref = str(kaggle_ref or "").strip()
        if not ref or "/" not in ref:
            raise ValueError("kaggle_ref 格式应为 owner/dataset-slug")
        existed_dataset_id = self._find_existing_source_dataset(source="kaggle", source_id=ref)
        if existed_dataset_id:
            return {"dataset_id": existed_dataset_id, "kaggle_ref": ref, "downloaded": False, "skipped": True}

        cfg = self._load_kaggle_config()
        username = str(cfg.get("username", "")).strip()
        api_key = str(cfg.get("api_key", "")).strip()
        if not username or not api_key:
            raise ValueError("请先在管理页配置 Kaggle 用户名和 API Token")

        owner, slug = ref.split("/", 1)
        api_url = f"https://www.kaggle.com/api/v1/datasets/download/{urllib.parse.quote(owner)}/{urllib.parse.quote(slug)}"
        headers = {
            "User-Agent": "DataInterpreterAgent/1.0",
            "Authorization": self._kaggle_auth_header(cfg),
        }
        archive = self._http_bytes(api_url, headers=headers, timeout=120)
        if not archive:
            raise ValueError("Kaggle 下载内容为空")

        extracted = self._extract_first_tabular_file(archive=archive, default_slug=slug)
        if extracted is None:
            raise ValueError("Kaggle 数据集未找到可导入的 CSV/XLSX 文件")
        filename, content = extracted
        info = self.save_dataset(filename=filename, content=content)

        meta = self._load_meta()
        row = meta.get(info.dataset_id, {})
        row["source"] = "kaggle"
        row["module"] = "public"
        row["source_id"] = ref
        row["source_name"] = slug
        row["source_description"] = f"Kaggle dataset: {ref}"
        row["source_repo_url"] = f"https://www.kaggle.com/datasets/{ref}"
        row["source_data_url"] = api_url
        row["updated_at"] = dt.datetime.utcnow().isoformat()
        meta[info.dataset_id] = row
        self._save_meta(meta)
        return {
            "dataset_id": info.dataset_id,
            "kaggle_ref": ref,
            "downloaded": True,
            "skipped": False,
            "rows": info.rows,
            "cols": info.cols,
            "columns": info.columns,
            "source_repo_url": row["source_repo_url"],
        }

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

    def update_dataset_metadata(self, dataset_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        real_id = self._resolve_dataset_id(dataset_id)
        if not real_id:
            raise FileNotFoundError("dataset_id 不存在")
        if self._find_dataset(real_id) is None:
            raise FileNotFoundError("dataset 文件不存在")
        raw = payload if isinstance(payload, dict) else {}

        alias = str(raw.get("alias", "")).strip()
        category = str(raw.get("category", "")).strip()
        biz_description = str(raw.get("biz_description", "")).strip()
        analysis_notes = str(raw.get("analysis_notes", "")).strip()
        tags_raw = raw.get("tags", [])
        tags: List[str] = []
        if isinstance(tags_raw, list):
            tags = [str(v).strip() for v in tags_raw if str(v).strip()]
        elif isinstance(tags_raw, str):
            tags = [s.strip() for s in tags_raw.split(",") if s.strip()]
        tags = tags[:20]

        if alias and not re.match(r"^[A-Za-z0-9_\-\u4e00-\u9fff]{1,64}$", alias):
            raise ValueError("别名仅支持中英文、数字、下划线、中划线，长度 1-64")

        meta = self._load_meta()
        for did, info in meta.items():
            if did == real_id:
                continue
            if alias and str(info.get("alias", "")).strip() == alias:
                raise ValueError("别名已被其他数据集占用")

        row = meta.get(real_id, {})
        row["alias"] = alias
        row["category"] = category
        row["biz_description"] = biz_description
        row["analysis_notes"] = analysis_notes
        row["tags"] = tags
        row["updated_at"] = dt.datetime.utcnow().isoformat()
        meta[real_id] = row
        self._save_meta(meta)
        return {
            "dataset_id": real_id,
            "alias": alias,
            "category": category,
            "biz_description": biz_description,
            "analysis_notes": analysis_notes,
            "tags": tags,
        }

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

    def get_dataset_summary(self, dataset_id: str) -> Dict[str, Any]:
        real_id = self._resolve_dataset_id(dataset_id) or dataset_id
        path = self._find_dataset(dataset_id)
        if path is None:
            raise FileNotFoundError("dataset_id 不存在")
        df = self._load_df_cached(path)
        meta = self._load_meta().get(real_id, {})
        columns = [str(c) for c in df.columns]
        dtypes = {str(c): str(t) for c, t in df.dtypes.items()}
        numeric_cols = [c for c, t in dtypes.items() if any(k in t.lower() for k in ["int", "float", "double"])]
        date_cols = [c for c in columns if re.search(r"(date|time|日期|时间)", c, re.IGNORECASE)]
        category_cols = [c for c in columns if c not in numeric_cols][:8]

        analyses: List[str] = []
        if numeric_cols:
            analyses.append("数值统计分析（均值/中位数/最大最小/分布）")
        if category_cols and numeric_cols:
            analyses.append("分类对比分析（按维度聚合求和/均值/TopN）")
        if date_cols and numeric_cols:
            analyses.append("时间趋势分析（按日/周/月变化趋势）")
        if len(numeric_cols) >= 2:
            analyses.append("相关性分析（字段相关关系）")
        analyses.append("数据质量检查（缺失值、异常值、重复记录）")
        analyses = analyses[:6]

        summary = {
            "dataset_id": real_id,
            "source": str(meta.get("source", "upload") or "upload"),
            "module": str(meta.get("module", "") or "").strip().lower()
            or ("public" if str(meta.get("source", "")).strip().lower() in {"uci", "kaggle"} else "private"),
            "source_name": str(meta.get("source_name", "")),
            "alias": str(meta.get("alias", "")),
            "category": str(meta.get("category", "")),
            "biz_description": str(meta.get("biz_description", "")),
            "analysis_notes": str(meta.get("analysis_notes", "")),
            "tags": meta.get("tags", []) if isinstance(meta.get("tags", []), list) else [],
            "rows": int(df.shape[0]),
            "cols": int(df.shape[1]),
            "columns": columns,
            "dtypes": dtypes,
            "numeric_columns": numeric_cols,
            "date_columns": date_cols,
            "recommended_analyses": analyses,
        }
        return summary

    def auto_sales_insights(
        self,
        dataset_id: str,
        max_rows: int = 10,
        requirement: str = "",
        topic: str = "sales",
    ) -> Dict[str, Any]:
        path = self._find_dataset(dataset_id)
        if path is None:
            raise FileNotFoundError("dataset_id 不存在")
        df = self._load_df_cached(path)
        safe_rows = max(3, min(int(max_rows or 10), 50))
        topic_norm = str(topic or "sales").strip().lower() or "sales"
        session_id = uuid.uuid4().hex
        session_dir = self.sessions / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        requirement_text = str(requirement or "").strip()
        columns = [str(c) for c in df.columns]

        amount_col = (
            self._best_match_column("销售金额", columns)
            or self._best_match_column("金额", columns)
            or self._best_match_column("成交金额", columns)
            or self._best_match_column("revenue", columns)
            or self._best_match_column("sales", columns)
            or self._best_match_column("gmv", columns)
            or self._best_match_column("amount", columns)
        )
        customer_col = (
            self._best_match_column("客户", columns)
            or self._best_match_column("客户名称", columns)
            or self._best_match_column("交易对方", columns)
            or self._best_match_column("交易对象", columns)
            or self._best_match_column("customer", columns)
            or self._best_match_column("counterparty", columns)
        )
        date_col = (
            self._best_match_column("交易时间", columns)
            or self._best_match_column("订单日期", columns)
            or self._best_match_column("日期", columns)
            or self._best_match_column("date", columns)
            or self._best_match_column("created_at", columns)
        )

        available = {
            "amount_col": amount_col,
            "customer_col": customer_col,
            "date_col": date_col,
            "columns": columns,
        }

        if not amount_col:
            summary = "当前数据缺少可识别的销售金额字段，已输出可分析范围说明。"
            insights = [
                {
                    "finding": "销售金额字段缺失，无法计算营收指标",
                    "evidence": f"现有字段: {', '.join(columns[:12])}" if columns else "字段为空",
                    "suggestion": "请补充金额/销售额字段后重试自动分析。",
                    "priority": "high",
                    "impact_estimation": "定性：补齐字段后可输出营收规模、客户贡献和趋势建议。",
                },
                {
                    "finding": "仍可进行基础数据质量检查",
                    "evidence": f"当前数据 {int(df.shape[0])} 行，{int(df.shape[1])} 列。",
                    "suggestion": "先核对关键业务字段命名并统一口径（金额、时间、客户）。",
                    "priority": "medium",
                    "impact_estimation": "定性：可降低后续分析误判风险。",
                },
                {
                    "finding": "建议建立销售数据最小字段规范",
                    "evidence": "最小推荐字段：金额、日期、客户。",
                    "suggestion": "在导入流程增加字段校验提示，缺失即提示用户补齐。",
                    "priority": "medium",
                    "impact_estimation": "定性：提升自动分析成功率与建议可信度。",
                },
            ]
            return {
                "session_id": session_id,
                "dataset_id": dataset_id,
                "topic": topic_norm,
                "requirement": requirement_text,
                "summary": summary,
                "insights": insights,
                "tables": [],
                "plots": [],
                "confidence": "low",
                "analysis_scope": "limited",
                "available_fields": available,
            }

        amount = pd.to_numeric(df[amount_col], errors="coerce").fillna(0.0)
        total_sales = float(amount.sum())
        avg_sales = float(amount.mean()) if len(amount) else 0.0
        p90_sales = float(amount.quantile(0.9)) if len(amount) else 0.0
        order_cnt = int(len(df))
        high_value_threshold = p90_sales if p90_sales > 0 else avg_sales
        high_value_cnt = int((amount >= high_value_threshold).sum()) if high_value_threshold > 0 else 0

        insights: List[Dict[str, Any]] = []
        tables: List[Dict[str, Any]] = []
        plots: List[str] = []
        customer_sales = None
        monthly = None

        insights.append(
            {
                "finding": "销售规模与客单价已形成基础盘点",
                "evidence": f"总销售额={total_sales:.2f}，订单数={order_cnt}，平均每单={avg_sales:.2f}。",
                "suggestion": "将平均客单价作为周度核心指标，持续跟踪波动并拆解来源。",
                "priority": "high",
                "impact_estimation": "量化：客单价每提升 5%，理论总销售额可提升约 5%。",
            }
        )

        if customer_col:
            customer_sales = (
                pd.DataFrame({"customer": df[customer_col].astype(str), "amount": amount})
                .groupby("customer")["amount"]
                .sum()
                .sort_values(ascending=False)
            )
            top_rows = customer_sales.head(safe_rows).reset_index()
            top_rows.columns = [customer_col, "sales_sum"]
            table_rows = [
                {
                    customer_col: str(r[customer_col]),
                    "sales_sum": float(r["sales_sum"]),
                }
                for _, r in top_rows.iterrows()
            ]
            tables.append({"name": "top_customers", "rows": table_rows})
            top1_ratio = float(customer_sales.iloc[0] / total_sales) if len(customer_sales) and total_sales > 0 else 0.0
            insights.append(
                {
                    "finding": "客户贡献集中度可识别",
                    "evidence": f"Top1 客户销售占比约 {top1_ratio * 100:.2f}%，客户数={int(customer_sales.shape[0])}。",
                    "suggestion": "对 Top 客户建立分层运营策略，同时扩展腰部客户以降低集中风险。",
                    "priority": "high" if top1_ratio >= 0.3 else "medium",
                    "impact_estimation": (
                        f"量化：若腰部客户转化提升 10%，预期可新增约 {total_sales * 0.1:.2f} 销售额。"
                        if total_sales > 0
                        else "定性：可优化收入结构稳定性。"
                    ),
                }
            )
        else:
            insights.append(
                {
                    "finding": "缺少客户维度字段，无法输出客户结构分析",
                    "evidence": f"识别到金额字段 `{amount_col}`，但未识别客户字段。",
                    "suggestion": "建议补充客户字段后再进行客户贡献和分层建议。",
                    "priority": "medium",
                    "impact_estimation": "定性：补齐客户维度后可输出更可执行的运营建议。",
                }
            )

        if date_col:
            dt_series = pd.to_datetime(df[date_col], errors="coerce")
            valid = dt_series.notna()
            if bool(valid.any()):
                trend_df = pd.DataFrame(
                    {
                        "period": dt_series[valid].dt.to_period("M").astype(str),
                        "amount": amount[valid],
                    }
                )
                monthly = trend_df.groupby("period")["amount"].sum().sort_index()
                if len(monthly) >= 2:
                    growth = float((monthly.iloc[-1] - monthly.iloc[-2]) / monthly.iloc[-2]) if monthly.iloc[-2] != 0 else 0.0
                    trend_rows = [{"period": str(k), "sales_sum": float(v)} for k, v in monthly.tail(safe_rows).items()]
                    tables.append({"name": "monthly_trend", "rows": trend_rows})
                    insights.append(
                        {
                            "finding": "销售趋势可追踪到月度变化",
                            "evidence": f"最近两期环比变化约 {growth * 100:.2f}%，最近周期={monthly.index[-1]}。",
                            "suggestion": "对下降周期做原因排查（渠道、促销、客群），对增长周期复用成功策略。",
                            "priority": "high" if growth < -0.05 else "medium",
                            "impact_estimation": (
                                f"量化：若恢复至上期水平，预计可回补约 {max(monthly.iloc[-2] - monthly.iloc[-1], 0):.2f} 销售额。"
                                if growth < 0
                                else "定性：保持当前增长节奏可持续提升销售规模。"
                            ),
                        }
                    )

        if len(insights) < 3:
            insights.append(
                {
                    "finding": "高价值订单识别可用于资源倾斜",
                    "evidence": f"P90 阈值约 {high_value_threshold:.2f}，高价值订单数={high_value_cnt}。",
                    "suggestion": "对高价值订单来源进行专项运营，提高复购与转介绍转化。",
                    "priority": "medium",
                    "impact_estimation": "定性：聚焦高价值订单可提升投放效率。",
                }
            )

        if customer_sales is not None and len(customer_sales) > 0:
            top_for_plot = customer_sales.head(min(8, len(customer_sales)))
            p = self._save_insights_plot(
                session_dir=session_dir,
                filename="top_customers.png",
                title="Top Customers by Sales",
                x_labels=[str(x) for x in top_for_plot.index.tolist()],
                y_values=[float(v) for v in top_for_plot.values.tolist()],
                chart_type="bar",
            )
            if p:
                plots.append(f"/sessions/{session_id}/{p}")
        if monthly is not None and len(monthly) > 0:
            trend_for_plot = monthly.tail(min(12, len(monthly)))
            p = self._save_insights_plot(
                session_dir=session_dir,
                filename="monthly_trend.png",
                title="Monthly Sales Trend",
                x_labels=[str(x) for x in trend_for_plot.index.tolist()],
                y_values=[float(v) for v in trend_for_plot.values.tolist()],
                chart_type="line",
            )
            if p:
                plots.append(f"/sessions/{session_id}/{p}")

        summary = (
            f"已完成{topic_norm}主题自动分析：识别到金额字段 `{amount_col}`，"
            f"总销售额 {total_sales:.2f}，输出 {len(insights)} 条建议。"
        )
        if requirement_text:
            summary += f" 已结合你的需求“{requirement_text}”生成建议。"
        confidence = "high" if customer_col and date_col else "medium"
        return {
            "session_id": session_id,
            "dataset_id": dataset_id,
            "topic": topic_norm,
            "requirement": requirement_text,
            "summary": summary,
            "insights": insights[: max(3, safe_rows)],
            "tables": tables,
            "plots": plots,
            "confidence": confidence,
            "analysis_scope": topic_norm,
            "available_fields": available,
        }

    def export_insights_report(self, report: Dict[str, Any], export_format: str = "markdown") -> Dict[str, Any]:
        fmt = str(export_format or "markdown").strip().lower()
        if fmt not in {"markdown", "pdf"}:
            raise ValueError("仅支持 markdown 或 pdf")
        session_id = uuid.uuid4().hex
        out_dir = self.sessions / session_id
        out_dir.mkdir(parents=True, exist_ok=True)
        plot_assets = self._collect_export_plot_assets(report=report, out_dir=out_dir)
        md_text = self._render_insights_markdown(
            report=report,
            plot_assets=plot_assets,
            embed_images=(fmt == "markdown"),
        )

        if fmt == "markdown":
            filename = "business_insights.md"
            out_path = out_dir / filename
            out_path.write_text(md_text, encoding="utf-8")
            return {
                "session_id": session_id,
                "format": "markdown",
                "filename": filename,
                "download_url": f"/sessions/{session_id}/{filename}",
            }

        filename = "business_insights.pdf"
        out_path = out_dir / filename
        self._render_insights_pdf(markdown_text=md_text, out_path=out_path, plot_assets=plot_assets)
        return {
            "session_id": session_id,
            "format": "pdf",
            "filename": filename,
            "download_url": f"/sessions/{session_id}/{filename}",
        }

    def _collect_export_plot_assets(self, report: Dict[str, Any], out_dir: Path) -> List[Dict[str, str]]:
        plots = report.get("plots", [])
        if not isinstance(plots, list):
            return []
        assets: List[Dict[str, str]] = []
        for idx, p in enumerate(plots, 1):
            url = str(p or "").strip()
            if not url:
                continue
            src = self._resolve_plot_url_to_local_path(url)
            if src is None or not src.exists() or not src.is_file():
                continue
            ext = src.suffix.lower()
            if ext not in {".png", ".jpg", ".jpeg", ".webp"}:
                continue
            filename = f"plot_{idx}{ext}"
            dst = out_dir / filename
            try:
                shutil.copy2(src, dst)
            except Exception:  # noqa: BLE001
                continue
            data_uri = self._image_file_to_data_uri(dst)
            assets.append(
                {
                    "source_url": url,
                    "filename": filename,
                    "path": str(dst),
                    "data_uri": data_uri,
                }
            )
        return assets

    def _resolve_plot_url_to_local_path(self, url: str) -> Optional[Path]:
        raw = str(url or "").strip()
        if not raw:
            return None
        if raw.startswith("/sessions/"):
            rel = Path(raw.lstrip("/"))
            parts = rel.parts
            if len(parts) >= 3 and parts[0] == "sessions":
                candidate = self.sessions / Path(*parts[1:])
                try:
                    candidate_resolved = candidate.resolve()
                    sessions_resolved = self.sessions.resolve()
                except Exception:  # noqa: BLE001
                    return None
                if sessions_resolved == candidate_resolved or sessions_resolved in candidate_resolved.parents:
                    return candidate_resolved
            return None
        p = Path(raw)
        if p.is_absolute():
            try:
                resolved = p.resolve()
                sessions_resolved = self.sessions.resolve()
            except Exception:  # noqa: BLE001
                return None
            if sessions_resolved == resolved or sessions_resolved in resolved.parents:
                return resolved
        return None

    @staticmethod
    def _image_file_to_data_uri(path: Path) -> str:
        ext = path.suffix.lower().lstrip(".")
        mime = {
            "png": "image/png",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "webp": "image/webp",
        }.get(ext, "")
        if not mime:
            return ""
        try:
            b64 = base64.b64encode(path.read_bytes()).decode("ascii")
        except Exception:  # noqa: BLE001
            return ""
        return f"data:{mime};base64,{b64}"

    def auto_business_insights(
        self,
        dataset_id: str,
        topic: str = "sales",
        requirement: str = "",
        max_rows: int = 10,
    ) -> Dict[str, Any]:
        topic_norm = str(topic or "sales").strip().lower()
        if topic_norm not in {"sales", "operations", "finance", "customer"}:
            topic_norm = "sales"
        if topic_norm == "sales":
            return self.auto_sales_insights(
                dataset_id=dataset_id,
                max_rows=max_rows,
                requirement=requirement,
                topic=topic_norm,
            )
        return self._auto_non_sales_insights(
            dataset_id=dataset_id,
            topic=topic_norm,
            requirement=requirement,
            max_rows=max_rows,
        )

    def _auto_non_sales_insights(
        self,
        dataset_id: str,
        topic: str,
        requirement: str,
        max_rows: int,
    ) -> Dict[str, Any]:
        path = self._find_dataset(dataset_id)
        if path is None:
            raise FileNotFoundError("dataset_id 不存在")
        df = self._load_df_cached(path)
        safe_rows = max(3, min(int(max_rows or 10), 50))
        requirement_text = str(requirement or "").strip()
        columns = [str(c) for c in df.columns]
        session_id = uuid.uuid4().hex
        session_dir = self.sessions / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        amount_col = (
            self._best_match_column("销售金额", columns)
            or self._best_match_column("金额", columns)
            or self._best_match_column("成交金额", columns)
            or self._best_match_column("revenue", columns)
            or self._best_match_column("sales", columns)
            or self._best_match_column("gmv", columns)
            or self._best_match_column("amount", columns)
        )
        customer_col = (
            self._best_match_column("客户", columns)
            or self._best_match_column("客户名称", columns)
            or self._best_match_column("交易对方", columns)
            or self._best_match_column("交易对象", columns)
            or self._best_match_column("customer", columns)
            or self._best_match_column("counterparty", columns)
        )
        date_col = (
            self._best_match_column("交易时间", columns)
            or self._best_match_column("订单日期", columns)
            or self._best_match_column("日期", columns)
            or self._best_match_column("date", columns)
            or self._best_match_column("created_at", columns)
        )
        status_col = (
            self._best_match_column("状态", columns)
            or self._best_match_column("订单状态", columns)
            or self._best_match_column("履约状态", columns)
            or self._best_match_column("status", columns)
            or self._best_match_column("state", columns)
        )
        duration_col = (
            self._best_match_column("时长", columns)
            or self._best_match_column("耗时", columns)
            or self._best_match_column("处理时长", columns)
            or self._best_match_column("duration", columns)
            or self._best_match_column("lead_time", columns)
        )
        cost_col = (
            self._best_match_column("成本", columns)
            or self._best_match_column("费用", columns)
            or self._best_match_column("cost", columns)
            or self._best_match_column("expense", columns)
        )
        income_col = (
            self._best_match_column("收入", columns)
            or self._best_match_column("营收", columns)
            or self._best_match_column("revenue", columns)
            or self._best_match_column("income", columns)
        )
        expense_col = (
            self._best_match_column("支出", columns)
            or self._best_match_column("支出金额", columns)
            or self._best_match_column("expense", columns)
            or cost_col
        )
        profit_col = (
            self._best_match_column("利润", columns)
            or self._best_match_column("净利润", columns)
            or self._best_match_column("profit", columns)
        )
        churn_col = (
            self._best_match_column("流失", columns)
            or self._best_match_column("是否流失", columns)
            or self._best_match_column("churn", columns)
        )
        segment_col = (
            self._best_match_column("客户等级", columns)
            or self._best_match_column("客户分层", columns)
            or self._best_match_column("segment", columns)
        )

        available = {
            "amount_col": amount_col,
            "customer_col": customer_col,
            "date_col": date_col,
            "status_col": status_col,
            "duration_col": duration_col,
            "cost_col": cost_col,
            "income_col": income_col,
            "expense_col": expense_col,
            "profit_col": profit_col,
            "churn_col": churn_col,
            "segment_col": segment_col,
            "columns": columns,
        }

        insights: List[Dict[str, Any]] = []
        tables: List[Dict[str, Any]] = []
        plots: List[str] = []
        confidence = "medium"

        if topic == "operations":
            order_cnt = int(df.shape[0])
            insights.append(
                {
                    "finding": "运营吞吐规模已可量化",
                    "evidence": f"当前记录数={order_cnt}，可作为履约与工单处理的基础吞吐指标。",
                    "suggestion": "按周建立吞吐监控看板，持续跟踪峰值与低谷并安排资源弹性。",
                    "priority": "medium",
                    "impact_estimation": "定性：可提前识别资源不足导致的履约风险。",
                }
            )

            if status_col:
                status_series = df[status_col].astype(str).fillna("").str.strip()
                done_mask = status_series.str.contains(
                    r"完成|完结|已交付|已签收|closed|done|success|completed",
                    case=False,
                    regex=True,
                )
                completion_rate = float(done_mask.mean()) if len(status_series) else 0.0
                status_dist = status_series.value_counts(dropna=False).head(safe_rows)
                rows = [{"status": str(k), "count": int(v)} for k, v in status_dist.items()]
                tables.append({"name": "operations_status_distribution", "rows": rows})
                insights.append(
                    {
                        "finding": "履约状态分布可用于定位堵点",
                        "evidence": f"完成率约 {completion_rate * 100:.2f}%，状态字段={status_col}。",
                        "suggestion": "对未完成或异常状态设置 SLA 预警，并建立按状态的责任人闭环。",
                        "priority": "high" if completion_rate < 0.8 else "medium",
                        "impact_estimation": "量化：完成率每提升 5%，通常可减少延期工单与返工成本。",
                    }
                )
                status_plot = self._save_insights_plot(
                    session_dir=session_dir,
                    filename="operations_status.png",
                    title="Operations Status Distribution",
                    x_labels=[str(k) for k in status_dist.index.tolist()],
                    y_values=[float(v) for v in status_dist.values.tolist()],
                    chart_type="bar",
                )
                if status_plot:
                    plots.append(f"/sessions/{session_id}/{status_plot}")

            if date_col:
                dt_series = pd.to_datetime(df[date_col], errors="coerce")
                valid = dt_series.notna()
                if bool(valid.any()):
                    monthly_orders = (
                        pd.DataFrame({"period": dt_series[valid].dt.to_period("M").astype(str)})
                        .groupby("period")
                        .size()
                        .sort_index()
                    )
                    rows = [{"period": str(k), "order_count": int(v)} for k, v in monthly_orders.tail(safe_rows).items()]
                    tables.append({"name": "operations_monthly_orders", "rows": rows})
                    if len(monthly_orders) >= 2 and monthly_orders.iloc[-2] > 0:
                        growth = float((monthly_orders.iloc[-1] - monthly_orders.iloc[-2]) / monthly_orders.iloc[-2])
                    else:
                        growth = 0.0
                    insights.append(
                        {
                            "finding": "运营吞吐趋势可追踪到月度节奏",
                            "evidence": f"最新周期={monthly_orders.index[-1]}，环比变化约 {growth * 100:.2f}%。",
                            "suggestion": "对高峰月提前准备产能，对低谷月集中做流程优化与标准化。",
                            "priority": "medium",
                            "impact_estimation": "定性：平滑吞吐波动可降低排队时长与服务超时概率。",
                        }
                    )
                    trend_plot = self._save_insights_plot(
                        session_dir=session_dir,
                        filename="operations_monthly_orders.png",
                        title="Operations Monthly Throughput",
                        x_labels=[str(x) for x in monthly_orders.index.tolist()],
                        y_values=[float(v) for v in monthly_orders.values.tolist()],
                        chart_type="line",
                    )
                    if trend_plot:
                        plots.append(f"/sessions/{session_id}/{trend_plot}")

            if duration_col:
                duration = pd.to_numeric(df[duration_col], errors="coerce")
                duration = duration[duration > 0]
                if not duration.empty:
                    avg_duration = float(duration.mean())
                    p90_duration = float(duration.quantile(0.9))
                    insights.append(
                        {
                            "finding": "处理时效具备优化空间",
                            "evidence": f"平均处理时长约 {avg_duration:.2f}，P90 约 {p90_duration:.2f}（字段={duration_col}）。",
                            "suggestion": "优先压缩长尾工单时长，拆解 P90 以上任务的等待与审批环节。",
                            "priority": "high" if p90_duration > avg_duration * 1.5 else "medium",
                            "impact_estimation": "量化：若 P90 时长下降 10%，整体超时风险可显著下降。",
                        }
                    )
            elif cost_col:
                cost = pd.to_numeric(df[cost_col], errors="coerce").fillna(0.0)
                avg_cost = float(cost.mean()) if len(cost) else 0.0
                insights.append(
                    {
                        "finding": "运营成本可作为效率代理指标",
                        "evidence": f"平均单笔成本约 {avg_cost:.2f}（字段={cost_col}）。",
                        "suggestion": "按业务类型拆分成本并识别高成本环节，建立成本责任归因。",
                        "priority": "medium",
                        "impact_estimation": "量化：单笔成本每降低 5%，总运营费用可同比例下降。",
                    }
                )

            while len(insights) < 3:
                insights.append(
                    {
                        "finding": "关键运营字段仍有补齐空间",
                        "evidence": "当前可识别字段不足以覆盖完整时效与质量分析。",
                        "suggestion": "补充状态、时长和关键事件时间戳字段，提升运营建议精度。",
                        "priority": "medium",
                        "impact_estimation": "定性：字段完善后可输出更稳定的履约优化建议。",
                    }
                )
            req = f" 已结合你的需求“{requirement_text}”生成建议。" if requirement_text else ""
            summary = f"已完成运营主题自动分析：共 {int(df.shape[0])} 条记录，输出 {len(insights)} 条建议。{req}".strip()
            confidence = "high" if status_col and date_col else "medium"

        elif topic == "finance":
            revenue_series = None
            expense_series = None
            profit_series = None
            if income_col:
                revenue_series = pd.to_numeric(df[income_col], errors="coerce").fillna(0.0)
            elif amount_col:
                revenue_series = pd.to_numeric(df[amount_col], errors="coerce").fillna(0.0)
            if expense_col:
                expense_series = pd.to_numeric(df[expense_col], errors="coerce").fillna(0.0)
            if profit_col:
                profit_series = pd.to_numeric(df[profit_col], errors="coerce").fillna(0.0)
            elif revenue_series is not None and expense_series is not None:
                profit_series = revenue_series - expense_series

            total_revenue = float(revenue_series.sum()) if revenue_series is not None else 0.0
            total_expense = float(expense_series.sum()) if expense_series is not None else 0.0
            total_profit = float(profit_series.sum()) if profit_series is not None else 0.0

            if revenue_series is not None:
                insights.append(
                    {
                        "finding": "财务规模可形成收入基线",
                        "evidence": f"累计收入约 {total_revenue:.2f}（字段={income_col or amount_col}）。",
                        "suggestion": "将收入拆分到业务线或区域，定位增长主驱动与薄弱环节。",
                        "priority": "high",
                        "impact_estimation": "定性：收入结构更清晰后，资源投放更可控。",
                    }
                )
            else:
                insights.append(
                    {
                        "finding": "缺少可识别收入字段，无法完成完整收支分析",
                        "evidence": f"当前字段: {', '.join(columns[:12])}" if columns else "字段为空",
                        "suggestion": "请补充收入/营收字段，以输出更可靠的财务健康建议。",
                        "priority": "high",
                        "impact_estimation": "定性：补齐字段后可评估增长与风险暴露程度。",
                    }
                )

            if expense_series is not None:
                expense_ratio = float(total_expense / total_revenue) if total_revenue > 0 else 0.0
                insights.append(
                    {
                        "finding": "支出强度可用于衡量成本压力",
                        "evidence": (
                            f"累计支出约 {total_expense:.2f}，收支比约 {expense_ratio * 100:.2f}%（字段={expense_col}）。"
                            if total_revenue > 0
                            else f"累计支出约 {total_expense:.2f}（字段={expense_col}）。"
                        ),
                        "suggestion": "按成本项建立预算偏差追踪，对高偏差项设置审批与复盘机制。",
                        "priority": "high" if expense_ratio > 0.85 and total_revenue > 0 else "medium",
                        "impact_estimation": "量化：重点成本项压降 5%-10% 可直接改善利润空间。",
                    }
                )

            if profit_series is not None:
                profit_margin = float(total_profit / total_revenue) if total_revenue > 0 else 0.0
                insights.append(
                    {
                        "finding": "利润表现可用于识别财务健康度",
                        "evidence": (
                            f"累计利润约 {total_profit:.2f}，利润率约 {profit_margin * 100:.2f}%。"
                            if total_revenue > 0
                            else f"累计利润约 {total_profit:.2f}。"
                        ),
                        "suggestion": "优先治理低毛利业务与异常费用，建立利润率目标红线。",
                        "priority": "high" if total_profit < 0 else "medium",
                        "impact_estimation": "量化：利润率每提升 1pct，资金安全垫同步提升。",
                    }
                )

            if date_col and (revenue_series is not None or profit_series is not None):
                metric_name = "profit" if profit_series is not None else "revenue"
                metric = profit_series if profit_series is not None else revenue_series
                dt_series = pd.to_datetime(df[date_col], errors="coerce")
                valid = dt_series.notna()
                if metric is not None and bool(valid.any()):
                    monthly_metric = (
                        pd.DataFrame(
                            {
                                "period": dt_series[valid].dt.to_period("M").astype(str),
                                "metric": metric[valid],
                            }
                        )
                        .groupby("period")["metric"]
                        .sum()
                        .sort_index()
                    )
                    rows = [{"period": str(k), metric_name: float(v)} for k, v in monthly_metric.tail(safe_rows).items()]
                    tables.append({"name": f"finance_monthly_{metric_name}", "rows": rows})
                    if len(monthly_metric) >= 2 and monthly_metric.iloc[-2] != 0:
                        growth = float((monthly_metric.iloc[-1] - monthly_metric.iloc[-2]) / monthly_metric.iloc[-2])
                    else:
                        growth = 0.0
                    insights.append(
                        {
                            "finding": "财务趋势可追踪到月度波动",
                            "evidence": f"{metric_name} 最近周期={monthly_metric.index[-1]}，环比约 {growth * 100:.2f}%。",
                            "suggestion": "对异常波动月份复盘业务事件与费用结构，形成财务解释口径。",
                            "priority": "high" if growth < -0.1 else "medium",
                            "impact_estimation": "定性：提前识别下滑月份可降低资金与经营风险。",
                        }
                    )
                    trend_plot = self._save_insights_plot(
                        session_dir=session_dir,
                        filename=f"finance_monthly_{metric_name}.png",
                        title=f"Finance Monthly {metric_name.title()}",
                        x_labels=[str(x) for x in monthly_metric.index.tolist()],
                        y_values=[float(v) for v in monthly_metric.values.tolist()],
                        chart_type="line",
                    )
                    if trend_plot:
                        plots.append(f"/sessions/{session_id}/{trend_plot}")

            if customer_col and revenue_series is not None:
                cust_rev = (
                    pd.DataFrame({"customer": df[customer_col].astype(str), "revenue": revenue_series})
                    .groupby("customer")["revenue"]
                    .sum()
                    .sort_values(ascending=False)
                )
                if not cust_rev.empty:
                    top_ratio = float(cust_rev.iloc[0] / total_revenue) if total_revenue > 0 else 0.0
                    rows = [{"customer": str(k), "revenue": float(v)} for k, v in cust_rev.head(safe_rows).items()]
                    tables.append({"name": "finance_revenue_by_customer", "rows": rows})
                    insights.append(
                        {
                            "finding": "收入集中度反映财务稳定性风险",
                            "evidence": f"Top1 收入占比约 {top_ratio * 100:.2f}%，客户数={int(cust_rev.shape[0])}。",
                            "suggestion": "对高集中客户设置信用与回款监控，同时扩展收入来源。",
                            "priority": "high" if top_ratio >= 0.35 else "medium",
                            "impact_estimation": "定性：降低集中度可提升现金流稳定性与抗风险能力。",
                        }
                    )

            while len(insights) < 3:
                insights.append(
                    {
                        "finding": "财务字段完整性仍可提升",
                        "evidence": "当前字段不足以覆盖预算、利润、现金流全链路。",
                        "suggestion": "补充利润、费用分类和回款字段，增强财务建议可执行性。",
                        "priority": "medium",
                        "impact_estimation": "定性：完整字段有助于构建更可靠的财务预警。",
                    }
                )
            req = f" 已结合你的需求“{requirement_text}”生成建议。" if requirement_text else ""
            summary = (
                f"已完成财务主题自动分析：收入 {total_revenue:.2f}，支出 {total_expense:.2f}，利润 {total_profit:.2f}，"
                f"输出 {len(insights)} 条建议。{req}"
            ).strip()
            confidence = "high" if (revenue_series is not None and date_col) else "medium"

        else:
            customer_cnt = 0
            if customer_col:
                customer_series = df[customer_col].astype(str).fillna("").str.strip()
                customer_series = customer_series[customer_series != ""]
                customer_cnt = int(customer_series.nunique())
                insights.append(
                    {
                        "finding": "客户覆盖规模已可量化",
                        "evidence": f"识别客户字段 `{customer_col}`，去重客户数约 {customer_cnt}。",
                        "suggestion": "按客户规模和价值建立分层运营策略，明确维护频次与服务等级。",
                        "priority": "high",
                        "impact_estimation": "定性：客户分层可提升运营资源利用效率。",
                    }
                )
                if amount_col:
                    amount = pd.to_numeric(df[amount_col], errors="coerce").fillna(0.0)
                    customer_sales = (
                        pd.DataFrame({"customer": df[customer_col].astype(str), "amount": amount})
                        .groupby("customer")["amount"]
                        .sum()
                        .sort_values(ascending=False)
                    )
                    if not customer_sales.empty:
                        total_sales = float(customer_sales.sum())
                        top1_ratio = float(customer_sales.iloc[0] / total_sales) if total_sales > 0 else 0.0
                        rows = [{"customer": str(k), "amount": float(v)} for k, v in customer_sales.head(safe_rows).items()]
                        tables.append({"name": "customer_value_top", "rows": rows})
                        insights.append(
                            {
                                "finding": "客户价值集中度可识别",
                                "evidence": f"Top1 客户贡献占比约 {top1_ratio * 100:.2f}%，总客户数={customer_cnt}。",
                                "suggestion": "降低对单一客户依赖，扩展高潜客户转化并设置预警阈值。",
                                "priority": "high" if top1_ratio >= 0.35 else "medium",
                                "impact_estimation": "量化：若腰部客户贡献提升 10%，整体收入结构更稳健。",
                            }
                        )
                        top_plot = self._save_insights_plot(
                            session_dir=session_dir,
                            filename="customer_value_top.png",
                            title="Customer Value Distribution",
                            x_labels=[str(k) for k in customer_sales.head(8).index.tolist()],
                            y_values=[float(v) for v in customer_sales.head(8).values.tolist()],
                            chart_type="bar",
                        )
                        if top_plot:
                            plots.append(f"/sessions/{session_id}/{top_plot}")
            else:
                insights.append(
                    {
                        "finding": "缺少客户字段，无法进行客户分层与留存分析",
                        "evidence": f"当前字段: {', '.join(columns[:12])}" if columns else "字段为空",
                        "suggestion": "请补充客户标识字段后再执行客户主题分析。",
                        "priority": "high",
                        "impact_estimation": "定性：补齐客户维度后可输出更精准的客户经营建议。",
                    }
                )

            if date_col and customer_col:
                dt_series = pd.to_datetime(df[date_col], errors="coerce")
                valid = dt_series.notna()
                if bool(valid.any()):
                    tmp = pd.DataFrame(
                        {
                            "period": dt_series[valid].dt.to_period("M").astype(str),
                            "customer": df.loc[valid, customer_col].astype(str),
                        }
                    )
                    actives = tmp.groupby("period")["customer"].nunique().sort_index()
                    if not actives.empty:
                        rows = [{"period": str(k), "active_customers": int(v)} for k, v in actives.tail(safe_rows).items()]
                        tables.append({"name": "customer_active_trend", "rows": rows})
                        if len(actives) >= 2 and actives.iloc[-2] > 0:
                            active_growth = float((actives.iloc[-1] - actives.iloc[-2]) / actives.iloc[-2])
                        else:
                            active_growth = 0.0
                        insights.append(
                            {
                                "finding": "活跃客户趋势可反映客户健康度",
                                "evidence": f"最新周期活跃客户={int(actives.iloc[-1])}，环比约 {active_growth * 100:.2f}%。",
                                "suggestion": "对活跃客户下降周期执行召回活动，并结合渠道评估流量质量。",
                                "priority": "high" if active_growth < -0.1 else "medium",
                                "impact_estimation": "定性：稳定活跃客户规模可提高收入可预测性。",
                            }
                        )
                        trend_plot = self._save_insights_plot(
                            session_dir=session_dir,
                            filename="customer_active_trend.png",
                            title="Customer Active Trend",
                            x_labels=[str(x) for x in actives.index.tolist()],
                            y_values=[float(v) for v in actives.values.tolist()],
                            chart_type="line",
                        )
                        if trend_plot:
                            plots.append(f"/sessions/{session_id}/{trend_plot}")

            if churn_col:
                churn_series = df[churn_col].astype(str).str.lower()
                churn_yes = churn_series.str.contains(r"1|是|true|yes|流失", regex=True)
                churn_rate = float(churn_yes.mean()) if len(churn_series) else 0.0
                insights.append(
                    {
                        "finding": "客户流失风险可被量化跟踪",
                        "evidence": f"流失率约 {churn_rate * 100:.2f}%（字段={churn_col}）。",
                        "suggestion": "建立高风险客户预警名单并配置挽回动作包，缩短干预时滞。",
                        "priority": "high" if churn_rate >= 0.2 else "medium",
                        "impact_estimation": "量化：流失率每下降 1pct，客户生命周期价值通常可同步提升。",
                    }
                )

            if segment_col:
                seg = df[segment_col].astype(str).value_counts(dropna=False).head(safe_rows)
                rows = [{"segment": str(k), "count": int(v)} for k, v in seg.items()]
                tables.append({"name": "customer_segment_distribution", "rows": rows})
                insights.append(
                    {
                        "finding": "客户分层分布可指导差异化运营",
                        "evidence": f"已识别客户分层字段 `{segment_col}`，层级数约 {int(seg.shape[0])}。",
                        "suggestion": "对高价值层级提高服务深度，对低活跃层级设计激活任务。",
                        "priority": "medium",
                        "impact_estimation": "定性：分层运营可提升营销投入产出比。",
                    }
                )

            while len(insights) < 3:
                insights.append(
                    {
                        "finding": "客户主题分析字段仍需补齐",
                        "evidence": "当前字段不足以完整覆盖价值、留存和流失分析。",
                        "suggestion": "补充客户标识、交易时间与价值字段，提升建议可执行性。",
                        "priority": "medium",
                        "impact_estimation": "定性：字段越完整，客户建议越接近可落地动作。",
                    }
                )
            req = f" 已结合你的需求“{requirement_text}”生成建议。" if requirement_text else ""
            summary = f"已完成客户主题自动分析：识别客户数约 {customer_cnt}，输出 {len(insights)} 条建议。{req}".strip()
            confidence = "high" if customer_col and (amount_col or date_col) else "medium"

        return {
            "session_id": session_id,
            "dataset_id": dataset_id,
            "topic": topic,
            "requirement": requirement_text,
            "summary": summary,
            "insights": insights[: max(3, safe_rows)],
            "tables": tables,
            "plots": plots,
            "confidence": confidence,
            "analysis_scope": topic,
            "available_fields": available,
        }

    @staticmethod
    def _save_insights_plot(
        session_dir: Path,
        filename: str,
        title: str,
        x_labels: List[str],
        y_values: List[float],
        chart_type: str = "bar",
    ) -> str:
        if not x_labels or not y_values or len(x_labels) != len(y_values):
            return ""
        try:
            fig, ax = plt.subplots(figsize=(8, 4))
            if chart_type == "line":
                ax.plot(x_labels, y_values, marker="o", color="#0f766e", linewidth=2.0)
            else:
                ax.bar(x_labels, y_values, color="#0f766e")
            ax.set_title(title)
            ax.tick_params(axis="x", rotation=30, labelsize=8)
            fig.tight_layout()
            out = session_dir / filename
            fig.savefig(out)
            plt.close(fig)
            return filename
        except Exception:  # noqa: BLE001
            return ""

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

    def get_kaggle_settings(self) -> Dict[str, Any]:
        cfg = self._load_kaggle_config()
        return {
            "config": cfg,
            "configured": bool(str(cfg.get("username", "")).strip() and str(cfg.get("api_key", "")).strip()),
        }

    def update_kaggle_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        cfg = self._sanitize_kaggle_config(payload=payload, base=self._load_kaggle_config())
        cfg["updated_at"] = dt.datetime.utcnow().isoformat()
        self._save_kaggle_config(cfg)
        return {
            "config": cfg,
            "configured": bool(str(cfg.get("username", "")).strip() and str(cfg.get("api_key", "")).strip()),
        }

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

    def run_automated_tests(self, include_pytest: bool = True) -> Dict[str, Any]:
        run_id = uuid.uuid4().hex
        start = time.time()
        commands: List[Dict[str, Any]] = [
            {
                "name": "python_compile",
                "cmd": [
                    "python3",
                    "-m",
                    "py_compile",
                    "app/main.py",
                    "app/agent.py",
                    "pipelines/uci_auto_pipeline.py",
                    "tests/test_iter003_pipeline.py",
                    "tests/test_kaggle_repository.py",
                ],
                "optional": False,
            },
            {
                "name": "node_check",
                "cmd": ["node", "--check", "app/static/app.js"],
                "optional": True,
            },
            {
                "name": "iter003_demo",
                "cmd": ["./scripts/run_iter003_demo.sh"],
                "optional": False,
            },
        ]
        if include_pytest:
            commands.append(
                {
                    "name": "pytest_core",
                    "cmd": [
                        "python3",
                        "-m",
                        "pytest",
                        "-q",
                        "tests/test_iter003_pipeline.py",
                        "tests/test_uci_repository.py",
                        "tests/test_kaggle_repository.py",
                        "tests/test_dataset_metadata_summary.py",
                    ],
                    "optional": True,
                }
            )

        items: List[Dict[str, Any]] = []
        pass_count = 0
        fail_count = 0
        skip_count = 0
        for c in commands:
            item = self._execute_test_command(cmd=c["cmd"], timeout=360)
            item["name"] = str(c["name"])
            item["optional"] = bool(c.get("optional", False))
            status = str(item.get("status", "fail"))
            if status == "pass":
                pass_count += 1
            elif status == "skipped":
                skip_count += 1
            else:
                fail_count += 1
            items.append(item)

        hard_fail = any(i.get("status") == "fail" and not i.get("optional", False) for i in items)
        status = "fail" if hard_fail else "pass"
        duration_ms = int((time.time() - start) * 1000)
        payload = {
            "run_id": run_id,
            "time": dt.datetime.utcnow().isoformat(),
            "status": status,
            "duration_ms": duration_ms,
            "include_pytest": bool(include_pytest),
            "pass_count": pass_count,
            "fail_count": fail_count,
            "skip_count": skip_count,
            "items": items,
        }
        self._append_jsonl(self.test_runs_file, payload)
        detail_dir = self.sessions / "test_runs" / run_id
        detail_dir.mkdir(parents=True, exist_ok=True)
        (detail_dir / "result.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    def list_test_runs(self, limit: int = 20) -> Dict[str, Any]:
        rows = self._read_jsonl(self.test_runs_file)
        safe_limit = max(1, min(int(limit or 20), 200))
        items = list(reversed(rows))[:safe_limit]
        return {"items": items, "total": len(rows)}

    def get_test_run(self, run_id: str) -> Dict[str, Any]:
        rid = str(run_id or "").strip()
        if not rid:
            raise FileNotFoundError("run_id 不能为空")
        detail = self.sessions / "test_runs" / rid / "result.json"
        if detail.exists():
            try:
                return json.loads(detail.read_text(encoding="utf-8"))
            except Exception as exc:  # noqa: BLE001
                raise ValueError("测试结果详情损坏") from exc
        rows = self._read_jsonl(self.test_runs_file)
        for row in reversed(rows):
            if str(row.get("run_id", "")) == rid:
                return row
        raise FileNotFoundError("run_id 不存在")

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

    def _local_source_dataset_map(self, source: str) -> Dict[str, str]:
        source_norm = str(source or "").strip().lower()
        if not source_norm:
            return {}
        meta = self._load_meta()
        out: Dict[str, str] = {}
        for did, info in meta.items():
            if str(info.get("source", "")).strip().lower() != source_norm:
                continue
            sid = str(info.get("source_id", "")).strip()
            if not sid:
                continue
            if self._find_dataset(did) is None:
                continue
            out[sid] = did
        return out

    def _local_uci_dataset_map(self) -> Dict[str, str]:
        return self._local_source_dataset_map("uci")

    def _find_existing_uci_dataset(self, uci_id: str) -> str:
        sid = str(uci_id or "").strip()
        if not sid:
            return ""
        return self._local_uci_dataset_map().get(sid, "")

    def _find_existing_source_dataset(self, source: str, source_id: str) -> str:
        sid = str(source_id or "").strip()
        if not sid:
            return ""
        return self._local_source_dataset_map(source).get(sid, "")

    @staticmethod
    def _build_uci_filename(name: str, data_url: str, uci_id: int) -> str:
        path = urllib.parse.urlsplit(data_url).path
        ext = Path(path).suffix.lower()
        if ext not in {".csv", ".xlsx"}:
            ext = ".csv"
        safe_name = re.sub(r"[^\w\-\u4e00-\u9fff]+", "_", str(name or "").strip(), flags=re.UNICODE).strip("_")
        if not safe_name:
            safe_name = f"uci_{uci_id}"
        return f"{safe_name}{ext}"

    @staticmethod
    def _http_json(url: str, timeout: int = 20, headers: Optional[Dict[str, str]] = None) -> Any:
        raw = DataInterpreterAgent._http_bytes(url=url, timeout=timeout, headers=headers)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"远程接口返回非 JSON: {url}") from exc
        if isinstance(payload, (dict, list)):
            return payload
        raise ValueError(f"远程接口返回格式异常: {url}")

    @staticmethod
    def _http_bytes(url: str, timeout: int = 30, headers: Optional[Dict[str, str]] = None) -> bytes:
        req_headers = {"User-Agent": "DataInterpreterAgent/1.0"}
        if isinstance(headers, dict):
            req_headers.update({str(k): str(v) for k, v in headers.items() if str(k).strip() and str(v).strip()})
        req = urllib.request.Request(url, headers=req_headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="ignore")
            except Exception:  # noqa: BLE001
                body = ""
            msg = f"远程请求失败({exc.code}): {url}"
            if body:
                msg = f"{msg} | {body[:200]}"
            raise ValueError(msg) from exc
        except urllib.error.URLError as exc:
            raise ValueError(f"远程请求失败: {url} ({exc.reason})") from exc

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

    @staticmethod
    def _default_kaggle_config() -> Dict[str, Any]:
        return {
            "username": "",
            "api_key": "",
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

    def _load_kaggle_config(self) -> Dict[str, Any]:
        default = self._default_kaggle_config()
        if not self.kaggle_config_file.exists():
            return self._sanitize_kaggle_config(payload=default, base=default)
        try:
            raw = json.loads(self.kaggle_config_file.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return default
            return self._sanitize_kaggle_config(payload=raw, base=default)
        except Exception:  # noqa: BLE001
            return default

    def _save_llm_config(self, config: Dict[str, Any]) -> None:
        self.llm_config_file.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")

    def _save_kaggle_config(self, config: Dict[str, Any]) -> None:
        self.kaggle_config_file.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")

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
    def _sanitize_kaggle_config(payload: Dict[str, Any], base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        out = dict(base or DataInterpreterAgent._default_kaggle_config())
        out["username"] = str(payload.get("username", out.get("username", ""))).strip()
        out["api_key"] = str(payload.get("api_key", out.get("api_key", ""))).strip()
        out["updated_at"] = str(payload.get("updated_at", out.get("updated_at", "")))
        return out

    @staticmethod
    def _kaggle_auth_header(cfg: Dict[str, Any]) -> str:
        username = str(cfg.get("username", "")).strip()
        api_key = str(cfg.get("api_key", "")).strip()
        if not username or not api_key:
            return ""
        token = base64.b64encode(f"{username}:{api_key}".encode("utf-8")).decode("utf-8")
        return f"Basic {token}"

    @staticmethod
    def _extract_first_tabular_file(archive: bytes, default_slug: str) -> Optional[Tuple[str, bytes]]:
        with tempfile.TemporaryDirectory(prefix="kaggle_import_") as td:
            zip_path = Path(td) / "dataset.zip"
            zip_path.write_bytes(archive)
            try:
                with zipfile.ZipFile(zip_path, "r") as zf:
                    zf.extractall(td)
            except zipfile.BadZipFile:
                # Some Kaggle downloads can be direct CSV.
                if archive.lstrip().startswith(b"{") or archive.lstrip().startswith(b"["):
                    return None
                return (f"{default_slug}.csv", archive)

            root = Path(td)
            candidates: List[Path] = []
            for p in root.rglob("*"):
                if not p.is_file():
                    continue
                if p.suffix.lower() not in {".csv", ".xlsx"}:
                    continue
                candidates.append(p)
            if not candidates:
                return None
            candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
            chosen = candidates[0]
            return (chosen.name, chosen.read_bytes())

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
    def _append_jsonl(path: Path, row: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
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

    def _execute_test_command(self, cmd: List[str], timeout: int = 300) -> Dict[str, Any]:
        started = time.time()
        cmd_text = " ".join([str(v) for v in cmd])
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(self.base_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=timeout,
                check=False,
            )
            output = proc.stdout or ""
            status = "pass" if proc.returncode == 0 else "fail"
            duration_ms = int((time.time() - started) * 1000)
            return {
                "cmd": cmd_text,
                "returncode": int(proc.returncode),
                "status": status,
                "duration_ms": duration_ms,
                "output": output[-12000:],
            }
        except FileNotFoundError as exc:
            duration_ms = int((time.time() - started) * 1000)
            return {
                "cmd": cmd_text,
                "returncode": 127,
                "status": "skipped",
                "duration_ms": duration_ms,
                "output": f"命令不存在: {exc}",
            }
        except subprocess.TimeoutExpired as exc:
            duration_ms = int((time.time() - started) * 1000)
            output = str(exc.stdout or "")
            return {
                "cmd": cmd_text,
                "returncode": 124,
                "status": "fail",
                "duration_ms": duration_ms,
                "output": f"执行超时({timeout}s)\n{output[-8000:]}",
            }

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
    def _render_insights_markdown(
        report: Dict[str, Any],
        plot_assets: Optional[List[Dict[str, str]]] = None,
        embed_images: bool = False,
    ) -> str:
        dataset_id = str(report.get("dataset_id", ""))
        topic = str(report.get("topic", "sales"))
        requirement = str(report.get("requirement", ""))
        summary = str(report.get("summary", ""))
        confidence = str(report.get("confidence", "medium"))
        insights = report.get("insights", [])
        if not isinstance(insights, list):
            insights = []

        lines = [
            "# 商业自动分析报告",
            "",
            f"- 数据集ID: `{dataset_id}`",
            f"- 主题: `{topic}`",
            f"- 用户需求: `{requirement}`" if requirement else "- 用户需求: `未填写`",
            f"- 置信度: `{confidence}`",
            "",
            "## 摘要",
            summary or "无摘要",
            "",
            "## 建议清单",
        ]
        if not insights:
            lines.append("- 暂无建议")
        else:
            for idx, item in enumerate(insights, 1):
                if not isinstance(item, dict):
                    continue
                lines.extend(
                    [
                        f"### 建议 {idx}",
                        f"- 发现: {item.get('finding', '')}",
                        f"- 证据: {item.get('evidence', '')}",
                        f"- 建议动作: {item.get('suggestion', '')}",
                        f"- 优先级: {item.get('priority', 'medium')}",
                        f"- 影响预估: {item.get('impact_estimation', '')}",
                        "",
                    ]
                )

        tables = report.get("tables", [])
        if isinstance(tables, list) and tables:
            lines.extend(["## 关键表格"])
            for table in tables:
                if not isinstance(table, dict):
                    continue
                name = str(table.get("name", "table"))
                rows = table.get("rows", [])
                lines.append(f"### {name}")
                if isinstance(rows, list) and rows:
                    for row in rows[:20]:
                        lines.append(f"- {DataInterpreterAgent._to_json_safe(row)}")
                else:
                    lines.append("- 无数据")
                lines.append("")
        assets = plot_assets if isinstance(plot_assets, list) else []
        if assets:
            lines.extend(["## 图表"])
            for idx, a in enumerate(assets, 1):
                src = str(a.get("source_url", ""))
                data_uri = str(a.get("data_uri", ""))
                filename = str(a.get("filename", ""))
                lines.append(f"### 图表 {idx}")
                if embed_images and data_uri:
                    lines.append(f"![图表{idx}]({data_uri})")
                else:
                    lines.append(f"- 文件: `{filename}`")
                    if src:
                        lines.append(f"- 来源: {src}")
                lines.append("")
        else:
            plots = report.get("plots", [])
            if isinstance(plots, list) and plots:
                lines.extend(["## 图表链接"])
                for p in plots:
                    lines.append(f"- {p}")
        return "\n".join(lines).strip() + "\n"

    @staticmethod
    def _render_insights_pdf(
        markdown_text: str,
        out_path: Path,
        plot_assets: Optional[List[Dict[str, str]]] = None,
    ) -> None:
        from PIL import Image, ImageDraw, ImageFont

        raw_lines = (markdown_text or "").splitlines()
        if not raw_lines:
            raw_lines = ["商业自动分析报告", "无内容"]

        font_candidates = [
            "/System/Library/Fonts/PingFang.ttc",
            "/System/Library/Fonts/Songti.ttc",
            "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
            "/System/Library/Fonts/STHeiti Light.ttc",
        ]
        font = None
        for fp in font_candidates:
            try:
                if Path(fp).exists():
                    font = ImageFont.truetype(fp, size=24)
                    break
            except Exception:  # noqa: BLE001
                continue
        if font is None:
            font = ImageFont.load_default()

        width, height = 1240, 1754  # A4-ish at ~150 DPI
        margin_x, margin_y = 70, 80
        line_height = 38
        max_w = width - margin_x * 2
        max_lines_per_page = max(1, (height - margin_y * 2) // line_height)

        wrapped: List[str] = []
        for raw in raw_lines:
            text = str(raw or "")
            if not text:
                wrapped.append("")
                continue
            buf = ""
            for ch in text:
                nxt = buf + ch
                try:
                    tw, _ = font.getsize(nxt)
                except Exception:  # noqa: BLE001
                    tw = len(nxt) * 12
                if tw <= max_w:
                    buf = nxt
                else:
                    wrapped.append(buf)
                    buf = ch
            wrapped.append(buf)

        if not wrapped:
            wrapped = ["商业自动分析报告", "无内容"]

        pages: List[Image.Image] = []
        for i in range(0, len(wrapped), max_lines_per_page):
            chunk = wrapped[i : i + max_lines_per_page]
            img = Image.new("RGB", (width, height), "white")
            draw = ImageDraw.Draw(img)
            y = margin_y
            for line in chunk:
                draw.text((margin_x, y), line, font=font, fill="black")
                y += line_height
            pages.append(img)

        assets = plot_assets if isinstance(plot_assets, list) else []
        for idx, a in enumerate(assets, 1):
            p = Path(str(a.get("path", "")))
            if not p.exists() or not p.is_file():
                continue
            try:
                src_img = Image.open(p).convert("RGB")
            except Exception:  # noqa: BLE001
                continue
            canvas = Image.new("RGB", (width, height), "white")
            draw = ImageDraw.Draw(canvas)
            title = f"图表 {idx}"
            draw.text((margin_x, margin_y // 2), title, font=font, fill="black")
            max_img_w = width - margin_x * 2
            max_img_h = height - margin_y * 2 - 60
            src_img.thumbnail((max_img_w, max_img_h))
            x = (width - src_img.width) // 2
            y = margin_y + 30 + max(0, (max_img_h - src_img.height) // 2)
            canvas.paste(src_img, (x, y))
            pages.append(canvas)

        if not pages:
            pages = [Image.new("RGB", (width, height), "white")]

        first = pages[0]
        rest = pages[1:]
        first.save(out_path, "PDF", save_all=True, append_images=rest, resolution=150.0)

    @staticmethod
    def _split_questions(question: str) -> List[str]:
        raw = (question or "").strip()
        if not raw:
            return [""]
        parts = re.split(r"(?:[；;\n]+|[。？！?!]+|\s*(?:并且|同时|而且|另外|然后|此外)\s*)", raw)
        out = [p.strip(" ，,。；;") for p in parts if p and p.strip(" ，,。；;")]
        return out or [raw]
