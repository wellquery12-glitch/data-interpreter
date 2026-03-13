from __future__ import annotations

import datetime as dt
import json
import logging
import re
import shutil
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from analyzers.analysis_planner import AnalysisPlanner
from analyzers.data_understanding import DataUnderstandingAnalyzer
from charts.template_library import ChartTemplateLibrary
from configs.template_registry import load_registry
from reports.template_renderer import ReportRenderer
from validators.result_validator import ResultValidator


class UciAutoPipeline:
    def __init__(
        self,
        datasets_dir: str = "datasets",
        outputs_dir: str = "outputs",
        log_name: str = "pipeline.log",
    ) -> None:
        self.datasets_dir = Path(datasets_dir)
        self.outputs_dir = Path(outputs_dir)
        self.log_name = log_name
        self.datasets_dir.mkdir(parents=True, exist_ok=True)
        self.outputs_dir.mkdir(parents=True, exist_ok=True)

    def run(self, source: str, run_name: str = "") -> Dict[str, Any]:
        rid = run_name.strip() or f"run_{dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        out = self.outputs_dir / rid
        out.mkdir(parents=True, exist_ok=True)
        logger = self._build_logger(out / self.log_name)

        logger.info("pipeline_start source=%s", source)
        dataset_path, dataset_name = self._resolve_source(source=source, logger=logger)
        df = self._load_dataset(dataset_path)

        understanding = DataUnderstandingAnalyzer()
        profile = understanding.profile(df)

        planner = AnalysisPlanner()
        plan = planner.plan(profile)

        chart_lib = ChartTemplateLibrary()
        chart_files = chart_lib.render_minimum_set(df=df, output_dir=out / "charts", plan=plan)
        chart_idx = chart_lib.chart_index(chart_files)

        analysis_outputs = {
            "analysis_count": len(plan),
            "chart_count": len(chart_files),
        }
        validator = ResultValidator()
        validation = validator.validate(df=df, analysis_outputs=analysis_outputs)

        facts, inferences, model_conclusions, risks = self._build_conclusions(df=df, profile=profile, plan=plan, validation=validation)

        registry = load_registry()
        renderer = ReportRenderer()
        report_paths = renderer.render(
            output_dir=out,
            dataset_name=dataset_name,
            profile={
                "rows": profile.rows,
                "cols": profile.cols,
                "column_types": profile.column_types,
                "numeric_cols": profile.numeric_cols,
                "categorical_cols": profile.categorical_cols,
                "datetime_cols": profile.datetime_cols,
                "text_cols": profile.text_cols,
                "id_cols": profile.id_cols,
                "label_col": profile.label_col,
                "quality": profile.quality,
            },
            plan=plan,
            facts=facts,
            inferences=inferences,
            model_conclusions=model_conclusions,
            risks=risks,
            validation=validation,
            chart_index=chart_idx,
            template_names=registry.report_templates,
        )

        summary = {
            "run_id": rid,
            "dataset_name": dataset_name,
            "dataset_path": str(dataset_path),
            "analysis_count": len(plan),
            "chart_count": len(chart_files),
            "chart_templates_total": len(registry.chart_templates),
            "report_templates_total": len(registry.report_templates),
            "plan": plan,
            "validation": validation,
            "reports": report_paths,
        }
        (out / "pipeline_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("pipeline_done run_id=%s reports=%s", rid, report_paths)
        return summary

    def _resolve_source(self, source: str, logger: logging.Logger) -> Tuple[Path, str]:
        s = source.strip()
        # 1) local file
        p = Path(s)
        if p.exists() and p.is_file():
            return p, p.stem

        # 2) URL direct file
        if s.startswith("http://") or s.startswith("https://"):
            return self._download_from_url(url=s, logger=logger)

        # 3) UCI dataset name
        return self._download_uci_by_name(name=s, logger=logger)

    def _download_from_url(self, url: str, logger: logging.Logger) -> Tuple[Path, str]:
        logger.info("download_url_start url=%s", url)
        filename = Path(urllib.parse.urlsplit(url).path).name or "dataset"
        target = self.datasets_dir / filename
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                target.write_bytes(resp.read())
        except Exception as exc:  # noqa: BLE001
            logger.exception("download_url_failed url=%s", url)
            raise RuntimeError(f"下载失败: {url} ({exc})") from exc

        file_path = self._maybe_unzip(target)
        logger.info("download_url_done file=%s", file_path)
        return file_path, file_path.stem

    def _download_uci_by_name(self, name: str, logger: logging.Logger) -> Tuple[Path, str]:
        logger.info("download_uci_by_name_start name=%s", name)
        list_url = "https://archive.ics.uci.edu/api/datasets/list?search=" + urllib.parse.quote(name)
        try:
            with urllib.request.urlopen(list_url, timeout=40) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            logger.exception("uci_list_failed name=%s", name)
            raise RuntimeError(f"UCI 列表查询失败: {name} ({exc})") from exc

        rows = payload.get("data", []) if isinstance(payload, dict) else []
        if not rows:
            raise RuntimeError(f"未找到 UCI 数据集: {name}")
        top = rows[0]
        dataset_id = int(top.get("ID", 0))
        dataset_name = str(top.get("Name", name))

        detail_url = f"https://archive.ics.uci.edu/api/dataset?id={dataset_id}"
        try:
            with urllib.request.urlopen(detail_url, timeout=40) as resp:
                detail = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            logger.exception("uci_detail_failed id=%s", dataset_id)
            raise RuntimeError(f"UCI 详情查询失败: id={dataset_id} ({exc})") from exc

        data_url = str(detail.get("data", {}).get("data_url", "")).strip()
        if not data_url:
            raise RuntimeError(f"UCI 数据集缺少可下载 data_url: {dataset_name}")

        return self._download_from_url(data_url, logger=logger)

    def _maybe_unzip(self, path: Path) -> Path:
        if path.suffix.lower() != ".zip":
            return path
        unzip_dir = path.with_suffix("")
        if unzip_dir.exists():
            shutil.rmtree(unzip_dir)
        unzip_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(path, "r") as zf:
            zf.extractall(unzip_dir)

        files = [p for p in unzip_dir.rglob("*") if p.is_file()]
        candidates = [p for p in files if p.suffix.lower() in {".csv", ".xlsx", ".xls", ".txt", ".data", ".arff"}]
        if candidates:
            candidates.sort(key=lambda x: x.stat().st_size, reverse=True)
            return candidates[0]
        return path

    def _load_dataset(self, path: Path) -> pd.DataFrame:
        ext = path.suffix.lower()
        if ext in {".csv", ".txt", ".data"}:
            return pd.read_csv(path, engine="python")
        if ext in {".xlsx", ".xls"}:
            return pd.read_excel(path)
        if ext == ".arff":
            return self._load_arff(path)
        raise RuntimeError(f"不支持的数据格式: {path.name}")

    def _load_arff(self, path: Path) -> pd.DataFrame:
        attrs: List[str] = []
        rows: List[List[str]] = []
        in_data = False
        for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.strip()
            if not line or line.startswith("%"):
                continue
            low = line.lower()
            if low.startswith("@attribute"):
                m = re.match(r"@attribute\s+([\w\-']+)", line, flags=re.IGNORECASE)
                if m:
                    attrs.append(m.group(1).strip("'\""))
                continue
            if low.startswith("@data"):
                in_data = True
                continue
            if in_data:
                rows.append([x.strip() for x in line.split(",")])
        if not attrs or not rows:
            raise RuntimeError("ARFF 文件解析失败")
        df = pd.DataFrame(rows, columns=attrs)
        # attempt numeric casting
        for c in df.columns:
            casted = pd.to_numeric(df[c], errors="ignore")
            df[c] = casted
        return df

    @staticmethod
    def _build_logger(path: Path) -> logging.Logger:
        logger = logging.getLogger(f"uci_pipeline_{path}")
        logger.setLevel(logging.INFO)
        logger.handlers = []
        fh = logging.FileHandler(path, encoding="utf-8")
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        return logger

    def _build_conclusions(self, df: pd.DataFrame, profile: Any, plan: List[str], validation: Dict[str, Any]) -> Tuple[List[str], List[str], List[str], List[str]]:
        facts = [
            f"数据集包含 {len(df)} 行、{len(df.columns)} 列。",
            f"识别数值列 {len(profile.numeric_cols)} 个、类别列 {len(profile.categorical_cols)} 个、时间列 {len(profile.datetime_cols)} 个。",
            f"分析计划覆盖 {len(plan)} 类分析。",
        ]
        inferences = [
            "缺失值和异常值比例将影响统计结论稳定性。",
            "若存在高相关数值列，可用于特征筛选但需防止共线性误判。",
            "类别分布不均衡时，分类相关结论需谨慎外推。",
        ]
        model_conclusions = [
            "当前流水线采用规则驱动通用分析，不绑定具体数据集字段。",
            "输出结果可作为后续建模与业务分析的基线。",
            "图表与统计表联合输出，便于人工复核。",
        ]
        risks = [
            "样本量不足或缺失值过高会降低结论可信度。",
            "自动字段语义识别存在启发式误差，需人工校验关键字段。",
            f"综合可信度: {validation.get('confidence', 'medium')}。",
        ]
        return facts, inferences, model_conclusions, risks
