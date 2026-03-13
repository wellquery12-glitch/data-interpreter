from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


class ResultValidator:
    def validate(self, df: pd.DataFrame, analysis_outputs: Dict[str, Any]) -> Dict[str, Any]:
        checks: List[Dict[str, str]] = []
        n = int(len(df))
        if n < 30:
            checks.append({"rule": "sample_size", "status": "warn", "message": f"样本量较小({n})，统计推断可信度下降"})
        else:
            checks.append({"rule": "sample_size", "status": "pass", "message": f"样本量({n})可支持基础统计分析"})

        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if len(numeric_cols) >= 2:
            corr = df[numeric_cols].corr(numeric_only=True).abs()
            high = float(corr.where(~corr.eye(len(corr), dtype=bool)).max().max()) if len(corr) else 0.0
            if high > 0.9:
                checks.append({"rule": "correlation_stability", "status": "warn", "message": "存在强相关关系，需关注异常值驱动风险"})
            else:
                checks.append({"rule": "correlation_stability", "status": "pass", "message": "相关性水平正常"})

        missing_rate = float(df.isna().mean().mean()) if n else 0.0
        if missing_rate > 0.2:
            checks.append({"rule": "missing_data", "status": "warn", "message": f"整体缺失率较高({missing_rate:.2%})"})
        else:
            checks.append({"rule": "missing_data", "status": "pass", "message": f"整体缺失率可控({missing_rate:.2%})"})

        warn_count = sum(1 for c in checks if c["status"] == "warn")
        if warn_count == 0:
            confidence = "high"
        elif warn_count <= 2:
            confidence = "medium"
        else:
            confidence = "low"

        consistency = "pass" if analysis_outputs.get("chart_count", 0) >= 1 and analysis_outputs.get("analysis_count", 0) >= 3 else "warn"
        checks.append(
            {
                "rule": "output_consistency",
                "status": consistency,
                "message": "图表与分析数量满足硬性要求" if consistency == "pass" else "图表或分析数量未达标",
            }
        )

        return {
            "checks": checks,
            "confidence": confidence,
        }
