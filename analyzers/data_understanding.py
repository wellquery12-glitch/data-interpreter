from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd


@dataclass
class DataProfile:
    rows: int
    cols: int
    column_types: Dict[str, str]
    numeric_cols: List[str]
    categorical_cols: List[str]
    datetime_cols: List[str]
    text_cols: List[str]
    id_cols: List[str]
    label_col: str
    quality: Dict[str, Any]


class DataUnderstandingAnalyzer:
    def profile(self, df: pd.DataFrame) -> DataProfile:
        column_types: Dict[str, str] = {}
        numeric_cols: List[str] = []
        categorical_cols: List[str] = []
        datetime_cols: List[str] = []
        text_cols: List[str] = []
        id_cols: List[str] = []

        for col in df.columns:
            name = str(col)
            s = df[col]
            lname = name.lower()

            if "id" in lname and s.nunique(dropna=True) >= max(3, int(len(s) * 0.6)):
                column_types[name] = "id"
                id_cols.append(name)
                continue

            if pd.api.types.is_numeric_dtype(s):
                column_types[name] = "numeric"
                numeric_cols.append(name)
                continue

            parsed = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)
            if parsed.notna().mean() > 0.7:
                column_types[name] = "datetime"
                datetime_cols.append(name)
                continue

            nunique = s.nunique(dropna=True)
            ratio = nunique / max(1, len(s))
            if ratio < 0.3:
                column_types[name] = "categorical"
                categorical_cols.append(name)
            else:
                column_types[name] = "text"
                text_cols.append(name)

        label_col = ""
        if categorical_cols:
            cand = sorted(categorical_cols, key=lambda c: df[c].nunique(dropna=True))
            for c in cand:
                n = df[c].nunique(dropna=True)
                if 2 <= n <= 20:
                    label_col = c
                    break
        if not label_col and numeric_cols and len(numeric_cols) == 1:
            label_col = numeric_cols[0]

        quality = self._quality_checks(df=df, numeric_cols=numeric_cols, categorical_cols=categorical_cols)
        return DataProfile(
            rows=int(df.shape[0]),
            cols=int(df.shape[1]),
            column_types=column_types,
            numeric_cols=numeric_cols,
            categorical_cols=categorical_cols,
            datetime_cols=datetime_cols,
            text_cols=text_cols,
            id_cols=id_cols,
            label_col=label_col,
            quality=quality,
        )

    def _quality_checks(self, df: pd.DataFrame, numeric_cols: List[str], categorical_cols: List[str]) -> Dict[str, Any]:
        missing_rate = {str(c): float(df[c].isna().mean()) for c in df.columns}
        duplicate_rows = int(df.duplicated().sum())
        constant_cols = [str(c) for c in df.columns if df[c].nunique(dropna=True) <= 1]
        high_cardinality = [str(c) for c in categorical_cols if df[c].nunique(dropna=True) > max(20, int(len(df) * 0.3))]

        outlier_cols: Dict[str, float] = {}
        for c in numeric_cols:
            s = pd.to_numeric(df[c], errors="coerce").dropna()
            if s.empty:
                outlier_cols[str(c)] = 0.0
                continue
            q1 = s.quantile(0.25)
            q3 = s.quantile(0.75)
            iqr = q3 - q1
            if iqr <= 0:
                outlier_cols[str(c)] = 0.0
                continue
            mask = (s < q1 - 1.5 * iqr) | (s > q3 + 1.5 * iqr)
            outlier_cols[str(c)] = float(mask.mean())

        dtype_issues = []
        for c in df.columns:
            s = df[c]
            if pd.api.types.is_object_dtype(s):
                parsed_num = pd.to_numeric(s, errors="coerce")
                if parsed_num.notna().mean() > 0.8:
                    dtype_issues.append(str(c))

        return {
            "missing_rate": missing_rate,
            "duplicate_rows": duplicate_rows,
            "constant_columns": constant_cols,
            "high_cardinality_columns": high_cardinality,
            "outlier_ratio": outlier_cols,
            "dtype_issue_columns": dtype_issues,
        }
