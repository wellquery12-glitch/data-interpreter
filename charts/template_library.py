from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import matplotlib

matplotlib.use("Agg")
import pandas as pd
from matplotlib import pyplot as plt


class ChartTemplateLibrary:
    def render_minimum_set(self, df: pd.DataFrame, output_dir: Path, plan: List[str]) -> List[str]:
        output_dir.mkdir(parents=True, exist_ok=True)
        files: List[str] = []
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        cat_cols = [c for c in df.columns if c not in num_cols]

        if num_cols:
            c = str(num_cols[0])
            fig, ax = plt.subplots(figsize=(6, 4))
            pd.to_numeric(df[c], errors="coerce").dropna().hist(ax=ax, bins=20)
            ax.set_title(f"Histogram: {c}")
            path = output_dir / "histogram.png"
            fig.savefig(path, dpi=120, bbox_inches="tight")
            plt.close(fig)
            files.append(str(path))

        if len(num_cols) >= 2:
            x = str(num_cols[0])
            y = str(num_cols[1])
            fig, ax = plt.subplots(figsize=(6, 4))
            ax.scatter(pd.to_numeric(df[x], errors="coerce"), pd.to_numeric(df[y], errors="coerce"), alpha=0.6)
            ax.set_xlabel(x)
            ax.set_ylabel(y)
            ax.set_title(f"Scatter: {x} vs {y}")
            path = output_dir / "scatter.png"
            fig.savefig(path, dpi=120, bbox_inches="tight")
            plt.close(fig)
            files.append(str(path))

            corr = df[num_cols].corr(numeric_only=True)
            fig, ax = plt.subplots(figsize=(6, 5))
            im = ax.imshow(corr.values, cmap="coolwarm", aspect="auto")
            ax.set_xticks(range(len(corr.columns)))
            ax.set_yticks(range(len(corr.index)))
            ax.set_xticklabels([str(c) for c in corr.columns], rotation=45, ha="right")
            ax.set_yticklabels([str(c) for c in corr.index])
            ax.set_title("Correlation Matrix")
            fig.colorbar(im, ax=ax)
            path = output_dir / "correlation_matrix.png"
            fig.savefig(path, dpi=120, bbox_inches="tight")
            plt.close(fig)
            files.append(str(path))

        if cat_cols:
            c = str(cat_cols[0])
            vc = df[c].astype(str).value_counts().head(10)
            fig, ax = plt.subplots(figsize=(7, 4))
            vc.plot(kind="bar", ax=ax)
            ax.set_title(f"Category Distribution: {c}")
            path = output_dir / "bar.png"
            fig.savefig(path, dpi=120, bbox_inches="tight")
            plt.close(fig)
            files.append(str(path))

        return files

    @staticmethod
    def chart_index(chart_files: List[str]) -> List[Dict[str, str]]:
        return [{"name": Path(p).name, "path": p} for p in chart_files]
