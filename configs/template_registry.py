from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class TemplateRegistry:
    chart_templates: List[str]
    report_templates: List[str]


CHART_TEMPLATES: List[str] = [
    "histogram",
    "boxplot",
    "violin",
    "bar",
    "stacked_bar",
    "line",
    "scatter",
    "bubble",
    "heatmap",
    "correlation_matrix",
]

REPORT_TEMPLATES: List[str] = [
    "dataset_overview",
    "field_type_identification",
    "missing_values",
    "duplicate_values",
    "outlier_detection",
    "numeric_statistics",
    "category_distribution",
    "label_distribution",
    "correlation",
    "group_aggregation",
    "top_bottom",
    "high_risk_fields",
    "feature_label_relation",
    "classification_evaluation",
    "regression_evaluation",
    "clustering_result",
    "time_trend",
    "text_overview",
    "chart_index",
    "final_summary",
]


def load_registry() -> TemplateRegistry:
    return TemplateRegistry(chart_templates=CHART_TEMPLATES, report_templates=REPORT_TEMPLATES)
