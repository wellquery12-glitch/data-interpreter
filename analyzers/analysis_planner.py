from __future__ import annotations

from typing import List

from analyzers.data_understanding import DataProfile


class AnalysisPlanner:
    CANDIDATES = [
        "basic_descriptive_statistics",
        "missing_value_analysis",
        "univariate_distribution_analysis",
        "bivariate_relationship_analysis",
        "multivariate_correlation_analysis",
        "classification_target_analysis",
        "regression_target_analysis",
        "clustering_exploration_analysis",
        "time_series_trend_analysis",
        "anomaly_detection_analysis",
        "category_proportion_analysis",
        "group_comparison_analysis",
        "feature_importance_proxy_analysis",
        "dimensionality_reduction_visualization",
        "text_length_frequency_analysis",
    ]

    def plan(self, profile: DataProfile) -> List[str]:
        picks: List[str] = []
        picks.append("basic_descriptive_statistics")
        picks.append("missing_value_analysis")

        if profile.numeric_cols:
            picks.append("univariate_distribution_analysis")
            picks.append("multivariate_correlation_analysis")
            picks.append("anomaly_detection_analysis")

        if profile.categorical_cols:
            picks.append("category_proportion_analysis")
            picks.append("group_comparison_analysis")

        if profile.datetime_cols and profile.numeric_cols:
            picks.append("time_series_trend_analysis")

        if profile.label_col:
            if profile.label_col in profile.categorical_cols:
                picks.append("classification_target_analysis")
            elif profile.label_col in profile.numeric_cols:
                picks.append("regression_target_analysis")

        if profile.text_cols:
            picks.append("text_length_frequency_analysis")

        uniq: List[str] = []
        for name in picks:
            if name not in uniq:
                uniq.append(name)

        if len(uniq) < 3:
            for name in self.CANDIDATES:
                if name not in uniq:
                    uniq.append(name)
                if len(uniq) >= 3:
                    break
        return uniq
