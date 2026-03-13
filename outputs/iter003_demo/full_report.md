# Full Report - demo

## 1. 数据集概览
- 行数: 3
- 列数: 4
- 数值列: amount
- 类别列: -
- 时间列: trade_date

## 2. 分析规划
- basic_descriptive_statistics
- missing_value_analysis
- univariate_distribution_analysis
- multivariate_correlation_analysis
- anomaly_detection_analysis
- time_series_trend_analysis
- regression_target_analysis
- text_length_frequency_analysis

## 3. 数据事实
- 数据集包含 3 行、4 列。
- 识别数值列 1 个、类别列 0 个、时间列 1 个。
- 分析计划覆盖 8 类分析。

## 4. 统计推断
- 缺失值和异常值比例将影响统计结论稳定性。
- 若存在高相关数值列，可用于特征筛选但需防止共线性误判。
- 类别分布不均衡时，分类相关结论需谨慎外推。

## 5. 模型结论
- 当前流水线采用规则驱动通用分析，不绑定具体数据集字段。
- 输出结果可作为后续建模与业务分析的基线。
- 图表与统计表联合输出，便于人工复核。

## 6. 风险与限制
- 样本量不足或缺失值过高会降低结论可信度。
- 自动字段语义识别存在启发式误差，需人工校验关键字段。
- 综合可信度: medium。

## 7. 校验步骤与可信度
- [warn] sample_size: 样本量较小(3)，统计推断可信度下降
- [pass] missing_data: 整体缺失率可控(0.00%)
- [pass] output_consistency: 图表与分析数量满足硬性要求
- 可信度: medium

## 8. 图表索引
- histogram.png: outputs/iter003_demo/charts/histogram.png
- bar.png: outputs/iter003_demo/charts/bar.png

## 9. 报表模板覆盖
- dataset_overview
- field_type_identification
- missing_values
- duplicate_values
- outlier_detection
- numeric_statistics
- category_distribution
- label_distribution
- correlation
- group_aggregation
- top_bottom
- high_risk_fields
- feature_label_relation
- classification_evaluation
- regression_evaluation
- clustering_result
- time_trend
- text_overview
- chart_index
- final_summary