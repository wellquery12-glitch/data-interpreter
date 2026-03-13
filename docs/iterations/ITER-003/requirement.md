# ITER-003 需求文档（已确认）

## 1. 背景
来源于 `docs/UCI数据分析需求.docx`：目标是面向 UCI 数据集形成通用分析 agent，具备下载、理解、规划、分析、校验、报告沉淀能力。

## 2. 目标
1. 支持数据集下载（URL/名称）与格式识别（csv/xlsx/data/arff/txt/zip）。
2. 自动识别字段类型与数据质量问题。
3. 自动规划不少于 3 类分析并至少输出 1 个图表。
4. 沉淀至少 10 种图表模板与 20 种报表模板注册。
5. 输出标准报告：`executive_summary.md`、`full_report.md`、`machine_readable_summary.json`、`full_report.html`。
6. 每个结论带可信度与校验说明。

## 3. 交付范围
- 新增目录：`datasets/`、`configs/`、`analyzers/`、`charts/`、`reports/`、`validators/`、`pipelines/`、`outputs/`。
- 新增主流程：`pipelines/uci_auto_pipeline.py`。
- 新增模板注册与分析模块。
- 新增样例运行脚本：`scripts/run_iter003_demo.sh`。

## 4. 验收标准
- [ ] 可运行流水线并生成 4 份报告文件。
- [ ] 分析计划至少 3 项。
- [ ] 至少生成 1 张图表。
- [ ] 图表模板注册数量 >= 10。
- [ ] 报表模板注册数量 >= 20。
- [ ] 报告中区分数据事实/统计推断/模型结论/风险限制。
