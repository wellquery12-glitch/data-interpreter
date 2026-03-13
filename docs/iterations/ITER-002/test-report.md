# ITER-002 测试报告

## 1. 测试范围
- 运营主题自动分析
- 财务主题自动分析
- 客户主题自动分析
- 报告导出能力（Markdown/PDF）
- 关键回归能力（销售主题、`/ask`、工具管理）

## 2. 执行命令
```bash
python3 -m pytest -q
```

```bash
scripts/e2e_repository_workflow.sh
```

## 3. 结果
- 结果：`43 passed, 974 warnings in 14.16s`
- 结论：通过。运营/财务/客户主题分析、导出能力与关键回归均未出现失败用例。

## 4. 新增测试用例
- `tests/test_insights.py::test_auto_business_insights_topic_specific_semantics`
  - 校验运营主题输出包含履约语义与 `operations_` 表格结果
  - 校验财务主题输出包含财务/利润语义与 `finance_` 表格结果
  - 校验客户主题输出包含客户语义与 `customer_` 表格结果

## 5. 风险与说明
- 当前测试环境会产生较多字体相关 `DeprecationWarning`（Pillow `getsize`），不影响本次功能正确性。
- 测试依赖 `python3 -m pytest`（系统 Python），如使用其他 Python 发行版需确认 `matplotlib` 字体环境一致性。
- 截至 2026-03-12，本地 `127.0.0.1:8010` 服务未启动，端到端脚本尚未在本机执行；已完成脚本语法校验（`bash -n`）。
- 端到端脚本支持 `SKIP_PUBLIC=1`，用于公网受限时先验证私人数据集链路。
