# ITER-001 测试报告

## 1. 测试范围
- 新增销售自动分析能力（后端）
- 新增 Markdown/PDF 导出能力
- 新增商业建议页面前端接入
- 关键回归（历史分析与工具能力）

## 2. 执行命令
```bash
pytest -q tests/test_insights.py tests/test_agent_recovery.py tests/test_tools_hub.py
```

## 3. 结果
- 结果：33 passed, 1 warning
- 结论：本次新增能力通过测试，关键回归通过。

## 4. 新增测试用例
- `tests/test_insights.py`
  - `test_auto_sales_insights_generates_structured_output`
  - `test_auto_sales_insights_without_amount_field`
  - `test_export_insights_markdown_and_pdf`

## 5. 风险与说明
- PDF 导出采用轻量文本 PDF 生成方式，优先保证稳定性与可下载性。
- 中文在极少数字体环境下可能存在显示差异，不影响导出成功。
