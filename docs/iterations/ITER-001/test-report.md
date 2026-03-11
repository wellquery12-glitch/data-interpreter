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

## 6. 需求变更后的补充验证（2026-03-11）
- 新增主题单选：销售/运营/财务/客户。
- 新增用户需求输入并参与自动分析。
- 修复报告结果查看：结果区域支持滚动，新增图表展示。
- 修复 PDF 导出乱码：采用 CJK 字体映射输出（`STSong-Light` + `UniGB-UCS2-H`）。
- 执行结果：通过（见 34 passed）。

## 7. 导出图片嵌入修复验证（2026-03-11）
- 问题：导出文件中仅有图片地址，未嵌入图片内容。
- 修复点：
  - Markdown 导出改为内嵌 data URI 图片。
  - PDF 导出追加图表页并嵌入图片。
- 验证命令：`pytest -q tests/test_insights.py tests/test_agent_recovery.py tests/test_tools_hub.py`
- 验证结果：`34 passed, 1 warning`。
