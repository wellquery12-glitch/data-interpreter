# ITER-003 测试报告

## 1. 语法校验
```bash
python3 -m py_compile \
  configs/template_registry.py \
  analyzers/data_understanding.py \
  analyzers/analysis_planner.py \
  charts/template_library.py \
  validators/result_validator.py \
  reports/template_renderer.py \
  pipelines/uci_auto_pipeline.py
```

## 2. 端到端执行
```bash
scripts/run_iter003_demo.sh
```

## 3. 验证结果
- 流水线成功生成报告与图表。
- 输出包含可信度评估与校验项。
- 模板注册计数满足要求（图表10、报表20）。
