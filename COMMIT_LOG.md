# Commit Log

## 2026-03-11 - 提交记录 1
- 修改内容:
  - 调整 `.gitignore`，默认忽略 `storage/*` 与 `sessions/*`，仅保留 `storage/testdata/**` 与 `sessions/test_logs/**`。
  - 新增测试样例数据 `storage/testdata/demo.csv`。
  - 新增测试样例日志 `sessions/test_logs/demo_records.jsonl`。
  - 更新 `services/tool_backend_service/main.py`，工具包 endpoint 由硬编码本机地址改为基于项目地址动态生成（支持 `TOOL_BACKEND_PUBLIC_URL`）。
  - 更新 `services/frontend_service/main.py`，`/health` 不再暴露内部服务本机地址。
  - 更新 `README.md`，移除本机绝对路径/本机地址示例，改为 `<PROJECT_ROOT>` 与 `<PROJECT_URL>`。
- 修改原因:
  - 避免把用户真实数据集和运行记录上传到仓库。
  - 避免在代码与文档中暴露本机地址信息。
  - 统一以项目地址作为对外初始地址。
- 作用:
  - 仓库可保留可复现的测试数据与测试日志，同时自动屏蔽用户数据。
  - 部署和协作时不依赖个人机器路径，减少敏感信息泄露风险。
  - 远程工具安装后默认 endpoint 更通用，适配不同环境域名。

## 2026-03-11 - 提交记录 2
- 修改内容:
  - 新增 `docs/process.md`，定义项目标准迭代流程与强制执行规则。
- 修改原因:
  - 明确后续迭代的统一流程，避免需求、开发、测试、验收阶段缺失。
- 作用:
  - 为后续每次迭代提供标准执行路径与留痕依据，提升可追踪性与交付稳定性。
