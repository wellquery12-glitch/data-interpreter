# Data Interpreter 智能体系统（MVP）

这是一个可本地运行的 Data Interpreter 智能体系统，支持：
- 上传 CSV / XLSX 数据集
- 用自然语言提问
- 自动生成并执行 Python 数据分析代码
- 返回文本答案、表格预览、图表文件路径

## 快速启动

```bash
cd <PROJECT_ROOT>
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8010
```

默认项目地址记为：`<PROJECT_URL>`

打开接口文档：`<PROJECT_URL>/docs`
打开网页端工具：`<PROJECT_URL>/`
打开 LLM 管理页：`<PROJECT_URL>/manage`
打开工具管理页：`<PROJECT_URL>/tools`
打开接口文档中心：`<PROJECT_URL>/api-docs`

## 多服务分层运行（本地）

项目支持前后端及执行链路拆分为独立本地服务，通过 API 通信：
- `frontend-service` `:18000`（默认）
- `orchestrator-service` `:18010`（默认）
- `planner-service` `:18011`（默认）
- `executor-service` `:18012`（默认）
- `tool-backend-service` `:18013`（默认）

一键启动：

```bash
cd <PROJECT_ROOT>
./scripts/run_local_services.sh
```

前端入口（推荐）：`<PROJECT_URL>`
接口文档中心：`<PROJECT_URL>/api-docs`
如果系统里有多个 Python 环境，建议显式指定：

```bash
cd <PROJECT_ROOT>
PYTHON_BIN=<PROJECT_ROOT>/.venv/bin/python ./scripts/run_local_services.sh
```

如需自定义端口，可在启动时覆盖环境变量：

```bash
HOST=0.0.0.0 \
FRONTEND_PORT=19000 \
ORCHESTRATOR_PORT=19010 \
PLANNER_PORT=19011 \
EXECUTOR_PORT=19012 \
TOOL_BACKEND_PORT=19013 \
./scripts/run_local_services.sh
```

如需让工具包返回“项目地址”而非本机地址，可设置：

```bash
TOOL_BACKEND_PUBLIC_URL=<PROJECT_URL> ./scripts/run_local_services.sh
```
更多见 [`ARCHITECTURE.md`](./ARCHITECTURE.md)

## API

0) 查看已有数据集
- `GET /datasets`
- 返回本地 `storage` 中已存在的数据文件列表，可用于前端下拉选择
- 支持别名字段 `alias`
- `PUT /datasets/{dataset_id}/alias`：设置/更新别名（传空字符串可清空）
- `DELETE /datasets/{dataset_id}`：删除数据集文件及元信息

1) 上传数据集
- `POST /upload`
- form-data: `file`(CSV/XLSX)
- 校验规则：拒绝空文件；单文件最大 20MB；文件内容必须可被解析为有效 CSV/XLSX

返回：
- `dataset_id`
- 数据行列信息与字段列表

2) 提问分析
- `POST /ask`
- JSON:

```json
{
  "dataset_id": "...",
  "question": "哪个司机运单最多？",
  "max_rows": 20
}
```

返回：
- `answer`: 文本总结
- `generated_code`: 智能体生成并执行的 Python 代码
- `table`: 结果表（最多 max_rows 行）
- `plots`: 生成的图表 URL（可直接在网页展示）
- `stdout`: 执行日志
- `llm_trace`: 本次执行链路中每次 LLM 调用的 token/cost 明细
- `llm_usage_delta`: 本次请求新增 token/cost 汇总
- `llm_quota`: 当前预算状态（是否达到阈值/是否耗尽）

3) LLM 配置与资源管理
- `GET /llm/config`: 获取当前 LLM 配置、累计 token/cost、预算状态
- `PUT /llm/config`: 更新 LLM 参数（api_key/base_url/model/价格/额度/阈值等）
- `POST /llm/reset`: 重置累计 token 与成本统计

4) 远程工具包管理（运行时生效，无需重启）
- `GET /tools/state`: 查看已安装工具包、工具列表、当前选中工具
- `GET /tools/catalog?source_url=...`: 从远程工具平台拉取可安装工具包清单
- `GET /tools/catalog`（不传 source_url）: 返回内置可选工具包列表（用于前端默认展示）
- `POST /tools/packages/install`: 按工具包安装（`source_url+package_id` 或 `manifest_url`）
- `POST /tools/add`: 手动添加单个远程工具（可指定归属的已安装工具包）
- `POST /tools/select`: 保存用户选中的工具列表
- `POST /tools/{tool_id}/enabled`: 启用/停用工具
- `GET /architecture/services`: 查看当前分层服务配置与健康状态（planner/executor/tool-backend）
- `GET /bugs/summary?limit=60`: 错误类型聚合统计（按意图+错误信息，含是否已修复标记）

## 智能体策略

- `auto`：优先尝试 LLM，若未配置 `OPENAI_API_KEY`（或 SDK 不可用）会自动降级到内置规则生成器，离线可用。
- `smart`：优先使用 LLM；当 LLM 不可用时会自动降级到 `auto` 路径，并在返回中标记降级原因。
- `tool`：固定走工具规划路径（规则 + 可选 LLM 规划）。
- `tool` 模式会优先尝试用户选中的远程工具（通过 API 实时访问），未命中时再回退本地工具规划。
- 代码生成阶段会自动注入“用户已选工具”的名称、描述、输入输出 schema 作为上下文，便于动态集成新工具能力。
- 分析过程使用分层图结构（问题→意图→策略→代码/工具→执行→结果）进行规划，并在每次分析后自动生成执行图。
- 工具与工具包在运行时动态加载到 `sessions/tools_hub.json`，新增后立即可用，无需重启服务。
- 若配置 `TOOL_BACKEND_SERVICE_URL`，`/tools/catalog` 与工具包安装会默认走远程工具后端服务，不传 `source_url` 也可直接获取/安装。
- 支持 OpenAI 兼容接口（`api_key + base_url + model`），可接入主流兼容模型服务。
- 资源治理：达到阈值（默认 80%）提醒；额度耗尽后自动阻断 LLM 调用，直到更新配置或重置资源统计。
- 当前版本已停用本地代码缓存复用，每次按当前问句重新规划代码，避免历史缓存污染结果。

## 目录结构

- `app/main.py`: API 层
- `app/agent.py`: 智能体编排
- `app/planner.py`: 代码生成器（规则 + 可扩展 LLM）
- `app/sandbox.py`: 代码沙箱执行器
- `tests/test_planner.py`: 规则生成测试
