# Local Multi-Service Architecture

## Services

1. `frontend-service` (`127.0.0.1:18000` default)
- Hosts UI pages (`/`, `/tools`, `/manage`)
- Hosts API docs center (`/api-docs`)
- Proxies API requests to orchestrator service

2. `orchestrator-service` (`127.0.0.1:18010` default)
- Core Data Interpreter chain orchestration
- Coordinates planner, executor, tool backend
- Persists records/sessions
- Exposes service topology/health aggregation: `GET /architecture/services`

3. `planner-service` (`127.0.0.1:18011` default)
- Intent recognition
- Rule/LLM code planning
- Tool-plan generation

4. `executor-service` (`127.0.0.1:18012` default)
- Sandbox code execution
- Returns table/plots/stdout/error

5. `tool-backend-service` (`127.0.0.1:18013` default)
- Tool package catalog
- Package details
- Tool runtime APIs

## Service APIs

### Orchestrator -> Planner
- `POST /intent_key`
- `POST /generate`
- `POST /generate_llm_only`
- `POST /plan_tool`
- `POST /build_tool_code`
- `POST /is_fallback_code`
- `POST /generate_auto_tool`
- `POST /repair_code`
- `POST /failure_solution`
- `POST /fallback_code`

### Orchestrator -> Executor
- `POST /execute`

### Orchestrator -> Tool Backend
- `GET /packages`
- `GET /packages/{package_id}`
- `POST /run/...`

When `TOOL_BACKEND_SERVICE_URL` is configured, orchestrator uses it as default tool catalog source and package install source.

## Boot sequence

Use:

```bash
./scripts/run_local_services.sh
```

Then access:
- Frontend: `http://127.0.0.1:18000`
- Orchestrator direct: `http://127.0.0.1:18010`

You can override ports via env vars:
- `HOST`
- `FRONTEND_PORT`
- `ORCHESTRATOR_PORT`
- `PLANNER_PORT`
- `EXECUTOR_PORT`
- `TOOL_BACKEND_PORT`
