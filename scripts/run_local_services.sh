#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${ROOT}/sessions/service_logs"
mkdir -p "${LOG_DIR}"

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "${PYTHON_BIN}" ]]; then
  if [[ -x "${ROOT}/.venv/bin/python" ]]; then
    PYTHON_BIN="${ROOT}/.venv/bin/python"
  else
    PYTHON_BIN="$(command -v python3)"
  fi
fi

if ! "${PYTHON_BIN}" -m uvicorn --version >/dev/null 2>&1; then
  echo "[FAIL] 当前 Python 环境缺少 uvicorn: ${PYTHON_BIN}"
  echo "请先安装依赖，或指定正确解释器："
  echo "  cd ${ROOT}"
  echo "  python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
  echo "或："
  echo "  PYTHON_BIN=${ROOT}/.venv/bin/python ./scripts/run_local_services.sh"
  exit 1
fi

HOST="${HOST:-127.0.0.1}"
FRONTEND_PORT="${FRONTEND_PORT:-18000}"
ORCHESTRATOR_PORT="${ORCHESTRATOR_PORT:-18010}"
PLANNER_PORT="${PLANNER_PORT:-18011}"
EXECUTOR_PORT="${EXECUTOR_PORT:-18012}"
TOOL_BACKEND_PORT="${TOOL_BACKEND_PORT:-18013}"

FRONTEND_URL="http://${HOST}:${FRONTEND_PORT}"
ORCHESTRATOR_URL="http://${HOST}:${ORCHESTRATOR_PORT}"
PLANNER_URL="http://${HOST}:${PLANNER_PORT}"
EXECUTOR_URL="http://${HOST}:${EXECUTOR_PORT}"
TOOL_BACKEND_URL="http://${HOST}:${TOOL_BACKEND_PORT}"

kill_port() {
  local port="$1"
  local pids
  pids=$(lsof -ti "tcp:${port}" 2>/dev/null || true)
  if [[ -n "${pids}" ]]; then
    kill ${pids} 2>/dev/null || true
  fi
}

kill_port "${ORCHESTRATOR_PORT}"
kill_port "${PLANNER_PORT}"
kill_port "${EXECUTOR_PORT}"
kill_port "${TOOL_BACKEND_PORT}"
kill_port "${FRONTEND_PORT}"
# Backward-compatible cleanup for old default ports.
kill_port 8010
kill_port 8011
kill_port 8012
kill_port 8013
kill_port 8000

cd "${ROOT}"

start_service() {
  local name="$1"
  local log_file="$2"
  shift 2
  nohup "$@" >"${log_file}" 2>&1 < /dev/null &
  local pid=$!
  sleep 0.4
  if ! kill -0 "${pid}" 2>/dev/null; then
    echo "[FAIL] ${name} 启动失败，日志: ${log_file}"
    if [[ -f "${log_file}" ]]; then
      tail -n 60 "${log_file}" || true
    fi
    exit 1
  fi
  SERVICE_PID="${pid}"
}

check_health() {
  local name="$1"
  local url="$2"
  local log_file="$3"
  local i
  for i in $(seq 1 30); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "[FAIL] ${name} 健康检查失败: ${url}"
  if [[ -f "${log_file}" ]]; then
    tail -n 80 "${log_file}" || true
  fi
  exit 1
}

start_service planner-service "${LOG_DIR}/planner.log" "${PYTHON_BIN}" -m uvicorn services.planner_service.main:app --host "${HOST}" --port "${PLANNER_PORT}"
echo "${SERVICE_PID}" > "${ROOT}/sessions/planner.pid"

start_service executor-service "${LOG_DIR}/executor.log" "${PYTHON_BIN}" -m uvicorn services.executor_service.main:app --host "${HOST}" --port "${EXECUTOR_PORT}"
echo "${SERVICE_PID}" > "${ROOT}/sessions/executor.pid"

start_service tool-backend-service "${LOG_DIR}/tool_backend.log" "${PYTHON_BIN}" -m uvicorn services.tool_backend_service.main:app --host "${HOST}" --port "${TOOL_BACKEND_PORT}"
echo "${SERVICE_PID}" > "${ROOT}/sessions/tool_backend.pid"

start_service orchestrator-service "${LOG_DIR}/orchestrator.log" env \
  PLANNER_SERVICE_URL="${PLANNER_URL}" \
  EXECUTOR_SERVICE_URL="${EXECUTOR_URL}" \
  TOOL_BACKEND_SERVICE_URL="${TOOL_BACKEND_URL}" \
  "${PYTHON_BIN}" -m uvicorn app.main:app --host "${HOST}" --port "${ORCHESTRATOR_PORT}"
echo "${SERVICE_PID}" > "${ROOT}/sessions/orchestrator.pid"

start_service frontend-service "${LOG_DIR}/frontend.log" env \
  ORCHESTRATOR_URL="${ORCHESTRATOR_URL}" \
  PLANNER_SERVICE_URL="${PLANNER_URL}" \
  EXECUTOR_SERVICE_URL="${EXECUTOR_URL}" \
  TOOL_BACKEND_SERVICE_URL="${TOOL_BACKEND_URL}" \
  "${PYTHON_BIN}" -m uvicorn services.frontend_service.main:app --host "${HOST}" --port "${FRONTEND_PORT}"
echo "${SERVICE_PID}" > "${ROOT}/sessions/frontend.pid"

check_health "planner-service" "${PLANNER_URL}/health" "${LOG_DIR}/planner.log"
check_health "executor-service" "${EXECUTOR_URL}/health" "${LOG_DIR}/executor.log"
check_health "tool-backend-service" "${TOOL_BACKEND_URL}/health" "${LOG_DIR}/tool_backend.log"
check_health "orchestrator-service" "${ORCHESTRATOR_URL}/health" "${LOG_DIR}/orchestrator.log"
check_health "frontend-service" "${FRONTEND_URL}/health" "${LOG_DIR}/frontend.log"

echo "frontend:     ${FRONTEND_URL}"
echo "orchestrator: ${ORCHESTRATOR_URL}"
echo "planner:      ${PLANNER_URL}"
echo "executor:     ${EXECUTOR_URL}"
echo "tool-backend: ${TOOL_BACKEND_URL}"
