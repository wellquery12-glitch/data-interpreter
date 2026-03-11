#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SESSIONS_DIR="${ROOT}/sessions"

FRONTEND_PORT="${FRONTEND_PORT:-18000}"
ORCHESTRATOR_PORT="${ORCHESTRATOR_PORT:-18010}"
PLANNER_PORT="${PLANNER_PORT:-18011}"
EXECUTOR_PORT="${EXECUTOR_PORT:-18012}"
TOOL_BACKEND_PORT="${TOOL_BACKEND_PORT:-18013}"

stop_pid_file() {
  local name="$1"
  local pid_file="$2"

  if [[ ! -f "${pid_file}" ]]; then
    return 0
  fi

  local pid
  pid="$(cat "${pid_file}")"

  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true

    local i
    for i in $(seq 1 25); do
      if ! kill -0 "${pid}" 2>/dev/null; then
        break
      fi
      sleep 0.2
    done

    if kill -0 "${pid}" 2>/dev/null; then
      kill -KILL "${pid}" 2>/dev/null || true
      sleep 0.2
    fi

    if kill -0 "${pid}" 2>/dev/null; then
      echo "[WARN] ${name} 仍在运行: ${pid}"
    else
      echo "[STOP] ${name} (${pid})"
    fi
  else
    echo "[SKIP] ${name} pid 不存在或已退出: ${pid}"
  fi

  rm -f "${pid_file}"
}

kill_port() {
  local port="$1"
  local pids
  pids="$(lsof -ti "tcp:${port}" 2>/dev/null || true)"
  if [[ -n "${pids}" ]]; then
    kill -TERM ${pids} 2>/dev/null || true

    local i
    for i in $(seq 1 15); do
      pids="$(lsof -ti "tcp:${port}" 2>/dev/null || true)"
      if [[ -z "${pids}" ]]; then
        break
      fi
      sleep 0.2
    done

    if [[ -n "${pids}" ]]; then
      kill -KILL ${pids} 2>/dev/null || true
      sleep 0.2
      pids="$(lsof -ti "tcp:${port}" 2>/dev/null || true)"
    fi

    if [[ -n "${pids}" ]]; then
      echo "[WARN] tcp:${port} 仍被占用: ${pids}"
    else
      echo "[CLEAN] tcp:${port}"
    fi
  fi
}

stop_pid_file "frontend-service" "${SESSIONS_DIR}/frontend.pid"
stop_pid_file "orchestrator-service" "${SESSIONS_DIR}/orchestrator.pid"
stop_pid_file "tool-backend-service" "${SESSIONS_DIR}/tool_backend.pid"
stop_pid_file "executor-service" "${SESSIONS_DIR}/executor.pid"
stop_pid_file "planner-service" "${SESSIONS_DIR}/planner.pid"

kill_port "${FRONTEND_PORT}"
kill_port "${ORCHESTRATOR_PORT}"
kill_port "${PLANNER_PORT}"
kill_port "${EXECUTOR_PORT}"
kill_port "${TOOL_BACKEND_PORT}"

# Backward-compatible cleanup for old default ports.
kill_port 8000
kill_port 8010
kill_port 8011
kill_port 8012
kill_port 8013

echo "local services stopped"
