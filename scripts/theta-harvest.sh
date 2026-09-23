#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
PID_FILE="$ROOT_DIR/run/theta-harvest.pid"

usage() {
  cat >&2 <<EOF
Usage:
  $0 start YYYY-MM-DD YYYY-MM-DD [--storage csv|clickhouse] [--force]
  $0 status
  $0 stop
EOF
}

read_pid() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid="$(<"$PID_FILE")"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "$pid"
}

process_is_running() {
  kill -0 "$1" 2>/dev/null
}

process_is_ours() {
  local pid="$1"
  local command_line
  [[ -r "/proc/$pid/cmdline" ]] || return 1
  command_line="$(tr '\0' ' ' <"/proc/$pid/cmdline")"
  [[ "$command_line" == *"$ROOT_DIR/main.py"* ]]
}

find_python() {
  local candidate
  for candidate in python3.12 python3.13 python3.14 python3; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import sys; raise SystemExit(sys.version_info < (3, 12))'; then
      command -v "$candidate"
      return 0
    fi
  done
  return 1
}

ensure_runtime() {
  if [[ ! -f "$ROOT_DIR/config.toml" ]]; then
    echo "Error: $ROOT_DIR/config.toml does not exist. Copy config.example.toml and configure it first." >&2
    return 1
  fi

  local python_bin
  python_bin="$(find_python || true)"
  if [[ -z "$python_bin" ]]; then
    echo "Error: Python 3.12 or newer is required." >&2
    return 1
  fi

  local venv_dir="$ROOT_DIR/.venv"
  local venv_python="$venv_dir/bin/python"
  if [[ ! -x "$venv_python" ]]; then
    echo "Creating virtual environment: $venv_dir"
    "$python_bin" -m venv "$venv_dir"
    "$venv_python" -m pip install --upgrade pip
    "$venv_python" -m pip install -r "$ROOT_DIR/requirements.txt"
  elif ! "$venv_python" -c 'import sys; raise SystemExit(sys.version_info < (3, 12))'; then
    echo "Error: $venv_dir was created with Python below 3.12. Back it up or remove it, then start again." >&2
    return 1
  fi
}

start_service() {
  if [[ $# -lt 2 ]]; then
    usage
    return 2
  fi

  local start_date="$1"
  local end_date="$2"
  shift 2
  local -a extra_arguments=("$@")
  local date_pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
  if [[ ! "$start_date" =~ $date_pattern || ! "$end_date" =~ $date_pattern ]]; then
    echo "Error: dates must use YYYY-MM-DD format." >&2
    return 2
  fi
  cd "$ROOT_DIR"
  mkdir -p "$ROOT_DIR/logs" "$ROOT_DIR/run"

  local existing_pid
  existing_pid="$(read_pid || true)"
  if [[ -n "$existing_pid" ]] && process_is_running "$existing_pid"; then
    if process_is_ours "$existing_pid"; then
      echo "Error: theta-harvest is already running with PID $existing_pid." >&2
    else
      echo "Error: PID $existing_pid belongs to another process; refusing to overwrite the PID file." >&2
    fi
    return 1
  fi
  rm -f -- "$PID_FILE"

  ensure_runtime

  local run_id
  local log_file
  local process_pid
  run_id="$(date '+%Y%m%d-%H%M%S')"
  log_file="$ROOT_DIR/logs/theta-harvest-$run_id.log"

  local -a command=(
    "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/main.py"
    --start-date "$start_date" --end-date "$end_date"
  )
  command+=("${extra_arguments[@]}")
  nohup "${command[@]}" >"$log_file" 2>&1 &
  process_pid=$!
  printf '%s\n' "$process_pid" >"$PID_FILE"

  sleep 1
  if ! process_is_running "$process_pid"; then
    echo "Error: theta-harvest exited during startup. Recent log output:" >&2
    tail -n 50 "$log_file" >&2 || true
    rm -f -- "$PID_FILE"
    return 1
  fi

  echo "theta-harvest started successfully."
  echo "PID: $process_pid"
  echo "Log: $log_file"
  echo "Follow log: tail -f '$log_file'"
}

show_status() {
  local pid
  pid="$(read_pid || true)"
  if [[ -z "$pid" ]]; then
    echo "theta-harvest is not running (no valid PID file)."
    return 1
  fi
  if ! process_is_running "$pid"; then
    echo "theta-harvest is not running (stale PID $pid)."
    return 1
  fi
  if ! process_is_ours "$pid"; then
    echo "theta-harvest is not running (PID $pid belongs to another process)." >&2
    return 1
  fi
  echo "theta-harvest is running with PID $pid."
  ps -p "$pid" -f
}

stop_service() {
  local pid
  pid="$(read_pid || true)"
  if [[ -z "$pid" ]]; then
    echo "theta-harvest is not running (no valid PID file)."
    return 0
  fi
  if ! process_is_running "$pid"; then
    echo "theta-harvest is already stopped; removing stale PID $pid."
    rm -f -- "$PID_FILE"
    return 0
  fi
  if ! process_is_ours "$pid"; then
    echo "Error: PID $pid belongs to another process; it was not stopped." >&2
    return 1
  fi

  echo "Stopping theta-harvest with PID $pid..."
  kill -TERM "$pid"
  local attempt
  for attempt in {1..30}; do
    if ! process_is_running "$pid"; then
      rm -f -- "$PID_FILE"
      echo "theta-harvest stopped successfully."
      return 0
    fi
    sleep 1
  done

  echo "Error: PID $pid is still running after 30 seconds; the PID file was kept." >&2
  return 1
}

if [[ $# -lt 1 ]]; then
  usage
  exit 2
fi

COMMAND="$1"
shift
case "$COMMAND" in
  start)
    start_service "$@"
    ;;
  status)
    [[ $# -eq 0 ]] || { usage; exit 2; }
    show_status
    ;;
  stop)
    [[ $# -eq 0 ]] || { usage; exit 2; }
    stop_service
    ;;
  *)
    usage
    exit 2
    ;;
esac
