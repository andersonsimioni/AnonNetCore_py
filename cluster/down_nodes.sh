#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.generated.yml"

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required command not found: $command_name" >&2
    exit 1
  fi
}

main() {
  require_command docker

  if [[ ! -f "$COMPOSE_FILE" ]]; then
    echo "Generated compose file not found at: $COMPOSE_FILE" >&2
    exit 1
  fi

  if ! docker info >/dev/null 2>&1; then
    echo "Docker daemon is not accessible. Start Docker Desktop/engine before stopping nodes." >&2
    exit 1
  fi

echo "Stopping cluster containers..."
  (
    cd "$PROJECT_ROOT"
    docker compose -f "$COMPOSE_FILE" down
  )
}

main "$@"
