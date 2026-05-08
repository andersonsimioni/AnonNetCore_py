#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.generated.yml"

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Comando obrigatorio nao encontrado: $command_name" >&2
    exit 1
  fi
}

main() {
  require_command docker

  if [[ ! -f "$COMPOSE_FILE" ]]; then
    echo "Compose gerado nao encontrado em: $COMPOSE_FILE" >&2
    exit 1
  fi

  if ! docker info >/dev/null 2>&1; then
    echo "O Docker daemon nao esta acessivel. Inicie o Docker Desktop/engine antes de derrubar os nodes." >&2
    exit 1
  fi

  echo "Derrubando containers do cluster..."
  (
    cd "$PROJECT_ROOT"
    docker compose -f "$COMPOSE_FILE" down
  )
}

main "$@"
