#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker/cluster/docker-compose.generated.yml"
GENERATOR_SCRIPT="$SCRIPT_DIR/scripts/generate_docker_cluster.py"
CLUSTER_STATE_ROOT="$SCRIPT_DIR/docker/cluster/state"

usage() {
  echo "Uso: ./up_nodes.sh <quantidade_de_nodes> [--detach]"
  echo "Exemplo: ./up_nodes.sh 20"
  echo "Exemplo: ./up_nodes.sh 20 --detach"
}

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Comando obrigatorio nao encontrado: $command_name" >&2
    exit 1
  fi
}

resolve_python() {
  if command -v python3 >/dev/null 2>&1; then
    echo "python3"
    return
  fi

  if command -v python >/dev/null 2>&1; then
    echo "python"
    return
  fi

  echo "Nenhum interpretador Python encontrado (python3/python)." >&2
  exit 1
}

main() {
  if [[ $# -lt 1 || $# -gt 2 ]]; then
    usage
    exit 1
  fi

  local node_count="$1"
  local detach_flag="${2:-}"

  if ! [[ "$node_count" =~ ^[0-9]+$ ]]; then
    echo "A quantidade de nodes precisa ser um inteiro positivo." >&2
    exit 1
  fi

  if (( node_count < 2 )); then
    echo "Use pelo menos 2 nodes para manter os bootstraps fixos." >&2
    exit 1
  fi

  if [[ -n "$detach_flag" && "$detach_flag" != "--detach" ]]; then
    usage
    exit 1
  fi

  require_command docker
  local python_command
  python_command="$(resolve_python)"

  if ! docker info >/dev/null 2>&1; then
    echo "O Docker daemon nao esta acessivel. Inicie o Docker Desktop/engine antes de subir os nodes." >&2
    exit 1
  fi

  echo "Gerando cluster com $node_count nodes..."
  "$python_command" "$GENERATOR_SCRIPT" --nodes "$node_count"

  echo "Limpando bancos e logs locais do cluster..."
  if [[ -d "$CLUSTER_STATE_ROOT" ]]; then
    find "$CLUSTER_STATE_ROOT" -maxdepth 2 -type f -name "anonnetcore.db" -delete
    find "$CLUSTER_STATE_ROOT" -maxdepth 3 -type f -path "*/logs/*" -delete
  fi

  echo "Subindo containers..."
  if [[ "$detach_flag" == "--detach" ]]; then
    docker compose -f "$COMPOSE_FILE" up --build -d
  else
    docker compose -f "$COMPOSE_FILE" up --build
  fi
}

main "$@"
