#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

VENV_DIR="$ROOT_DIR/.venv"
PYTHON_BIN="$VENV_DIR/bin/python3"
PIP_BIN="$VENV_DIR/bin/pip"

ensure_python() {
  if ! command -v python3 >/dev/null 2>&1; then
    echo "[RAG_Rokas] python3 not found. Install Python 3 first."
    exit 1
  fi
}

ensure_venv() {
  if [ ! -d "$VENV_DIR" ]; then
    echo "[RAG_Rokas] Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
  fi
}

install_deps() {
  echo "[RAG_Rokas] Installing dependencies..."
  "$PIP_BIN" install --upgrade pip >/dev/null
  "$PIP_BIN" install -r requirements.txt >/dev/null
}

es_up() {
  curl --silent --show-error --max-time 2 http://localhost:9200 >/dev/null 2>&1
}

start_es_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    return 1
  fi

  if docker ps --format '{{.Names}}' | grep -q '^rag-rokas-es$'; then
    return 0
  fi

  if docker ps -a --format '{{.Names}}' | grep -q '^rag-rokas-es$'; then
    echo "[RAG_Rokas] Starting existing Elasticsearch container..."
    docker start rag-rokas-es >/dev/null
  else
    echo "[RAG_Rokas] Creating Elasticsearch container..."
    docker run -d \
      --name rag-rokas-es \
      -p 9200:9200 \
      -e discovery.type=single-node \
      -e xpack.security.enabled=false \
      docker.elastic.co/elasticsearch/elasticsearch:8.16.0 >/dev/null
  fi

  echo "[RAG_Rokas] Waiting for Elasticsearch..."
  for _ in {1..40}; do
    if es_up; then
      return 0
    fi
    sleep 1
  done

  return 1
}

ensure_elasticsearch() {
  if es_up; then
    return 0
  fi

  echo "[RAG_Rokas] Elasticsearch is not running on localhost:9200."
  if command -v docker >/dev/null 2>&1; then
    read -r -p "Start Elasticsearch automatically with Docker? (Y/n): " answer
    answer="${answer:-y}"
    if [[ "$answer" =~ ^[Yy]$ ]]; then
      if start_es_docker; then
        echo "[RAG_Rokas] Elasticsearch is ready."
        return 0
      fi
      echo "[RAG_Rokas] Failed to start Elasticsearch container."
      return 1
    fi
  fi

  echo "[RAG_Rokas] Please start Elasticsearch manually and retry."
  return 1
}

main() {
  ensure_python
  ensure_venv
  install_deps

  if ! ensure_elasticsearch; then
    echo ""
    read -r -p "Press Enter to close..." _
    exit 1
  fi

  echo "[RAG_Rokas] Launching app..."
  if [ "$#" -eq 0 ]; then
    "$PYTHON_BIN" rag_rokas.py
  else
    "$PYTHON_BIN" rag_rokas.py "$@"
  fi

  echo ""
  read -r -p "Press Enter to close..." _
}

main "$@"
