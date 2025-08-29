#!/usr/bin/env bash
# setup_consultas.sh
# - Cria/rehusa .venv com Python 3 via 'py'
# - Atualiza pip e instala duckdb + pandas
# - Se receber argumentos, executa o script Python dentro do venv
# Uso:
#   bash scripts/setup_consultas.sh
#   bash scripts/setup_consultas.sh consultas/consulta_saude_uberlandia.py --parquet-dir data/cnpj_parquet --out exports/clinicas.csv --porte 3 5 --opcao-sim

set -euo pipefail

# --- helpers -------------------------------------------------------------
die() { echo "Erro: $*" >&2; exit 1; }

need_cmd() { command -v "$1" >/dev/null 2>&1 || die "comando '$1' não encontrado"; }

pick_py() {
  # tenta versões específicas antes do genérico -3
  for v in -3.12 -3.11 -3.10 -3.9 -3; do
    if py "$v" -V >/dev/null 2>&1; then
      echo "py $v"
      return 0
    fi
  done
  return 1
}

# --- checagens -----------------------------------------------------------
need_cmd py

PY_CMD="$(pick_py)" || die "nenhum Python 3 encontrado pelo launcher 'py'. Instale Python 3.12+."
echo "🔎 Usando Python via launcher: $PY_CMD"

VENV_DIR=".venv"
if [[ ! -d "$VENV_DIR" ]]; then
  echo "🐍 Criando venv em $VENV_DIR ..."
  $PY_CMD -m venv "$VENV_DIR"
else
  echo "♻️  Reutilizando venv existente: $VENV_DIR"
fi

# Ativa venv (Git Bash)
# shellcheck source=/dev/null
source "$VENV_DIR/Scripts/activate"

echo "⬆️  Atualizando pip..."
py -m pip install --upgrade pip

echo "📦 Instalando dependências (duckdb, pandas)..."
py -m pip install duckdb pandas

py -3 -m pip install --upgrade pip
py -3 -m pip install pyarrow

echo "✅ Ambiente pronto."

# Se o usuário passou um script/comando, executa dentro do venv:
if [[ $# -gt 0 ]]; then
  echo "▶️  Executando: python $*"
  py "$@"
else
  cat <<'MSG'
ℹ️  Dica de uso:
  # exemplo 1: preparar ambiente (somente)
  bash scripts/setup_consultas.sh

  # exemplo 2: preparar e já rodar uma consulta
  bash scripts/setup_consultas.sh consultas/consulta_saude_uberlandia.py \
    --parquet-dir data/cnpj_parquet \
    --out exports/saude_uberlandia.csv \
    --porte 3 5 --opcao-sim
MSG
fi
