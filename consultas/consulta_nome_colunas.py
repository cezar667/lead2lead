#!/usr/bin/env python3
import duckdb
import argparse

# --- parser de argumentos
parser = argparse.ArgumentParser(description="Descreve colunas dos Parquet")
parser.add_argument(
    "--filter",
    help="Lista de bases separadas por | (ex: 'Motivos|Municipios|Naturezas|qualificacoes')",
    required=True
)
parser.add_argument(
    "--parquet-dir",
    default="scripts/parquet",
    help="Diretório onde estão os arquivos parquet (default: scripts/parquet)"
)
args = parser.parse_args()

# --- conecta
con = duckdb.connect(database=":memory:")

# divide filtro por '|'
bases = args.filter.split("|")

for base in bases:
    base = base.strip()
    if not base:
        continue
    sql = f"""
        DESCRIBE 
        SELECT * 
        FROM read_parquet('{args.parquet_dir}/{base}*.parquet') 
        LIMIT 0
    """
    rows = con.execute(sql).fetchall()
    print(f"\n=== {base} ===")
    for row in rows:
        print(row)
