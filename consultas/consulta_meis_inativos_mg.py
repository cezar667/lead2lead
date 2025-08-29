#!/usr/bin/env python3
# contar_meis_inativos_mg_codigos.py
# Uso:
#   pip install duckdb
#   python contar_meis_inativos_mg_codigos.py --parquet-dir data/cnpj_parquet
#   (opcional) --incluir-filiais  -> conta matriz + filiais

import argparse
import os
import time
from glob import glob
import duckdb

def has_any(pattern: str) -> bool:
    return bool(glob(pattern))

def main():
    ap = argparse.ArgumentParser(description="Conta MEIs inativos (códigos 3,4,8) em MG a partir de Parquet.")
    ap.add_argument("--parquet-dir", required=True, help="Pasta com os .parquet (Estabelecimentos*, Simples*).")
    ap.add_argument("--uf", default="MG", help="UF (padrão: MG).")
    ap.add_argument("--incluir-filiais", action="store_true",
                    help="Se setado, conta matriz + filiais; por padrão conta somente a matriz (identificador=1).")
    args = ap.parse_args()

    t0 = time.perf_counter()
    base = os.path.abspath(args.parquet_dir).replace("\\", "/")
    est_glob = f"{base}/Estabelecimentos*.parquet"
    sim_glob = f"{base}/Simples*.parquet"

    # Checagens
    missing = []
    if not has_any(est_glob): missing.append("Estabelecimentos*.parquet")
    if not has_any(sim_glob): missing.append("Simples*.parquet")
    if missing:
        raise SystemExit(f"❌ Arquivos necessários ausentes: {', '.join(missing)}")

    somente_matriz = not args.incluir_filiais
    uf = (args.uf or "MG").upper()

    print("▶️  Iniciando contagem de MEIs inativos…")
    print(f"   • Base Parquet : {base}")
    print(f"   • UF           : {uf}")
    print(f"   • Critérios    : MEI='S', situacao_cadastral ∈ {{3=SUSPENSA, 4=INAPTA, 8=BAIXADA}}, "
          f"{'somente matriz' if somente_matriz else 'matriz + filiais'}")

    filtro_matriz = "AND TRY_CAST(est.identificador_matriz_filial AS INTEGER) = 1" if somente_matriz else ""

    # FROM + WHERE comum
    from_where = f"""
      FROM read_parquet('{est_glob}') est
      JOIN read_parquet('{sim_glob}') sim USING (cnpj_basico)
      WHERE UPPER(est.uf) = '{uf}'
        AND UPPER(COALESCE(sim.opcao_mei, 'N')) = 'S'
        AND TRY_CAST(est.situacao_cadastral AS INTEGER) IN (3,4,8)
        {filtro_matriz}
    """

    # Total de CNPJs distintos (considerando apenas matriz por padrão)
    total_sql = f"""
      SELECT COUNT(DISTINCT (est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv)) AS total_meis_inativos
      {from_where}
    """

    # Breakdown por código + label
    by_code_sql = f"""
      SELECT
        TRY_CAST(est.situacao_cadastral AS INTEGER) AS codigo,
        CASE TRY_CAST(est.situacao_cadastral AS INTEGER)
          WHEN 2 THEN 'ATIVA'
          WHEN 3 THEN 'SUSPENSA'
          WHEN 4 THEN 'INAPTA'
          WHEN 8 THEN 'BAIXADA'
          ELSE COALESCE(CAST(est.situacao_cadastral AS VARCHAR), '(DESCONHECIDA)')
        END AS descricao,
        COUNT(*) AS registros,
        COUNT(DISTINCT (est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv)) AS cnpjs_distintos
      {from_where}
      GROUP BY 1,2
      ORDER BY registros DESC
    """

    con = duckdb.connect(database=":memory:")

    total = con.execute(total_sql).fetchone()[0]
    print("📊 Resultado")
    print(f"   • MEIs inativos ({uf}) : {total:,}".replace(",", "."))

    rows = con.execute(by_code_sql).fetchall()
    if rows:
        print("   • Por código/descrição:")
        for codigo, desc, registros, cnpjs in rows:
            print(f"     - {codigo:>2} – {desc:<8}  registros={registros:,}  cnpjs={cnpjs:,}".replace(",", "."))

    con.close()
    dt = time.perf_counter() - t0
    mm, ss = divmod(int(dt), 60)
    print("✅ Concluído.")
    print(f"   • Tempo decorrido     : {mm:02d}:{ss:02d} (mm:ss)")

if __name__ == "__main__":
    main()
