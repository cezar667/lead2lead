#!/usr/bin/env python3
# gerar_meis_uberlandia_mg_csv.py
# Uso:
#   python gerar_meis_uberlandia_mg_csv.py --parquet-dir data/cnpj_parquet --out exports/meis_uberlandia_mg_ativas.csv

import argparse
import os
import duckdb

DELIM = ";"  # separador

BASE_FROM_WHERE = """
FROM read_parquet('{base}/Estabelecimentos*.parquet') AS est
JOIN read_parquet('{base}/Empresas*.parquet')         AS emp USING (cnpj_basico)
JOIN read_parquet('{base}/Simples*.parquet')          AS sim USING (cnpj_basico)
JOIN read_parquet('{base}/Municipios*.parquet')       AS mun
  ON TRY_CAST(est.municipio AS INTEGER) = mun.codigo
WHERE
  -- MEI
  UPPER(COALESCE(sim.opcao_mei, 'N')) = 'S'
  -- Minas Gerais
  AND UPPER(est.uf) = 'MG'
  -- Situação ativa
  AND (
    TRY_CAST(est.situacao_cadastral AS INTEGER) = 2
    OR UPPER(est.situacao_cadastral) = 'ATIVA'
  )
"""

SQL_COPY = f"""
COPY (
  SELECT
    -- identificação
    (est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv) AS cnpj,
    emp.razao_social,
    COALESCE(est.nome_fantasia, emp.razao_social)      AS nome_fantasia,

    -- endereço
    COALESCE(est.tipo_logradouro, '') AS tipo_logradouro,
    COALESCE(est.logradouro,      '') AS logradouro,
    COALESCE(est.numero,          '') AS numero,
    COALESCE(est.complemento,     '') AS complemento,
    COALESCE(est.bairro,          '') AS bairro,
    COALESCE(est.cep,             '') AS cep,
    est.uf,
    est.municipio                 AS municipio_codigo,
    mun.descricao                 AS municipio_nome,

    -- contatos
    (COALESCE(est.ddd_1, '') || COALESCE(est.telefone_1, '')) AS telefone1,
    (COALESCE(est.ddd_2, '') || COALESCE(est.telefone_2, '')) AS telefone2,
    (COALESCE(est.ddd_fax, '') || COALESCE(est.fax, ''))      AS fax,
    COALESCE(est.correio_eletronico, '') AS email,

    -- status
    est.situacao_cadastral
  {BASE_FROM_WHERE}
) TO '{{out}}' (HEADER, DELIMITER '{DELIM}');
"""

SQL_COUNT = f"""
SELECT COUNT(*) AS n
{BASE_FROM_WHERE}
"""

def main():
    ap = argparse.ArgumentParser(description="Gera CSV (;) das MEIs ATIVAS de Uberlândia–MG com endereço, telefones e e-mail (join com Municipios).")
    ap.add_argument("--parquet-dir", required=True, help="Pasta dos .parquet (Empresas*, Estabelecimentos*, Simples*, Municipios*).")
    ap.add_argument("--out", default="exports/meis_uberlandia_mg_ativas.csv", help="Caminho do CSV de saída.")
    args = ap.parse_args()

    parquet_dir = os.path.abspath(args.parquet_dir).replace("\\", "/")
    out_csv = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    con = duckdb.connect(database=":memory:")
    # gera CSV
    con.execute(SQL_COPY.format(base=parquet_dir, out=out_csv.replace("\\", "/")))
    # conta linhas
    total = con.execute(SQL_COUNT.format(base=parquet_dir)).fetchone()[0]
    con.close()

    print(f"✅ CSV gerado com separador ';': {out_csv}")
    print(f"Linhas geradas: {total}")

if __name__ == "__main__":
    main()
