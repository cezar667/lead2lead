#!/usr/bin/env python3
# consulta_saude_uberlandia.py
# Uso:
#   python consulta_saude_uberlandia.py --parquet-dir data/cnpj_parquet --out exports/saude_uberlandia_600k.csv
#
# Requer: Empresas*, Estabelecimentos*, (opcional) Simples*, Municipios*, Cnaes*

import argparse, os, time, shutil
from glob import glob
import duckdb

def has_any(pat): return bool(glob(pat))

def main():
    ap = argparse.ArgumentParser(description="Exporta CNPJs de Saúde (CNAE 86) em Uberlândia/MG com proxy de faturamento >~ R$600k (EPP+).")
    ap.add_argument("--parquet-dir", required=True)
    ap.add_argument("--out", default="exports/saude_uberlandia_600k.csv")
    ap.add_argument("--incluir-filiais", action="store_true", help="Se setado, inclui filiais (padrão: só matriz).")
    args = ap.parse_args()

    t0 = time.perf_counter()
    base = os.path.abspath(args.parquet_dir).replace("\\","/")
    out_csv = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    emp_glob  = f"{base}/Empresas*.parquet"
    est_glob  = f"{base}/Estabelecimentos*.parquet"
    sim_glob  = f"{base}/Simples*.parquet"
    mun_glob  = f"{base}/Municipios*.parquet"
    cnae_glob = f"{base}/Cnaes*.parquet"

    missing = []
    if not has_any(emp_glob): missing.append("Empresas*.parquet")
    if not has_any(est_glob): missing.append("Estabelecimentos*.parquet")
    if missing: raise SystemExit(f"❌ Ausentes: {', '.join(missing)}")

    has_sim  = has_any(sim_glob)
    has_mun  = has_any(mun_glob)
    has_cnae = has_any(cnae_glob)

    print("▶️  Exportando Saúde Uberlândia >~600k (EPP+)")
    print(f"   • Parquet : {base}")
    print(f"   • Saída   : {out_csv}")
    print(f"   • Tabelas : Simples={'sim' if has_sim else 'não'}  Municipios={'sim' if has_mun else 'não'}  Cnaes={'sim' if has_cnae else 'não'}")

    # JOINs opcionais
    join_sim  = f"LEFT JOIN read_parquet('{sim_glob}') sim USING (cnpj_basico)" if has_sim else ""
    join_mun  = f"LEFT JOIN read_parquet('{mun_glob}') mun ON TRY_CAST(est.municipio AS INTEGER)=TRY_CAST(mun.codigo AS INTEGER)" if has_mun else ""
    join_cnae = f"LEFT JOIN read_parquet('{cnae_glob}') cnae ON TRY_CAST(est.cnae_fiscal_principal AS INTEGER)=TRY_CAST(cnae.codigo AS INTEGER)" if has_cnae else ""

    # Filtros:
    filtro_matriz = "" if args.incluir_filiais else "AND TRY_CAST(est.identificador_matriz_filial AS INTEGER)=1"
    filtro_mei    = "AND UPPER(COALESCE(sim.opcao_mei,'N'))<>'S'" if has_sim else ""  # exclui MEI se info existir

    # Proxy de porte para >~600k: EPP e acima (texto ou código). Seu dataset tem 'porte_empresa' (VARCHAR).
    filtro_porte = """
      AND (
            TRY_CAST(emp.porte_empresa AS INTEGER) IN (3,5)   -- se vier número em algum dataset
      )
    """

    # Uberlândia/MG (5403). Se não houver Municipios*, filtra por código em est.municipio + UF.
    filtro_municipio = """
      WHERE UPPER(est.uf)='MG' AND (
        TRY_CAST(est.municipio AS INTEGER)=5403
        OR (COALESCE(mun.codigo,'') <> '' AND TRY_CAST(mun.codigo AS INTEGER)=5403)
      )
    """

    # Saúde: CNAE principal começando com 86
    filtro_cnae86 = "AND LEFT(COALESCE(CAST(est.cnae_fiscal_principal AS VARCHAR),''),2)='86'"

    # FROM + WHERE final
    from_where = f"""
      FROM read_parquet('{est_glob}') est
      JOIN read_parquet('{emp_glob}') emp USING (cnpj_basico)
      {join_sim}
      {join_mun}
      {join_cnae}
      {filtro_municipio}
      {filtro_cnae86}
      {filtro_mei}
      {filtro_porte}
      {filtro_matriz}
    """

    # Heurística "tipo clínica" pela descrição do CNAE (se disponível)
    tipo_expr = ("CASE WHEN (UPPER(COALESCE(cnae.descricao,'')) LIKE '%CLÍNIC%' "
                 "OR UPPER(COALESCE(cnae.descricao,'')) LIKE '%CLINIC%') "
                 "THEN 'CLINICA' ELSE 'OUTROS_SAUDE' END") if has_cnae else "'SAUDE'"

    select_sql = f"""
      SELECT
        (est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv) AS cnpj,
        emp.razao_social,
        COALESCE(est.nome_fantasia, emp.razao_social) AS nome_fantasia,
        emp.porte_empresa AS porte,
        est.data_inicio_atividade,
        est.situacao_cadastral,
        est.uf,
        {('mun.descricao AS municipio' if has_mun else "CAST(est.municipio AS VARCHAR) AS municipio")},
        est.tipo_logradouro, est.logradouro, est.numero, est.complemento, est.bairro, est.cep,
        (COALESCE(est.ddd_1,'')||COALESCE(est.telefone_1,'')) AS telefone1,
        (COALESCE(est.ddd_2,'')||COALESCE(est.telefone_2,'')) AS telefone2,
        est.correio_eletronico AS email,
        est.cnae_fiscal_principal AS cnae_principal,
        {('cnae.descricao AS cnae_principal_nome' if has_cnae else "NULL AS cnae_principal_nome")},
        {tipo_expr} AS tipo_saude
      {from_where}
      ORDER BY emp.razao_social
    """

    # Contagem
    count_sql = f"SELECT COUNT(*) FROM ({select_sql}) t"

    con = duckdb.connect(database=":memory:")
    total = con.execute(count_sql).fetchone()[0]
    print(f"📦 Registros filtrados: {total:,}".replace(",", "."))

    # Exporta CSV com BOM
    tmp = out_csv + ".tmp"
    tmp_duck = tmp.replace("\\","/")
    copy_sql = f"COPY ({select_sql}) TO '{tmp_duck}' (HEADER, DELIMITER ';');"

    print("📝 Exportando CSV…")
    con.execute(copy_sql)
    with open(tmp, "rb") as src, open(out_csv, "wb") as dst:
        dst.write(b"\xef\xbb\xbf")
        shutil.copyfileobj(src, dst, length=1024*1024)
    os.remove(tmp)

    con.close()
    dt = time.perf_counter()-t0
    mm, ss = divmod(int(dt),60)
    print("✅ Concluído.")
    print(f"   • Linhas exportadas : {total:,}".replace(",", "."))
    print(f"   • Tempo decorrido   : {mm:02d}:{ss:02d} (mm:ss)")
    print("   • Observação        : Faturamento real não está na base; usamos porte (EPP/DEMAIS) como proxy >~R$600k.")

if __name__ == "__main__":
    main()
