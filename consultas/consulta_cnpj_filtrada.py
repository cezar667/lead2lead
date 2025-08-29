#!/usr/bin/env python3
# consulta_cnpjs_filtrada.py
# Uso:
#   pip install duckdb
#   py ./consultas/consulta_cnpj_filtrada.py --parquet-dir data/cnpj_parquet --out exports/cnpjs_saude_uberlandia.csv --uf MG --municipio Uberlandia --limit 5000 --cnae 86 --porte 3 5 --situacao 8 --ativos
#
# Observa: exige que seus .parquet usem nomes friendly (snake_case), por ex.:
#   Empresas*: cnpj_basico, razao_social, ...
#   Estabelecimentos*: cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, uf, municipio, cnae_fiscal_principal, ...
#   Simples* (opcional): opcao_simples, opcao_mei, ...
#   Municipios* (opcional): codigo, descricao
#   Cnaes* (opcional): codigo, descricao

import argparse
import os
import re
import time
from glob import glob
import duckdb
import shutil

def has_any(pattern: str) -> bool:
    return bool(glob(pattern))

def norm_str(s: str) -> str:
    return (s or "").strip()

def is_intlike(s: str) -> bool:
    return bool(re.fullmatch(r"\d+", s or ""))

def main():
    ap = argparse.ArgumentParser(description="Consulta CNPJs em .parquet com filtros opcionais e exporta CSV (;).")
    ap.add_argument("--parquet-dir", required=True, help="Pasta com os .parquet (Empresas*, Estabelecimentos*, Simples*, Municipios*, Cnaes*).")
    ap.add_argument("--out", default="exports/cnpjs_filtrados.csv", help="Arquivo CSV de saída.")
    ap.add_argument("--uf", help="Filtro de UF (ex.: MG).")
    ap.add_argument("--municipio", help="Filtro de município (nome ou código IBGE).")
    ap.add_argument("--cnae", type=int, help="Ramo de atuação. Dois primeiros digitos do Cnae.")
    ap.add_argument("--porte", nargs="+", type=int, choices=[0,1,3,5], default=[3,5],
                    help="Lista de códigos de porte (ex.: --porte 3 5). Válidos: 0,1,3,5. Default: 3 5")
    ap.add_argument("--situacao", nargs="+", type=int, choices=[1,2,3,4,8],
                    help="Lista de códigos de situação cadastral (ex.: --situacao 3 5). Válidos: 1,2,3,4,8.")
    ap.add_argument("--limit", type=int, help="Quantidade máxima de registros a exportar.")
    ap.add_argument("--opcao-sim", action="store_true",
                    help="Se presente, filtra apenas optantes do Simples (sim.opcao_simples='S').")
    ap.add_argument("--ativos", action="store_true", help="Se presente, filtra apenas cnpjs ativos.")

    args = ap.parse_args()

    t0 = time.perf_counter()
    parquet_dir = os.path.abspath(args.parquet_dir).replace("\\", "/")
    out_csv = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    print("▶️  Iniciando consulta…")
    print(f"   • Base Parquet : {parquet_dir}")
    print(f"   • Saída CSV    : {out_csv}")

    # Checagem de arquivos necessários
    emp_glob  = f"{parquet_dir}/Empresas*.parquet"
    est_glob  = f"{parquet_dir}/Estabelecimentos*.parquet"
    sim_glob  = f"{parquet_dir}/Simples*.parquet"
    mun_glob  = f"{parquet_dir}/Municipios*.parquet"
    cnae_glob = f"{parquet_dir}/Cnaes*.parquet"

    missing = []
    if not has_any(emp_glob): missing.append("Empresas*.parquet")
    if not has_any(est_glob): missing.append("Estabelecimentos*.parquet")
    if missing:
        raise SystemExit(f"❌ Arquivos obrigatórios ausentes: {', '.join(missing)}")

    has_sim  = has_any(sim_glob)
    has_cnae = has_any(cnae_glob)
    has_mun  = has_any(mun_glob)

    if not has_sim:
        print("ℹ️  Aviso: Simples*.parquet não encontrado — colunas de Simples (opcao_simples/opcao_mei) virão vazias e filtro MEI não será aplicado.")
    if not has_cnae:
        print("ℹ️  Aviso: Cnaes*.parquet não encontrado — descrição do CNAE principal virá vazia.")
    if not has_mun and args.municipio and not is_intlike(args.municipio):
        print("⚠️  Municipios*.parquet ausente e município informado por NOME — filtro por município será ignorado.")
        args.municipio = None  # Sem Municipios, só dá para filtrar por código numérico

    # Normaliza filtros
    uf = norm_str(args.uf).upper() if args.uf else None
    municipio_raw = norm_str(args.municipio) if args.municipio else None
    municipio_is_code = is_intlike(municipio_raw) if municipio_raw else False
    lim = args.limit if (isinstance(args.limit, int) and args.limit > 0) else None
    cnae = args.cnae if (isinstance(args.cnae, int) and args.cnae > 0) else None
    porte_codes = sorted(set(args.porte or [3,5]))  # garante únicos/ordenados
    placeholders_porte = ", ".join(["?"] * len(porte_codes))  # para o IN (?, ? , ...)
    situacao_codes = sorted(set(args.situacao or [2]))  # garante únicos/ordenados
    placeholders_situacao = ", ".join(["?"] * len(situacao_codes))  # para o IN (?, ? , ...)

    print("🔎 Filtros aplicados:")
    print(f"   • UF           : {uf or '(sem)'}")
    if municipio_raw:
        print(f"   • Município    : {municipio_raw} {'(código)' if municipio_is_code else '(nome)'}")
    else:
        print("   • Município    : (sem)")
    print(f"   • Limite       : {lim or '(sem)'}")
    print(f"   • Cnae           : {cnae or '(sem)'}")
    

    # Montagem dinâmica do SQL
    join_sim = f"LEFT JOIN read_parquet('{sim_glob}') sim USING (cnpj_basico)" if has_sim else ""
    if has_cnae:
        join_cnae = f"""LEFT JOIN read_parquet('{cnae_glob}') cnae
          ON TRY_CAST(est.cnae_fiscal_principal AS INTEGER) = cnae.codigo"""
        sel_cnae = "est.cnae_fiscal_principal AS cnae_principal, cnae.descricao AS cnae_principal_nome"
    else:
        join_cnae = ""
        sel_cnae = "est.cnae_fiscal_principal AS cnae_principal, NULL AS cnae_principal_nome"

    if has_mun:
        join_mun = f"""LEFT JOIN read_parquet('{mun_glob}') mun
          ON TRY_CAST(est.municipio AS INTEGER) = mun.codigo"""
        sel_mun = "est.municipio AS municipio_codigo, mun.descricao AS municipio_nome"
    else:
        join_mun = ""
        sel_mun = "est.municipio AS municipio_codigo, NULL AS municipio_nome"

    where_clauses = []
    params = []

    if args.situacao:
        where_clauses.append(f"TRY_CAST(est.situacao_cadastral AS INTEGER) IN ({placeholders_situacao})")
        params.extend(situacao_codes)
    if args.ativos and not args.situacao:
        where_clauses.append("TRY_CAST(est.situacao_cadastral AS INTEGER) = 2")


    if has_sim and args.opcao_sim:
        where_clauses.append("UPPER(COALESCE(sim.opcao_mei, 'N')) = 'S'")

    if uf:
        where_clauses.append("UPPER(est.uf) = ?")
        params.append(uf)

    if municipio_raw:
        if has_mun:
            if municipio_is_code:
                where_clauses.append("mun.codigo = ?")
                params.append(int(municipio_raw))
            else:
                where_clauses.append("UPPER(mun.descricao) = UPPER(?)")
                params.append(municipio_raw)
        else:
            # Sem Municipios*, só conseguimos filtrar se for código dentro de est.municipio
            where_clauses.append("TRY_CAST(est.municipio AS INTEGER) = ?")
            params.append(int(municipio_raw))

    if cnae:
        where_clauses.append("LEFT(COALESCE(CAST(est.cnae_fiscal_principal AS VARCHAR),''),2)= ?")
        params.append(int(cnae))
    
    where_clauses.append(f"TRY_CAST(emp.porte_empresa AS INTEGER) IN ({placeholders_porte})")
    params.extend(porte_codes)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    # SELECT base (colunas completas úteis)
    select_cols = f"""
        (est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv) AS cnpj,
        emp.razao_social,
        COALESCE(est.nome_fantasia, emp.razao_social) AS nome_fantasia,
        {sel_cnae},
        est.data_inicio_atividade,
        est.uf,
        {sel_mun},
        est.tipo_logradouro, est.logradouro, est.numero, est.complemento, est.bairro, est.cep,
        (COALESCE(est.ddd_1, '') || COALESCE(est.telefone_1, '')) AS telefone1,
        (COALESCE(est.ddd_2, '') || COALESCE(est.telefone_2, '')) AS telefone2,
        (COALESCE(est.ddd_fax, '') || COALESCE(est.fax, ''))      AS fax,
        est.correio_eletronico AS email,
        {("COALESCE(sim.opcao_simples,'')" if has_sim else "''")} AS opcao_simples,
        {("COALESCE(sim.opcao_mei,'')"     if has_sim else "''")} AS opcao_mei
    """

    from_sql = f"""
      FROM read_parquet('{est_glob}') est
      JOIN read_parquet('{emp_glob}') emp USING (cnpj_basico)
      {join_sim}
      {join_mun}
      {join_cnae}
      {where_sql}
    """

    count_sql  = f"SELECT COUNT(*) AS n {from_sql}"

    # SELECT final (com LIMIT ? paramétrico, quando houver)
    select_sql = f"SELECT {select_cols} {from_sql} ORDER BY emp.razao_social"
    if lim:
        select_sql += " LIMIT ?"

    # Gera primeiro em .tmp; depois adiciona BOM (utf-8-sig) para Excel
    out_csv_tmp = out_csv + ".tmp"
    out_csv_tmp_duck = out_csv_tmp.replace("\\", "/")

    copy_sql = f"""
    COPY (
      {select_sql}
    ) TO '{out_csv_tmp_duck}' (HEADER, DELIMITER ';');
    """

    # Execução
    con = duckdb.connect(database=":memory:")

    print("📦 Contando registros…")
    total = con.execute(count_sql, params).fetchone()[0]
    print(f"   • Total no filtro: {total}")

    print("📝 Exportando CSV… (com ';' e BOM para Excel)")
    copy_params = params + ([lim] if lim else [])
    con.execute(copy_sql, copy_params)

    # Prefixa BOM (UTF-8-SIG) sem carregar o arquivo todo na memória
    with open(out_csv_tmp, "rb") as src, open(out_csv, "wb") as dst:
        dst.write(b"\xef\xbb\xbf")  # BOM
        shutil.copyfileobj(src, dst, length=1024 * 1024)
    os.remove(out_csv_tmp)

    # Estatísticas
    exported = min(total, lim) if lim else total
    con.close()

    dt = time.perf_counter() - t0
    mm, ss = divmod(int(dt), 60)
    print("✅ Exportação concluída.")
    print(f"   • Linhas exportadas : {exported}")
    print(f"   • Tempo decorrido   : {mm:02d}:{ss:02d} (mm:ss)")

if __name__ == "__main__":
    main()
