#!/usr/bin/env python3
# consulta_cnpj_parquet.py
# Uso:
#   pip install duckdb
#   python consulta_cnpj_parquet.py --parquet-dir data/cnpj_parquet --cnpj 12.345.678/0001-90

import argparse
import os
import re
from glob import glob
import duckdb

def only_digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def valida_cnpj(cnpj: str) -> bool:
    c = only_digits(cnpj)
    if len(c) != 14 or len(set(c)) == 1:
        return False
    pesos1 = [5,4,3,2,9,8,7,6,5,4,3,2]
    soma1 = sum(int(d)*p for d,p in zip(c[:12], pesos1))
    dv1 = 0 if soma1 % 11 < 2 else 11 - (soma1 % 11)
    pesos2 = [6]+pesos1
    soma2 = sum(int(d)*p for d,p in zip(c[:13], pesos2))
    dv2 = 0 if soma2 % 11 < 2 else 11 - (soma2 % 11)
    return c[-2:] == f"{dv1}{dv2}"

def exists_any(pattern: str) -> bool:
    return bool(glob(pattern))

def main():
    ap = argparse.ArgumentParser(description="Consulta um CNPJ nos arquivos .parquet e imprime os dados no log.")
    ap.add_argument("--parquet-dir", required=True, help="Pasta contendo os .parquet (Empresas*, Estabelecimentos*, Simples*, Municipios*, Socios*).")
    ap.add_argument("--cnpj", required=True, help="CNPJ (com ou sem pontuação).")
    args = ap.parse_args()

    base = os.path.abspath(args.parquet_dir).replace("\\", "/")
    cnpj = only_digits(args.cnpj)

    if len(cnpj) != 14:
        raise SystemExit("❌ CNPJ inválido: deve ter 14 dígitos (com ou sem pontuação).")
    if not valida_cnpj(cnpj):
        print("⚠️  Aviso: DV do CNPJ não confere. Continuando mesmo assim…")

    cnpj_basico, cnpj_ordem, cnpj_dv = cnpj[:8], cnpj[8:12], cnpj[12:14]

    # Confere presença dos arquivos necessários
    need = {
        "emp": f"{base}/Empresas*.parquet",
        "est": f"{base}/Estabelecimentos*.parquet",
        "sim": f"{base}/Simples*.parquet",
        "mun": f"{base}/Municipios*.parquet",
        "soc": f"{base}/Socios*.parquet",
    }
    missing = [k for k,p in need.items() if k in ("emp","est") and not exists_any(p)]
    if missing:
        raise SystemExit(f"❌ Arquivos ausentes para consulta: {missing}. Verifique a pasta: {base}")

    con = duckdb.connect(database=":memory:")

    # ---------------- Empresa (1 linha) ----------------
    q_emp = f"""
    SELECT DISTINCT
        emp.cnpj_basico,
        emp.razao_social,
        emp.natureza_juridica,
        emp.porte_empresa,
        emp.capital_social_empresa
    FROM read_parquet('{base}/Empresas*.parquet') emp
    JOIN read_parquet('{base}/Estabelecimentos*.parquet') est USING (cnpj_basico)
    WHERE est.cnpj_basico = ? AND est.cnpj_ordem = ? AND est.cnpj_dv = ?
    """
    emp = con.execute(q_emp, [cnpj_basico, cnpj_ordem, cnpj_dv]).fetchone()

    if not emp:
        con.close()
        raise SystemExit("❌ CNPJ não encontrado nos Parquet (verifique nomes/paths ou padrão de colunas).")

    print("\n===================== EMPRESA =====================")
    print(f"CNPJ: {cnpj_basico}.{cnpj_ordem}/{cnpj_dv}")
    print(f"Razão social           : {emp[1]}")
    print(f"Natureza jurídica      : {emp[2]}")
    print(f"Porte                  : {emp[3]}")
    print(f"Capital social (R$)    : {emp[4]}")

    # ---------------- Simples/MEI (1 linha, se existir) ----------------
    if exists_any(need["sim"]):
        q_sim = f"""
        SELECT
            COALESCE(sim.opcao_simples,'N')         AS opcao_simples,
            sim.data_opcao_simples,
            sim.data_exclusao_simples,
            COALESCE(sim.opcao_mei,'N')             AS opcao_mei,
            sim.data_opcao_mei,
            sim.data_exclusao_mei
        FROM read_parquet('{base}/Simples*.parquet') sim
        WHERE sim.cnpj_basico = ?
        """
        sim = con.execute(q_sim, [cnpj_basico]).fetchone()
        if sim:
            print("\n------------------- SIMPLES / MEI ------------------")
            print(f"Opção pelo Simples     : {sim[0]}")
            print(f"Data opção Simples     : {sim[1]}")
            print(f"Data exclusão Simples  : {sim[2]}")
            print(f"Opção pelo MEI         : {sim[3]}")
            print(f"Data opção MEI         : {sim[4]}")
            print(f"Data exclusão MEI      : {sim[5]}")

    # ---------------- Estabelecimentos (N linhas) ----------------
    join_mun = ""
    sel_mun  = "est.municipio AS municipio_codigo, NULL AS municipio_nome"
    if exists_any(need["mun"]):
        join_mun = f"""
        LEFT JOIN read_parquet('{base}/Municipios*.parquet') mun
          ON TRY_CAST(est.municipio AS INTEGER) = mun.codigo
        """
        sel_mun = "est.municipio AS municipio_codigo, mun.descricao AS municipio_nome"

    q_est = f"""
    SELECT
        est.identificador_matriz_filial,
        est.nome_fantasia,
        est.cnae_fiscal_principal,
        est.cnae_fiscal_secundaria,
        est.data_inicio_atividade,
        est.situacao_cadastral,
        est.tipo_logradouro,
        est.logradouro,
        est.numero,
        est.complemento,
        est.bairro,
        est.cep,
        est.uf,
        {sel_mun},
        est.ddd_1, est.telefone_1, est.ddd_2, est.telefone_2, est.ddd_fax, est.fax,
        est.correio_eletronico
    FROM read_parquet('{base}/Estabelecimentos*.parquet') est
    {join_mun}
    WHERE est.cnpj_basico = ? AND est.cnpj_ordem = ? AND est.cnpj_dv = ?
    ORDER BY TRY_CAST(est.identificador_matriz_filial AS INTEGER) NULLS LAST, est.data_inicio_atividade
    """
    est_rows = con.execute(q_est, [cnpj_basico, cnpj_ordem, cnpj_dv]).fetchall()

    print("\n================= ESTABELECIMENTOS =================")
    if not est_rows:
        print("(Sem estabelecimentos)")
    for i, r in enumerate(est_rows, 1):
        (id_mf, nome_fantasia, cnae_pri, cnae_sec, dt_ini, sit, tipo_log, lograd, numero,
         compl, bairro, cep, uf, mun_cod, mun_nome, ddd1, tel1, ddd2, tel2, dddfax, fax, email) = r
        tag = "MATRIZ" if str(id_mf) == "1" else ("FILIAL" if str(id_mf) == "2" else str(id_mf))
        fone1 = (ddd1 or "") + (tel1 or "")
        fone2 = (ddd2 or "") + (tel2 or "")
        fax_c = (dddfax or "") + (fax or "")
        print(f"\n--- Estab. {i} [{tag}] ---")
        print(f"Nome fantasia          : {nome_fantasia}")
        print(f"CNAE principal         : {cnae_pri}")
        print(f"CNAEs secundárias      : {cnae_sec}")
        print(f"Início atividade       : {dt_ini}")
        print(f"Situação cadastral     : {sit}")
        print(f"Endereço               : {tipo_log or ''} {lograd or ''}, {numero or ''} {compl or ''} - {bairro or ''}")
        print(f"CEP/UF/Mun             : {cep or ''} / {uf or ''} / {mun_nome or mun_cod or ''}")
        print(f"Contatos               : tel1={fone1} tel2={fone2} fax={fax_c} email={email or ''}")

    # ---------------- Sócios (opcional) ----------------
    if exists_any(need["soc"]):
        q_soc = f"""
        SELECT
            s.identificador_socio,
            s.nome_socio_ou_razao_social,
            s.cnpj_cpf_socio,
            s.qualificacao_socio,
            s.data_entrada_sociedade,
            s.pais,
            s.representante_legal,
            s.nome_representante,
            s.qualificacao_representante_legal,
            s.faixa_etaria
        FROM read_parquet('{base}/Socios*.parquet') s
        WHERE s.cnpj_basico = ?
        ORDER BY s.nome_socio_ou_razao_social
        """
        socios = con.execute(q_soc, [cnpj_basico]).fetchall()
        if socios:
            print("\n======================= SÓCIOS ======================")
            for j, s in enumerate(socios, 1):
                (id_s, nome, doc, qual, dt_ent, pais, rep, nome_rep, qual_rep, faixa) = s
                print(f"\n- Sócio {j}")
                print(f"  Identificador        : {id_s}")
                print(f"  Nome/Razão           : {nome}")
                print(f"  Documento            : {doc}")
                print(f"  Qualificação         : {qual}")
                print(f"  Entrada sociedade    : {dt_ent}")
                print(f"  País                 : {pais}")
                print(f"  Representante legal  : {rep} - {nome_rep} ({qual_rep})")
                print(f"  Faixa etária         : {faixa}")

    con.close()
    print("\n✅ Consulta finalizada.")

if __name__ == "__main__":
    main()
