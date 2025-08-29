#!/usr/bin/env python3
# Limpa CPF/CNPJ no início OU no fim de campos de nome e regrava Parquet.
# Trata:
#   - Empresas.razao_social
#   - Estabelecimentos.nome_fantasia
#   - Socios.nome_socio_ou_razao_social, Socios.nome_representante

import argparse, os, tempfile
from glob import glob
import duckdb

def detect_kind(path: str) -> str:
    b = os.path.basename(path).lower()
    if "empresas" in b: return "empresas"
    if "estabelec" in b: return "estabelecimentos"
    if "socios" in b or "sócios" in b: return "socios"
    return ""

# ---
# MACRO ESCALAR:
# - Remove CPF (form./digits), CNPJ (form./digits) e CNPJ-BÁSICO (form. dd.ddd.ddd OU 8 dígitos)
# - Remove múltiplos tokens no começo/fim
# - Normaliza espaços e limpa pontuações "sobrando" nas bordas
# ---
CLEAN_MACRO = r"""
CREATE OR REPLACE MACRO clean_name(x) AS
trim(
  regexp_replace(                                         -- 5) compacta múltiplos espaços
    regexp_replace(                                       -- 4) limpa pontuação solta no fim
      regexp_replace(                                     -- 3) limpa pontuação solta no começo
        regexp_replace(                                   -- 2) remove token no FIM (pode repetir)
          regexp_replace(                                 -- 1) remove token no INÍCIO (pode repetir)
            coalesce(x, ''),
            '^(?:\s*[\(\[\-]*\s*(?:' ||
              -- CPF (formatado ou só dígitos)
              '\d{3}[.\s]?\d{3}[.\s]?\d{3}[-\s]?\d{2}|\d{11}|' ||
              -- CNPJ (formatado ou só dígitos)
              '\d{2}[.\s]?\d{3}[.\s]?\d{3}[\/\s]?\d{4}[-\s]?\d{2}|\d{14}|' ||
              -- CNPJ-BÁSICO (formatado dd.ddd.ddd OU 8 dígitos)
              '\d{2}[.\s]?\d{3}[.\s]?\d{3}|\d{8}' ||
            ')\s*[\)\]\-,:|]*\s*)+',
            ''
          ),
          '(?:\s*[\(\[\-,:|]*\s*(?:' ||
            '\d{3}[.\s]?\d{3}[.\s]?\d{3}[-\s]?\d{2}|\d{11}|' ||
            '\d{2}[.\s]?\d{3}[.\s]?\d{3}[\/\s]?\d{4}[-\s]?\d{2}|\d{14}|' ||
            '\d{2}[.\s]?\d{3}[.\s]?\d{3}|\d{8}' ||
          ')\s*[\)\]\-]*\s*)+$',
          ''
        ),
        '^\s*[\-\(\)\[\]\.,:|]+\s*', ''
      ),
      '\s*[\-\(\)\[\]\.,:|]+\s*$', ''
    ),
    '\s+', ' '
  )
);
"""

SQL_EMPRESAS = """
COPY (
  SELECT * REPLACE (
    clean_name(razao_social) AS razao_social
  )
  FROM read_parquet('{src}')
) TO '{dst}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
"""

SQL_ESTAB = """
COPY (
  SELECT * REPLACE (
    clean_name(nome_fantasia) AS nome_fantasia
  )
  FROM read_parquet('{src}')
) TO '{dst}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
"""

SQL_SOCIOS = """
COPY (
  SELECT * REPLACE (
    clean_name(nome_socio_ou_razao_social) AS nome_socio_ou_razao_social,
    clean_name(nome_representante)         AS nome_representante
  )
  FROM read_parquet('{src}')
) TO '{dst}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
"""

def main():
    ap = argparse.ArgumentParser(description="Limpa CPF/CNPJ no início/fim de campos de nome em Parquet.")
    ap.add_argument("--src", required=True, help="Pasta de origem (recursiva).")
    ap.add_argument("--dst", help="Pasta de destino; se omitir e usar --inplace, sobrescreve no lugar.")
    ap.add_argument("--inplace", action="store_true", help="Sobrescreve os arquivos no lugar (usa arquivo temporário).")
    ap.add_argument("--delete-source", action="store_true",
                    help="Apaga o arquivo original após gravar o novo (somente quando --dst).")
    args = ap.parse_args()

    src_dir = os.path.abspath(args.src)
    if not os.path.isdir(src_dir):
        raise SystemExit(f"Pasta não encontrada: {src_dir}")

    if args.inplace:
        dst_dir = None
    else:
        dst_dir = os.path.abspath(args.dst) if args.dst else (src_dir.rstrip('/\\') + "_limpo")
        os.makedirs(dst_dir, exist_ok=True)

    files = glob(os.path.join(src_dir, "**", "*.parquet"), recursive=True)
    if not files:
        raise SystemExit("Nenhum .parquet encontrado.")

    con = duckdb.connect(database=":memory:")
    con.execute(CLEAN_MACRO)

    for src in files:
        kind = detect_kind(src)
        if kind not in {"empresas", "estabelecimentos", "socios"}:
            continue

        if args.inplace:
            import tempfile
            fd, tmp_out = tempfile.mkstemp(prefix=".__tmp__", suffix=".parquet", dir=os.path.dirname(src))
            os.close(fd)
            out = tmp_out
        else:
            rel = os.path.relpath(src, src_dir)
            out = os.path.join(dst_dir, rel)
            os.makedirs(os.path.dirname(out), exist_ok=True)

        print(f"[+] Limpando {kind}: {src}")
        try:
            sql = {"empresas": SQL_EMPRESAS, "estabelecimentos": SQL_ESTAB, "socios": SQL_SOCIOS}[kind]
            con.execute(sql.format(src=src.replace("\\", "/"), dst=out.replace("\\", "/")))
            print(f"    ✔ Gravado: {out}")

            if not args.inplace and args.delete_source:
                try:
                    os.remove(src)
                    print(f"    [x] Antigo deletado: {src}")
                except Exception as e:
                    print(f"    [!] Falha ao deletar {src}: {e}")
        except Exception as e:
            print(f"    ✖ ERRO: {e}")
            if args.inplace and os.path.exists(out):
                try: os.remove(out)
                except: pass
            continue

        if args.inplace:
            try:
                os.replace(out, src)
                print("    → Substituído in-place.")
            except Exception as e:
                print(f"    ✖ ERRO ao substituir: {e}")
                try: os.remove(out)
                except: pass

    con.close()
    print("\nConcluído.")

if __name__ == "__main__":
    main()
