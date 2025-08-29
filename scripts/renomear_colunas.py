#!/usr/bin/env python3
# renomear_parquet_stream_dict_receita.py
import argparse, os, re, tempfile, shutil
from glob import glob
from unidecode import unidecode
import pyarrow as pa
import pyarrow.parquet as pq

# ===== Mapas oficiais → snake_case =====
# (Empresas/Estabelecimentos/Simples INTENCIONALMENTE omitidos por padrão)

SOCIOS_SQL = [
    "cnpj_basico",
    "identificador_socio",
    "nome_socio_ou_razao_social",
    "cnpj_cpf_socio",
    "qualificacao_socio",
    "data_entrada_sociedade",
    "pais",
    "representante_legal",
    "nome_representante",
    "qualificacao_representante_legal",
    "faixa_etaria",
]

PAISES_SQL = ["codigo", "descricao"]
MUNICIPIOS_SQL = ["codigo", "descricao"]
QUALIFICACOES_SOCIOS_SQL = ["codigo", "descricao"]
NATUREZAS_JURIDICAS_SQL = ["codigo", "descricao"]
CNAES_SQL = ["codigo", "descricao"]

MAPAS = {
    "socios": SOCIOS_SQL,
    "paises": PAISES_SQL,
    "municipios": MUNICIPIOS_SQL,
    "qualificacoes": QUALIFICACOES_SOCIOS_SQL,
    "naturezas": NATUREZAS_JURIDICAS_SQL,
    "cnaes": CNAES_SQL,
    # opcionalmente, você pode reativar os três abaixo com --include-e-e-s
    "empresas": None,
    "estabelecimentos": None,
    "simples": None,
}

# ===== Helpers de nome =====
def to_sql_name(raw: str) -> str:
    s = unidecode(str(raw)).lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def unique(names):
    seen, out = {}, []
    for n in names:
        if not n:
            n = "col"
        if n not in seen:
            seen[n] = 1
            out.append(n)
        else:
            seen[n] += 1
            out.append(f"{n}_{seen[n]}")
    return out

def auto_sql_headers(cols):
    res = []
    for i, c in enumerate(cols):
        n = to_sql_name(c)
        if (not n) or n.startswith("unnamed") or re.fullmatch(r"\d+(\.\d+)?", n or ""):
            n = f"col_{i:02d}"
        res.append(n)
    return unique(res)

def normaliza_nome_arquivo(path: str) -> str:
    base = os.path.splitext(os.path.basename(path))[0]
    return unidecode(base).lower()

def detectar_base(path: str) -> str:
    b = normaliza_nome_arquivo(path)
    # procura palavras-chave no nome do arquivo
    if "socios" in b or "sócios" in b:
        return "socios"
    if "paises" in b or "países" in b or "pais" in b:
        return "paises"
    if "municipios" in b or "municípios" in b or "municipio" in b:
        return "municipios"
    if "qualificacoes" in b or "qualificações" in b:
        return "qualificacoes"
    if "naturezas" in b:
        return "naturezas"
    if "cnaes" in b or "cnae" in b:
        return "cnaes"
    if "empresas" in b:
        return "empresas"
    if "estabelec" in b:
        return "estabelecimentos"
    if "simples" in b:
        return "simples"
    return ""

def decidir_novos_nomes(path: str, schema: pa.Schema, include_ees: bool):
    base = detectar_base(path)
    # pular E/E/S se não solicitado
    if base in {"empresas", "estabelecimentos", "simples"} and not include_ees:
        return None, base  # None sinaliza "pular"
    alvo = MAPAS.get(base)
    if alvo and len(alvo) == len(schema.names):
        return alvo, base
    # fallback genérico
    return auto_sql_headers(schema.names), base

# ===== Renomeio em streaming =====
def renomear_parquet_streaming(src_path: str, dst_path: str, novos_nomes, compression: str = "zstd"):
    import os
    import pyarrow as pa
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(src_path)
    old_schema = pf.schema_arrow

    # monta schema novo preservando os tipos originais
    new_fields = [pa.field(novos_nomes[i], old_schema.types[i]) for i in range(len(old_schema))]
    new_schema = pa.schema(new_fields)

    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
    writer = pq.ParquetWriter(
        dst_path,
        new_schema,
        compression=compression,
        use_dictionary=True,
    )

    # lê e escreve por row group, renomeando as colunas da TABELA (sem lidar com arrays individuais)
    for rg in range(pf.num_row_groups):
        tbl = pf.read_row_group(rg)
        # garante arrays consolidadas (evita problemas com chunks esquisitos)
        if hasattr(tbl, "combine_chunks"):
            tbl = tbl.combine_chunks()
        # renomeia as colunas da tabela inteira de uma vez
        tbl = tbl.rename_columns(novos_nomes)
        writer.write_table(tbl)  # escreve este row group

    writer.close()
    return old_schema.names


def main():
    ap = argparse.ArgumentParser(description="Renomeia colunas dos .parquet conforme dicionário da Receita (streaming).")
    ap.add_argument("--src", required=True, help="Pasta origem (recursivo).")
    ap.add_argument("--dst", help="Pasta destino. Se omitir com --inplace, sobrescreve no lugar.")
    ap.add_argument("--inplace", action="store_true", help="Processa in-place (usa arquivo temporário e substitui).")
    ap.add_argument("--delete-source", action="store_true", help="Apaga o arquivo original após gravar o novo/cópia.")
    ap.add_argument("--include-e-e-s", action="store_true", help="Inclui Empresas/Estabelecimentos/Simples no processamento.")
    args = ap.parse_args()

    src = os.path.abspath(args.src)
    if not os.path.isdir(src):
        raise SystemExit(f"Pasta não encontrada: {src}")

    if args.inplace:
        dst = None
    else:
        dst = os.path.abspath(args.dst) if args.dst else src.rstrip("/\\") + "_sql"
        os.makedirs(dst, exist_ok=True)

    arquivos = glob(os.path.join(src, "**", "*.parquet"), recursive=True)
    if not arquivos:
        raise SystemExit("Nenhum .parquet encontrado.")

    for src_path in arquivos:
        base = detectar_base(src_path)
        norm_name = normaliza_nome_arquivo(src_path)

        # destino
        if args.inplace:
            # tmp ao lado
            tmp_fd, tmp_out = tempfile.mkstemp(prefix=".__tmp__", suffix=".parquet", dir=os.path.dirname(src_path))
            os.close(tmp_fd)
            out_path = tmp_out
        else:
            rel = os.path.relpath(src_path, src)
            out_path = os.path.join(dst, rel)
            os.makedirs(os.path.dirname(out_path), exist_ok=True)

        print(f"\n[+] Lendo: {src_path}  (detectado: {base or 'desconhecido'})")

        # decidir se processa ou só carrega/copiar
        try:
            pf = pq.ParquetFile(src_path)
            novos_nomes, detected = decidir_novos_nomes(src_path, pf.schema_arrow, args.include_e_e_s)

            if novos_nomes is None:
                print("    (Pulando Empresas/Estabelecimentos/Simples — já tratadas)")
                # copiar como está se há --dst ou --delete-source
                if not args.inplace:
                    shutil.copy2(src_path, out_path)
                    print(f"[→] Copiado sem alterações: {out_path}")
                    if args.delete_source:
                        os.remove(src_path)
                        print(f"[x] Antigo deletado: {src_path}")
                else:
                    # in-place: não faz nada
                    if args.delete_source:
                        print("    Aviso: --delete-source sem efeito para arquivos pulados em --inplace.")
                continue

            old_cols = renomear_parquet_streaming(src_path, out_path, novos_nomes)
            print(f"[✔] Gravado: {out_path}")
            print("    Renomeações (origem → destino):")
            for o, n in zip(old_cols, novos_nomes):
                if o != n:
                    print(f"      - {o} -> {n}")

        except Exception as e:
            # limpeza de tmp se falhar
            if args.inplace and os.path.exists(out_path):
                try: os.remove(out_path)
                except: pass
            print(f"[ERRO] {src_path}: {e}")
            continue

        # substituir/apagar original
        if args.inplace:
            try:
                os.replace(out_path, src_path)
                print(f"[→] Substituído in-place: {src_path}")
            except Exception as e:
                print(f"[ERRO] ao substituir {src_path}: {e}")
                try: os.remove(out_path)
                except: pass
        else:
            if args.delete_source:
                try:
                    os.remove(src_path)
                    print(f"[x] Antigo deletado: {src_path}")
                except Exception as e:
                    print(f"[ERRO] ao deletar {src_path}: {e}")

    print("\nConcluído.")

if __name__ == "__main__":
    main()
