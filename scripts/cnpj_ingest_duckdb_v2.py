#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingestão CNPJ → Parquet com nomes de colunas corretos na origem.
- Baixa os .zip do mês da Receita, extrai 1 CSV por zip, converte em Parquet.
- Garante nomes de colunas corretos por tipo de base (Empresas, Estabelecimentos, etc).
- Lê em chunks (dtype=str), escreve Parquet ZSTD, com logs claros.

Uso típico:
  python cnpj_ingest_duckdb.py --base-url https://.../2025-07/ --out-dir ./parquet
  # Filtros:
  python cnpj_ingest_duckdb.py --filter "Empresas|Estabelec" --limit 5
"""

import os
import re
import sys
import time
import shutil
import zipfile
import argparse
from pathlib import Path
from urllib.parse import urljoin

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ===================== Configurações padrão =====================

DEFAULT_BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-07/"
ZIP_PATTERN = re.compile(r'href="([^"]+\.zip)"', re.IGNORECASE)

CSV_DELIM = ";"         # Receita
CSV_QUOTE = '"'         # aspas padrão
ENCODING_READ = "latin1"  # dumps costumam vir em latin1
CHUNKSIZE = 200_000

PARQUET_COMPRESSION = "ZSTD"
USE_DICTIONARY = False  # menos CPU/memória
PARQUET_PER_FILE = True

# ===================== Dicionários de colunas (oficiais do projeto) =====================
# Tudo como texto/VARCHAR. Ajustado a partir das definições salvas no projeto.

COLS = {
    # --- Tabelas principais do projeto ---
    "cnaes": [
        "codigo", "descricao"
    ],
    "empresas": [
        "cnpj_basico", "razao_social", "natureza_juridica", "qualificacao_responsavel",
        "capital_social_empresa", "porte_empresa", "ente_federativo_responsavel"
    ],
    "estabelecimentos": [
        "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
        "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
        "motivo_situacao_cadastral", "nome_cidade_exterior", "pais",
        "data_inicio_atividade", "cnae_fiscal_principal", "cnae_fiscal_secundaria",
        "tipo_logradouro", "logradouro", "numero", "complemento", "bairro", "cep",
        "uf", "municipio", "ddd_1", "telefone_1", "ddd_2", "telefone_2", "ddd_fax",
        "fax", "correio_eletronico", "situacao_especial", "data_situacao_especial"
    ],
    "municipios": [
        "codigo", "descricao"
    ],
    "simples": [
        "cnpj_basico", "opcao_simples", "data_opcao_simples", "data_exclusao_simples",
        "opcao_mei", "data_opcao_mei", "data_exclusao_mei"
    ],
    "motivos": [
        "codigo", "descricao"  # conforme padrão salvo
    ],
    "naturezas": [
        "codigo", "descricao"
    ],

    # --- Demais auxiliares frequentes no pacote RF ---
    "socios": [
        "cnpj_basico", "identificador_socio", "nome_socio_ou_razao_social",
        "cnpj_cpf_socio", "qualificacao_socio", "data_entrada_sociedade", "pais",
        "representante_legal", "nome_representante", "qualificacao_representante_legal",
        "faixa_etaria",
    ],
    "paises": ["codigo", "descricao"],
    "qualificacoes": ["codigo", "descricao"],
}

# Palavras-chave para detecção por nome de arquivo
DETECT_MAP = [
    ("empresas", ("empresas", "emprecsv")),
    ("estabelecimentos", ("estabele", "estabcsv")),
    ("simples", ("simples",)),
    ("cnaes", ("cnaes", "cnae")),
    ("municipios", ("municipios", "municípios", "municipio", "municcsv")),
    ("naturezas", ("naturezas", "natureza", "natjucsv")),
    ("motivos", ("motivos", "motivo", "moticsv")),
    ("socios", ("socios", "sócios", "sociccsv", "socioscsv", "sociocsv")),
    ("paises", ("paises", "países", "pais")),
    ("qualificacoes", ("qualificacoes", "qualificações", "qualif", "qualscsv")),
]


# ===================== Utilitários =====================

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def human(nbytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(nbytes)
    for u in units:
        if x < 1024 or u == units[-1]:
            return f"{x:.1f} {u}"
        x /= 1024.0

def list_zip_links(base_url: str):
    r = requests.get(base_url, timeout=60)
    r.raise_for_status()
    links = ZIP_PATTERN.findall(r.text)
    zips = sorted(set(urljoin(base_url, href) for href in links))
    if not zips:
        raise RuntimeError("Nenhum .zip encontrado. Verifique o base_url.")
    return zips

def download_file(url: str, dest: Path, chunk=1024*1024):
    ensure_dir(dest.parent)
    tmp = dest.with_suffix(dest.suffix + ".part")
    mode = "ab" if tmp.exists() else "wb"
    headers = {}
    downloaded = tmp.stat().st_size if tmp.exists() else 0
    if downloaded > 0:
        headers["Range"] = f"bytes={downloaded}-"
    with requests.get(url, stream=True, timeout=120, headers=headers) as r:
        r.raise_for_status()
        with open(tmp, mode) as f:
            for chunk_data in r.iter_content(chunk_size=chunk):
                if chunk_data:
                    f.write(chunk_data)
    tmp.rename(dest)
    return dest

def unzip_single_csv(zip_path: Path, extract_dir: Path) -> Path:
    ensure_dir(extract_dir)
    with zipfile.ZipFile(zip_path, 'r') as z:
        names = z.namelist()
        if not names:
            raise RuntimeError(f"Nenhum arquivo dentro de {zip_path.name}")
        cand = [m for m in names if m.lower().endswith(("csv", ".csv", ".txt"))]
        if not cand:
            cand = [m for m in names if "csv" in m.lower()]
        if not cand:
            cand = [max(names, key=lambda m: z.getinfo(m).file_size)]
        member = cand[0]
        original_name = Path(member).name
        base_name = original_name if original_name.lower().endswith(".csv") else original_name + ".csv"
        out_path = extract_dir / base_name
        if out_path.exists():
            out_path.unlink()
        z.extract(member, path=extract_dir)
        extracted = extract_dir / member
        if extracted != out_path:
            ensure_dir(out_path.parent)
            shutil.move(str(extracted), str(out_path))
            # tenta limpar subpastas vazias
            try:
                subdir = extract_dir / Path(member).parent
                if subdir.exists():
                    for p in sorted(subdir.glob("**/*"), reverse=True):
                        if p.is_dir():
                            try: p.rmdir()
                            except: pass
                    try: subdir.rmdir()
                    except: pass
            except Exception:
                pass
        else:
            if out_path.suffix.lower() != ".csv":
                final_csv = out_path.with_suffix(out_path.suffix + ".csv")
                if final_csv.exists():
                    final_csv.unlink()
                out_path.rename(final_csv)
                out_path = final_csv
    return out_path

def detect_base_from_filename(path: Path) -> str:
    n = path.name.lower()
    for base, keys in DETECT_MAP:
        if any(k in n for k in keys):
            return base
    return ""


# --------------------- Heurística de header e nomes ---------------------

def _first_line(path: Path) -> str:
    with open(path, "rb") as f:
        return f.readline(1024 * 1024).decode(ENCODING_READ, errors="ignore")

def looks_like_header(line: str) -> bool:
    # Tem letras? (rótulos) e separadores plausíveis?
    return (CSV_DELIM in line) and bool(re.search(r"[A-Za-zÀ-ÿ_]", line))

def choose_column_names(csv_path: Path, base: str, ncols_detected: int, header_present: bool):
    """
    Decide o vetor final de nomes a aplicar no DataFrame:
      - Se 'base' reconhecida e o número de colunas bate, usa COLS[base]
      - Caso contrário, se header presente, deixa pandas usar header
      - Caso contrário, gera nomes auto col_00..col_NN
    """
    target = COLS.get(base)
    if target and len(target) == ncols_detected:
        return target, f"[OK] Atribuindo nomes do dicionário: {base} ({len(target)} col.)"
    if header_present:
        return None, "[OK] CSV já possui header legível — mantendo nomes originais"
    # fallback: gerar col_00..col_NN
    auto = [f"col_{i:02d}" for i in range(ncols_detected)]
    return auto, f"[WARN] Cabeçalho ausente/inesperado ({ncols_detected} col.) — usando nomes automáticos"

# --------------------- Conversão CSV → Parquet ---------------------

def csv_to_parquet(csv_path: Path, parquet_path: Path):
    ensure_dir(parquet_path.parent)

    base = detect_base_from_filename(csv_path)
    first = True
    writer = None

    # espiar a primeira linha p/ heurística de header
    first_line = _first_line(csv_path)
    header_present = looks_like_header(first_line)

    # Lê um pequeno pedaço para descobrir quantas colunas existem
    sample = pd.read_csv(
        csv_path, sep=CSV_DELIM, encoding=ENCODING_READ,
        dtype=str, nrows=5, header=0 if header_present else None,
        engine="python", on_bad_lines="warn", quotechar=CSV_QUOTE
    )
    ncols = sample.shape[1]
    decided_names, msg = choose_column_names(csv_path, base, ncols, header_present)
    print(f"    [HEADER] base='{base or 'desconhecida'}' → {msg}")

    # Leitura definitiva em chunks
    read_kwargs = dict(
        sep=CSV_DELIM,
        encoding=ENCODING_READ,
        dtype=str,
        chunksize=CHUNKSIZE,
        engine="python",
        on_bad_lines="warn",
        quotechar=CSV_QUOTE
    )
    if decided_names is None and header_present:
        # usar header do arquivo
        read_kwargs["header"] = 0
    else:
        # sem header: força nomes decididos
        read_kwargs["header"] = None
        read_kwargs["names"] = decided_names

    # Converter
    try:
        for chunk in pd.read_csv(csv_path, **read_kwargs):
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if first:
                writer = pq.ParquetWriter(
                    where=str(parquet_path),
                    schema=table.schema,
                    compression=PARQUET_COMPRESSION.lower(),
                    use_dictionary=USE_DICTIONARY,
                )
                first = False
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

# --------------------- Pipeline de um ZIP ---------------------

def process_one_zip(zip_url: str, work_dir: Path, out_dir: Path, keep_zip: bool, keep_csv: bool):
    zip_name = zip_url.rstrip("/").split("/")[-1]
    stem = Path(zip_name).stem
    zip_path = work_dir / "zips" / zip_name
    csv_dir = work_dir / "csv_tmp"
    parquet_path = out_dir / f"{stem}.parquet"

    if parquet_path.exists():
        print(f"[SKIP] Já existe Parquet para {zip_name}: {parquet_path.name}")
        return

    # download (com verificação de integridade)
    if zip_path.exists():
        print(f"[PULANDO DOWNLOAD] {zip_path.name} já existe ({human(zip_path.stat().st_size)}).")
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                bad = z.testzip()
                if bad is not None:
                    print(f"[AVISO] ZIP existente corrompido ({bad}). Rebaixando...")
                    zip_path.unlink(missing_ok=True)
                    print(f"[BAIXANDO] {zip_url}")
                    download_file(zip_url, zip_path)
                    print(f"  -> {zip_path.name} ({human(zip_path.stat().st_size)})")
        except zipfile.BadZipFile:
            print(f"[AVISO] ZIP inválido. Rebaixando...")
            zip_path.unlink(missing_ok=True)
            print(f"[BAIXANDO] {zip_url}")
            download_file(zip_url, zip_path)
            print(f"  -> {zip_path.name} ({human(zip_path.stat().st_size)})")
    else:
        print(f"[BAIXANDO] {zip_url}")
        download_file(zip_url, zip_path)
        print(f"  -> {zip_path.name} ({human(zip_path.stat().st_size)})")

    # unzip
    print(f"[EXTRAINDO] {zip_path.name}")
    csv_path = unzip_single_csv(zip_path, csv_dir)
    print(f"  -> {csv_path.name} ({human(csv_path.stat().st_size)})")

    # csv → parquet (com nomes corretos)
    print(f"[CONVERTENDO] {csv_path.name} -> {parquet_path.name}")
    t0 = time.time()
    csv_to_parquet(csv_path, parquet_path)
    print(f"  -> OK em {time.time() - t0:.1f}s | {human(parquet_path.stat().st_size)}")

    # limpeza
    if not keep_csv and csv_path.exists():
        csv_path.unlink()
    if not keep_zip and zip_path.exists():
        zip_path.unlink()


# ===================== CLI =====================

def main():
    parser = argparse.ArgumentParser(
        description="Ingestão CNPJ: CSV → Parquet com nomes de colunas corretos, por arquivo."
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL,
                        help="URL base do mês (ex.: .../2025-07/).")
    parser.add_argument("--work-dir", default="./work",
                        help="Diretório de trabalho (temporários/ZIP/CSV).")
    parser.add_argument("--out-dir", default="./parquet",
                        help="Diretório de saída dos Parquets.")
    parser.add_argument("--keep-zip", action="store_true",
                        help="Mantém os .zip após converter.")
    parser.add_argument("--keep-csv", action="store_true",
                        help="Mantém os .csv extraídos após converter.")
    parser.add_argument("--filter", default="",
                        help="Processa apenas zips cujo nome contenha esta regex (ex.: 'Empresas|Estabelecimentos').")
    parser.add_argument("--limit", type=int, default=0,
                        help="Processa no máximo N arquivos.")
    args = parser.parse_args()

    work_dir = Path(args.work_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    ensure_dir(work_dir / "zips")
    ensure_dir(work_dir / "csv_tmp")
    ensure_dir(out_dir)

    print(f"[INFO] Base URL: {args.base_url}")
    print(f"[INFO] Work dir: {work_dir}")
    print(f"[INFO] Out  dir: {out_dir}")

    # Lista todos os .zip do mês
    zips = list_zip_links(args.base_url)

    # Filtro opcional
    if args.filter:
        zips = [z for z in zips if re.search(args.filter, z, flags=re.IGNORECASE)]

    if not zips:
        print("Nenhum zip para processar após filtros.")
        sys.exit(0)

    if args.limit > 0:
        zips = zips[:args.limit]

    print(f"[INFO] {len(zips)} arquivos para processar.")
    for i, zip_url in enumerate(zips, start=1):
        print(f"\n=== ({i}/{len(zips)}) ===")
        try:
            process_one_zip(
                zip_url=zip_url,
                work_dir=work_dir,
                out_dir=out_dir,
                keep_zip=args.keep_zip,
                keep_csv=args.keep_csv
            )
        except Exception as e:
            print(f"[ERRO] Falha ao processar {zip_url}: {e}")
            # continua nos próximos

    print("\n[OK] Finalizado.")

if __name__ == "__main__":
    main()
