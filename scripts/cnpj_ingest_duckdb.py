#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import shutil
import zipfile
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from urllib.parse import urljoin

import duckdb
import requests

# ========== Configurações padrão ==========
DEFAULT_BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-07/"
# Ex.: mude para outro mês: 2025-08/, 2025-06/, etc.

# Alguns arquivos muito grandes são divididos em vários zips (ex.: Estabelecimentos, Sócios)
# O script encontra todos automaticamente parseando a página do mês.
ZIP_PATTERN = re.compile(r'href="([^"]+\.zip)"', re.IGNORECASE)

# Parâmetros de ingestão
DUCKDB_THREADS = 4       # ou um número inteiro ex.: "8"
CSV_DELIM = ";"               # conforme layout da Receita
CSV_QUOTE = '"'               # aspas padrão
CSV_HEADER = True             # arquivos possuem header
ENCODING = "latin1"           # costuma ser latin1; troque para "utf8" se necessário

# Parquet
PARQUET_ROW_GROUP_SIZE = 128 * 1024 * 1024  # ~128MB
PARQUET_COMPRESSION = "ZSTD"                # "ZSTD" ou "SNAPPY"
PARQUET_PER_FILE = True                     # True = 1 parquet por CSV; False = shards múltiplos

# ========== Funções utilitárias ==========

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
    """Lê a página de índice do mês e retorna a lista completa de URLs .zip"""
    r = requests.get(base_url, timeout=60)
    r.raise_for_status()
    links = ZIP_PATTERN.findall(r.text)
    # remove duplicatas, ordena por nome
    zips = sorted(set(urljoin(base_url, href) for href in links))
    if not zips:
        raise RuntimeError("Nenhum .zip encontrado. Verifique o base_url.")
    return zips

def download_file(url: str, dest: Path, chunk=1024*1024):
    """Baixa um arquivo em chunks, com recomeço simples se já existir parcial."""
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
    """
    Extrai exatamente UM arquivo tabular do zip da Receita e
    garante que o arquivo final tenha extensão .csv.
    Muitos zips trazem nomes como 'F.K03200$Z.D50712.MUNICCSV' (sem .csv).
    """
    ensure_dir(extract_dir)

    with zipfile.ZipFile(zip_path, 'r') as z:
        names = z.namelist()
        if not names:
            raise RuntimeError(f"Nenhum arquivo dentro de {zip_path.name}")

        # 1) Prioriza arquivos que terminem com 'csv' ou '.csv' ou '.txt'
        cand = [m for m in names if m.lower().endswith(("csv", ".csv", ".txt"))]

        # 2) Se não achou, pega os que CONTÊM 'csv' no nome (caso MUNICCSV, EMPRECSV, ESTABELE etc.)
        if not cand:
            cand = [m for m in names if "csv" in m.lower()]

        # 3) Como fallback absoluto (bem raro), pega o MAIOR arquivo do zip
        if not cand:
            cand = [max(names, key=lambda m: z.getinfo(m).file_size)]

        # Usa o primeiro candidato
        member = cand[0]

        # Caminho alvo com extensão .csv garantida
        original_name = Path(member).name  # nome dentro do zip (sem subpastas)
        base_name = original_name
        if not base_name.lower().endswith(".csv"):
            base_name = base_name + ".csv"
        out_path = extract_dir / base_name

        # Remove arquivo de saída se já existir
        if out_path.exists():
            out_path.unlink()

        # Extrai para a pasta temporária
        z.extract(member, path=extract_dir)

        extracted = extract_dir / member  # pode vir com subpasta
        final_src = extracted

        # Se veio com subpastas, movemos para a raiz de extract_dir
        if extracted != out_path:
            # Garante pasta destino
            ensure_dir(out_path.parent)
            # Move/renomeia para o out_path com .csv
            shutil.move(str(extracted), str(out_path))
            final_src = out_path

            # Tenta limpar subpastas vazias
            try:
                subdir = extract_dir / Path(member).parent
                if subdir.exists():
                    # remove diretórios vazios intermediários
                    for p in sorted(subdir.glob("**/*"), reverse=True):
                        if p.is_dir():
                            try: p.rmdir()
                            except: pass
                    try: subdir.rmdir()
                    except: pass
            except Exception:
                pass
        else:
            # O arquivo was extraído na raiz; se não tem .csv, renomeia
            if final_src.suffix.lower() != ".csv":
                final_csv = final_src.with_suffix(final_src.suffix + ".csv")
                if final_csv.exists():
                    final_csv.unlink()
                final_src.rename(final_csv)
                out_path = final_csv

    return out_path

def ensure_utf8(src: Path) -> Path:
    """
    Garante um CSV em UTF-8. Se 'src' já estiver em UTF-8, retorna 'src'.
    Caso contrário, cria um arquivo .utf8.csv ao lado e retorna o novo path.
    A conversão é feita em streaming (linha a linha), sem estourar memória.
    """
    try:
        # Teste rápido: tenta ler alguns KB como UTF-8
        with open(src, "rb") as f:
            chunk = f.read(1_000_000)  # 1MB de amostra
        chunk.decode("utf-8")  # se falhar, vai pro except
        return src  # já é UTF-8
    except UnicodeDecodeError:
        pass

    dst = src.with_suffix(src.suffix + ".utf8.csv")
    # Se já existe de tentativa anterior, reaproveita
    if dst.exists() and dst.stat().st_size > 0:
        return dst

    # Transcodifica latin1 -> utf-8 em streaming
    print(f"[INFO] Transcodificando para UTF-8: {src.name} -> {dst.name}")
    with open(src, "r", encoding="latin1", newline="") as fin, \
         open(dst, "w", encoding="utf-8", newline="") as fout:
        # copia em blocos de linhas para ser eficiente
        buf = fin.readlines(1024 * 1024)  # ~1MB por lote de linhas (ajuste se quiser)
        while buf:
            fout.writelines(buf)
            buf = fin.readlines(1024 * 1024)
    return dst

def csv_to_parquet_with_duckdb(csv_path: Path, parquet_path: Path):
    """
    Lê CSV gigante (latin1, ; como separador) em chunks e grava Parquet via PyArrow.
    Evita o parser do DuckDB e resolve de vez problemas de encoding/UTF-8.
    """
    ensure_dir(parquet_path.parent)

    # Configurações
    chunksize = 200_000          # ajuste se quiser menos/memória
    sep = CSV_DELIM or ";"       # conforme especificação da Receita
    encoding = "latin1"          # RF usa latin1 nos dumps
    first = True
    writer = None

    try:
        for chunk in pd.read_csv(
            csv_path,
            sep=sep,
            encoding=encoding,
            dtype=str,                 # tudo como texto (seguro p/ qualidade dos dados)
            chunksize=chunksize,
            engine="python",           # engine robusta p/ CSV “difícil”
            on_bad_lines="warn"        # se encontrar linha inválida, pula e avisa
        ):
            # Normaliza nomes de colunas (se houver header) — muitos arquivos não têm;
            # pandas pode criar nomes 0..N-1 automaticamente. Mantemos assim.
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if first:
                writer = pq.ParquetWriter(
                    where=str(parquet_path),
                    schema=table.schema,
                    compression=PARQUET_COMPRESSION.lower(),  # 'zstd' ou 'snappy'
                    use_dictionary=False                       # menos CPU/memória
                )
                first = False
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

def process_one_zip(zip_url: str, work_dir: Path, out_dir: Path, keep_zip: bool, keep_csv: bool):
    """Baixa 1 zip (se não existir), extrai CSV, converte para Parquet e limpa temporários conforme flags."""
    zip_name = zip_url.rstrip("/").split("/")[-1]
    stem = Path(zip_name).stem  # sem .zip

    zip_path = work_dir / "zips" / zip_name
    csv_dir = work_dir / "csv_tmp"
    parquet_path = out_dir / f"{stem}.parquet"

    if parquet_path.exists():
        print(f"[SKIP] Já existe Parquet para {zip_name}: {parquet_path.name}")
        return

    # 1) download (somente se não existir)
    if zip_path.exists():
        print(f"[PULANDO DOWNLOAD] {zip_path.name} já existe ({human(zip_path.stat().st_size)}).")
        # Valida o ZIP existente; se estiver corrompido, rebaixa
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                bad = z.testzip()
                if bad is not None:
                    print(f"[AVISO] ZIP existente está corrompido (arquivo com problema: {bad}). Rebaixando...")
                    zip_path.unlink(missing_ok=True)
                    print(f"[BAIXANDO] {zip_url}")
                    download_file(zip_url, zip_path)
                    print(f"  -> {zip_path.name} ({human(zip_path.stat().st_size)})")
        except zipfile.BadZipFile:
            print(f"[AVISO] ZIP existente é inválido. Rebaixando...")
            zip_path.unlink(missing_ok=True)
            print(f"[BAIXANDO] {zip_url}")
            download_file(zip_url, zip_path)
            print(f"  -> {zip_path.name} ({human(zip_path.stat().st_size)})")
    else:
        print(f"[BAIXANDO] {zip_url}")
        download_file(zip_url, zip_path)
        print(f"  -> {zip_path.name} ({human(zip_path.stat().st_size)})")

    # 2) unzip  (1 CSV por zip)
    print(f"[EXTRAINDO] {zip_path.name}")
    csv_path = unzip_single_csv(zip_path, csv_dir)
    print(f"  -> {csv_path.name} ({human(csv_path.stat().st_size)})")

    # 3) csv -> parquet (DuckDB)
    print(f"[CONVERTENDO] {csv_path.name} -> {parquet_path.name}")
    t0 = time.time()
    csv_to_parquet_with_duckdb(csv_path, parquet_path)
    print(f"  -> OK em {time.time() - t0:.1f}s | {human(parquet_path.stat().st_size)}")

    # 4) limpeza
    if not keep_csv and csv_path.exists():
        csv_path.unlink()
    if not keep_zip and zip_path.exists():
        zip_path.unlink()


def main():
    parser = argparse.ArgumentParser(
        description="Ingestão 'arquivo por arquivo' dos Dados Abertos do CNPJ -> Parquet (DuckDB)."
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL,
                        help="URL base do mês (ex.: .../2025-07/).")
    parser.add_argument("--work-dir", default="./work",
                        help="Diretório de trabalho (temporários/ZIP/CSV). Pode ser no HD externo.")
    parser.add_argument("--out-dir", default="./parquet",
                        help="Diretório de saída dos Parquets (coloque no HD/SSD externo).")
    parser.add_argument("--keep-zip", action="store_true",
                        help="Mantém os .zip após converter.")
    parser.add_argument("--keep-csv", action="store_true",
                        help="Mantém os .csv extraídos após converter.")
    parser.add_argument("--filter", default="",
                        help="Processa apenas zips cujo nome contenha este trecho (regex simples).")
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

    # Filtro opcional (ex.: 'Estabelecimentos', 'Empresas', 'Socios', 'Simples')
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
            # segue para o próximo arquivo

    print("\n[OK] Finalizado.")

if __name__ == "__main__":
    main()
