# Importação CNPJ / Lead2Lead

Pipeline local para ingestão e consultas da base pública da Receita (CNPJ) usando Python + DuckDB, com exportações em CSV (BOM UTF-8, separador `;`) e suporte a filtros comuns (UF, município, CNAE etc.).

## Requisitos
- Python 3.11+ (recomendado)
- DuckDB (via pip)
- Git
- (Opcional) Git LFS

## Setup rápido
```bash
# Windows (PowerShell)
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
