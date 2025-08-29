import requests
import os
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("CNPJ_API_TOKEN")
HEADERS = {"x_api_token": API_TOKEN, "Accept": "application/json"}
SEARCH_URL = "https://comercial.cnpj.ws/pesquisa"


def pesquisar_empresas(filtros, max_paginas=5):
    resultados = []
    pagina = 1

    while pagina <= max_paginas:
        params = {**filtros, "pagina": pagina}
        response = requests.get(SEARCH_URL, headers=HEADERS, params=params, timeout=30)

        if response.status_code != 200:
            print(f"Erro na página {pagina}: {response.status_code} text: {response.text}")
            break

        body = response.json()
        lista = body.get("data", [])
        if not lista:
            break

        for item in lista:
            resultados.append({
                "cnpj": item.get("cnpj"),
                "razao_social": item.get("razao_social"),
                "nome_fantasia": item.get("nome_fantasia", ""),
                "uf": item.get("uf", ""),
                "municipio": item.get("municipio", ""),
                "situacao_cadastral": item.get("situacao_cadastral", ""),
                "porte": item.get("porte", ""),
                "cnae": item.get("atividade_principal", {}).get("descricao", "")
            })

        total_paginas = body.get("paginacao", {}).get("paginas", pagina)
        if pagina >= total_paginas:
            break

        pagina += 1

    return resultados
