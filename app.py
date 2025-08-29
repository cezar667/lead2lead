from flask import Flask, request, jsonify, render_template, send_file
import pandas as pd
from services.cnpj_ws import pesquisar_empresas

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/gerar_excel")
def gerar_excel():
    modo = request.args.get("modo")

    if modo == "simulado":
        empresas = [
            {
                "cnpj": "12345678000100",
                "razao_social": "EMPRESA EXEMPLO LTDA",
                "nome_fantasia": "EXEMPLO",
                "uf": "MG",
                "municipio": "Belo Horizonte",
                "situacao_cadastral": "Ativa",
                "porte": "ME",
                "cnae": "Desenvolvimento de software sob encomenda"
            },
            {
                "cnpj": "23456789000111",
                "razao_social": "EXEMPLO 2 SERVIÇOS DIGITAIS",
                "nome_fantasia": "EXEMPLO 2",
                "uf": "MG",
                "municipio": "Uberlândia",
                "situacao_cadastral": "Ativa",
                "porte": "ME",
                "cnae": "Suporte técnico em TI"
            }
        ]

        df = pd.DataFrame(empresas)
        df.to_excel("exports/empresas.xlsx", index=False)

        return jsonify({
            "status": "sucesso",
            "mensagem": f"{len(empresas)} empresas simuladas exportadas com sucesso!",
            "arquivo": "/baixar_excel",
            "empresas": empresas[:10]
        })

    # Se não for simulado, continua com filtros reais
    filtros = {}
    filtros_permitidos = [
        "atividade_principal_id",
        "natureza_juridica_id",
        "estado_id",
        "situacao_cadastral",
        "data_inicio_atividade_de",
        "data_inicio_atividade_ate",
        "porte_id",
        "municipio_id",
        "cnae_secundario_id"
    ]

    for chave in filtros_permitidos:
        valor = request.args.get(chave)
        if valor:
            filtros[chave] = valor

    max_paginas = int(request.args.get("max_paginas", 10))
    empresas = pesquisar_empresas(filtros, max_paginas=max_paginas)

    if not empresas:
        return jsonify({
            "status": "erro",
            "mensagem": "Nenhum resultado encontrado com esses filtros."
        })

    df = pd.DataFrame(empresas)
    df.to_excel("exports/empresas.xlsx", index=False)

    return jsonify({
        "status": "sucesso",
        "mensagem": f"{len(empresas)} empresas exportadas com sucesso!",
        "arquivo": "/baixar_excel",
        "empresas": empresas[:10]
    })
    

@app.route("/baixar_excel")
def baixar_excel():
    return send_file("exports/empresas.xlsx", as_attachment=True)

if __name__ == "__main__":
    app.run(debug=True)