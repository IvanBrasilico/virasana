# exportacao_app.py

from datetime import date, timedelta, datetime, time
from flask import render_template, request, flash, url_for
from flask import Blueprint, render_template
from flask_wtf.csrf import generate_csrf
from sqlalchemy import text


def configure(app):
    '''  exportacao_app = Blueprint(
        'exportacao_app',
        __name__,
        url_prefix='/exportacao'
    )
    app.register_blueprint(exportacao_app)
    '''

    @app.route('/exportacao/', methods=['GET'])
    def exportacao_app_index():
        return render_template(
            'exportacao_index.html',
            csrf_token=generate_csrf
        )

    def get_imagens_container_data(mongodb, numero, inicio_scan, fim_scan, vazio=False) -> list:
        query = {
            'metadata.contentType': 'image/jpeg',
            'metadata.dataescaneamento': {'$gte': inicio_scan, '$lte': fim_scan}
        }

        # Adiciona o filtro por número apenas se fornecido
        if numero:
            query['metadata.numeroinformado'] = numero

        projection = {
            'metadata.numeroinformado': 1,
            'metadata.dataescaneamento': 1,
            'metadata.predictions.vazio': 1
        }

        cursor = (
            mongodb['fs.files']
            .find(query, projection)
            .sort('metadata.dataescaneamento', -1)
            .limit(10)
        )

        return list(cursor)

    def get_imagens_container_sem_data(mongodb, numero, vazio=False) -> list:
        query = {
            'metadata.contentType': 'image/jpeg',
        }

        # Adiciona o filtro por número apenas se fornecido
        if numero:
            query['metadata.numeroinformado'] = numero

        projection = {
            'metadata.numeroinformado': 1,
            'metadata.dataescaneamento': 1,
            'metadata.predictions.vazio': 1
        }

        cursor = (
            mongodb['fs.files']
            .find(query, projection)
            .sort('metadata.dataescaneamento', -1)
            .limit(10)
        )

        return list(cursor)

    @app.route('/exportacao/stats', methods=['POST'])
    def exportacao_stats():

        mongodb = app.config['mongodb']
        session = app.config['db_session']

        numero = request.form.get('numero')
        start = request.form.get('start')
        end = request.form.get('end')

        if not start and not end and numero:
            arquivos = get_imagens_container_sem_data(mongodb, numero)
            return render_template(
                'exportacao.html',
                arquivos=arquivos,
                csrf_token=generate_csrf
            )

        inicio_scan = datetime.strptime(start, '%Y-%m-%d')
        fim_scan = datetime.strptime(end, '%Y-%m-%d')

        arquivos = get_imagens_container_data(mongodb, numero, inicio_scan, fim_scan)

        return render_template(
            'exportacao.html',
            arquivos=arquivos,
            csrf_token=generate_csrf
        )

    # rota para listar entradas (E) em um recinto em uma data
    @app.route('/transit_time', methods=['GET'])
    def transit_time():
        """
        Lista todos os containers que ENTRARAM (direcao = 'E') no codigoRecinto = '8931356'
        no dia 15/09/2025 entre 00:00:00 e 23:59:59.
        """
        session = app.config['db_session']

        # Janela fixa conforme solicitado
        inicio = datetime(2025, 9, 15, 0, 0, 0)
        fim    = datetime(2025, 9, 15, 23, 59, 59)

        sql = text("""
            SELECT
                numeroConteiner,
                placa,
                codigoRecinto,
                dataHoraOcorrencia
            FROM apirecintos_acessosveiculo
            WHERE
                codigoRecinto = :recinto
                AND direcao = 'E'
                AND numeroConteiner IS NOT NULL
                AND numeroConteiner <> ''
                AND dataHoraOcorrencia BETWEEN :inicio AND :fim
            ORDER BY dataHoraOcorrencia ASC
        """)

        rows = session.execute(sql, {
            "recinto": "8931356",
            "inicio":  inicio,
            "fim":     fim
        }).fetchall()

        # rows é uma lista de Row objects; vamos padronizar para dicts simples
        resultados = [{
            "numeroConteiner": r.numeroConteiner,
            "placa": r.placa,
            "codigoRecinto": r.codigoRecinto,
            "dataHoraOcorrencia": r.dataHoraOcorrencia
        } for r in rows]

        return render_template('exportacao_transit_time.html', resultados=resultados, csrf_token=generate_csrf)