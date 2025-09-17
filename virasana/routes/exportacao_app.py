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
    @app.route('/exportacao/transit_time', methods=['GET'])
    def transit_time():
        """
        Lista todos os containers que ENTRARAM (direcao = 'E') no codigoRecinto = '8931356'
        em uma data escolhida (via query string ?data=YYYY-MM-DD).
        Padrão: ontem (janela 00:00:00 inclusivo até 00:00 do dia seguinte exclusivo).
        """
        session = app.config['db_session']

        # Data selecionada via query string (?data=YYYY-MM-DD); fallback = ontem
        data_str = request.args.get('data')     # ex.: "2025-09-16"
        destino  = request.args.get('destino')     # ex.: "8931356" | "8931359"
        if data_str:
            try:
                data_base = datetime.strptime(data_str, "%Y-%m-%d").date()
            except ValueError:
                # Se formato inválido, volta para ontem
                data_base = datetime.now().date() - timedelta(days=1)
        else:
            data_base = datetime.now().date() - timedelta(days=1)

        # janela meio-aberta [00:00:00, +1 dia)
        inicio = datetime.combine(data_base, time.min)
        fim    = inicio + timedelta(days=1)

        # Rótulos para o template
        data_label = data_base.strftime("%d/%m/%Y")      # exibir no título
        data_iso   = data_base.strftime("%Y-%m-%d")      # preencher o <input type="date">

        # Filtro de RECINTO DE DESTINO (para a ENTRADA E)
        # default = 8931356 se nada/ou inválido
        if destino not in ("8931356", "8931359"):
            destino = "8931356"

        # Para cada ENTRADA (E), encontrar a última SAÍDA (S) anterior
        # em QUALQUER recinto (sem filtrar por codigoRecinto na subconsulta).
        sql = text("""
            SELECT
                -- Campos da ENTRADA (E) no recinto 8931356 (no dia)
                e.numeroConteiner,
                e.dataHoraOcorrencia,
                e.cnpjTransportador,
                e.placa,
                e.cpfMotorista,
                e.nomeMotorista,
                e.vazioConteiner,

                -- Campos da SAÍDA (S) imediatamente anterior (qualquer recinto)
                s.codigoRecinto         AS s_codigoRecinto,
                s.dataHoraOcorrencia    AS s_dataHoraOcorrencia,
                s.cnpjTransportador     AS s_cnpjTransportador,
                s.placa                 AS s_placa,
                s.cpfMotorista          AS s_cpfMotorista,
                s.nomeMotorista         AS s_nomeMotorista,
                s.vazioConteiner        AS s_vazioConteiner,
                TIMESTAMPDIFF(SECOND, s.dataHoraOcorrencia, e.dataHoraOcorrencia) / 3600.0 AS transit_time_horas

            FROM apirecintos_acessosveiculo e
            LEFT JOIN apirecintos_acessosveiculo s
              ON s.id = (
                 SELECT s2.id
                 FROM apirecintos_acessosveiculo s2
                 WHERE s2.numeroConteiner = e.numeroConteiner
                   AND s2.direcao = 'S'
                   AND s2.dataHoraOcorrencia < e.dataHoraOcorrencia
                   -- garantir que a saída seja de recinto diferente da entrada
                   -- restringir recintos de origem que comecem com 89327
                   AND s2.codigoRecinto LIKE '89327%'
                   AND s2.codigoRecinto <> e.codigoRecinto
                 ORDER BY s2.dataHoraOcorrencia DESC, s2.id DESC
                 LIMIT 1
              )
            WHERE
                e.codigoRecinto = :recinto_destino
                AND e.direcao = 'E'
                AND e.numeroConteiner IS NOT NULL
                AND e.numeroConteiner <> ''
                AND e.dataHoraOcorrencia >= :inicio
                AND e.dataHoraOcorrencia < :fim
            ORDER BY e.numeroConteiner ASC, e.dataHoraOcorrencia ASC
        """)

        rows = session.execute(sql, {
            "recinto_destino": destino,
            "inicio": inicio,
            "fim": fim
        }).fetchall()

        # rows é uma lista de Row objects; vamos padronizar para dicts simples
        resultados = [{
            "numeroConteiner": r.numeroConteiner,
            "dataHoraOcorrencia": r.dataHoraOcorrencia,
            "cnpjTransportador": r.cnpjTransportador,
            "placa": r.placa,
            "cpfMotorista": r.cpfMotorista,
            "nomeMotorista": r.nomeMotorista,
            "vazioConteiner": r.vazioConteiner,
            # campos da S anterior (podem ser None se não houver S anterior)
            "s_codigoRecinto": r.s_codigoRecinto,
            "s_dataHoraOcorrencia": r.s_dataHoraOcorrencia,
            "s_cnpjTransportador": r.s_cnpjTransportador,
            "s_placa": r.s_placa,
            "s_cpfMotorista": r.s_cpfMotorista,
            "s_nomeMotorista": r.s_nomeMotorista,
            "s_vazioConteiner": r.s_vazioConteiner,
            "transit_time_horas": r.transit_time_horas,            
        } for r in rows]

        return render_template(
            'exportacao_transit_time.html',
            resultados=resultados,
            data_label=data_label,
            # valor para manter o input <date> preenchido com a data escolhida
            data_iso=data_iso,
            destino=destino,
            csrf_token=generate_csrf
         )