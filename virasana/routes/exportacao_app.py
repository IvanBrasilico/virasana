# exportacao_app.py

from datetime import date, timedelta, datetime, time
from flask import render_template, request, flash, url_for, jsonify
from flask import Blueprint, render_template
from flask_wtf.csrf import generate_csrf
from sqlalchemy import text
from decimal import Decimal
from typing import Optional, Dict
import logging

def configure(app):
    '''  exportacao_app = Blueprint(
        'exportacao_app',
        __name__,
        url_prefix='/exportacao'
    )
    app.register_blueprint(exportacao_app)
    '''
    
    app.logger.setLevel(logging.DEBUG)


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


    # ---------------------------------------------------------
    # Consulta de PESO: primeira pesagem válida (I/R) do contêiner
    # no recinto de destino após dt_min (hora da entrada).
    # Mantido neste arquivo por simplicidade de integração.
    # ---------------------------------------------------------
    def consulta_peso_container(session, numero_conteiner: str, codigo_recinto: str, dt_min: datetime) -> Optional[Dict]:
        """
        Retorna a primeira pesagem efetiva (I/R) do contêiner no recinto,
        ocorrida após dt_min (entrada), já consolidada contra retificações/exclusões.
        """

        logging.getLogger().debug(
            f"[consulta_peso_container] in: numero={numero_conteiner}, recinto={codigo_recinto}, dt_min={dt_min}"
        )

        # Versão COM janela (preferencial se disponível)
        sql_window = text("""
            WITH ranked AS (
              SELECT
                id,
                codigoRecinto,
                dataHoraTransmissao,
                dataHoraOcorrencia,
                tipoOperacao,
                pesoBrutoBalanca,
                numeroConteiner,
                ROW_NUMBER() OVER (
                  PARTITION BY numeroConteiner, codigoRecinto, dataHoraOcorrencia
                  ORDER BY dataHoraTransmissao DESC, id DESC
                ) AS rn
              FROM apirecintos_pesagensveiculo
              WHERE numeroConteiner = :numeroConteiner
                AND codigoRecinto   = :codigoRecinto
                AND dataHoraOcorrencia >= :dtMin
            )
            SELECT
              id,
              codigoRecinto,
              dataHoraOcorrencia,
              dataHoraTransmissao,
              tipoOperacao,
              pesoBrutoBalanca
            FROM ranked
            WHERE rn = 1
              AND tipoOperacao IN ('I','R')     -- ignora ocorrências cujo último evento foi 'E'
            ORDER BY dataHoraOcorrencia ASC     -- primeira pesagem após a entrada
            LIMIT 1
        """)

        # Versão SEM janela (compatível com MariaDB antigas)
        # Estratégia:
        # 1) Para cada (numeroConteiner, codigoRecinto, dataHoraOcorrencia) após dt_min,
        #    pegar a última dataHoraTransmissao.
        # 2) Juntar de volta para obter a linha final; filtrar tipoOperacao IN (I,R).
        sql_no_window = text("""
            SELECT
              p.id,
              p.codigoRecinto,
              p.dataHoraOcorrencia,
              p.dataHoraTransmissao,
              p.tipoOperacao,
              p.pesoBrutoBalanca
            FROM apirecintos_pesagensveiculo p
            JOIN (
              SELECT
                numeroConteiner,
                codigoRecinto,
                dataHoraOcorrencia,
               MAX(dataHoraTransmissao) AS max_tx
              FROM apirecintos_pesagensveiculo
              WHERE numeroConteiner = :numeroConteiner
                AND codigoRecinto   = :codigoRecinto
                AND dataHoraOcorrencia >= :dtMin
              GROUP BY numeroConteiner, codigoRecinto, dataHoraOcorrencia
            ) ult
              ON  ult.numeroConteiner   = p.numeroConteiner
              AND ult.codigoRecinto     = p.codigoRecinto
              AND ult.dataHoraOcorrencia= p.dataHoraOcorrencia
              AND ult.max_tx            = p.dataHoraTransmissao
            WHERE p.tipoOperacao IN ('I','R')
            ORDER BY p.dataHoraOcorrencia ASC
            LIMIT 1
        """)

        # Se você souber que tem janela disponível, use direto sql_window.
        # Aqui tentamos primeiro janela; se falhar (ex.: ER_PARSE_ERROR), caímos no plano B.
        try:
            row = session.execute(sql_window, {
                "numeroConteiner": numero_conteiner,
                "codigoRecinto": codigo_recinto,
                "dtMin": dt_min
            }).mappings().first()
        except Exception:
            row = session.execute(sql_no_window, {
                "numeroConteiner": numero_conteiner,
                "codigoRecinto": codigo_recinto,
                "dtMin": dt_min
            }).mappings().first()

        if not row:
            # LOGS DIAGNÓSTICOS: Ver o que existe após dt_min, passo-a-passo
            logging.getLogger().debug("[consulta_peso_container] window/no_window sem resultado. Rodando checks…")
            # 1) Existe algo para esse contêiner após dt_min neste recinto?
            chk1 = session.execute(text("""
                SELECT COUNT(*) AS n
                FROM apirecintos_pesagensveiculo
                WHERE numeroConteiner=:n AND codigoRecinto=:r AND dataHoraOcorrencia>=:d
            """), {"n": numero_conteiner, "r": codigo_recinto, "d": dt_min}).scalar()
            logging.getLogger().debug(f"[consulta_peso_container] chk1 count(contêiner+recinto+>=dt_min)={chk1}")
            # 2) Quais são as top 3 ocorrências brutas (para inspecionar)?
            rows_dbg = session.execute(text("""
                SELECT id,codigoRecinto,dataHoraOcorrencia,dataHoraTransmissao,tipoOperacao,pesoBrutoBalanca
                FROM apirecintos_pesagensveiculo
                WHERE numeroConteiner=:n AND codigoRecinto=:r AND dataHoraOcorrencia>=:d
                ORDER BY dataHoraOcorrencia ASC, dataHoraTransmissao DESC, id DESC
                LIMIT 3
            """), {"n": numero_conteiner, "r": codigo_recinto, "d": dt_min}).mappings().all()
            logging.getLogger().debug(f"[consulta_peso_container] rows_dbg(top3)={rows_dbg}")
            return None

        peso = row["pesoBrutoBalanca"]
        if isinstance(peso, Decimal):
            peso = float(peso)

        return {
            "id": row["id"],
            "codigoRecinto": row["codigoRecinto"],
            "dataHoraOcorrencia": row["dataHoraOcorrencia"].strftime("%Y-%m-%d %H:%M:%S"),
            "dataHoraTransmissao": row["dataHoraTransmissao"].strftime("%Y-%m-%d %H:%M:%S") if row["dataHoraTransmissao"] else None,
            "tipoOperacao": row["tipoOperacao"],
            "pesoBrutoBalanca": peso
        }

    @app.route('/exportacao/consulta_peso', methods=['GET'])
    def exportacao_consulta_peso():
        """
        Endpoint para o fetch() do front-end.
        Parâmetros:
          - numeroConteiner: str
          - codigoRecinto  : str
          - dtMin          : 'YYYY-MM-DD HH:MM:SS' (hora da ENTRADA no destino)
        """
        session = app.config['db_session']
        numero  = request.args.get('numeroConteiner')
        recinto = request.args.get('codigoRecinto')
        dt_min  = request.args.get('dtMin')

        app.logger.debug(f"[consulta_peso] QS raw: numeroConteiner={numero!r}, codigoRecinto={recinto!r}, dtMin={dt_min!r}")
        if not (numero and recinto and dt_min):
            return jsonify({"error": "Parâmetros obrigatórios: numeroConteiner, codigoRecinto, dtMin"}), 400
        try:
            dt_min_parsed = datetime.strptime(dt_min, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return jsonify({"error": "dtMin inválido. Formato esperado: YYYY-MM-DD HH:MM:SS"}), 400
        app.logger.debug(f"[consulta_peso] Parsed: numero={numero}, recinto={recinto}, dt_min={dt_min_parsed} (type={type(dt_min_parsed)})")

        result = consulta_peso_container(session, numero, recinto, dt_min_parsed)
        if not result:
            app.logger.debug(f"[consulta_peso] NOT FOUND para numero={numero}, recinto={recinto}, dt_min={dt_min_parsed}")
            return jsonify({"found": False}), 404
        app.logger.debug(f"[consulta_peso] FOUND: {result}")
        return jsonify({"found": True, **result})

    # rota para listar entradas (E) em um recinto em uma data
    @app.route('/exportacao/transit_time', methods=['GET'])
    def transit_time():
        """
        Lista todos os containers que ENTRARAM (direcao = 'E') em um recinto específico
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
                -- Campos da ENTRADA (E) em um recinto (no dia)
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
                
                -- diferença de tempo
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
                  -- ou que estejam na lista adicional
                  AND (
                        s2.codigoRecinto LIKE '89327%' -- A maioria dos Redex de Santos possuem esse padrão de código
                     OR s2.codigoRecinto IN ('8931309', '8933204', '8931404') -- Redex da Localfrio/Movecta, Redex da Santos Brasil Clia e Redex da DPW/EMBRAPORT
                  )
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