# exportacao_app.py

from datetime import date, timedelta, datetime, time
from flask import render_template, request, flash, url_for, jsonify, Response
from flask import Blueprint, render_template
from flask_wtf.csrf import generate_csrf
from sqlalchemy import text
from decimal import Decimal
from typing import Optional, Dict, List
import logging
import csv
from io import StringIO

def configure(app):
    '''  exportacao_app = Blueprint(
        'exportacao_app',
        __name__,
        url_prefix='/exportacao'
    )
    app.register_blueprint(exportacao_app)
    '''
    
    app.logger.setLevel(logging.DEBUG)

    # Tolerância (em minutos) para cruzar timestamp de entrada/saída com pesagens
    TOL_MINUTOS_PESAGEM = 5

    # -------------------------------------------------
    # Opções de RECINTOS DE ORIGEM (para filtro checkboxes)
    # (mantém alinhado com o template)
    # -------------------------------------------------
    ORIGENS_OPCOES = {
      "8932793": "GTMINAS",
      "8932761": "JBS",
      "8932762": "SATEL",
      "8932799": "DALASTRA MONITORAMENTO",
      "8932724": "HIPERCON 1",
      "8932739": "HIPERCON 2",
      "8932706": "FASSINA 1",
      "8932707": "FASSINA 2",
      "8932711": "CORTÊS",
      "8932774": "DINAMO 1",
      "8932794": "DINAMO 2",
      "8932714": "ALAMO 1",
      "8932747": "ALAMO 2",
      "8932759": "DEICMAR",
      "8932754": "DELLA VOLPE",
      "8932778": "PAULISTA TERMINAL RETROPORTUARIO (GELOG)",
      "8932722": "ESTRELA",
      "8932758": "BRADO",
      "8932775": "SIGMA TRANSPORTES E LOGISTICA",
      "8932797": "SERRA & MARQUES",
      "8932709": "S MAGALHÃES 1",
      "8932710": "S MAGALHÃES 2",
      "8932796": "S MAGALHÃES 3",
      "8932798": "S MAGALHÃES 4",
      "8933204": "CLIA REDEX DA SANTOS BRASIL",
      "8931309": "LOCALFRIO/MOVECTA",
      "8932773": "ISIS 1",
      "8932788": "ISIS 2",
      "8931305": "TRANSBRASA",
      "8931356": "Santos Brasil",
      "8931359": "BTP",
      "8931404": "DPW/EMBRAPORT",
      "8931318": "Ecoporto"
    }

    def _sanitize_origens(lst):
        """Mantém apenas códigos válidos declarados em ORIGENS_OPCOES."""
        if not lst:
            return []
        return [c for c in lst if c in ORIGENS_OPCOES]

    # -----------------------------------------------
    # Utilidades para Transit Time (consulta + IQR)
    # -----------------------------------------------
    def _normaliza_destino(valor: Optional[str]) -> str:
        destinos_validos = ("8931356", "8931359", "8931404", "8931318")
        return valor if valor in destinos_validos else "8931356"

    def _quartis(sorted_vals):
        """ Q1/Q3 pelo método de Tukey (exclui o elemento central se ímpar). """
        n = len(sorted_vals)
        if n == 0:
            return None, None
        def _mediana(vals):
            m = len(vals)
            if m == 0: 
                return None
            mid = m // 2
            if m % 2 == 1:
                return float(vals[mid])
            else:
                return (float(vals[mid - 1]) + float(vals[mid])) / 2.0
        mid = n // 2
        if n % 2 == 0:
            lower = sorted_vals[:mid]
            upper = sorted_vals[mid:]
        else:
            lower = sorted_vals[:mid]
            upper = sorted_vals[mid+1:]
        q1 = _mediana(lower)
        q3 = _mediana(upper)
        return q1, q3

    def consultar_transit_time(session, recinto_destino: str, inicio: datetime, fim: datetime, origens_filtrar: Optional[List[str]] = None):
        """
        Executa a mesma SQL da rota /exportacao/transit_time e marca outliers (IQR).
        Retorna (resultados:list[dict], stats:dict).
        """
        recinto_destino = _normaliza_destino(recinto_destino)
        origens_filtrar = _sanitize_origens(origens_filtrar or [])

        # placeholders dinâmicos para IN (:o0, :o1, ...)
        o_ph = ", ".join([f":o{i}" for i in range(len(origens_filtrar))]) if origens_filtrar else ""
        sub_filtro_origem = (f" AND s2.codigoRecinto IN ({o_ph})" if origens_filtrar else "")
        where_filtro_origem = (f" AND s.codigoRecinto IN ({o_ph})" if origens_filtrar else "")

        sql_txt = f"""
            SELECT
                e.numeroConteiner,
                e.codigoRecinto,
                e.dataHoraOcorrencia,
                e.cnpjTransportador,
                e.placa,
                e.cpfMotorista,
                e.nomeMotorista,
                e.vazioConteiner,

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
                   AND (
                        s2.codigoRecinto LIKE '89327%'
                     OR s2.codigoRecinto IN ('8931309', '8933204', '8931404')
                   )
                   AND s2.codigoRecinto <> e.codigoRecinto
                   {sub_filtro_origem}
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
                {where_filtro_origem}
            ORDER BY e.numeroConteiner ASC, e.dataHoraOcorrencia ASC
        """

        params = {
            "recinto_destino": recinto_destino,
            "inicio": inicio,
            "fim": fim
        }
        # adiciona valores dos IN (:o0, :o1, ...)
        for i, code in enumerate(origens_filtrar):
            params[f"o{i}"] = code

        rows = session.execute(text(sql_txt), params).fetchall()

        resultados = [{
            "numeroConteiner": r.numeroConteiner,
            "codigoRecinto": r.codigoRecinto,
            "dataHoraOcorrencia": r.dataHoraOcorrencia,
            "cnpjTransportador": r.cnpjTransportador,
            "placa": r.placa,
            "cpfMotorista": r.cpfMotorista,
            "nomeMotorista": r.nomeMotorista,
            "vazioConteiner": r.vazioConteiner,
            "s_codigoRecinto": r.s_codigoRecinto,
            "s_dataHoraOcorrencia": r.s_dataHoraOcorrencia,
            "s_cnpjTransportador": r.s_cnpjTransportador,
            "s_placa": r.s_placa,
            "s_cpfMotorista": r.s_cpfMotorista,
            "s_nomeMotorista": r.s_nomeMotorista,
            "s_vazioConteiner": r.s_vazioConteiner,
            "transit_time_horas": r.transit_time_horas,
        } for r in rows]

        # IQR / outliers
        vals = sorted([
            float(x["transit_time_horas"]) for x in resultados
            if x["transit_time_horas"] is not None
        ])
        q1, q3 = _quartis(vals)
        if q1 is None or q3 is None:
            iqr = 0.0
            limite_outlier = None
        else:
            iqr = float(q3 - q1)
            limite_outlier = float(q3 + 1.5 * iqr)

        for item in resultados:
            v = item["transit_time_horas"]
            if v is None or limite_outlier is None:
                item["is_outlier"] = False
            else:
                item["is_outlier"] = (float(v) > limite_outlier)

        stats = {"q1": q1, "q3": q3, "iqr": iqr, "limite_outlier": limite_outlier}
        return resultados, stats

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
        Aplica tolerância: considera pesagens desde (dt_min - TOL_MINUTOS_PESAGEM).
        """
        
        dt_min_lower = dt_min - timedelta(minutes=TOL_MINUTOS_PESAGEM)        
        
        sql = text("""
            SELECT
              p.id,
              p.codigoRecinto,
              p.dataHoraOcorrencia,
              p.dataHoraTransmissao,
              p.tipoOperacao,
              p.pesoBrutoBalanca,
              p.placa
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
                AND dataHoraOcorrencia >= :dtMinLower
              GROUP BY numeroConteiner, codigoRecinto, dataHoraOcorrencia
            ) ult
              ON  ult.numeroConteiner    = p.numeroConteiner
              AND ult.codigoRecinto      = p.codigoRecinto
              AND ult.dataHoraOcorrencia = p.dataHoraOcorrencia
              AND ult.max_tx             = p.dataHoraTransmissao
            WHERE p.tipoOperacao IN ('I','R')
            ORDER BY p.dataHoraOcorrencia ASC
            LIMIT 1
        """)

        row = session.execute(sql, {
            "numeroConteiner": numero_conteiner,
            "codigoRecinto": codigo_recinto,
            "dtMinLower": dt_min_lower
        }).mappings().first()

        if not row:
            # LOGS DIAGNÓSTICOS
            try:
                n = session.execute(text("""
                    SELECT COUNT(*) AS n
                    FROM apirecintos_pesagensveiculo
                    WHERE numeroConteiner=:n AND codigoRecinto=:r AND dataHoraOcorrencia>=:d
                """), {"n": numero_conteiner, "r": codigo_recinto, "d": dt_min_lower}).scalar()
                app.logger.debug(f"[consulta_peso][chk-count] numero={numero_conteiner} recinto={codigo_recinto} >=(dtMin-5m)={dt_min_lower} -> count={n}")

                dbg = session.execute(text("""
                    SELECT id,codigoRecinto,dataHoraOcorrencia,dataHoraTransmissao,tipoOperacao,pesoBrutoBalanca
                    FROM apirecintos_pesagensveiculo
                    WHERE numeroConteiner=:n AND codigoRecinto=:r AND dataHoraOcorrencia>=:d
                    ORDER BY dataHoraOcorrencia ASC, dataHoraTransmissao DESC, id DESC
                    LIMIT 3
                """), {"n": numero_conteiner, "r": codigo_recinto, "d": dt_min_lower}).mappings().all()
                app.logger.debug(f"[consulta_peso][rows top3] {dbg}")
            except Exception as e:
                app.logger.exception("[consulta_peso] erro nos logs de diagnóstico")
            return None

        peso = row["pesoBrutoBalanca"]
        if peso is not None and isinstance(peso, Decimal):
            peso = float(peso)

        return {
            "id": row["id"],
            "codigoRecinto": row["codigoRecinto"],
            "dataHoraOcorrencia": row["dataHoraOcorrencia"].strftime("%Y-%m-%d %H:%M:%S"),
            "dataHoraTransmissao": row["dataHoraTransmissao"].strftime("%Y-%m-%d %H:%M:%S") if row["dataHoraTransmissao"] else None,
            "tipoOperacao": row["tipoOperacao"],
            "pesoBrutoBalanca": peso,
            "placa": row["placa"]
        }

    # ---------------------------------------------------------
    # Consulta de PESO: última pesagem válida (I/R) no recinto de ORIGEM
    # ocorrida ATÉ dt_max (hora da SAÍDA S do recinto anterior).
    # ---------------------------------------------------------
    def consulta_peso_ate(session, numero_conteiner: str, codigo_recinto: str, dt_max: datetime) -> Optional[Dict]:
        """
        Retorna a última pesagem efetiva (I/R) do contêiner no recinto,
        com dataHoraOcorrencia <= dt_max, consolidada por MAX(dataHoraTransmissao)
        por dataHoraOcorrencia.
        Aplica tolerância: considera pesagens até (dt_max + TOL_MINUTOS_PESAGEM).
        """
        
        dt_max_upper = dt_max + timedelta(minutes=TOL_MINUTOS_PESAGEM)        
        
        sql = text("""
            SELECT
              p.id,
              p.codigoRecinto,
              p.dataHoraOcorrencia,
              p.dataHoraTransmissao,
              p.tipoOperacao,
              p.pesoBrutoBalanca,
              p.placa
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
                AND dataHoraOcorrencia <= :dtMaxUpper
              GROUP BY numeroConteiner, codigoRecinto, dataHoraOcorrencia
            ) ult
              ON  ult.numeroConteiner    = p.numeroConteiner
              AND ult.codigoRecinto      = p.codigoRecinto
              AND ult.dataHoraOcorrencia = p.dataHoraOcorrencia
              AND ult.max_tx             = p.dataHoraTransmissao
            WHERE p.tipoOperacao IN ('I','R')
            ORDER BY p.dataHoraOcorrencia DESC
            LIMIT 1
        """)

        row = session.execute(sql, {
            "numeroConteiner": numero_conteiner,
            "codigoRecinto": codigo_recinto,
            "dtMaxUpper": dt_max_upper
        }).mappings().first()

        if not row:
            return None

        peso = row["pesoBrutoBalanca"]
        if peso is not None and isinstance(peso, Decimal):
            peso = float(peso)

        return {
            "id": row["id"],
            "codigoRecinto": row["codigoRecinto"],
            "dataHoraOcorrencia": row["dataHoraOcorrencia"].strftime("%Y-%m-%d %H:%M:%S"),
            "dataHoraTransmissao": row["dataHoraTransmissao"].strftime("%Y-%m-%d %H:%M:%S") if row["dataHoraTransmissao"] else None,
            "tipoOperacao": row["tipoOperacao"],
            "pesoBrutoBalanca": peso,
            "placa": row["placa"]
        }

    @app.route('/exportacao/consulta_peso', methods=['GET'])
    def exportacao_consulta_peso():
        """
        Endpoint para o fetch() do front-end.
        Parâmetros:
          - numeroConteiner: str
          - codigoRecinto  : str
          - dtMin          : 'YYYY-MM-DD HH:MM:SS' (hora da ENTRADA no destino)
          - sCodigoRecinto : str (opcional — recinto da SAÍDA anterior)
          - sDataHora      : 'YYYY-MM-DD HH:MM:SS' (opcional — hora da SAÍDA anterior)
        """
        session = app.config['db_session']
        numero  = request.args.get('numeroConteiner')
        recinto = request.args.get('codigoRecinto')
        dt_min  = request.args.get('dtMin')
        s_recinto = request.args.get('sCodigoRecinto')
        s_dh      = request.args.get('sDataHora')

        app.logger.debug(f"[consulta_peso] QS raw: numeroConteiner={numero!r}, codigoRecinto={recinto!r}, dtMin={dt_min!r}")
        if not (numero and recinto and dt_min):
            return jsonify({"error": "Parâmetros obrigatórios: numeroConteiner, codigoRecinto, dtMin"}), 400
        try:
            dt_min_parsed = datetime.strptime(dt_min, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return jsonify({"error": "dtMin inválido. Formato esperado: YYYY-MM-DD HH:MM:SS"}), 400
        app.logger.debug(f"[consulta_peso] Parsed: numero={numero}, destino={recinto}, dt_min={dt_min_parsed} (type={type(dt_min_parsed)})")

        entrada = consulta_peso_container(session, numero, recinto, dt_min_parsed)

        origem = None
        if s_recinto and s_dh:
            try:
                s_dt_parsed = datetime.strptime(s_dh, "%Y-%m-%d %H:%M:%S")
                origem = consulta_peso_ate(session, numero, s_recinto, s_dt_parsed)
            except Exception as e:
                app.logger.exception(f"[consulta_peso] erro ao consultar origem: {e}")
                origem = None

        delta = None
        if entrada and origem:
            pe = entrada.get("pesoBrutoBalanca")
            po = origem.get("pesoBrutoBalanca")
            if pe is not None and po is not None:
                delta = float(pe) - float(po)

        # Comparação de placas (quando ambas existem)
        placa_changed = None
        if entrada and origem:
            pe_placa = (entrada.get("placa") or "").strip().upper()
            po_placa = (origem.get("placa") or "").strip().upper()
            if pe_placa or po_placa:
                placa_changed = (pe_placa != "" and po_placa != "" and pe_placa != po_placa)


        if not entrada and not origem:
            app.logger.debug(f"[consulta_peso] NOT FOUND (entrada e origem) numero={numero}, destino={recinto}")
            return jsonify({"found": False}), 404

        payload = {
            "found": True,
            "entrada": entrada,   # pode ser None
            "origem": origem,     # pode ser None
            "delta_kg": delta,    # pode ser None
            "placa_changed": placa_changed  # True/False/None
        }
        app.logger.debug(f"[consulta_peso] payload: {payload}")
        return jsonify(payload)

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
        origens_sel = request.args.getlist('origem')  # múltiplos ?origem=...
        
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
        destino = _normaliza_destino(destino)

        # Para cada ENTRADA (E), encontrar a última SAÍDA (S) anterior
        # em QUALQUER recinto (sem filtrar por codigoRecinto na subconsulta).
        resultados, stats = consultar_transit_time(session, destino, inicio, fim, origens_filtrar=origens_sel)

        return render_template(
            'exportacao_transit_time.html',
            resultados=resultados,
            data_label=data_label,
            # valor para manter o input <date> preenchido com a data escolhida
            data_iso=data_iso,
            destino=destino,
            origens_sel=origens_sel,  # manter estado das checkboxes
            q1=stats["q1"], q3=stats["q3"], iqr=stats["iqr"], limite_outlier=stats["limite_outlier"],
            csrf_token=generate_csrf
         )
         
    @app.route('/exportacao/transit_time/exportar_csv', methods=['GET'])
    def transit_time_export():
        """
        Exporta os resultados filtrados (mesma lógica da tela) em CSV compatível com Excel.
        Query string: ?data=YYYY-MM-DD&destino=CODIGO
        """
        session = app.config['db_session']

        data_str = request.args.get('data')
        destino  = _normaliza_destino(request.args.get('destino'))
        origens_sel = request.args.getlist('origem')

        if data_str:
            try:
                data_base = datetime.strptime(data_str, "%Y-%m-%d").date()
            except ValueError:
                data_base = datetime.now().date() - timedelta(days=1)
        else:
            data_base = datetime.now().date() - timedelta(days=1)

        inicio = datetime.combine(data_base, time.min)
        fim    = inicio + timedelta(days=1)

        resultados, stats = consultar_transit_time(session, destino, inicio, fim, origens_filtrar=origens_sel)


        # Monta CSV (delimitador ';' funciona bem no Excel pt-BR)
        buf = StringIO()
        buf.write('\ufeff')  # BOM para acentuação correta no Excel (Windows)
        w = csv.writer(buf, delimiter=';', lineterminator='\r\n')

        w.writerow([
            "numeroConteiner",
            "destino_codigoRecinto",
            "entrada_dataHora",
            "vazioConteiner",
            "origem_codigoRecinto",
            "origem_dataHora",
            "transit_time_horas",
            "outlier_IQR",
            "cnpjTransportador",
            "placa",
            "nomeMotorista",
            "cpfMotorista"
        ])

        def _fmt_dt(dt):
            return dt.strftime("%d/%m/%Y %H:%M:%S") if dt else ""

        for r in resultados:
            w.writerow([
                r["numeroConteiner"] or "",
                r["codigoRecinto"] or "",
                _fmt_dt(r["dataHoraOcorrencia"]),
                ("Sim" if r["vazioConteiner"] else "Não") if r["vazioConteiner"] is not None else "",
                r["s_codigoRecinto"] or "",
                _fmt_dt(r["s_dataHoraOcorrencia"]),
                (f"{float(r['transit_time_horas']):.2f}" if r["transit_time_horas"] is not None else ""),
                ("1" if r.get("is_outlier") else "0"),
                r["cnpjTransportador"] or "",
                r["placa"] or "",
                r["nomeMotorista"] or "",
                r["cpfMotorista"] or ""
            ])

        filename = f"transit_time_{destino}_{data_base.strftime('%Y-%m-%d')}.csv"
        return Response(
            buf.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )