# exportacao_app.py

import logging
import csv
import os
import tempfile

from datetime import timedelta, datetime, time, timezone
from flask import render_template, request, jsonify, Response
from flask_wtf.csrf import generate_csrf
from sqlalchemy import text, bindparam
from sqlalchemy.exc import SQLAlchemyError
from decimal import Decimal
from copy import deepcopy
from typing import Optional, Dict, List
from io import StringIO
from pathlib import Path
from io import BytesIO
from zoneinfo import ZoneInfo
from pymongo import ASCENDING
from bson import ObjectId
from gridfs import GridFS
from PIL import Image


def configure(app):
    '''  exportacao_app = Blueprint(
        'exportacao_app',
        __name__,
        url_prefix='/exportacao'
    )
    app.register_blueprint(exportacao_app)
    '''

    app.logger.setLevel(logging.DEBUG)

    # Timezone da aplicação (entradas exibidas/fornecidas no horário local)
    APP_TZ = ZoneInfo("America/Sao_Paulo")

    # Tolerância para cruzar timestamp de entrada/saída com pesagens
    TOL_MINUTOS_PESAGEM = 20
    
    # usado quando numero= e não há de/ate/data
    DEFAULT_LOOKBACK_DAYS = 30

    # -------------------------------------------------
    # Opções de RECINTOS DE ORIGEM (para filtro checkboxes)
    # (mantém alinhado com o template)
    # -------------------------------------------------
    ORIGENS_OPCOES = {
      "8932793": "GT MINAS",
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
      "8933204": "REDEX/CLIA DA SANTOS BRASIL - cidade de Santos",
      "8933203": "REDEX/CLIA DA SANTOS BRASIL - cidade de Guarujá",
      "8931309": "LOCALFRIO/MOVECTA REDEX",
      "8933001": "LOCALFRIO/MOVECTA CLIA/REDEX",
      "8932792": "REDEX E DEPOT CE - TANK CONTEINER - CESARI",
      "8932773": "ISIS 1",
      "8932788": "ISIS 2",
      "8931305": "TRANSBRASA",
      "8931356": "SANTOS BRASIL",
      "8931359": "BTP",
      "8931304": "REDEX/IPA ECOPORTO TERMARES",
      "8931339": "ECOPORTO PATIO 2",
      "8931404": "DPW/EMBRAPORT",
      "8931318": "ECOPORTO"
    }

    # Tupla imutável com todas as origens possíveis (ordem preservada)
    ORIGENS_TODAS = tuple(ORIGENS_OPCOES.keys())

    # -------------------------------------------
    # Helper: Carrega mapa {codigo_recinto: email}
    # -------------------------------------------
    def load_emails_recintos(session) -> Dict[str, str]:
        rows = session.execute(text("""
            SELECT codigo_recinto, email_recinto
            FROM emails_recintos
        """)).all()
        # Compatível com Row mappings do SQLAlchemy
        return {str(r.codigo_recinto): str(r.email_recinto) for r in rows}

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
        """ Q1/Q3 pelo método de Tukey
        (exclui o elemento central se ímpar). """
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

    # -------------------------------------------
    # Helpers: CPF (normalização) e risco
    # -------------------------------------------
    def _cpf_digits(s: Optional[str]) -> str:
        """Mantém apenas dígitos do CPF (ou '' se None)."""
        if not s:
            return ""
        return "".join(ch for ch in str(s) if ch.isdigit())

    def _cpfs_em_risco(session, cpfs_iter):
        """
        Recebe um iterável de CPFs (possivelmente com pontuação, None, etc.),
        normaliza para dígitos e retorna um set com os CPFs encontrados
        na tabela risco_motoristas.

        Observação: usamos REPLACE na coluna do banco
        p/ normalizar também do lado SQL,
        garantindo match mesmo se estiverem com pontuação na tabela.
        """
        # Coleta únicos normalizados (só dígitos) e remove vazios
        cpfs_norm = sorted({
            _cpf_digits(c) for c in (cpfs_iter or []) if _cpf_digits(c)
        })
        if not cpfs_norm:
            return set()

        encontrados = set()
        CHUNK = 1000  # segurança para listas grandes
        for i in range(0, len(cpfs_norm), CHUNK):
            bloco = cpfs_norm[i:i+CHUNK]
            # Expanding bind param evita construir IN (...) manualmente
            sql = text("""
                SELECT cpf
                FROM risco_motoristas
                WHERE REPLACE(REPLACE(REPLACE(cpf, '.', ''), '-', ''), ' ', '') IN :cpfs
            """).bindparams(bindparam("cpfs", expanding=True))
            params = {"cpfs": tuple(bloco)}
            try:
                rows = session.execute(sql, params).all()
            except Exception:
                # Se a tabela não existir ou houver erro, não quebrar a tela
                rows = []
            for r in rows:
                # normaliza o que veio do banco e adiciona
                encontrados.add(_cpf_digits(getattr(r, "cpf", None)))
        return encontrados

    def consultar_transit_time(
        session,
        recinto_destino: str,
        inicio: datetime,
        fim: datetime,
        origens_filtrar: Optional[List[str]] = None
    ):
        """
        Executa a mesma SQL da rota /exportacao/transit_time
        e marca outliers (IQR).
        Retorna (resultados:list[dict], stats:dict).
        """
        recinto_destino = _normaliza_destino(recinto_destino)
        origens_filtrar = _sanitize_origens(origens_filtrar or [])

        # Usa expanding bind param para listas IN
        sub_filtro_origem = (" AND s2.codigoRecinto IN :origens" if origens_filtrar else "")
        where_filtro_origem = (" AND s.codigoRecinto IN :origens" if origens_filtrar else "")

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
                     OR s2.codigoRecinto IN ('8931309', '8933204', '8931404', '8933203', '8933001', '8931339', '8931305', '8931304')
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
                AND (
                  e.tipoDeclaracao IS NULL OR e.tipoDeclaracao = '' OR e.tipoDeclaracao NOT IN ('DI','DUI','DTA')
                )
                AND e.dataHoraOcorrencia >= :inicio
                AND e.dataHoraOcorrencia < :fim
--                 AND NOT EXISTS (
--                   SELECT 1
--                   FROM apirecintos_acessosveiculo s3
--                   WHERE s3.numeroConteiner    = e.numeroConteiner
--                     AND s3.codigoRecinto      = e.codigoRecinto
--                     AND s3.direcao            = 'S'
--                     AND s3.dataHoraOcorrencia > e.dataHoraOcorrencia
--                 )
                {where_filtro_origem}
            ORDER BY e.numeroConteiner ASC, e.dataHoraOcorrencia ASC
        """

        params = {
            "recinto_destino": recinto_destino,
            "inicio": inicio,
            "fim": fim
        }
        if origens_filtrar:
            params["origens"] = tuple(origens_filtrar)
            rows = session.execute(
                text(sql_txt).bindparams(bindparam("origens", expanding=True)),
                params
            ).fetchall()
        else:
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

        # Pós-processamento comum (DUE, risco, IQR)
        return _posprocessar_transit_rows(session, resultados)

    def _posprocessar_transit_rows(session, resultados: List[Dict]):
        """Enriquece linhas de trânsito (DUE, risco CPF) e calcula IQR/outliers.
        Retorna (resultados_enriquecidos, stats)."""
        resultados = deepcopy(resultados)  # não mutar quem chamou
        # 1) DUE/Exportador/NCM — âncora = ENTRADA
        anchors = []
        for it in resultados:
            num = (it.get("numeroConteiner") or "").strip().upper()
            dt = it.get("dataHoraOcorrencia")
            if num and dt:
                anchors.append((num, dt))
        mapa_due = _enriquecer_due_por_container(session, anchors, janela_dias=15)
        for item in resultados:
            k = ((item.get("numeroConteiner") or "").strip().upper(), item.get("dataHoraOcorrencia"))
            info = mapa_due.get(k)
            item["numero_due"] = info["numero_due"] if info else None
            item["cnpj_estabelecimento_exportador"] = info["cnpj_estabelecimento_exportador"] if info else None
            item["nfe_ncm"] = info["nfe_ncm"] if info else None
            item["due_itens"] = info.get("due_itens") if info else []
            item["exportador_nome"] = info.get("exportador_nome") if info else None
        # 2) Risco por CPF (apenas ENTRADA)
        cpfs_entrada = [it.get("cpfMotorista") for it in resultados if it.get("cpfMotorista")]
        try:
            em_risco = _cpfs_em_risco(session, cpfs_entrada)
        except Exception:
            app.logger.exception("[risco_motoristas] falha ao consultar CPFs de risco")
            em_risco = set()
        for it in resultados:
            it["motorista_risco"] = (_cpf_digits(it.get("cpfMotorista")) in em_risco)
        # 3) IQR/outliers
        vals = sorted([
            float(x["transit_time_horas"]) for x in resultados
            if x["transit_time_horas"] is not None
        ])
        q1, q3 = _quartis(vals)
        if q1 is None or q3 is None:
            iqr = 0.0
            limite_outlier_mild = None
            limite_outlier_strict = None
        else:
            iqr = float(q3 - q1)
            limite_outlier_mild = float(q3 + 1.5 * iqr)
            limite_outlier_strict = float(q3 + 15.0 * iqr)
        for item in resultados:
            v = item["transit_time_horas"]
            if v is None or iqr <= 0 or limite_outlier_mild is None or limite_outlier_strict is None:
                item["is_outlier"] = False
            else:
                vv = float(v)
                item["is_outlier"] = (vv >= limite_outlier_mild) and (vv <= limite_outlier_strict)
        stats = {
            "q1": q1, "q3": q3, "iqr": iqr,
            "limite_outlier_mild": limite_outlier_mild,
            "limite_outlier_strict": limite_outlier_strict
        }
        return resultados, stats

    def consultar_transit_time_por_container(
         session,
         numero_conteiner: str,
         inicio: datetime,
         fim: datetime,
         origens_filtrar: Optional[List[str]] = None,
         destinos_validos: Optional[List[str]] = None
     ):
        """
        Lista TODAS as ENTRADAS (E) do contêiner nos terminais de destino (default 4)
        no intervalo [inicio, fim), emparelhando com a última SAÍDA (S) anterior,
        e aplicando o mesmo pós-processamento da tela de transit time.
        """
        destinos_validos = destinos_validos or ["8931356", "8931359", "8931404", "8931318"]
        origens_filtrar = _sanitize_origens(origens_filtrar or [])
        sub_filtro_origem = (" AND s2.codigoRecinto IN :origens" if origens_filtrar else "")
        where_filtro_origem = (" AND s.codigoRecinto IN :origens" if origens_filtrar else "")

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
                 s.codigoRecinto       AS s_codigoRecinto,
                 s.dataHoraOcorrencia  AS s_dataHoraOcorrencia,
                 s.cnpjTransportador   AS s_cnpjTransportador,
                 s.placa               AS s_placa,
                 s.cpfMotorista        AS s_cpfMotorista,
                 s.nomeMotorista       AS s_nomeMotorista,
                 s.vazioConteiner      AS s_vazioConteiner,
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
                      OR s2.codigoRecinto IN ('8931309', '8933204', '8931404', '8933203', '8933001', '8931339', '8931305', '8931304')
                    )
                    AND s2.codigoRecinto <> e.codigoRecinto
                    {sub_filtro_origem}
                  ORDER BY s2.dataHoraOcorrencia DESC, s2.id DESC
                  LIMIT 1
               )
             WHERE
                 e.direcao = 'E'
                 AND e.numeroConteiner = :numero
                 AND e.codigoRecinto IN :destinos
                 AND (
                   e.tipoDeclaracao IS NULL OR e.tipoDeclaracao = '' OR e.tipoDeclaracao NOT IN ('DI','DUI','DTA')
                 )
                 AND e.dataHoraOcorrencia >= :inicio
                 AND e.dataHoraOcorrencia < :fim
--                AND NOT EXISTS (
--                  SELECT 1
--                  FROM apirecintos_acessosveiculo s3
--                  WHERE s3.numeroConteiner    = e.numeroConteiner
--                    AND s3.codigoRecinto      = e.codigoRecinto
--                    AND s3.direcao            = 'S'
--                    AND s3.dataHoraOcorrencia > e.dataHoraOcorrencia
--                )
                 {where_filtro_origem}
             ORDER BY e.dataHoraOcorrencia ASC
         """
        params = {
             "numero": (numero_conteiner or "").strip().upper(),
             "destinos": tuple(destinos_validos),
             "inicio": inicio,
             "fim": fim
        }
        bind = text(sql_txt).bindparams(bindparam("destinos", expanding=True))
        if origens_filtrar:
            bind = bind.bindparams(bindparam("origens", expanding=True))
            params["origens"] = tuple(origens_filtrar)
        rows = session.execute(bind, params).fetchall()
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
        return _posprocessar_transit_rows(session, resultados)

    @app.route('/exportacao/', methods=['GET'])
    def exportacao_app_index():
        return render_template(
            'exportacao_index.html',
            csrf_token=generate_csrf
        )


# TRECHO COMENTADO A SEGUIR NÃO É MAIS UTILIZADO NO PROJETO
#    def get_imagens_container_data(
#        mongodb,
#        numero,
#        inicio_scan,
#        fim_scan,
#        vazio=False
#    ) -> list:
#        query = {
#            'metadata.contentType': 'image/jpeg',
#            'metadata.dataescaneamento': {'$gte': inicio_scan, '$lte': fim_scan}
#        }
#
#        # Adiciona o filtro por número apenas se fornecido
#        if numero:
#            query['metadata.numeroinformado'] = numero
#
#        projection = {
#            'metadata.numeroinformado': 1,
#            'metadata.dataescaneamento': 1,
#            'metadata.predictions.vazio': 1
#        }
#
#        cursor = (
#            mongodb['fs.files']
#            .find(query, projection)
#            .sort('metadata.dataescaneamento', -1)
#            .limit(10)
#        )
#
#        return list(cursor)
#
#    def get_imagens_container_sem_data(mongodb, numero, vazio=False) -> list:
#        query = {
#            'metadata.contentType': 'image/jpeg',
#        }
#
#        # Adiciona o filtro por número apenas se fornecido
#        if numero:
#            query['metadata.numeroinformado'] = numero
#
#        projection = {
#            'metadata.numeroinformado': 1,
#            'metadata.dataescaneamento': 1,
#            'metadata.predictions.vazio': 1
#        }
#
#        cursor = (
#            mongodb['fs.files']
#            .find(query, projection)
#            .sort('metadata.dataescaneamento', -1)
#            .limit(10)
#        )
#
#        return list(cursor)
#
#    @app.route('/exportacao/stats', methods=['POST'])
#    def exportacao_stats():
#
#        mongodb = app.config['mongodb']
#        session = app.config['db_session']
#
#        numero = request.form.get('numero')
#        start = request.form.get('start')
#        end = request.form.get('end')
#
#        if not start and not end and numero:
#            arquivos = get_imagens_container_sem_data(mongodb, numero)
#            return render_template(
#                'exportacao.html',
#                arquivos=arquivos,
#                csrf_token=generate_csrf
#            )
#
#        inicio_scan = datetime.strptime(start, '%Y-%m-%d')
#        fim_scan = datetime.strptime(end, '%Y-%m-%d')
#
#        arquivos = get_imagens_container_data(
#            mongodb,
#            numero,
#            inicio_scan,
#            fim_scan,
#        )
#
#        return render_template(
#            'exportacao.html',
#            arquivos=arquivos,
#            csrf_token=generate_csrf
#        )


    # ---------------------------------------------------------
    # Consulta de PESO: primeira pesagem válida (I/R) do contêiner
    # no recinto de destino após dt_min (hora da entrada).
    # Mantido neste arquivo por simplicidade de integração.
    # ---------------------------------------------------------
    def consulta_peso_container(
        session,
        numero_conteiner: str,
        codigo_recinto: str,
        dt_min: datetime
    ) -> Optional[Dict]:
        """
        Retorna a pesagem efetiva (I/R) do contêiner no recinto mais próxima de dt_min,
        já consolidada contra retificações/exclusões.
        Aplica tolerância SIMÉTRICA: considera pesagens no intervalo
        [dt_min - TOL_MINUTOS_PESAGEM, dt_min + TOL_MINUTOS_PESAGEM].
        """

        dt_min_lower = dt_min - timedelta(minutes=TOL_MINUTOS_PESAGEM)
        dt_max_upper = dt_min + timedelta(minutes=TOL_MINUTOS_PESAGEM)

        sql = text("""
            SELECT
              p.id,
              p.codigoRecinto,
              p.dataHoraOcorrencia,
              p.dataHoraTransmissao,
              p.tipoOperacao,
              p.pesoBrutoBalanca,
              p.placa,
              /* distância em segundos até dt_min para escolher a mais próxima */
              ABS(TIMESTAMPDIFF(SECOND, p.dataHoraOcorrencia, :dtRef)) AS dist_seg
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
                AND dataHoraOcorrencia BETWEEN :dtMinLower AND :dtMaxUpper
              GROUP BY numeroConteiner, codigoRecinto, dataHoraOcorrencia
            ) ult
              ON  ult.numeroConteiner    = p.numeroConteiner
              AND ult.codigoRecinto      = p.codigoRecinto
              AND ult.dataHoraOcorrencia = p.dataHoraOcorrencia
              AND ult.max_tx             = p.dataHoraTransmissao
            WHERE p.tipoOperacao IN ('I','R')
            /* 1) mais próxima de dt_min; 2) em caso de empate, preferir a mais cedo */
            ORDER BY dist_seg ASC, p.dataHoraOcorrencia ASC
            LIMIT 1
        """)

        row = session.execute(sql, {
            "numeroConteiner": numero_conteiner,
            "codigoRecinto": codigo_recinto,
            "dtMinLower": dt_min_lower,
            "dtMaxUpper": dt_max_upper,
            "dtRef":      dt_min
        }).mappings().first()

        if not row:
            # LOGS DIAGNÓSTICOS
            try:
                n = session.execute(text("""
                    SELECT COUNT(*) AS n
                    FROM apirecintos_pesagensveiculo
                    WHERE numeroConteiner=:n AND codigoRecinto=:r
                      AND dataHoraOcorrencia BETWEEN :d1 AND :d2
                """), {"n": numero_conteiner, "r": codigo_recinto, "d1": dt_min_lower, "d2": dt_max_upper}).scalar()
                app.logger.debug(f"[consulta_peso][chk-count] numero={numero_conteiner} recinto={codigo_recinto} BETWEEN({dt_min_lower},{dt_max_upper}) -> count={n}")

                dbg = session.execute(text("""
                    SELECT id,codigoRecinto,dataHoraOcorrencia,
                    dataHoraTransmissao,tipoOperacao,pesoBrutoBalanca
                    FROM apirecintos_pesagensveiculo
                    WHERE numeroConteiner=:n AND codigoRecinto=:r
                      AND dataHoraOcorrencia BETWEEN :d1 AND :d2
                    ORDER BY
                      ABS(TIMESTAMPDIFF(SECOND, dataHoraOcorrencia, :dtRef)) ASC,
                      dataHoraOcorrencia ASC,
                      dataHoraTransmissao DESC, id DESC
                    LIMIT 3
                """), {"n": numero_conteiner, "r": codigo_recinto, "d1": dt_min_lower, "d2": dt_max_upper, "dtRef": dt_min}).mappings().all()
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
    # ocorrida PRÓXIMA de dt_max (hora da SAÍDA S do recinto anterior),
    # com tolerância simétrica ±TOL_MINUTOS_PESAGEM.
    # ---------------------------------------------------------
    def consulta_peso_ate(
        session,
        numero_conteiner: str,
        codigo_recinto: str,
        dt_max: datetime
    ) -> Optional[Dict]:
        """
        Retorna a pesagem efetiva (I/R) do contêiner no recinto mais próxima de dt_max,
        consolidada por MAX(dataHoraTransmissao) por dataHoraOcorrencia.
        Aplica tolerância SIMÉTRICA: considera pesagens no intervalo
        [dt_max - TOL_MINUTOS_PESAGEM, dt_max + TOL_MINUTOS_PESAGEM].
        """

        dt_max_upper = dt_max + timedelta(minutes=TOL_MINUTOS_PESAGEM)
        dt_min_lower = dt_max - timedelta(minutes=TOL_MINUTOS_PESAGEM)

        sql = text("""
            SELECT
              p.id,
              p.codigoRecinto,
              p.dataHoraOcorrencia,
              p.dataHoraTransmissao,
              p.tipoOperacao,
              p.pesoBrutoBalanca,
              p.placa,
              /* distância em segundos até dt_max para escolher a mais próxima */
              ABS(TIMESTAMPDIFF(SECOND, p.dataHoraOcorrencia, :dtRef)) AS dist_seg
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
                AND dataHoraOcorrencia BETWEEN :dtMinLower AND :dtMaxUpper
              GROUP BY numeroConteiner, codigoRecinto, dataHoraOcorrencia
            ) ult
              ON  ult.numeroConteiner    = p.numeroConteiner
              AND ult.codigoRecinto      = p.codigoRecinto
              AND ult.dataHoraOcorrencia = p.dataHoraOcorrencia
              AND ult.max_tx             = p.dataHoraTransmissao
            WHERE p.tipoOperacao IN ('I','R')
            /* 1) mais próxima de dt_max; 2) em caso de empate, a ocorrência mais recente */
            ORDER BY dist_seg ASC, p.dataHoraOcorrencia DESC
            LIMIT 1
        """)

        row = session.execute(sql, {
            "numeroConteiner": numero_conteiner,
            "codigoRecinto": codigo_recinto,
            "dtMinLower": dt_min_lower,
            "dtMaxUpper": dt_max_upper,
            "dtRef":      dt_max
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
          - codigoRecinto: str
          - dtMin: 'YYYY-MM-DD HH:MM:SS' (hora da ENTRADA no destino)
          - sCodigoRecinto: str (opcional — recinto da SAÍDA anterior)
          - sDataHora: 'YYYY-MM-DD HH:MM:SS' (opcional — hora da SAÍDA anterior)
        """
        session = app.config['db_session']
        numero = request.args.get('numeroConteiner')
        recinto = request.args.get('codigoRecinto')
        dt_min = request.args.get('dtMin')
        s_recinto = request.args.get('sCodigoRecinto')
        s_dh = request.args.get('sDataHora')

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
        data_str = request.args.get('data')            # ex.: "2025-09-16" (modo antigo por dia)
        de_str   = request.args.get('de')              # ex.: "2025-10-01"
        ate_str  = request.args.get('ate')             # ex.: "2025-10-31"
        destino  = request.args.get('destino')         # ex.: "8931356" | "8931359"
        numero_param = (request.args.get('numero') or request.args.get('numeroConteiner') or "").strip().upper()

        # múltiplos ?origem=...
        # Regra:
        #  - Se NÃO houver chave 'origem' na query string (primeiro acesso), default = TODAS as origens.
        #  - Se houver chave 'origem' (mesmo que lista vazia porque o usuário limpou), respeitamos o valor enviado:
        #      * lista vazia -> sem filtro (comportamento atual)
        #      * lista com itens -> filtra pelos itens.
        if 'origem' in request.args:
            origens_sel = _sanitize_origens(request.args.getlist('origem'))
        else:
            # primeiro acesso: marcar todas como selecionadas
            origens_sel = list(ORIGENS_TODAS)

        if numero_param:
            # ---------------------------
            # MODO CONTÊINER COM INTERVALO
            # ---------------------------
            # Priorização de intervalo:
            # 1) se "de" ou "ate" vierem -> usa intervalo [de, ate+1d)
            # 2) senão, se "data" vier -> usa janela do dia [data, data+1d)
            # 3) senão -> usa últimos DEFAULT_LOOKBACK_DAYS até agora
            parse = lambda s: datetime.strptime(s, "%Y-%m-%d").date()
            today = datetime.now().date()

            if de_str or ate_str:
                try:
                    if de_str:
                        de_date = parse(de_str)
                    else:
                        # sem "de": usa ate - DEFAULT_LOOKBACK_DAYS
                        ate_date_tmp = parse(ate_str)
                        de_date = ate_date_tmp - timedelta(days=DEFAULT_LOOKBACK_DAYS)
                    if ate_str:
                        ate_date = parse(ate_str)
                    else:
                        # sem "ate": usa hoje
                        ate_date = today
                except ValueError:
                    # se qualquer inválido, cai no fallback 3
                    de_date = today - timedelta(days=DEFAULT_LOOKBACK_DAYS)
                    ate_date = today
                inicio = datetime.combine(de_date, time.min)
                fim = datetime.combine(ate_date, time.min) + timedelta(days=1)
                data_label = f"De {de_date.strftime('%d/%m/%Y')} até {ate_date.strftime('%d/%m/%Y')}"
                data_iso = None
                de_iso = de_date.strftime("%Y-%m-%d")
                ate_iso = ate_date.strftime("%Y-%m-%d")
            elif data_str:
                # comportamento antigo por dia, se o usuário quiser
                try:
                    data_base = parse(data_str)
                except ValueError:
                    data_base = today - timedelta(days=1)
                inicio = datetime.combine(data_base, time.min)
                fim = inicio + timedelta(days=1)
                data_label = data_base.strftime("%d/%m/%Y")
                data_iso = data_base.strftime("%Y-%m-%d")
                de_iso = None
                ate_iso = None
            else:
                # fallback: últimos N dias
                ate_date = today
                de_date = today - timedelta(days=DEFAULT_LOOKBACK_DAYS)
                inicio = datetime.combine(de_date, time.min)
                fim = datetime.combine(ate_date, time.min) + timedelta(days=1)
                data_label = f"Últimos {DEFAULT_LOOKBACK_DAYS} dias (até {ate_date.strftime('%d/%m/%Y')})"
                data_iso = None
                de_iso = de_date.strftime("%Y-%m-%d")
                ate_iso = ate_date.strftime("%Y-%m-%d")

            destino = "multi"  # rótulo especial no template
            resultados, stats = consultar_transit_time_por_container(
                session, numero_param, inicio, fim, origens_filtrar=origens_sel
            )
        else:
            # ---------------------------
            # MODO ANTIGO POR DIA + DESTINO
            # ---------------------------
            if data_str:
                try:
                    data_base = datetime.strptime(data_str, "%Y-%m-%d").date()
                except ValueError:
                    data_base = datetime.now().date() - timedelta(days=1)
            else:
                data_base = datetime.now().date() - timedelta(days=1)
            inicio = datetime.combine(data_base, time.min)
            fim = inicio + timedelta(days=1)
            data_label = data_base.strftime("%d/%m/%Y")
            data_iso = data_base.strftime("%Y-%m-%d")
            de_iso = None
            ate_iso = None

            destino = _normaliza_destino(destino)
            resultados, stats = consultar_transit_time(
                session, destino, inicio, fim, origens_filtrar=origens_sel
            )

        # Carrega mapa {codigo_recinto: email} para exibir ícone de e-mail no template
        emails_map = load_emails_recintos(session)

        return render_template(
            'exportacao_transit_time.html',
            resultados=resultados,
            data_label=data_label,
            data_iso=data_iso,
            de_iso=de_iso,
            ate_iso=ate_iso,
            destino=destino,
            numero=numero_param,
            origens_sel=origens_sel,  # manter estado das checkboxes
            q1=stats["q1"], q3=stats["q3"], iqr=stats["iqr"],
            limite_outlier_mild=stats["limite_outlier_mild"],
            limite_outlier_strict=stats["limite_outlier_strict"],
            csrf_token=generate_csrf,
            emails_recintos=emails_map
         )

    @app.route('/exportacao/transit_time/exportar_csv', methods=['GET'])
    def transit_time_export():
        """
        Exporta os resultados filtrados em CSV compatível com Excel.
        Query string: ?data=YYYY-MM-DD&destino=CODIGO  OU  ?data=YYYY-MM-DD&numero=CONT
        """
        session = app.config['db_session']

        data_str = request.args.get('data')
        de_str   = request.args.get('de')
        ate_str  = request.args.get('ate')
        numero_param = (request.args.get('numero') or request.args.get('numeroConteiner') or "").strip().upper()
        destino_arg = request.args.get('destino')
        destino = _normaliza_destino(destino_arg) if destino_arg else None

        # Mesma regra do endpoint da tela:
        #  - Sem chave 'origem' => default = TODAS (mantém checkboxes “todas marcadas” no primeiro acesso
        #    e exporta coerentemente).
        #  - Com chave 'origem' => respeita o que veio (inclusive vazio = sem filtro).
        if 'origem' in request.args:
            origens_sel = _sanitize_origens(request.args.getlist('origem'))
        else:
            origens_sel = list(ORIGENS_TODAS)

        if numero_param:
            parse = lambda s: datetime.strptime(s, "%Y-%m-%d").date()
            today = datetime.now().date()
            if de_str or ate_str:
                try:
                    if de_str:
                        de_date = parse(de_str)
                    else:
                        ate_date_tmp = parse(ate_str)
                        de_date = ate_date_tmp - timedelta(days=DEFAULT_LOOKBACK_DAYS)
                    if ate_str:
                        ate_date = parse(ate_str)
                    else:
                        ate_date = today
                except ValueError:
                    de_date = today - timedelta(days=DEFAULT_LOOKBACK_DAYS)
                    ate_date = today
                inicio = datetime.combine(de_date, time.min)
                fim = datetime.combine(ate_date, time.min) + timedelta(days=1)
                filename_suffix = f"{de_date.strftime('%Y-%m-%d')}_a_{ate_date.strftime('%Y-%m-%d')}"
            elif data_str:
                try:
                    data_base = parse(data_str)
                except ValueError:
                    data_base = today - timedelta(days=1)
                inicio = datetime.combine(data_base, time.min)
                fim = inicio + timedelta(days=1)
                filename_suffix = data_base.strftime('%Y-%m-%d')
            else:
                ate_date = today
                de_date = today - timedelta(days=DEFAULT_LOOKBACK_DAYS)
                inicio = datetime.combine(de_date, time.min)
                fim = datetime.combine(ate_date, time.min) + timedelta(days=1)
                filename_suffix = f"ult{DEFAULT_LOOKBACK_DAYS}d_ate_{ate_date.strftime('%Y-%m-%d')}"

            resultados, stats = consultar_transit_time_por_container(
                session, numero_param, inicio, fim, origens_filtrar=origens_sel
            )
        else:
            # modo antigo export por dia+destino
            if data_str:
                try:
                    data_base = datetime.strptime(data_str, "%Y-%m-%d").date()
                except ValueError:
                    data_base = datetime.now().date() - timedelta(days=1)
            else:
                data_base = datetime.now().date() - timedelta(days=1)
            inicio = datetime.combine(data_base, time.min)
            fim = inicio + timedelta(days=1)
            destino = _normaliza_destino(destino or "8931356")
            resultados, stats = consultar_transit_time(session, destino, inicio, fim, origens_filtrar=origens_sel)
            filename_suffix = data_base.strftime('%Y-%m-%d')

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
            "cpfMotorista",
            "numero_due",
            "exportador_nome",
            "cnpj_estabelecimento_exportador",
            "nfe_ncm"
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
                r["cpfMotorista"] or "",
                r.get("numero_due") or "",
                r.get("exportador_nome") or "",
                r.get("cnpj_estabelecimento_exportador") or "",
                r.get("nfe_ncm") or ""
            ])

        filename = (
            f"transit_time_{numero_param}_{filename_suffix}.csv"
            if numero_param else
            f"transit_time_{destino}_{filename_suffix}.csv"
        )

        return Response(
            buf.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

    # ======================
    #   IMAGENS (MongoDB)
    # ======================

    # Diretório local p/ cache de thumbnails (sem hardcode de /tmp)
    # Prioridade:
    # 1) app.config["THUMB_CACHE_DIR"]
    # 2) env VIRASANA_THUMB_CACHE_DIR
    # 3) diretório temp da plataforma + subpasta da app
    _thumb_cache_base = (
        app.config.get("THUMB_CACHE_DIR")
        or os.environ.get("VIRASANA_THUMB_CACHE_DIR")
        or str(Path(tempfile.gettempdir()) / "virasana_exportacao_thumbs")
    )
    THUMB_CACHE_DIR = Path(_thumb_cache_base).resolve()
    # Cria diretório com permissões restritivas em POSIX (best-effort)
    # Em Windows o chmod é ignorado para bits POSIX.
    if not THUMB_CACHE_DIR.exists():
        THUMB_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(THUMB_CACHE_DIR, 0o700)
        except Exception:
            # Se não suportado (Windows) ou sem permissão, segue o fluxo
            app.logger.debug("[thumbs] chmod 0700 não aplicado em %s", THUMB_CACHE_DIR)
    elif not THUMB_CACHE_DIR.is_dir():
        raise RuntimeError(f"THUMB_CACHE_DIR não é diretório: {THUMB_CACHE_DIR}")

    # Largura padrão das thumbs (pode ajustar no querystring ?w=)
    DEFAULT_THUMB_WIDTH = 320

    # Limites de segurança
    MAX_CONTAINERS_PER_BULK = 100         # limite de lote por requisição do front
    MAX_IN_NUMEROS_SIZE = 500         # quebra o $in em sublotes p/ queries Mongo muito grandes

    def _norm_numero(n: str) -> str:
        """Normaliza o número do contêiner para maiúsculas e sem espaços."""
        return (n or "").strip().upper()

    def _parse_local_to_utc_naive(s: str) -> datetime:
        """
        Interpreta 'YYYY-MM-DD HH:MM:SS' no horário local (APP_TZ),
        converte para UTC e retorna datetime **naive em UTC**,
        que é exatamente o que o PyMongo espera (naive ≡ UTC).
        """
        dt_local = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=APP_TZ)
        dt_utc = dt_local.astimezone(timezone.utc)
        return dt_utc.replace(tzinfo=None)

    def _thumb_cache_path(file_id: str, w: int) -> Path:
        # file_id vem de ObjectId; w é inteiro sanetizado abaixo.
        # Mantemos nomes determinísticos por ser cache; gravação é atômica via os.replace.
        return THUMB_CACHE_DIR / f"{file_id}_w{w}.jpg"

    def _make_thumb_bytes(img_bytes: bytes, width: int) -> bytes:
        """Gera JPEG thumbnail (lado máx = width), preservando proporção."""
        img = Image.open(BytesIO(img_bytes))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        # garante lado máximo = width
        img.thumbnail((width, width))
        out = BytesIO()
        img.save(out, format="JPEG", quality=85, optimize=True, progressive=True)
        return out.getvalue()

    def _read_gridfs_file_bytes(gridfs_obj: GridFS, oid: ObjectId) -> bytes:
        f = gridfs_obj.get(oid)
        return f.read()

    def _ensure_indexes(mongodb):
        """
        Cria índices importantes (idempotente).
        Chame 1x por ciclo ou deixe habilitado (barato se já existir).
        """
        try:
            mongodb["fs.files"].create_index(
                [("metadata.numeroinformado", ASCENDING), ("metadata.dataescaneamento", ASCENDING)]
            )
            # Se você padroniza contentType para image/jpeg, pode trocar por índice parcial.
            app.logger.info("[imgs] Índices verificados/criados em fs.files.")
        except Exception:
            app.logger.exception("[imgs] Erro ao criar/verificar índices.")

    _ensure_indexes(app.config["mongodb"])

    # -------------------------------------------------
    # Enriquecimento: DUE/Exportador/NCM
    # -------------------------------------------------
    def _enriquecer_due_por_container(session,
                                      anchors: List[tuple],
                                      janela_dias: int = 15) -> Dict[tuple, Dict[str, Optional[str]]]:
        """
        Para uma lista de âncoras por LINHA, retorna:
          (numero_conteiner, dt_anchor) -> {
            'numero_due': str|None,
            'cnpj_estabelecimento_exportador': str|None,
            'nfe_ncm': str|None,
            'due_itens': list[str],
            'exportador_nome': str|None
          }
        Regra temporal: considerar SOMENTE DUEs com d.data_criacao_due entre
        [dt_anchor - janela_dias, dt_anchor], escolhendo a DUE "mais recente"
        por COALESCE(d.data_registro_due, d.data_criacao_due) DESC.
        Compatível com MariaDB 5.5 (sem window functions).
        """
        if not anchors:
            return {}

        # Evita truncamento dos GROUP_CONCAT
        try:
            session.execute(text("SET SESSION group_concat_max_len = 4096"))
        except SQLAlchemyError as e:
            # Sem permissão/compatibilidade? Mantém fluxo e registra para diagnóstico.
            app.logger.debug("[due] SET group_concat_max_len ignorado: %s", e)

        # Normaliza âncoras: [(NUM_UC, DT)]
        norm_anchors: List[tuple] = []
        for (n, dt) in anchors:
            if not n or not dt:
                continue
            norm_anchors.append(((n or "").strip().upper(), dt))
        if not norm_anchors:
            return {}

        out: Dict[tuple, Dict[str, Optional[str]]] = {}
        # Chunk menor porque cada linha vira um SELECT no UNION ALL
        CHUNK = 300
        # Janela em dias como inteiro literal no SQL (MariaDB 5.5 não gosta de bind em INTERVAL)
        jdias = int(janela_dias or 15)

        for i in range(0, len(norm_anchors), CHUNK):
            lote = norm_anchors[i:i+CHUNK]
            if not lote:
                continue

            # Monta subquery de âncoras via UNION ALL parametrizado
            # SELECT :n0 AS numero_conteiner, :t0 AS dt_anchor UNION ALL SELECT :n1, :t1 ...
            parts = []
            params = {}
            for idx, (num, dt) in enumerate(lote):
                parts.append(f"SELECT :n{idx} AS numero_conteiner, :t{idx} AS dt_anchor")
                params[f"n{idx}"] = num
                # Garantir string 'YYYY-MM-DD HH:MM:SS' para o bind
                params[f"t{idx}"] = dt.strftime("%Y-%m-%d %H:%M:%S")
            anchors_sql = " \nUNION ALL\n ".join(parts)

            # Subselect dpc escolhe a DUE mais recente por (numero_conteiner, dt_anchor),
            # restrita à janela temporal baseada em data_criacao_due.
            sql_txt = f"""
                SELECT
                  dpc.numero_conteiner,
                  dpc.dt_anchor,
                  dpc.numero_due,
                  d.cnpj_estabelecimento_exportador,
                  ncm.nfe_ncm,
                  ncm.due_itens_concat,
                  le.nome AS exportador_nome
                FROM (
                  SELECT
                    a.numero_conteiner,
                    a.dt_anchor,
                    SUBSTRING_INDEX(
                      GROUP_CONCAT(
                        d.numero_due
                        ORDER BY COALESCE(d.data_registro_due, d.data_criacao_due) DESC
                        SEPARATOR ','
                      ), ',', 1
                    ) AS numero_due
                  FROM (
                    {anchors_sql}
                  ) AS a
                  JOIN pucomex_due_conteiner dc
                    ON dc.numero_conteiner = a.numero_conteiner
                  JOIN pucomex_due d
                    ON d.numero_due = dc.numero_due
                   AND d.data_criacao_due BETWEEN DATE_SUB(a.dt_anchor, INTERVAL {jdias} DAY) AND a.dt_anchor
                  GROUP BY a.numero_conteiner, a.dt_anchor
                ) AS dpc
                LEFT JOIN pucomex_due d
                  ON d.numero_due = dpc.numero_due
                /* nome do exportador pelos 8 primeiros dígitos do CNPJ (somente dígitos) */
                LEFT JOIN laudo_empresas le
                  ON le.cnpj = LEFT(
                       REPLACE(REPLACE(REPLACE(COALESCE(d.cnpj_estabelecimento_exportador,''), '.', ''), '/', ''), '-', ''),
                       8
                     )
                LEFT JOIN (
                  SELECT
                    i.nr_due,
                    GROUP_CONCAT(DISTINCT NULLIF(TRIM(i.nfe_ncm), '')
                                 ORDER BY i.nfe_ncm SEPARATOR ', ') AS nfe_ncm,
                    GROUP_CONCAT(
                      CONCAT_WS(
                        ' ',
                        NULLIF(TRIM(i.descricao_item), ''),
                        CASE
                          WHEN NULLIF(TRIM(i.nfe_ncm), '') IS NOT NULL
                          THEN CONCAT('(', TRIM(i.nfe_ncm), ')')
                        END
                      )
                      ORDER BY i.due_nr_item
                      SEPARATOR '||'
                    ) AS due_itens_concat
                  FROM pucomex_due_itens i
                  GROUP BY i.nr_due
                ) AS ncm
                  ON ncm.nr_due = dpc.numero_due
            """
            rows = session.execute(text(sql_txt), params).mappings().all()
            for r in rows:
                # Reconstrói a chave (numero, dt_anchor) a partir da linha
                num = (r.get("numero_conteiner") or "").strip().upper()
                dt_anchor = r.get("dt_anchor")
                # dt_anchor volta como string (bind), normalizar p/ datetime?
                # Para chave consistente com o chamador, parseamos:
                if isinstance(dt_anchor, str):
                    try:
                        dt_anchor = datetime.strptime(
                            dt_anchor, "%Y-%m-%d %H:%M:%S"
                        )
                    except (ValueError, TypeError) as e:
                        # Se o formato divergir, mantemos o valor original (string),
                        # mas registramos para diagnóstico sem interromper o fluxo.
                        app.logger.debug("[due] dt_anchor parse falhou: %r (%s)", dt_anchor, e)

                itens_list = []
                if r.get("due_itens_concat"):
                    itens_list = [s for s in r["due_itens_concat"].split("||") if s and s.strip()]

                out[(num, dt_anchor)] = {
                    "numero_due": r.get("numero_due"),
                    "cnpj_estabelecimento_exportador": r.get("cnpj_estabelecimento_exportador"),
                    "nfe_ncm": r.get("nfe_ncm"),
                    "due_itens": itens_list,
                    "exportador_nome": r.get("exportador_nome"),
                }
        return out

    def _query_images_bulk_for_containers(
        mongodb,
        entradas_por_numero: dict[str, datetime]
    ) -> dict[str, list[dict]]:
        """
        Consulta o Mongo em (poucas) queries para um conjunto de contêineres e
        retorna um mapa: numero -> [ {id, data(datetime)} ... ] contendo apenas
        imagens com metadata.dataescaneamento ∈ (entrada, entrada+2h].
        """
        if not entradas_por_numero:
            return {}

        # Normaliza chaves e calcula janela global
        entradas_por_numero = {_norm_numero(k): v for k, v in entradas_por_numero.items() if k}
        numeros = list(entradas_por_numero.keys())
        if not numeros:
            return {}

        T_min_global = min(entradas_por_numero.values())
        T_max_global = max(entradas_por_numero.values()) + timedelta(hours=2)

        # Monta filtro base para a janela global
        base_filter = {
            "metadata.numeroinformado": {"$in": []},  # preenchido por sublotes
            "metadata.dataescaneamento": {"$gt": T_min_global, "$lte": T_max_global},
            "metadata.contentType": {"$in": ["image/jpeg", "image/jpg"]},
        }
        projection = {
            "_id": 1,
            "metadata.numeroinformado": 1,
            "metadata.dataescaneamento": 1,
        }
        sort_spec = [("metadata.numeroinformado", 1), ("metadata.dataescaneamento", 1)]

        imgs_por_numero: dict[str, list[dict]] = {n: [] for n in numeros}

        # Quebra o $in em sublotes p/ evitar documentos e redes muito grandes
        for i in range(0, len(numeros), MAX_IN_NUMEROS_SIZE):
            lote = numeros[i:i + MAX_IN_NUMEROS_SIZE]
            if not lote:
                continue

            filtro = dict(base_filter)
            filtro["metadata.numeroinformado"] = {"$in": lote}

            cursor = (
                mongodb["fs.files"]
                .find(filtro, projection)
                .sort(sort_spec)
            )

            for doc in cursor:
                n = _norm_numero(doc.get("metadata", {}).get("numeroinformado", ""))
                dsc = doc.get("metadata", {}).get("dataescaneamento", None)
                if not (n and dsc and n in entradas_por_numero):
                    continue

                Te = entradas_por_numero[n]
                if Te < dsc <= (Te + timedelta(hours=2)):
                    imgs_por_numero[n].append({
                        "id": str(doc["_id"]),
                        "data": dsc,
                    })

        return imgs_por_numero

    def _query_carga_bulk_for_containers(
        mongodb,
        entradas_por_numero: dict[str, datetime]
    ) -> dict[str, dict]:
        """Busca um *snapshot* de carga por contêiner, próximo da ENTRADA.

        Estratégia:
          1) Janela global = [min(entradas)-7d, max(entradas)+2h+7d]
          2) Filtra apenas docs com metadata.carga presente
          3) Itera 1x e escolhe o melhor doc por contêiner com a prioridade:
             (a) Te < d <= Te+2h   (preferido)
             (b) d <= Te           (mais recente antes)
             (c) d > Te            (o mais próximo depois, até +7d)
        Retorna um mapa numero -> payload resumido.
        """
        if not entradas_por_numero:
            return {}

        entradas_por_numero = {_norm_numero(k): v for k, v in entradas_por_numero.items() if k}
        numeros = list(entradas_por_numero.keys())
        if not numeros:
            return {}

        t_min = min(entradas_por_numero.values())
        t_max = max(entradas_por_numero.values())
        janela_ini = t_min - timedelta(days=4)
        janela_fim = t_max + timedelta(hours=2, days=4)

        base_filter = {
            "metadata.carga": {"$exists": True},
            "metadata.dataescaneamento": {"$gte": janela_ini, "$lte": janela_fim},
        }
        projection = {
            "_id": 1,
            "metadata.numeroinformado": 1,
            "metadata.dataescaneamento": 1,
            "metadata.carga": 1,
            "metadata.xml.alerta": 1,
        }
        sort_spec = [("metadata.numeroinformado", 1), ("metadata.dataescaneamento", 1)]

        # Seleções parciais por número
        best_pref: dict[str, dict] = {}
        best_before: dict[str, dict] = {}
        best_after: dict[str, dict] = {}

        MAX_IN = MAX_IN_NUMEROS_SIZE
        for i in range(0, len(numeros), MAX_IN):
            lote = numeros[i:i+MAX_IN]
            if not lote:
                continue

            filtro = dict(base_filter)
            filtro["metadata.numeroinformado"] = {"$in": lote}

            cursor = (
                mongodb["fs.files"]
                .find(filtro, projection)
                .sort(sort_spec)
                
            )

            for doc in cursor:
                n = _norm_numero(doc.get("metadata", {}).get("numeroinformado", ""))
                if not n or n not in entradas_por_numero:
                    continue
                dsc = doc.get("metadata", {}).get("dataescaneamento", None)
                if not dsc:
                    continue
                Te = entradas_por_numero[n]

                if Te < dsc <= (Te + timedelta(hours=2)):
                    # mantém o primeiro da janela preferida (ordem crescente de dataescaneamento)
                    if n not in best_pref:
                        best_pref[n] = doc
                    continue

                if dsc <= Te:
                    # queremos o mais próximo *antes* => manter o MAIOR dsc
                    cur = best_before.get(n)
                    if (cur is None) or (cur["metadata"]["dataescaneamento"] < dsc):
                        best_before[n] = doc
                else:
                    # após Te => manter o MENOR dsc
                    cur = best_after.get(n)
                    if (cur is None) or (cur["metadata"]["dataescaneamento"] > dsc):
                        best_after[n] = doc

        # Escolhe por prioridade e formata
        out: dict[str, dict] = {}

        def _trunc(s: str, maxlen: int = 500) -> str:
            s = (s or "")
            return (s[:maxlen] + "…") if len(s) > maxlen else s

        def _to_upper(s: str) -> str:
            return (s or "").upper()

        def _num_from_str(x):
            # tenta converter '003890.000' -> 3890 (int) quando possível
            try:
                val = float(str(x).replace(",", "."))
                if abs(val - round(val)) < 1e-6:
                    return int(round(val))
                return val
            except Exception:
                return None

        def _build_payload(doc: dict) -> dict:
            meta = doc.get("metadata", {})
            carga = (meta.get("carga") or {}) if isinstance(meta.get("carga"), dict) else {}
            dsc = meta.get("dataescaneamento")
            xml = meta.get("xml") or {}
            alerta_terminal = bool(xml.get("alerta") in (True, "true", "True", "1", 1))

            # Manifestos / Portos
            manifestos = []
            portos_origem = set()
            portos_destino = set()

            for m in carga.get("manifesto", []) or []:
                man = m.get("manifesto")
                if man:
                    manifestos.append(str(man))
                po = m.get("codigoportocarregamento")
                pd = m.get("codigoportodescarregamento")
                if po: portos_origem.add(_to_upper(po))
                if pd: portos_destino.add(_to_upper(pd))

            # Conhecimentos
            conhecimentos = []
            for c in carga.get("conhecimento", []) or []:
                conhecimentos.append({
                    "tipo": c.get("tipo"),
                    "numero": c.get("conhecimento"),
                    "descricaomercadoria": _trunc(c.get("descricaomercadoria") or ""),
                    "agente": c.get("codigoagentenavegacao"),
                    "consignatario": c.get("cpfcnpjconsignatario"),
                    "porto_origem": _to_upper(c.get("codigoportoorigem")),
                    "porto_destino": _to_upper(c.get("codigoportodestino")),
                })
                if c.get("codigoportoorigem"): portos_origem.add(_to_upper(c.get("codigoportoorigem")))
                if c.get("codigoportodestino"): portos_destino.add(_to_upper(c.get("codigoportodestino")))

            # NCMs
            ncms = []
            for ncm in carga.get("ncm", []) or []:
                code = (ncm.get("ncm") or "").strip()
                if code:
                    ncms.append(code)

            # Container info (pega o primeiro item)
            cont_info = {}
            cont_list = carga.get("container", []) or []
            if cont_list:
                ci = cont_list[0]
                cont_info = {
                    "lacre": ci.get("lacre"),
                    "tara_kg": _num_from_str(ci.get("taracontainer")),
                    "peso_bruto_item_kg": _num_from_str(ci.get("pesobrutoitem")),
                    "volume_item_m3": _num_from_str(ci.get("volumeitem")),
                    "uso_parcial": ci.get("indicadorusoparcial") in (True, "true", "True", "1", 1),
                }

            payload = {
                "carga_present": True,
                "pesototal": carga.get("pesototal"),
                "manifestos": sorted(set(manifestos)),
                "portos": {
                    "origens": sorted(portos_origem),
                    "destinos": sorted(portos_destino),
                },
                "conhecimentos": conhecimentos,
                "ncm": sorted(set(ncms)),
                "container_info": cont_info,
                "alerta_terminal": alerta_terminal,
                "snapshot_ref": {
                    "file_id": str(doc.get("_id")),
                    "dataescaneamento": dsc.isoformat() if isinstance(dsc, datetime) else None,
                }
            }
            return payload

        for n in numeros:
            chosen = best_pref.get(n) or best_before.get(n) or best_after.get(n)
            if chosen:
                out[n] = _build_payload(chosen)
            else:
                # Sem snapshot de carga — ainda não vasculhamos XML-only.
                # Por ora, mantém alerta como False (pode-se evoluir com um segundo passe se necessário).
                out[n] = {"carga_present": False, "alerta_terminal": False}

        return out


    @app.route("/exportacao/transit_time/carga_bulk", methods=["POST"])
    def exportacao_transit_time_carga_bulk():
        """Endpoint bulk que retorna um *resumo* da carga por contêiner.

        Body JSON:
        {
          "containers": [
            {"numero": "MSCU1234567", "entrada": "YYYY-MM-DD HH:MM:SS"},
            ...
          ]
        }
        Restrições:
          - Máx. {MAX_CONTAINERS_PER_BULK} contêineres por chamada
          - Datas são interpretadas no fuso da aplicação e convertidas p/ UTC-naive
        """
        try:
            payload = request.get_json(silent=True) or {}
            items = payload.get("containers", [])
            if not isinstance(items, list):
                return jsonify({"error": "Formato inválido"}), 400

            if len(items) > MAX_CONTAINERS_PER_BULK:
                return jsonify({"error": f"Excede limite de {MAX_CONTAINERS_PER_BULK} contêineres por requisição"}), 400

            entradas_por_numero: dict[str, datetime] = {}
            for it in items:
                numero = _norm_numero(it.get("numero", ""))
                entrada_str = it.get("entrada", "")
                if not (numero and entrada_str):
                    continue
                try:
                    entradas_por_numero[numero] = _parse_local_to_utc_naive(entrada_str)
                except Exception:
                    app.logger.debug(f"[carga_bulk] Ignorando entrada inválida: numero={numero!r}, entrada={entrada_str!r}")

            mongodb = app.config["mongodb"]
            result_map = _query_carga_bulk_for_containers(mongodb, entradas_por_numero)
            return jsonify(result_map)

        except Exception:
            app.logger.exception("[carga_bulk] Erro inesperado")
            return jsonify({"error": "Erro interno"}), 500

    @app.route("/exportacao/transit_time/imgs_bulk", methods=["POST"])
    def exportacao_transit_time_imgs_bulk():
        """
        Recebe um lote de contêineres visíveis no front e devolve,
        para cada um, TODAS as imagens em (entrada, entrada+2h].
        Corpo JSON:
        {
          "containers": [
            {"numero": "MSCU1234567", "entrada": "YYYY-MM-DD HH:MM:SS"},
            ...
          ]
        }
        """
        try:
            payload = request.get_json(silent=True) or {}
            items = payload.get("containers", [])
            if not isinstance(items, list):
                return jsonify({"error": "Formato inválido"}), 400

            if len(items) > MAX_CONTAINERS_PER_BULK:
                return jsonify({"error": f"Excede limite de {MAX_CONTAINERS_PER_BULK} contêineres por requisição"}), 400

            entradas_por_numero: dict[str, datetime] = {}
            for it in items:
                numero = _norm_numero(it.get("numero", ""))
                entrada_str = it.get("entrada", "")
                if not (numero and entrada_str):
                    continue
                try:
                    # Converter ENTRADA (fornecida no horário local) para UTC naive
                    entradas_por_numero[numero] = _parse_local_to_utc_naive(entrada_str)
                except Exception:
                    app.logger.debug(f"[imgs_bulk] Ignorando entrada inválida: numero={numero!r}, entrada={entrada_str!r}")

            mongodb = app.config["mongodb"]
            result_map = _query_images_bulk_for_containers(mongodb, entradas_por_numero)

            # Serializa resposta (datas em ISO para eventual debug/uso futuro)
            resp = {}
            for n, lst in result_map.items():
                resp[n] = [
                    {"id": x["id"], "data": x["data"].isoformat()}
                    for x in lst
                ]
            return jsonify(resp)

        except Exception as e:
            app.logger.exception("[imgs_bulk] Erro inesperado")
            return jsonify({"error": "Erro interno"}), 500

    @app.route("/exportacao/img/<file_id>", methods=["GET"])
    def exportacao_img(file_id: str):
        """
        Serve thumbnail JPEG cacheável de uma imagem do GridFS por _id.
        Querystring: ?w=320 (largura máx. da thumb, default 320)
        Cache local em /tmp/exportacao_thumbs.
        """
        try:
            w = request.args.get("w", str(DEFAULT_THUMB_WIDTH)).strip()
            try:
                width = max(64, min(4096, int(w)))
            except Exception:
                width = DEFAULT_THUMB_WIDTH

            try:
                oid = ObjectId(file_id)
            except Exception:
                return Response("Not found", status=404)

            cache_path = _thumb_cache_path(file_id, width)
            if cache_path.exists():
                with open(cache_path, "rb") as f:
                    data = f.read()
                return Response(
                    data,
                    mimetype="image/jpeg",
                    headers={
                        "Cache-Control": "public, max-age=86400",
                        "ETag": f"{file_id}-w{width}",
                    }
                )

            mongodb = app.config["mongodb"]
            fs = GridFS(mongodb, collection="fs")
            try:
                original = _read_gridfs_file_bytes(fs, oid)
            except Exception:
                return Response("Not found", status=404)

            thumb_bytes = _make_thumb_bytes(original, width)

            # Grava em cache local usando arquivo temporário no MESMO diretório (evita TOCTOU)
            tmp_path = cache_path.with_suffix(cache_path.suffix + ".tmp")
            try:
                # grava em arquivo temporário para evitar corrupção em concorrência
                with open(tmp_path, "wb") as f:
                    f.write(thumb_bytes)
                # replace é atômico na maioria dos SOs (posix/nt modernos)
                os.replace(tmp_path, cache_path)
            except (OSError, IOError) as e:
                # se falhar cache, serve mesmo assim, mas registre o motivo
                app.logger.warning(
                    "[exportacao_img][cache-write] falhou ao gravar cache %s (w=%s): %s",
                    str(cache_path), width, str(e)
                )
                # melhor esforço: remove temporário, se existir
                try:
                    if tmp_path.exists():
                        tmp_path.unlink()
                except (OSError, IOError):
                    # não faz sentido propagar erro da limpeza
                    pass

            return Response(
                thumb_bytes,
                mimetype="image/jpeg",
                headers={
                    "Cache-Control": "public, max-age=86400",
                    "ETag": f"{file_id}-w{width}",
                }
            )
        except Exception:
            app.logger.exception("[exportacao_img] erro ao servir imagem")
            return Response("Internal error", status=500)
