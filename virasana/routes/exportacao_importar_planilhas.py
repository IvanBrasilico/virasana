import logging
import unicodedata
from flask import Blueprint, render_template, request, jsonify, current_app
from sqlalchemy import text
from flask_wtf.csrf import generate_csrf
from flask_login import current_user

from virasana.routes.exportacao_score_risco import processar_lote_risco

# 1. Blueprint
importar_planilhas_bp = Blueprint(
    'importar_planilhas',
    __name__,
    url_prefix='/exportacao'
)

@importar_planilhas_bp.route('/importar', methods=['GET'])
def importar_planilha_view():

    session = current_app.config['db_session']
    try:
        sql = text("""
            SELECT nome_arquivo_origem, MAX(data_importacao) AS data_importacao, COUNT(id) AS total_linhas
            FROM narcos_planilhas_importadas
            GROUP BY nome_arquivo_origem
            ORDER BY data_importacao DESC
            LIMIT 10
        """)
        ultimas_planilhas = session.execute(sql).mappings().all()
    except Exception as e:
        current_app.logger.exception("[importar_planilha_view] Erro ao buscar histórico de planilhas importadas.")
        ultimas_planilhas = []

    return render_template(
        'exportacao_importar_planilha.html',
        csrf_token=generate_csrf,
        ultimas_planilhas=ultimas_planilhas
    )

@importar_planilhas_bp.route('/importar_planilha_narcos', methods=['POST'])
def importar_planilha_narcos():
    file = request.files.get('file')
    if not file or file.filename == '':
        return jsonify({"error": "Nenhum arquivo enviado"}), 400

    try:
        import pandas as pd
        import numpy as np
        import unicodedata
    except ImportError:
        return jsonify({"error": "Bibliotecas ausentes. Instale no ambiente: pip install pandas openpyxl"}), 500

    try:
        filename = file.filename
        if filename.endswith('.csv'):
            # sep=None com engine='python' faz o pandas descobrir automaticamente se é vírgula ou ponto-e-vírgula
            df = pd.read_csv(file, sep=None, engine='python')
        elif filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file)
        else:
            return jsonify({"error": "Formato inválido. Use arquivo CSV ou Excel."}), 400

        # Limpeza inicial
        df = df.replace({np.nan: None})

        # Função para normalizar os cabeçalhos (tira acentos e coloca em minúsculo)
        def normalize_col(col):
            if not isinstance(col, str): return str(col)
            nfkd = unicodedata.normalize('NFKD', col)
            clean_str = u"".join([c for c in nfkd if not unicodedata.combining(c)]).lower()
            # .split() seguido de " ".join() elimina espaços duplos, trailing e tabs
            return " ".join(clean_str.split())

        df.columns = [normalize_col(c) for c in df.columns]

        # Mapeamento: vários formatos de nomes de planilhas apontando para a mesma coluna no BD
        col_map = {
            'conteiner': 'numero_conteiner',
            'tipo conteiner': 'tipo_conteiner',
            'tipo': 'tipo_conteiner',
            'iso code': 'iso_code',
            'iso': 'iso_code',
            'categoria': 'categoria',
            'entrada carreta': 'entrada_carreta',
            'viagem embarque': 'viagem_embarque',
            'navio embarque': 'navio_embarque',
            'viagem descarga': 'viagem_descarga',
            'navio descarga': 'navio_descarga',
            'local imagem': 'local_imagem',
            'local da imagem': 'local_imagem',
            'alerta / if': 'alerta_if',
            'alerta if': 'alerta_if',
            'erros na imagem': 'erros_imagem',
            'erros de imagem': 'erros_imagem',
            'ch/vz': 'ch_vz',
            'status': 'status_conteiner',
            'porto descarga': 'porto_descarga',
            'porto de descarga': 'porto_descarga',
            'porto destino final': 'porto_destino_final',
            'porto de destino final': 'porto_destino_final',
            'descricao ncm': 'descricao_ncm',
            'cpf motorista': 'cpf_motorista',
            'nome motorista': 'nome_motorista',
            'cpf operador scanner': 'cpf_operador_scanner',
            'cpf operador do scanner': 'cpf_operador_scanner',
            'nome operador scanner': 'nome_operador_scanner',
            'nome do operador do scanner': 'nome_operador_scanner',
            'cnpj transportadora': 'cnpj_transportadora',
            'transportadora': 'transportadora',
            'numero de lote': 'numero_lote',
            'lote': 'numero_lote',
            'razao social exportador / importador': 'razao_social_exportador_importador',
            'razao social do exportador/importador': 'razao_social_exportador_importador',
            'cnpj exportador / importador': 'cnpj_exportador_importador',
            'cnpj do exportador/importador': 'cnpj_exportador_importador',
            'data scanner': 'data_scanner',
            'data do scanner': 'data_scanner'
        }

        # Identifica quais colunas úteis realmente vieram neste arquivo para economizar loops
        colunas_presentes = [c for c in col_map.keys() if c in df.columns]

        # Validação de colunas obrigatórias
        colunas_obrigatorias = {'numero_conteiner', 'entrada_carreta'}
        colunas_mapeadas_nesta_planilha = {col_map[c] for c in colunas_presentes}
        
        faltando = colunas_obrigatorias - colunas_mapeadas_nesta_planilha
        if faltando:
            return jsonify({"error": f"Planilha inválida. Faltam as colunas obrigatórias: {', '.join(faltando)}"}), 400

        records = []
        usuario_identificador = getattr(current_user, 'id', None)
        linhas_sem_conteiner = 0
        
        for _, row in df.iterrows():
            row_dict = {"nome_arquivo_origem": filename, "usuario_id": usuario_identificador}
            
            # Inicializa com None para garantir que a inserção SQL receba os parâmetros certos
            for db_col in set(col_map.values()):
                row_dict[db_col] = None

            # Preenche com os dados encontrados
            for plan_col in colunas_presentes:
                val = row[plan_col]
                if pd.notnull(val): # Só sobrescreve se tiver algum dado válido
                    db_col = col_map[plan_col]
                    row_dict[db_col] = val
            
            if not row_dict.get('numero_conteiner'):
                linhas_sem_conteiner += 1
                continue # Ignora linhas totalmente vazias ou sem a chave de busca

            row_dict['numero_conteiner'] = str(row_dict['numero_conteiner']).strip().upper()

            # Trata identificadores numéricos com LPAD (zfill)
            campos_id = {
                'cpf_motorista': 11, 'cpf_operador_scanner': 11,
                'cnpj_transportadora': 14, 'cnpj_exportador_importador': 14
            }
            for col, size in campos_id.items():
                if row_dict.get(col):
                    val = str(row_dict[col]).strip()
                    if val.endswith('.0'): val = val[:-2]
                    # Remove possíveis máscaras e garante preenchimento de zeros à esquerda
                    digits = "".join(filter(str.isdigit, val))
                    row_dict[col] = digits.zfill(size) if digits else None

            # Limpa apenas o ".0" do lote (sem tamanho fixo)
            if row_dict.get('numero_lote'):
                lote = str(row_dict['numero_lote']).strip()
                if lote.endswith('.0'): lote = lote[:-2]
                row_dict['numero_lote'] = lote

            # Converte datas para string no formato correto do MariaDB
            for date_field in ['entrada_carreta', 'data_scanner']:
                if row_dict.get(date_field):
                    try:
                        dt_val = pd.to_datetime(row_dict[date_field])
                        if pd.notnull(dt_val):
                            row_dict[date_field] = dt_val.strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            row_dict[date_field] = None
                    except Exception:
                        row_dict[date_field] = None

            records.append(row_dict)

        if not records:
            return jsonify({"error": "Nenhum dado processável encontrado na planilha."}), 400

        session = current_app.config['db_session']
        
        # INSERT IGNORE cuidará de ignorar graciosamente registros duplicados
        sql_insert = text("""
            INSERT IGNORE INTO narcos_planilhas_importadas (
                nome_arquivo_origem, usuario_id,
                numero_conteiner, tipo_conteiner, iso_code, categoria,
                entrada_carreta, viagem_embarque, navio_embarque, viagem_descarga, navio_descarga,
                local_imagem, alerta_if, erros_imagem, ch_vz, status_conteiner,
                porto_descarga, porto_destino_final, descricao_ncm,
                cpf_motorista, nome_motorista, cpf_operador_scanner, nome_operador_scanner,
                cnpj_transportadora, transportadora, numero_lote,
                razao_social_exportador_importador, cnpj_exportador_importador, data_scanner
            ) VALUES (
                :nome_arquivo_origem, :usuario_id,
                :numero_conteiner, :tipo_conteiner, :iso_code, :categoria,
                :entrada_carreta, :viagem_embarque, :navio_embarque, :viagem_descarga, :navio_descarga,
                :local_imagem, :alerta_if, :erros_imagem, :ch_vz, :status_conteiner,
                :porto_descarga, :porto_destino_final, :descricao_ncm,
                :cpf_motorista, :nome_motorista, :cpf_operador_scanner, :nome_operador_scanner,
                :cnpj_transportadora, :transportadora, :numero_lote,
                :razao_social_exportador_importador, :cnpj_exportador_importador, :data_scanner
            )
        """)
        
        try:
            result = session.execute(sql_insert, records)
            session.commit()
            
            # O rowcount num INSERT IGNORE retorna exatamente as linhas que NÃO foram puladas
            linhas_afetadas = result.rowcount

            # -----------------------------------------------------------------
            # INTEGRAÇÃO DO MOTOR DE RISCO
            # Calcula e salva as notas de risco para o lote recém-importado
            # -----------------------------------------------------------------
            try:
                notas_calculadas = processar_lote_risco(session, records)
                current_app.logger.info(f"[importar_planilha_narcos] Risco calculado para {notas_calculadas} contêineres.")
            except Exception as e:
                current_app.logger.exception("[importar_planilha_narcos] Erro ao calcular risco do lote.")
                # O pass garante que não vamos quebrar a resposta de sucesso da importação se o risco falhar

            linhas_duplicadas = len(records) - linhas_afetadas

            return jsonify({
                "status": "success",
                "estatisticas": {
                    "total_planilha": len(df),
                    "inseridas_sucesso": linhas_afetadas,
                    "ignoradas_sem_conteiner": linhas_sem_conteiner,
                    "ignoradas_duplicadas": linhas_duplicadas
                }
            })

        except Exception as e:
            session.rollback()
            current_app.logger.exception("[importar_planilha_narcos] Erro banco de dados")
            return jsonify({"error": "Erro ao salvar os dados no banco."}), 500

    except Exception as e:
        current_app.logger.exception("[importar_planilha_narcos] Erro interno")
        return jsonify({"error": "Erro interno ao processar a planilha."}), 500


