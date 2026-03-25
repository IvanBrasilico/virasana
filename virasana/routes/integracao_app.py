'''
Módulo para disponilizar endpoints que estão no "scripts" e no módulo integração.

O módulo integração possui diversas necessidades de ETLs. Estes estão, em Março de 2026,
rodando via virasana_periodic (serviço no Servidor do Datacenter que chama
virasana/scritps/periodic_updates) para dados que é possível acessar sem certificado digital.
Os dados que só estão acessíveis no ReceitaData possuem scripts que exportam listas (csvs)[1],
depois estas listas são fornecidas ao Contágil ou a um script do Jupyter no RD[2], que gera outra
lista, que finalmente fazemos download e então chamamos o script original para integrar [3]
(ver por exemplo o script lista_conteineres_exportacao_sem_dues).

A partir desta data, vamos publicar endpoints para os passos [1] e [3] no virasaana, na app
integracao. E então será possível, via ContÁgil, acessar estes endpoints e fazer o passo [2]
em um script do ContÁgil (que será o equivalente do virasana_periodic).
'''
import io
import json
from datetime import datetime

import pandas as pd
from flask import request, jsonify

# from flask_login import login_required
from ajna_commons.flask.log import logger
from bhadrasana.models.laudo import get_empresa, Empresa
from virasana.integracao.due.due_manager import (get_conteineres_escaneados_sem_due,
                                                 set_conteineres_escaneados_sem_due,
                                                 integra_dues, integra_dues_itens)

CODIGOS_RECINTOS = ['8931318', '8931356', '8931359', '8931404']


def configure(app):
    """Configura rotas para app integracao."""

    @app.route('/integracao/exporta_ctrs_sem_due', methods=['GET'])
    # @login_required
    def integracao_exporta_ctrs_sem_due_api():
        """Retorna lista com escaneamentos sem due.

        Exemplo de resposta: {
          "success": true,
          "total_registros": 1500,
          "periodo": {"inicio": "2026-03-01T00:00:00", "fim": "2026-03-25T23:59:59"},
          "codigos_recintos": ["001", "002"],
          "data": [
            {'AcessoVeiculo': 9, 'InspecaoNaoInvasiva': 9, 'numero_conteiner': AAAA9999999, 'id_imagem': _id,
             'dataescaneamento': isodate, 'due_acesso': BR99999 [informada no acessoveiculo])}, ...
          ]
        }
        """
        db_session = app.config['db_session']
        inicio_str = request.args.get('inicio')
        fim_str = request.args.get('fim')
        codigos_str = request.args.get('codigos_recintos', CODIGOS_RECINTOS)
        if not inicio_str or not fim_str:
            return jsonify({'error': 'Parâmetros "inicio" e "fim" obrigatórios (YYYY-MM-DD)'}), 400
        try:
            inicio = datetime.fromisoformat(inicio_str)
            fim = datetime.fromisoformat(fim_str)
            codigos_recintos = [c.strip() for c in codigos_str.split(',') if c.strip()]
        except ValueError:
            return jsonify({'error': 'Datas inválidas. Use YYYY-MM-DDTHH:MM:SS ou YYYY-MM-DD'}), 400
        try:
            lista_final = get_conteineres_escaneados_sem_due(db_session, inicio, fim, codigos_recintos)
            df = pd.DataFrame(lista_final[1:], columns=lista_final[0])
            # JSON otimizado: array de records (padrão API)
            data_json = df.to_json(orient='records', date_format='iso', default_handler=str)
            response = {
                'success': True,
                'total_registros': len(df),
                'periodo': {
                    'inicio': inicio.isoformat(),
                    'fim': fim.isoformat()
                },
                'codigos_recintos': codigos_recintos,
                'data': json.loads(data_json)  # Lista de dicts serializável
            }
            return jsonify(response), 200
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.route('/integracao/importa_cnpjs', methods=['POST'])
    # @login_required
    def integracao_importa_cnpjs_api():
        db_session = app.config['db_session']
        empresas_data = request.get_json()
        if not empresas_data or not isinstance(empresas_data, list):
            return jsonify({'success': False, 'error': 'Body deve ser lista de objetos com "cnpj" e "nome"'}), 400
            # Valida colunas obrigatórias
        if len(empresas_data) == 0 or not all('cnpj' in emp and 'nome' in emp for emp in empresas_data):
            return jsonify({'success': False, 'error': 'Todos itens precisam de "cnpj" e "nome"'}), 400
        try:
            total_importados = 0
            total_criados = 0
            erros = []
            logger.info(f'Iniciando UPSERT de {len(empresas_data)} Empresas via JSON')
            for row in empresas_data:
                cnpj = str(int(row['cnpj'])).zfill(8)
                try:
                    empresa = get_empresa(db_session, cnpj)
                except ValueError:
                    empresa = None
                if empresa is None:
                    empresa = Empresa()
                    total_criados += 1
                empresa.cnpj = cnpj
                empresa.nome = row['nome']
                db_session.add(empresa)
                total_importados += 1
            db_session.commit()
            response = {
                'success': True,
                'timestamp': datetime.now().isoformat(),
                'total_lidos': len(empresas_data),
                'total_importados': total_importados,
                'total_criados': total_criados,
                'erros': erros
            }
            return jsonify(response), 200

        except Exception as e:
            if db_session:
                db_session.rollback()
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.route('/integracao/atualiza_acesso_mongo', methods=['POST'])
    def integracao_atualiza_acesso_e_mongo():
        mongodb = app.config['mongodb']
        db_session = app.config['db_session']

        if 'dues' not in request.files or 'escaneamentos_sem_due' not in request.files:
            return jsonify({'success': False, 'error': 'Arquivos "dues" e "escaneamentos_sem_due" obrigatórios'}), 400

        try:
            # Streams diretos para Pandas
            df_dues = pd.read_csv(io.BytesIO(request.files['dues'].stream.read()))
            df_escaneamentos_sem_due = pd.read_csv(io.BytesIO(request.files['escaneamentos_sem_due'].stream.read()))

            logger.info('Atualizando acesso e Mongo...')
            set_conteineres_escaneados_sem_due(mongodb, db_session, df_escaneamentos_sem_due, df_dues)

            return jsonify({
                'success': True,
                'dues_lidas': len(df_dues),
                'escaneamentos_lidos': len(df_escaneamentos_sem_due)
            }), 200

        except Exception as e:
            return jsonify({'success': False, 'error': f'Erro crítico: {str(e)}'}), 500

    @app.route('/integracao/importa_dues', methods=['POST'])
    def integracao_importa_dues():
        mongodb = app.config['mongodb']  # Não usado aqui, mas padrão
        db_session = app.config['db_session']
        processados = {'dues': 0, 'itens_dues': 0}
        # Opcional: dues.csv
        if 'dues' in request.files and request.files['dues'].filename:
            df_dues = pd.read_csv(io.BytesIO(request.files['dues'].stream.read()))
            df_dues = df_dues.fillna('').drop_duplicates()
            processados['dues'] = len(df_dues)
            if integra_dues(db_session, df_dues):
                logger.info(f'{len(df_dues)} DUEs inseridas')
        if 'itens_dues' in request.files and request.files['itens_dues'].filename:
            df_itens_dues = pd.read_csv(io.BytesIO(request.files['itens_dues'].stream.read()))
            df_itens_dues = df_itens_dues.fillna('').drop_duplicates()
            processados['itens_dues'] = len(df_itens_dues)
            if integra_dues_itens(db_session, df_itens_dues):
                logger.info(f'{len(df_itens_dues)} Itens de DUE inseridos')
        return jsonify({'success': True, 'processados': processados}), 200
