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
import sys
from datetime import datetime, timedelta, time

import pandas as pd
import requests
from flask import request, jsonify

sys.path.append('.')
sys.path.append('../bhadrasana2')
sys.path.append('../ajna_docs/commons')

# from flask_login import login_required
from ajna_commons.flask.log import logger
from bhadrasana.models.laudo import get_empresa, Empresa
from virasana.integracao.due.due_manager import (get_conteineres_escaneados_sem_due,
                                                 set_conteineres_escaneados_sem_due,
                                                 integra_dues, integra_dues_itens)

CODIGOS_RECINTOS = ['8931318', '8931356', '8931359', '8931404']


def configure(app):
    """Configura rotas para app integracao."""

    @app.route('/integracao/lista_ctrs_sem_due', methods=['GET'])
    # @login_required
    def integracao_lista_ctrs_sem_due_api():
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

    @app.route('/integracao/insere_cnpjs', methods=['POST'])
    # @login_required
    def integracao_insere_cnpjs_api():
        """Recebe lista de CNPJs formato cnpj, nome. Alimenta tabela laudo_empresas."""
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
                'error': erros
            }
            return jsonify(response), 200

        except Exception as e:
            if db_session:
                db_session.rollback()
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.route('/integracao/atualiza_due_acessoveiculo_e_mongo', methods=['POST'])
    def integracao_atualiza_due_acessoveiculo_e_mongo():
        """Recebe a planilha de DUEs e de escaneamentos_sem_due atualizados.

        Com estes dados, vincula a imagem e o acesso veiculo à DUE correta. Ver arquivos csv de exemplo.

        Provisório, estudar como atualizar registro a registro com REST.
        """
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

    @app.route('/integracao/upload_dues', methods=['POST'])
    def integracao_upload_dues():
        """Insere dados de dues e itens.

        Provisório, até

        Recebe duas lista csv, uma com as DUEs e outra com os itens."""
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


if __name__ == '__main__':
    if __name__ == "__main__":
        # CARREGA CSVs DO DISCO (simplificado)
        # Obs.: Como a lógica do script era trabalhar em grandes "batchs", aqui foi mantida a lógica de
        # transferência de arquivos. Depois, será necessário decompor os endpoints dos passos 4 e 5
        # para uma atualização/inclusão REST registro a registro.
        BASE_URL = 'http://localhost:5001'
        # BASE_URL = 'https://ajna1.rfoc.srf/virasana/'

        # PASSO 1: Gera lista de contêineres
        print('PASSO 1: GET /integracao/lista_ctrs_sem_due ')
        # Define período: 3 dias até hoje 00h
        fim = datetime.combine(datetime.now(), time.min)
        inicio = fim - timedelta(days=3)
        recintos = ['893618']
        print(f'Período: {inicio.isoformat()} → {fim.isoformat()}')
        print(f' Recintos: {recintos}')
        params = {
            'inicio': inicio.isoformat(),
            'fim': fim.isoformat(),
            'codigos_recintos': ','.join(recintos)
        }
        r1 = requests.get(f"{BASE_URL}/integracao/lista_ctrs_sem_due", params=params)
        print(f'Status: {r1.status_code}')
        print(f'Sucesso: {r1.json().get("success")}, Erros: {r1.json().get("error")}')
        escaneamentos_sem_due = r1.json().get('data')
        print(escaneamentos_sem_due[:10])

        # PASSO 2:
        # Aqui é o código que será feito no ContÁgil

        # PASSO 3: Insere CNPJs
        # Teste FAKE - carregando CSVs locais. Estes CSVs DEVEM ser gerados pelo passo 2.
        print('PASSO 1: POST /integracao/insere_cnpjs')
        df_cnpjs = pd.read_csv('cnpjs.csv')
        cnpjs_json = df_cnpjs[['cnpj', 'nome']].to_dict('records')
        r1 = requests.post(f'{BASE_URL}/integracao/insere_cnpjs', json=cnpjs_json)
        print(f'Status: {r1.status_code} | {r1.json()}')
        # Teste FAKE - carregando CSVs locais. Estes CSVs DEVEM ser gerados pelo passo 2.
        # PASSO 4: Atualiza acesso veículo e MongoDB ajna realizando vinculação com DUE
        print('PASSO 4: POST /integracao/atualiza_due_acessoveiculo_e_mongo')
        with open('escaneamentos_sem_due.csv', 'rb') as esc, open('dues.csv', 'rb') as due:
            files = {
                'escaneamentos_sem_due': ('escaneamentos_sem_due.csv', esc, 'text/csv'),
                'dues': ('dues.csv', due, 'text/csv')
            }
            r2 = requests.post(f'{BASE_URL}/integracao/atualiza_due_acessoveiculo_e_mongo', files=files)
        print(f'Status: {r2.status_code} | {r2.json()}')

        # Teste FAKE - carregando CSVs locais. Estes CSVs DEVEM ser gerados pelo passo 2.
        # PASSO 5: Insere ou atualiza DUEs + Itens
        print('PASSO 5: POST /integracao/insere_dues')
        with open('dues.csv', 'rb') as dues, open('itens_dues.csv', 'rb') as itens:
            files3 = {
                'dues': ('dues.csv', dues, 'text/csv'),
                'itens_dues': ('itens_dues.csv', itens, 'text/csv')
            }
            r3 = requests.post(f'{BASE_URL}/integracao/insere_dues', files=files3)
        print(f'Status: {r3.status_code} | {r3.json()}')

        print('PIPELINE CONCLUÍDA!')
