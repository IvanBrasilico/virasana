'''
Este módulo implementa a lógica necessária para puxar os dados de exportação para o Ajna
'''
import sys
import warnings
from datetime import datetime, timedelta
from typing import List

import requests
import urllib3
from bson import ObjectId

from virasana.integracao.due.due_alchemy import Due, DueItem, DueConteiner

# Suppress only the InsecureRequestWarning
warnings.simplefilter('ignore', urllib3.exceptions.InsecureRequestWarning)

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')
from ajna_commons.flask.log import logger

from bhadrasana.models.apirecintos import AcessoVeiculo, InspecaoNaoInvasiva

VIRASANA_URL = "https://ajna1.rfoc.srf/virasana/"


def raspa_container(containeres: List[str], datainicio: datetime, datafim: datetime,
                    virasana_url: str = VIRASANA_URL) -> List[dict]:
    """Recebe uma lista de siglas de contêineres e pesquisa as imagens no MongoDB.

    Args:
        containeres: lista de strings de siglas de contêineres, filtra numeroinformado
        datainicial, datafinal: período da pesquisa, filtra dataescaneamento

    Returns: [{_id, metadata: [numeroinformado, dataescaneamento, due]}]

    """
    datainicial = datetime.strftime(datainicio, '%Y-%m-%d %H:%M:%S')
    datafinal = datetime.strftime(datafim, '%Y-%m-%d %H:%M:%S')
    logger.debug('Conectando virasana, função raspa_container.')
    params = {'query':
                  {'metadata.dataescaneamento': {'$gte': datainicial, '$lte': datafinal},
                   'metadata.contentType': 'image/jpeg',
                   "$or": [
                       {"metadata.due": {"$exists": False}},
                       {"metadata.due": None},
                       {"metadata.due": ""}],
                   'metadata.numeroinformado': {'$in': containeres},
                   },
              'projection':
                  {'metadata.numeroinformado': 1,
                   'metadata.dataescaneamento': 1,
                   'metadata.due': 1,
                   }
              }
    r = requests.post(virasana_url + "grid_data", json=params, verify=False)
    logger.debug(f'{r}: {r.text}')
    lista_containeres = list(r.json())
    if lista_containeres and len(lista_containeres) > 0:
        logger.debug(f'GridData: {lista_containeres[0]}')
    return lista_containeres


def get_conteineres_escaneados_sem_due(session, datainicio: datetime, datafim: datetime,
                                       codigos_recintos: list = None) -> list:
    """1 - Pega acessos veículo do período e recinto, se passado. 2 - Em seguida pega escaneamentos
    correspondentes (efetuados logo após no fluxo). 3 - Recupera os _ids das imagens de escaneamento
    adquiridas do MongoDB. Obs.: o centro da pesquisa é sempre em torno do número do contêiner.

    Args:
        session: conexão SQLAlchemy
        datainicio, datafim: periodo between
        codigoRecinto: opcional

    Returns: lista com escaneamentos sem due. Ver campos abaixo em lista_final[0]

    """
    # Passo 1 - Pega os acessos para os parâmetros de data e recinto passados.
    # Filtros: A"C"esso, direção "E"ntrada, numeroNfe não vazio, numeroConteiner não vazio,
    q = session.query(AcessoVeiculo).filter(AcessoVeiculo.operacao == 'C')
    q = q.filter(AcessoVeiculo.direcao == 'E')
    q = q.filter(AcessoVeiculo.dataHoraOcorrencia.between(datainicio, datafim))
    if codigos_recintos:
        q = q.filter(AcessoVeiculo.codigoRecinto.in_(codigos_recintos))
    q = q.filter(AcessoVeiculo.numeroConteiner.isnot(None))
    q = q.filter(AcessoVeiculo.numeroConteiner != '')
    q = q.filter(AcessoVeiculo.listaNfe != '')
    lista_entradas = q.order_by(AcessoVeiculo.dataHoraOcorrencia).all()
    conteineres_exportacao_detalhes = {}
    conteineres_exportacao = set()
    for entrada in lista_entradas:
        conteineres_exportacao_detalhes[entrada.numeroConteiner] = {
            'id': entrada.id,
            'tipoDeclaracao': entrada.tipoDeclaracao,
            'numeroDeclaracao': entrada.numeroDeclaracao
        }
        conteineres_exportacao.add(entrada.numeroConteiner)
    logger.info(f'Acessos: {list(conteineres_exportacao)[:10]} {len(conteineres_exportacao)}')
    # Passo 2 - Pega Inspeções não Invasivas para a lista de contêineres extraídos dos Acessos
    # Filtra o mesmo recinto e adiciona três horas na data fim (escaneamento ocorre depois do acesso)
    datafim_escaneamento = datafim + timedelta(hours=3)
    q2 = session.query(InspecaoNaoInvasiva)
    q2 = q2.filter(InspecaoNaoInvasiva.dataHoraOcorrencia.between(datainicio, datafim_escaneamento))
    if codigos_recintos:
        q2 = q2.filter(InspecaoNaoInvasiva.codigoRecinto.in_(codigos_recintos))
    q2 = q2.filter(InspecaoNaoInvasiva.numeroConteiner.in_(conteineres_exportacao))
    lista_escaneamentos = q2.order_by(InspecaoNaoInvasiva.dataHoraOcorrencia).all()
    conteineres_escaneados_detalhes = {}
    conteineres_exportacao_escaneados = set()
    for escaneamento in lista_escaneamentos:
        conteineres_escaneados_detalhes[escaneamento.numeroConteiner] = {
            'id': escaneamento.id,
            'dataescaneamento': escaneamento.dataHoraOcorrencia
        }
        conteineres_exportacao_escaneados.add(escaneamento.numeroConteiner)
    logger.info(
        f'Escaneamentos: {list(conteineres_exportacao_escaneados)[:10]} {len(conteineres_exportacao_escaneados)}')
    # Passo 3 - Recupera o _id das imagens capturadas pelo Avatar e outros dados, para completar os dados
    griddata = raspa_container(list(conteineres_exportacao_escaneados), datainicio, datafim_escaneamento)
    lista_final = [('AcessoVeiculo', 'InspecaoNaoInvasiva',
                    'numero_conteiner', 'id_imagem', 'dataescaneamento', 'due_acesso')]
    for dados_imagem in griddata:
        _id = dados_imagem['_id']
        metadata = dados_imagem['metadata']
        numero_conteiner = metadata['numeroinformado']
        dataescaneamento = metadata['dataescaneamento']
        dados_acesso = conteineres_exportacao_detalhes[numero_conteiner]
        dados_inspecao = conteineres_escaneados_detalhes[numero_conteiner]
        due_acesso = None
        if dados_acesso.get('tipoDeclaracao') == 'DUE':
            due_acesso = dados_acesso['numeroDeclaracao']
        lista_final.append((dados_acesso['id'], dados_inspecao['id'],
                            numero_conteiner, _id, dataescaneamento, due_acesso))
    logger.info(f'Lista final: {lista_final[:2]}, {len(lista_final)}')
    return lista_final


def update_due_mongo_db(db, dues):
    for _id, due in dues.items():
        result = db.fs.files.update_one(
            {'_id': ObjectId(_id)},
            {'$set': {'metadata.due': due, 'metadata.carga.vazio': False}}
        )
        logger.debug(result)


def set_conteineres_escaneados_sem_due(db, session, df_escaneamentos_sem_due, df_dues):
    # Passo 5: Atualizar metadata e Acesso com número da DUE
    conteineres_due = dict()
    for index, row in df_dues.iterrows():
        conteineres = row['lista_id_conteiner']
        numero_due = row['numero_due']
        for conteiner in conteineres.split(','):
            conteineres_due[conteiner.strip()] = numero_due
    try:
        _ids_dues = dict()
        for index, row in df_escaneamentos_sem_due.iterrows():
            conteiner = row['numero_conteiner']
            due = conteineres_due.get(conteiner)
            if due is None:  # DUE-conteiner não foi encontrada pelo passo do RD
                continue
            _id = row['id_imagem']
            # Lista de acessos veículo atualizados
            acessoveiculo = session.query(AcessoVeiculo).filter(AcessoVeiculo.id == row['AcessoVeiculo']).one()
            acessoveiculo.numeroDeclaracao = due
            session.add(acessoveiculo)
            # Monta dict de _id: due
            _ids_dues[_id] = due
            logger.debug(f'{conteiner},{acessoveiculo.id},{_id},{due}')
        update_due_mongo_db(db, _ids_dues)
        session.commit()
    except Exception as err:
        session.rollback()
        raise err


def update_instance(model_instance, update_dict):
    for key, value in update_dict.items():
        if hasattr(model_instance, key):
            setattr(model_instance, key, value)


def integra_dues(session, df_dues):
    try:
        df_dues['ni_declarante'] = df_dues['ni_declarante'].astype(str).str[:14].str.zfill(14)
        df_dues['cnpj_estabelecimento_exportador'] = (
            df_dues['cnpj_estabelecimento_exportador'].astype(str).str[:14].str.zfill(14))
        try:
            for index, row in df_dues.iterrows():
                due = session.query(Due).filter(Due.numero_due == row['numero_due']).one_or_none()
                if due is None:
                    due = Due()
                update_instance(due, row.to_dict())
                session.add(due)
                # Passo 6b - popular DueConteiner
                for conteiner in due.lista_id_conteiner.split(', '):
                    conteiner = conteiner.strip()
                    due_conteiner = session.query(DueConteiner).\
                                     filter(DueConteiner.numero_due == row['numero_due'],
                                            DueConteiner.numero_conteiner == conteiner).one_or_none()
                    if due_conteiner is None:
                        due_conteiner = DueConteiner()
                    due_conteiner.numero_due = due.numero_due
                    due_conteiner.numero_conteiner = conteiner.strip()
                    session.add(due_conteiner)
            session.commit()
            return True
        except Exception as err:
            session.rollback()
            raise err
    except Exception as err:
        logger.error(err)
        return False


def integra_dues_itens(session, df_itens_dues):
    try:
        for index, row in df_itens_dues.iterrows():
            dueitem = session.query(DueItem).filter(
                DueItem.nr_due == row['nr_due'],
                DueItem.due_nr_item == row['due_nr_item']).one_or_none()
            if dueitem is None:
                dueitem = DueItem()
            update_instance(dueitem, row.to_dict())
            session.add(dueitem)
        session.commit()
        return True
    except Exception as err:
        session.rollback()
        logger.error(err)
        return False
