'''
Este módulo implementa a lógica necessária para puxar os dados de exportação para o Ajna
'''
import sys
import warnings
from datetime import datetime, timedelta
from typing import List, Optional

import requests
import urllib3
from bson import ObjectId

from bhadrasana.models.laudo import Empresa
from bhadrasana.models.ovr import Recinto
from bhadrasana.models.laudo import get_empresa
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
    logger.info(f'set_conteineres_escaneados_sem_due: {len(df_dues)} DUEs a atualizar.')
    for index, row in df_dues.iterrows():
        conteineres = row['lista_id_conteiner']
        numero_due = row['numero_due']
        for conteiner in conteineres.split(','):
            conteineres_due[conteiner.strip()] = numero_due
    logger.info(f'set_conteineres_escaneados_sem_due: {len(conteineres_due)} contêineres a atualizar.')
    try:
        _ids_dues = dict()
        acessos_veiculos_ids = df_escaneamentos_sem_due[
            df_escaneamentos_sem_due['numero_conteiner'].isin(conteineres_due.keys())]['AcessoVeiculo'].unique()
        logger.info(f'set_conteineres_escaneados_sem_due: {len(acessos_veiculos_ids)} AcessosVeiculo para atualizar.')
        acessosveiculos = session.query(AcessoVeiculo).filter(AcessoVeiculo.id.in_(acessos_veiculos_ids)).all()
        for acessoveiculo in acessosveiculos:
            due = conteineres_due.get(acessoveiculo.numeroConteiner)
            if due is None:  # DUE-conteiner não foi encontrada pelo passo do RD
                continue
            acessoveiculo.tipoDeclaracao = 'DUE'
            acessoveiculo.numeroDeclaracao = due
            session.add(acessoveiculo)
            logger.debug(f'{acessoveiculo.numeroConteiner},{acessoveiculo.id},{due}')
        session.commit()
        logger.info(f'set_conteineres_escaneados_sem_due: {len(acessosveiculos)} AcessosVeiculo atualizados.')
        for index, row in df_escaneamentos_sem_due.iterrows():
            _id = row['id_imagem']
            conteiner = row['numero_conteiner']
            due = conteineres_due.get(conteiner)
            if due is None:  # DUE-conteiner não foi encontrada pelo passo do RD
                continue
            # Monta dict de _id: due
            _ids_dues[_id] = due
            logger.debug(f'{conteiner},{_id},{due}')
        update_due_mongo_db(db, _ids_dues)
        logger.info(f'set_conteineres_escaneados_sem_due: {len(_ids_dues)} imagens MongoDB atualizadas.')
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
        logger.info(f'Iniciando "UPSERT" de {len(df_dues)} DUEs')
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
                    due_conteiner = session.query(DueConteiner). \
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
    logger.info(f'Iniciando "UPSERT" de {len(df_itens_dues)} Itens de DUEs')
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


def get_dados(session, grid_data):
    try:
        logger.info(f'get_dados: {grid_data["metadata"]["due"]}')
        numero_due = grid_data['metadata']['due']
        due = session.query(Due).filter(Due.numero_due == numero_due).one()
        declarante = due.ni_declarante
        empresa = session.query(Empresa).filter(Empresa.cnpj == declarante).one_or_none()
        if empresa:
            nome_declarante = declarante.get('Nome Declarante')
        else:
            nome_declarante = 'Não encontrado.'
        destino = due.codigo_iso3_pais_importador
        numero = due.numero_due
        return '%s - EXPORTADOR %s / %s - PAÍS DESTINO %s' % \
            (numero, declarante, nome_declarante, destino)
    except Exception as err:
        logger.error(f'get_dados: {err}')
        return ''


def due_summary(session, grid_out) -> dict:
    result = {}
    try:
        grid_data = grid_out.metadata
        logger.info(f'due_summary: {grid_data["due"]}')
        numero_due = grid_data['due']
        due = session.query(Due).filter(Due.numero_due == numero_due).one()
        declarante = due.ni_declarante
        empresa = session.query(Empresa).filter(Empresa.cnpj == declarante).one_or_none()
        if empresa:
            nome_declarante = declarante.get('Nome Declarante')
        else:
            nome_declarante = 'Não encontrado.'
        numero = due.numero_due
        destino = due.codigo_iso3_pais_importador
        result = {'RESUMO PUCOMEX': '',
                  'DUE Nº': numero,
                  'EXPORTADOR': '%s / %s' % (declarante, nome_declarante),
                  'PAÍS DESTINO': destino}
    except Exception as err:
        result['ERRO AO BUSCAR DADOS DUE'] = f'due_summary: {err}'
        logger.error(f'due_summary: {err}', exc_info=True)
    return result


def get_due(session, numerodeclaracao: str) -> Optional[Due]:
    """
    Retorna due se encontrada, ou None.
    Args:
        session: sessão SQL Alchemy
        numerodeclaracao: número da DUE
    """
    return session.query(Due).filter(Due.numero_due == numerodeclaracao).one_or_none()


def get_recinto_siscomex(session, codigo_siscomex) -> Optional[Recinto]:
    return session.query(Recinto).filter(
        Recinto.cod_siscomex == codigo_siscomex).one_or_none()


class DueView():
    """DueView é uma Due já com os campos adicionais para visualização."""

    def __init__(self, session, due: Due):
        # Reflect all columns - copia campos da due
        for key in due.__mapper__.attrs.keys():
            setattr(self, key, getattr(due, key))
        try:
            empresa = get_empresa(session, due.ni_declarante)
        except ValueError:
            empresa = None
        self.nome_declarante = 'Não encontrado na base' if empresa is None else '*' + empresa.nome
        recinto_embarque = get_recinto_siscomex(session, due.codigo_recinto_embarque)
        self.nome_recinto_embarque = '' if recinto_embarque is None else recinto_embarque.nome
        recinto_despacho = get_recinto_siscomex(session, due.codigo_recinto_despacho)
        self.nome_recinto_despacho = '' if recinto_despacho is None else recinto_despacho.nome
        self.itens = session.query(DueItem).filter(DueItem.nr_due == due.numero_due).all()


def get_due_view(session, numerodeclaracao: str) -> Optional[DueView]:
    """
    Retorna due_view se due encontrada, ou None.

    Args:
        session: sessão SQL Alchemy
        numerodeclaracao: número da DUE
    """
    due = session.query(Due).filter(Due.numero_due == numerodeclaracao).one_or_none()
    if due is not None:
        return DueView(session, due)
    return None


def get_itens_due(session, numerodeclaracao: str) -> List[DueItem]:
    return session.query(DueItem).filter(DueItem.nr_due == numerodeclaracao).all()


def get_dues_container(session, numero: str,
                       datainicio: datetime,
                       datafim: datetime,
                       limit=40
                       ) -> List[DueView]:
    if numero is None or numero == '':
        raise ValueError('get_dues_container: Informe o número do contêiner!')
    q = session.query(Due).join(DueConteiner, Due.numero_due == DueConteiner.numero_due). \
        filter(DueConteiner.numero_conteiner == numero,
               Due.data_criacao_due.between(datainicio, datafim)).limit(limit)
    logger.info(q.statement)
    dues = q.all()
    return [get_due_view(session, due.numero_due) for due in dues]


def get_dues_empresa(session, cnpj: str, limit=40) -> List[Due]:
    if cnpj is None or len(cnpj) < 8:
        raise ValueError('get_dues: Informe o CNPJ da empresa com no mínimo 8 posições!')
    return session.query(Due).filter(Due.cnpj_estabelecimento_exportador.like(cnpj + '%')). \
        limit(limit).all()
