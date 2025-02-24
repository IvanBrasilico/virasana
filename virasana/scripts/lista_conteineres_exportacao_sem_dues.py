import sys
import warnings
from datetime import datetime, timedelta, time

import pandas as pd
import requests
import urllib3

# Suppress only the InsecureRequestWarning
warnings.simplefilter('ignore', urllib3.exceptions.InsecureRequestWarning)

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana')
from ajna_commons.flask.log import logger

from bhadrasana.models.apirecintos import AcessoVeiculo, InspecaoNaoInvasiva

VIRASANA_URL = "https://ajna1.rfoc.srf/virasana/"


def raspa_container(containeres: list, datainicial: str, datafinal: str,
                    virasana_url: str = VIRASANA_URL) -> list:
    logger.debug('Conectando virasana')
    params = {'query':
                  {'metadata.dataescaneamento': {'$gte': datainicial, '$lte': datafinal},
                   'metadata.contentType': 'image/jpeg',
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
    logger.info(f'GridData: {lista_containeres[0]}')
    return lista_containeres


def get_eventos(session, datainicio: datetime, datafim: datetime, codigoRecinto=''):
    # Passo 1 - Pega os acessos para os parâmetros de data e recinto passados.
    # Filtros: A"C"esso, direção "E"ntrada, numeroNfe não vazio, numeroConteiner não vazio,
    q = session.query(AcessoVeiculo).filter(AcessoVeiculo.operacao == 'C')
    q = q.filter(AcessoVeiculo.direcao == 'E')
    q = q.filter(AcessoVeiculo.dataHoraOcorrencia.between(datainicio, datafim))
    q = q.filter(AcessoVeiculo.codigoRecinto == codigoRecinto)
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
    q2 = q2.filter(InspecaoNaoInvasiva.codigoRecinto == codigoRecinto)
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
    datainicial = datetime.strftime(datainicio, '%Y-%m-%d %H:%M:%S')
    datafinal = datetime.strftime(datafim_escaneamento, '%Y-%m-%d %H:%M:%S')
    # Passo 3 - Recupera o _id das imagens capturadas pelo Avatar e outros dados, para completar os dados
    # e para não pegar DUEs que não tenham imagem?
    # TODO: due_metadata será o lugar para guardar a DUE? Se for, então mudar raspa_container
    # para já filtrar se campo due estiver preenchido?
    griddata = raspa_container(list(conteineres_exportacao_escaneados), datainicial, datafinal)
    lista_final = [('AcessoVeiculo', 'InspecaoNaoInvasiva',
                    'numero_conteiner', 'id_imagem', 'dataescaneamento', 'due_metadata', 'due_acesso',
                    'due_posacd_rd')]
    for dados_imagem in griddata:
        _id = dados_imagem['_id']
        metadata = dados_imagem['metadata']
        numero_conteiner = metadata['numeroinformado']
        dataescaneamento = metadata['dataescaneamento']
        due_metadata = metadata.get('due')
        dados_acesso = conteineres_exportacao_detalhes[numero_conteiner]
        dados_inspecao = conteineres_escaneados_detalhes[numero_conteiner]
        due_acesso = None
        if dados_acesso.get('tipoDeclaracao') == 'DUE':
            due_acesso = dados_acesso['numeroDeclaracao']
        lista_final.append((dados_acesso['id'], dados_inspecao['id'],
                            numero_conteiner, _id, dataescaneamento, due_metadata, due_acesso,
                            ''))
    logger.info(f'Lista final: {lista_final[:2]}, {len(lista_final)}')
    df = pd.DataFrame(lista_final[1:], columns=lista_final[0])
    df.to_csv('escaneamentos_sem_due.csv', index=False)


if __name__ == '__main__':  # pragma: no-cover
    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    # Obtém a data de ontem
    ontem = datetime.now() - timedelta(days=4)

    # Define o início e o fim do dia de ontem
    inicio_ontem = datetime.combine(ontem, time.min)
    fim_ontem = datetime.combine(ontem, time.max)

    get_eventos(session, datainicio=inicio_ontem, datafim=fim_ontem, codigoRecinto='8931359')
