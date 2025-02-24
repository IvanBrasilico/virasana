import sys
import warnings
from datetime import datetime, timedelta, time

import requests
import urllib3

# Suppress only the InsecureRequestWarning
warnings.simplefilter('ignore', urllib3.exceptions.InsecureRequestWarning)

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../virasana')
from ajna_commons.flask.log import logger

from bhadrasana.models.apirecintos import AcessoVeiculo, InspecaoNaoInvasiva

VIRASANA_URL = "https://ajna1.rfoc.srf/virasana/"


def raspa_container(numero_container: str, datainicial: str, datafinal: str,
                    virasana_url: str = VIRASANA_URL) -> list:
    logger.debug('Conectando virasana')
    params = {'query':
                  {'metadata.dataescaneamento': {'$gte': datainicial, '$lte': datafinal},
                   'metadata.contentType': 'image/jpeg',
                   'metadata.numeroinformado': numero_container,
                   },
              'projection':
                  {'metadata.dataescaneamento': 1,
                   'metadata.due': 1}
              }
    r = requests.post(virasana_url + "grid_data", json=params, verify=False)
    logger.debug(f'{r}: {r.text}')
    lista_containeres = list(r.json())
    logger.info(f'GridData: {lista_containeres}')
    return lista_containeres


def get_eventos(session, datainicio: datetime, datafim: datetime, codigoRecinto=''):
    q = session.query(AcessoVeiculo).filter(AcessoVeiculo.operacao == 'C')
    q = q.filter(AcessoVeiculo.dataHoraOcorrencia.between(datainicio, datafim))
    q = q.filter(AcessoVeiculo.codigoRecinto == codigoRecinto)
    q = q.filter(AcessoVeiculo.numeroConteiner.isnot(None))
    q = q.filter(AcessoVeiculo.numeroConteiner != '')
    q = q.filter(AcessoVeiculo.direcao == 'E')
    q = q.filter(AcessoVeiculo.listaNfe.isnot(None))

    lista_entradas = q.order_by(AcessoVeiculo.dataHoraOcorrencia).all()
    conteineres_exportacao_detalhes = [(entrada.numeroConteiner, entrada.tipoDeclaracao, entrada.numeroDeclaracao)
                              for entrada in lista_entradas]
    conteineres_exportacao = [entrada[0] for entrada in conteineres_exportacao_detalhes]
    logger.info(f'{conteineres_exportacao[:10]} {len(conteineres_exportacao)}')
    datafim_escaneamento = datafim + timedelta(hours=3)
    q2 = session.query(InspecaoNaoInvasiva)
    q2 = q2.filter(InspecaoNaoInvasiva.dataHoraOcorrencia.between(datainicio, datafim))
    q = q.filter(AcessoVeiculo.codigoRecinto == codigoRecinto)
    q2 = q2.filter(InspecaoNaoInvasiva.numeroConteiner.in_(conteineres_exportacao))
    lista_escaneamentos = q2.order_by(InspecaoNaoInvasiva.dataHoraOcorrencia).all()
    conteineres_exportacao_escaneados = [(escaneamento.numeroConteiner, escaneamento.id)
                                         for escaneamento in lista_escaneamentos]
    logger.info(f'{conteineres_exportacao_escaneados[:10]} {len(conteineres_exportacao_escaneados)}')
    datainicial = datetime.strftime(datainicio, '%Y-%m-%d %H:%M:%S')
    datafinal = datetime.strftime(datafim_escaneamento, '%Y-%m-%d %H:%M:%S')
    lista_final = [('InspecaoNaoInvasiva', 'numero_conteiner', 'id_imagem', 'dataescaneamento',
                    'due_metadata', 'due_acesso', 'due_posacd_rd')]
    for conteiner, id_inspecao in conteineres_exportacao_escaneados[:10]:
        #TODO: due_metadata será o lugar para guardar a DUE? Se for, então mudar raspa_container
        # para já filtrar se campo due estiver preenchido
        griddata = raspa_container(conteiner, datainicial, datafinal)
        for dados_imagem in griddata:
            _id = dados_imagem['_id']
            metadata = dados_imagem['metadata']
            dataescaneamento = metadata['dataescaneamento']
            due_metadata = metadata.get('due')
            lista_final.append((id_inspecao, conteiner, _id, dataescaneamento, due_metadata))
    logger.info(lista_final[:10])


if __name__ == '__main__':  # pragma: no-cover
    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    # Obtém a data de ontem
    ontem = datetime.now() - timedelta(days=1)

    # Define o início e o fim do dia de ontem
    inicio_ontem = datetime.combine(ontem, time.min)
    fim_ontem = datetime.combine(ontem, time.max)

    get_eventos(session, datainicio=inicio_ontem, datafim=fim_ontem, codigoRecinto='8931359')
