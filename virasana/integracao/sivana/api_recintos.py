"""
Classes e funções para conectar e ler dados da API Recintos - objeto AcessoVeiculo
e transmitir eles para o Sivana - no formato de passagens

"""
import sys
from typing import Tuple

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../commons')
sys.path.append('../bhadrasana2')

from ajna_commons.flask.log import logger
from virasana.integracao.sivana import OrganizacaoSivana


from bhadrasana.models.apirecintos import AcessoVeiculo


def le_ultimo_id_transmitido(psession):
    organizacao: OrganizacaoSivana = psession.query(OrganizacaoSivana).filter(
        OrganizacaoSivana.codigoOrganizacao == 'APIRecintos').one_or_none()
    logger.info(f"Iniciando a transmissão a partir do ID: {organizacao.ultimo_id_transmitido}")
    return organizacao.ultimo_id_transmitido


def grava_ultimo_id_transmitido(psession, novo_id):
    # Abre o arquivo no modo de escrita e substitui o conteúdo
    organizacao: OrganizacaoSivana = psession.query(OrganizacaoSivana).filter(
        OrganizacaoSivana.codigoOrganizacao == 'APIRecintos').one_or_none()
    organizacao.ultimo_id_transmitido = novo_id
    psession.add(organizacao)
    psession.commit()
    logger.info(f"Atualizado 'ultimo_id_transmitido' com novo ID: {novo_id}")


def le_novos_acesssos_veiculo(psession, pultimo_id, limit=500) -> Tuple[dict, int]:
    """ Pesquisa novos registros na tabela.
    Se retornar payload com passagens vazio, ocorreu erro ou não há registros novos.

    Args:
        psession: conexão com o Banco SQL
        pultimo_id: ID do AcessoVeículo a partir do qual transmitir
        limit: quantidade máxima a buscar por vez

    Returns: dict payload no formato Sivana, maior ID recuperado

    """
    maior_id = pultimo_id
    payload = {'totalLinhas': 0, 'offset': '-03:00', 'passagens': []}
    try:
        acessos = psession.query(AcessoVeiculo).filter(AcessoVeiculo.id > pultimo_id).limit(limit).all()
        passagens = [acesso.to_sivana() for acesso in acessos]
        payload = {'totalLinhas': len(passagens), 'offset': '-03:00', 'passagens': passagens}
        # print(payload)
        maior_id = max(acesso.id for acesso in acessos)
    except Exception as err:
        logger.error(f'Erro ao recuperar registros de AcessoVeiculo. pultimo_id:{pultimo_id}')
        logger.error(err)
    return payload, maior_id
