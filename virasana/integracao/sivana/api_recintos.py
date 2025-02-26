"""
Classes e funções para conectar e ler dados da API Recintos - objeto AcessoVeiculo
e transmitir eles para o Sivana - no formato de passagens

"""
import sys
from datetime import datetime
from typing import Tuple

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../commons')
sys.path.append('../bhadrasana2')

from ajna_commons.flask.log import logger
from virasana.integracao.sivana import OrganizacaoSivana, TratamentoLPR

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
        if len(passagens) >0:
            maior_id = max(acesso.id for acesso in acessos)
    except Exception as err:
        logger.error(f'Erro ao recuperar registros de AcessoVeiculo. pultimo_id:{pultimo_id}')
        logger.error(err)
    return payload, maior_id


class APIRecintos(TratamentoLPR):
    def processa_fonte_alpr(self, pstart: datetime, pend: datetime):
        """Foi necessário sobreescrever porque, ao contrário das demais fontes,
        a Fonte API recintos não conecta a um servidor de câmeras ALPRs. Além disso, o
        controle de transmissão é pelo ID. Os parâmetros pstart e pend são mantidos para
        compatibilidade de código, e descartados.

        """
        ultimo_id = self.organizacao.ultimo_id_transmitido
        logger.info(f"Organização APIRecintos Iniciando a transmissão a partir do ID: {ultimo_id}")
        payload, maior_id = le_novos_acesssos_veiculo(self.session, ultimo_id)
        return payload, maior_id

    def set_ultima_transmissao(self, ultima_transmissao: int):
        self.organizacao.ultimo_id_transmitido = ultima_transmissao
        self.session.add(self.organizacao)
        self.session.commit()
        logger.info(f'Atualizado "ultimo_id_transmitido" da organização APIRecintos '
                    f'com novo ID: {ultima_transmissao}')
