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


def le_novos_acesssos_veiculo(psession, pultimo_id, limit=500) -> Tuple[dict, AcessoVeiculo]:
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
    ultimo_acesso = None
    try:
        acessos = (psession.query(AcessoVeiculo).filter(AcessoVeiculo.id > pultimo_id).
                   order_by(AcessoVeiculo.id).limit(limit).all())
        passagens = []
        for acesso in acessos:
            try:
                passagens.append(acesso.to_sivana())
                ultimo_acesso = acesso
            except Exception as err:
                logger.error(err)
        payload = {'totalLinhas': len(passagens), 'offset': '-03:00', 'passagens': passagens}
        # print(payload)
    except Exception as err:
        logger.error(f'Erro ao recuperar registros de AcessoVeiculo. pultimo_id:{pultimo_id}')
        logger.error(err)
    return payload, ultimo_acesso


class APIRecintos(TratamentoLPR):
    def processa_fonte_alpr(self, pstart: datetime, pend: datetime):
        """Foi necessário sobreescrever porque, ao contrário das demais fontes,
        a Fonte API recintos não conecta a um servidor de câmeras ALPRs. Além disso, o
        controle de transmissão é pelo ID. Os parâmetros pstart e pend são mantidos para
        compatibilidade de código, e descartados.

        """
        ultimo_id = self.organizacao.ultimo_id_transmitido
        logger.info(f"Organização APIRecintos Iniciando a transmissão a partir do ID: {ultimo_id}")
        payload, ultimo_acesso = le_novos_acesssos_veiculo(self.session, ultimo_id)
        return payload, ultimo_acesso


    def set_ultima_transmissao(self, ultimo_acesso: AcessoVeiculo):
        if ultimo_acesso:
            self.organizacao.ultimo_id_transmitido = ultimo_acesso.id
            self.organizacao.ultima_transmissao = ultimo_acesso.dataHoraTransmissao
            self.session.add(self.organizacao)
            self.session.commit()
            logger.info(f'Atualizado "ultimo_id_transmitido" da organização APIRecintos '
                        f'com novo ID: {ultimo_acesso.id}')
        else:
            logger.info(f'Não atualizado "ultimo_id_transmitido" da organização APIRecintos '
                        f'recebido ultimo_acesso {ultimo_acesso}')
