import sys

sys.path.append('.')
sys.path.insert(0, '../ajna_docs/commons')
sys.path.insert(0, '../bhadrasana')

from virasana.integracao.mercante.mercantealchemy import Conhecimento, Escala, EscalaManifesto, Item, Manifesto
from bhadrasana.models.apirecintos import AcessoVeiculo, EmbarqueDesembarque

from datetime import datetime, timedelta
from typing import Optional

TOLERANCIA_PREVISAO_ATRACACAO = 5  # 5 dias
TOLERANCIA_DATA_DESEMBARQUE = 1  # 1 dia
HORAS_PERMANENCIA_MOLHADO = 72  # 72 horas
HORAS_TRANSIT_TIME = 6  # 6 horas
HORAS_ESCANEAMENTO_SECO = 2  # 2 horas


def _parse_dt(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    for fmt in (
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d',
    ):
        try:
            return datetime.strptime(str(value)[:19], fmt)
        except ValueError:
            pass
    raise ValueError(f'Formato de data inválido: {value}')


def busca_ce_mercante_conteiner_desembarque(session, codigoConteiner, data_desembarque) -> Optional[Conhecimento]:
    data_desembarque = _parse_dt(data_desembarque)
    inicio = data_desembarque - timedelta(days=5)
    fim = data_desembarque + timedelta(days=5)
    q = (
        session.query(Conhecimento)
        .join(Item, Item.numeroCEmercante == Conhecimento.numeroCEmercante)
        .join(Manifesto, Manifesto.numero == Conhecimento.manifestoCE)
        .join(EscalaManifesto, EscalaManifesto.manifesto == Manifesto.numero)
        .join(Escala, Escala.numeroDaEscala == EscalaManifesto.escala)
        .filter(
            Item.codigoConteiner == codigoConteiner,
            Escala.dataPrevistaAtracacao.isnot(None),
            Escala.dataPrevistaAtracacao >= inicio.strftime('%Y-%m-%d'),
            Escala.dataPrevistaAtracacao <= fim.strftime('%Y-%m-%d'),
        )
        .order_by(Conhecimento.dataEmissao.asc())
    )
    return q.first()


def busca_desembarque_conteiner(session, numeroConteiner, data_desembarque) -> Optional[EmbarqueDesembarque]:
    inicio = data_desembarque - timedelta(days=1)
    fim = data_desembarque + timedelta(days=1)
    return session.query(EmbarqueDesembarque).filter(
        EmbarqueDesembarque.numeroConteiner == numeroConteiner,
        EmbarqueDesembarque.dataHoraOcorrencia >= data_desembarque,
        EmbarqueDesembarque.dataHoraOcorrencia <= data_desembarque,
    ).first()


def busca_acesso_veiculo_desembarque_conteiner(session, desembarque: EmbarqueDesembarque) -> Optional[AcessoVeiculo]:
    inicio = desembarque.dataHoraOcorrencia
    fim = inicio + timedelta(hours=72)
    return (
        session.query(AcessoVeiculo)
        .filter(
            AcessoVeiculo.codigoRecinto == desembarque.codigoRecinto,
            AcessoVeiculo.numeroConteiner == desembarque.numeroConteiner,
            AcessoVeiculo.direcao == 'E',
            AcessoVeiculo.dataHoraOcorrencia >= inicio,
            AcessoVeiculo.dataHoraOcorrencia < fim,
        )
        .order_by(AcessoVeiculo.dataHoraOcorrencia.asc())
        .first()
    )


def busca_escaneamento_desembarque_conteiner(
        session,
        desembarque: EmbarqueDesembarque,
        saida: AcessoVeiculo
) -> List[EmbarqueDesembarque]:
    if saida is None:
        return []

    inicio = desembarque.dataHoraOcorrencia
    fim = saida.dataHoraOcorrencia

    if fim <= inicio:
        return []

    return (
        session.query(EmbarqueDesembarque)
        .filter(
            EmbarqueDesembarque.codigoRecinto == desembarque.codigoRecinto,
            EmbarqueDesembarque.numeroConteiner == desembarque.numeroConteiner,
            EmbarqueDesembarque.dataHoraOcorrencia >= inicio,
            EmbarqueDesembarque.dataHoraOcorrencia < fim,
        )
        .order_by(EmbarqueDesembarque.dataHoraOcorrencia.asc())
        .all()
    )


if __name__ == '__main__':
    # Popular
    print('Escolher um ponto de partida e popular Ciclo de Vida!')