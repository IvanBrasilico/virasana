# coding: utf-8
"""
Completude de escaneamentos pelo Ciclo de Vida da importação, nos dois
caminhos que o contêiner pode seguir depois do desembarque:

  - TRA (recinto seco / zona secundária): desembarque no operador -> saída do
    operador -> entrada no seco -> escaneamento no seco -> saída do seco.
    Universo: ciclos com ENTRADA NO SECO no período, por recinto seco
    (`codigoRecintoArmazenagem`). Entradas com menos de 3 h ainda não tiveram
    tempo de escaneamento e ficam de fora. Mesma regra da tela antiga
    (`/analise/completude-escaneamentos-recintos-secos`).
  - Operador (desembaraço no porto): desembarque -> escaneamento -> saída,
    tudo no operador. Universo: ciclos CONCLUÍDOS, SEM entrada no seco e com
    SAÍDA no período, por operador (`codigoRecintoDesembarque`). É a "entrega
    pelo operador" que o loop do ciclo de vida já infere (sem entrada no seco
    até HORAS_TRANSIT_TIME depois da saída — ver manager_ciclo_de_vida).

Um ciclo tem escaneamento quando há linha em ..._escaneamentos; tem imagem
quando alguma dessas linhas tem `imagem_id`. Somente leitura.
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy import and_, case, exists, func, select

from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import (
    CicloDeVidaImportacao as Ciclo, CicloDeVidaImportacaoEscaneamentos as Esc,
)
from virasana.navii.busca_navii import tabela_recintos

HORAS_SEM_TEMPO_DE_ESCANEAR = 3
LIMITE_DETALHE = 2000

FLUXOS = {
    'tra': {
        'titulo': 'Recintos secos (TRA)',
        'caminho': 'desembarque no operador → saída → entrada no seco → escaneamento no seco → saída do seco',
        'recinto': Ciclo.codigoRecintoArmazenagem,
        'data': Ciclo.data_entrada_seco,
        'rotulo_total': 'entradas no seco',
    },
    'operador': {
        'titulo': 'Desembaraço no operador',
        'caminho': 'desembarque → escaneamento → saída, tudo no operador (sem entrada em recinto seco)',
        'recinto': Ciclo.codigoRecintoDesembarque,
        'data': Ciclo.data_saida,
        'rotulo_total': 'entregas pelo operador',
    },
}


def _fluxo(fluxo: str) -> dict:
    if fluxo not in FLUXOS:
        raise ValueError(f'fluxo inválido: {fluxo!r}')
    return FLUXOS[fluxo]


def _criterios(fluxo: str, inicio: datetime, fim: datetime, agora: Optional[datetime] = None) -> list:
    """Universo de cada fluxo no período (ver docstring do módulo)."""
    if fluxo == 'tra':
        limite = (agora or datetime.now()) - timedelta(hours=HORAS_SEM_TEMPO_DE_ESCANEAR)
        return [Ciclo.data_entrada_seco.isnot(None), Ciclo.data_entrada_seco.between(inicio, fim),
                Ciclo.data_entrada_seco < limite]
    if fluxo == 'operador':
        return [Ciclo.concluido.is_(True), Ciclo.data_entrada_seco.is_(None), Ciclo.data_saida.isnot(None),
                Ciclo.data_saida.between(inicio, fim)]
    _fluxo(fluxo)


def _percentual(parte: int, total: int) -> float:
    return parte / total if total else 0.0


def _item(codigo, nome, total, sem_esc, sem_img, saidas, saidas_sem_esc) -> dict:
    total, sem_esc, sem_img = total or 0, sem_esc or 0, sem_img or 0
    saidas, saidas_sem_esc = saidas or 0, saidas_sem_esc or 0
    return {
        'codigo_recinto': codigo, 'nome_recinto': nome or '(recinto não cadastrado)',
        'total': total, 'sem_escaneamento': sem_esc, 'sem_imagem': sem_img,
        'pct_sem_escaneamento': _percentual(sem_esc, total),
        'total_saidas': saidas, 'saidas_sem_escaneamento': saidas_sem_esc,
        'pct_saidas_sem_escaneamento': _percentual(saidas_sem_esc, saidas),
    }


def agregado(session, fluxo: str, inicio: datetime, fim: datetime, agora: Optional[datetime] = None) -> List[dict]:
    """Uma linha por recinto do fluxo, do maior para o menor % sem escaneamento.

    No fluxo TRA, "saídas" são as saídas do seco; no do operador toda entrega
    já é uma saída (total_saidas = total).
    """
    recinto = _fluxo(fluxo)['recinto']
    tem_escaneamento = exists().where(Esc.ciclo_de_vida_id == Ciclo.ID)
    tem_imagem = exists().where(and_(Esc.ciclo_de_vida_id == Ciclo.ID, Esc.imagem_id.isnot(None)))
    saiu = Ciclo.data_saida_seco.isnot(None) if fluxo == 'tra' else Ciclo.data_saida.isnot(None)
    nomes = (select(tabela_recintos.c.cod_siscomex, func.max(tabela_recintos.c.nome).label('nome'))
             .where(tabela_recintos.c.cod_siscomex.isnot(None))
             .group_by(tabela_recintos.c.cod_siscomex).subquery('nomes'))
    linhas = (
        session.query(
            recinto, func.max(nomes.c.nome), func.count(Ciclo.ID),
            func.sum(case((~tem_escaneamento, 1), else_=0)),
            func.sum(case((~tem_imagem, 1), else_=0)),
            func.sum(case((saiu, 1), else_=0)),
            func.sum(case((and_(saiu, ~tem_escaneamento), 1), else_=0)),
        )
        .outerjoin(nomes, nomes.c.cod_siscomex == recinto)
        .filter(*_criterios(fluxo, inicio, fim, agora))
        .group_by(recinto)
        .all()
    )
    resultados = [_item(*linha) for linha in linhas]
    return sorted(resultados, key=lambda r: (-r['pct_sem_escaneamento'], -r['total']))


def totais(resultados: List[dict]) -> dict:
    """Linha de TOTAL de uma tabela agregada."""
    soma = {chave: sum(r[chave] for r in resultados)
            for chave in ('total', 'sem_escaneamento', 'sem_imagem', 'total_saidas', 'saidas_sem_escaneamento')}
    return _item(None, 'TOTAL', soma['total'], soma['sem_escaneamento'], soma['sem_imagem'],
                 soma['total_saidas'], soma['saidas_sem_escaneamento'])


def panorama(session, inicio: datetime, fim: datetime, agora: Optional[datetime] = None) -> Dict[str, dict]:
    """Os dois fluxos lado a lado: tabela por recinto, totais e participação de cada um."""
    resultado = {}
    for fluxo in FLUXOS:
        linhas = agregado(session, fluxo, inicio, fim, agora)
        resultado[fluxo] = {**FLUXOS[fluxo], 'linhas': linhas, 'totais': totais(linhas)}
    todos = sum(r['totais']['total'] for r in resultado.values())
    for dados in resultado.values():
        dados['participacao'] = _percentual(dados['totais']['total'], todos)
    return resultado


def detalhe(session, fluxo: str, codigo_recinto: str, inicio: datetime, fim: datetime,
            agora: Optional[datetime] = None) -> List[dict]:
    """Ciclos de um recinto no fluxo: uma linha por contêiner, com as datas e
    os escaneamentos/imagens (os sem escaneamento primeiro)."""
    ciclos = (
        session.query(Ciclo)
        .filter(_fluxo(fluxo)['recinto'] == codigo_recinto, *_criterios(fluxo, inicio, fim, agora))
        .order_by(FLUXOS[fluxo]['data'])
        .limit(LIMITE_DETALHE)
        .all()
    )
    linhas = []
    for ciclo in ciclos:
        escaneamentos = sorted(ciclo.escaneamentos, key=lambda e: e.data_escaneamento or datetime.min)
        linhas.append({
            'conteiner': ciclo.numeroConteiner, 'ce': ciclo.numeroCEmercante,
            'desembarque': ciclo.data_desembarque, 'saida': ciclo.data_saida,
            'entrada_seco': ciclo.data_entrada_seco, 'saida_seco': ciclo.data_saida_seco,
            'escaneamentos': [{'id': e.escaneamento_id, 'data': e.data_escaneamento, 'imagem_id': e.imagem_id}
                              for e in escaneamentos],
            'imagens': [e.imagem_id for e in escaneamentos if e.imagem_id],
        })
    return sorted(linhas, key=lambda lin: bool(lin['escaneamentos']))


def nome_recinto(session, codigo: str) -> Optional[str]:
    return session.execute(
        select(tabela_recintos.c.nome).where(tabela_recintos.c.cod_siscomex == codigo).limit(1)
    ).scalar()
