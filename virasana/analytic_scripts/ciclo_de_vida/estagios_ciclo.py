# coding: utf-8
"""
ESTÁGIOS do contêiner no Ciclo de Vida — para filtrar e mostrar nas telas (NAVI-I, completude).

Decisão do usuário (02/10/2026): "entregue" é o que interessa por padrão (concluído no operador ou no recinto
seco), mas o usuário pode querer ver as cargas em outros estágios (ainda no operador, em recinto seco sem
entrega, etc.). O estágio sai das colunas v2 (`situacao`, `fluxo`) e das datas do ciclo:

  no_operador  aberto, sem saída do operador
  em_transito  aberto, saiu do operador e ainda não entrou em recinto seco (inclui passar por outro operador)
  no_seco      aberto, entrou em recinto seco (TRA) e não foi entregue
  entregue     concluído no operador ou no recinto seco
  embarcado    concluído com embarque (passagem ou baldeação)
  incompleto   incompleto (faltam eventos para fechar o ciclo) ou abandono (sem saída em 93 dias)

Ciclos do loop ANTIGO (sem `situacao`): só "entregue", quando `concluido` — no loop antigo "concluído" era
também abandono e baldeação depois de 48 h; é a melhor aproximação até o reprocessamento.

`criterio(codigo)` (SQL) e `estagio_do_ciclo(ciclo)` (Python) classificam do mesmo jeito (há teste).
"""
from typing import List, Optional

from sqlalchemy import and_, or_

from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import CicloDeVidaImportacao as Ciclo

NO_OPERADOR, EM_TRANSITO, NO_SECO = 'no_operador', 'em_transito', 'no_seco'
ENTREGUE, EMBARCADO, INCOMPLETO = 'entregue', 'embarcado', 'incompleto'

# codigo: (rótulo curto, descrição) — na ordem do percurso
ESTAGIOS = {
    NO_OPERADOR: ('No operador', 'desembarcado, ainda sem saída do operador portuário'),
    EM_TRANSITO: ('Em trânsito', 'saiu do operador e ainda não entrou em recinto seco'),
    NO_SECO: ('Em recinto seco', 'entrou em recinto seco (TRA) e ainda não foi entregue'),
    ENTREGUE: ('Entregue', 'entregue ao importador, pelo operador ou pelo recinto seco'),
    EMBARCADO: ('Reembarcado', 'passagem ou baldeação: embarcou de novo'),
    INCOMPLETO: ('Incompleto ou abandono', 'faltam eventos para fechar o ciclo, ou sem saída em 93 dias'),
}
_ENTREGA = (ldt.OPERADOR, ldt.TRA)
_EMBARQUE = (ldt.PASSAGEM, ldt.BALDEACAO)


def lista() -> List[dict]:
    """[{codigo, rotulo, descricao}] na ordem do percurso (para a tela)."""
    return [{'codigo': codigo, 'rotulo': rotulo, 'descricao': descricao}
            for codigo, (rotulo, descricao) in ESTAGIOS.items()]


def descreve(codigo: Optional[str]) -> Optional[dict]:
    if codigo not in ESTAGIOS:
        return None
    rotulo, descricao = ESTAGIOS[codigo]
    return {'codigo': codigo, 'rotulo': rotulo, 'descricao': descricao}


def criterio(codigo: str):
    """Condição SQL (colunas de `CicloDeVidaImportacao`) do estágio."""
    aberto = Ciclo.situacao == ldt.ABERTO
    criterios = {
        NO_OPERADOR: and_(aberto, Ciclo.data_saida.is_(None), Ciclo.data_entrada_seco.is_(None)),
        EM_TRANSITO: and_(aberto, Ciclo.data_saida.isnot(None), Ciclo.data_entrada_seco.is_(None)),
        NO_SECO: and_(aberto, Ciclo.data_entrada_seco.isnot(None)),
        ENTREGUE: or_(and_(Ciclo.situacao == ldt.CONCLUIDO, Ciclo.fluxo.in_(_ENTREGA)),
                      and_(Ciclo.situacao.is_(None), Ciclo.concluido.is_(True))),
        EMBARCADO: and_(Ciclo.situacao == ldt.CONCLUIDO, Ciclo.fluxo.in_(_EMBARQUE)),
        INCOMPLETO: Ciclo.situacao.in_((ldt.INCOMPLETO, ldt.ABANDONO)),
    }
    if codigo not in criterios:
        raise ValueError(f'estágio inválido: {codigo!r}')
    return criterios[codigo]


def estagio_do_ciclo(ciclo) -> Optional[str]:
    """Código do estágio de um ciclo (ORM, com as colunas v2 carregadas); None = ciclo antigo em aberto."""
    if ciclo is None:
        return None
    situacao = ciclo.situacao
    if situacao is None:
        return ENTREGUE if ciclo.concluido else None
    if situacao == ldt.ABERTO:
        if ciclo.data_entrada_seco is not None:
            return NO_SECO
        return EM_TRANSITO if ciclo.data_saida is not None else NO_OPERADOR
    if situacao == ldt.CONCLUIDO:
        return ENTREGUE if ciclo.fluxo in _ENTREGA else EMBARCADO if ciclo.fluxo in _EMBARQUE else None
    return INCOMPLETO if situacao in (ldt.INCOMPLETO, ldt.ABANDONO) else None
