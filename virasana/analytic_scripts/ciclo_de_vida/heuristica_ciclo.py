# coding: utf-8
"""
Heurística do Ciclo de Vida (plano v2): junta a linha do tempo do contêiner e a resolução dos CEs.

Duas passagens, ambas puras (sem banco):
  1. linha do tempo sem CE -> quais eventos são do ciclo -> CEs declarados nesses eventos;
  2. CEs do ciclo pela família (Mercante + declarados) -> linha do tempo de novo, agora usando a
     família para corroborar/desconfiar do CE declarado em cada evento.
"""
from datetime import datetime
from typing import Iterable, List, Optional, Set, Tuple

from virasana.analytic_scripts.ciclo_de_vida.ces_do_ciclo import (
    JANELA_ESCALA_DIAS, CandidatoCE, CEsResolvidos, familia, resolve_ces,
)
from virasana.analytic_scripts.ciclo_de_vida.linha_do_tempo import (
    CicloCalculado, Evento, Parametros, ciclos_do_conteiner,
)


def declarados_do_ciclo(ciclo: CicloCalculado) -> List[str]:
    """CEs informados no desembarque e nos eventos ligados ao ciclo."""
    return [ciclo.desembarque.ce_declarado] + [lig.evento.ce_declarado for lig in ciclo.ligacoes()]


def processa_conteiner(eventos: Iterable[Evento], candidatos: Iterable[CandidatoCE], operadores_escopo: Set[str],
                       operadores: Set[str], agora: datetime, params: Parametros = Parametros(),
                       janela_escala_dias: int = JANELA_ESCALA_DIAS,
                       conhecidos: Optional[Set[str]] = None) -> List[Tuple[CicloCalculado, CEsResolvidos]]:
    """Ciclos do contêiner com os CEs resolvidos (ver docstring do módulo).

    conhecidos: CEs que existem no Mercante (os declarados nos eventos que não existem não desconfiam).
    """
    eventos, candidatos = list(eventos), list(candidatos)
    resolvidos, familias = {}, {}
    for ciclo in ciclos_do_conteiner(eventos, operadores_escopo, operadores, agora, params=params):
        ces = resolve_ces(ciclo.desembarque.data, candidatos, declarados_do_ciclo(ciclo), janela_escala_dias)
        resolvidos[ciclo.desembarque.id] = ces
        familias[ciclo.desembarque.id] = familia(ces, candidatos)
    finais = ciclos_do_conteiner(eventos, operadores_escopo, operadores, agora, familias=familias, params=params,
                                 conhecidos=conhecidos)
    return [(ciclo, resolvidos[ciclo.desembarque.id]) for ciclo in finais]
