# coding: utf-8
"""
Resolução dos CEs de um ciclo de vida, por FAMÍLIA (master + filhotes). Função pura (sem banco).

Medição de produção (01/10/2026, CICLO_DE_VIDA.md): o contêiner consta nos itens do master E do
filhote, e os eventos da API declaram ora um, ora outro. Por isso a unidade é a família:

  - candidatos: CEs do Mercante cujo Item tem o contêiner (dado confiável), SEM exportação (tráfego 7)
    para desembarque de importação, com escala prevista perto do desembarque (janela maior que os ±5
    dias antigos: ~40% dos CEs perdidos estavam a 6–15 dias);
  - confiança: ALTA quando o CE declarado nos eventos do ciclo é da família escolhida; MÉDIA só com o
    Mercante e uma família possível; BAIXA quando há mais de uma família sem declaração que desempate;
  - o ciclo é ligado a TODOS os CEs da família que têm o contêiner (o NAVI-I acha o ciclo pelo card do
    master e pelo do filhote); o PRINCIPAL é o filhote (decisão do usuário: é o do importador real).
"""
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Sequence

from virasana.analytic_scripts.ciclo_de_vida.linha_do_tempo import ALTA, BAIXA, MEDIA

TRAFEGO_EXPORTACAO = '7'
JANELA_ESCALA_DIAS = 15
FILHOTE, MASTER, SIMPLES = 'filhote', 'master', 'simples'


@dataclass(frozen=True)
class CandidatoCE:
    ce: str
    master: Optional[str] = None
    trafego: Optional[str] = None
    datas_escala: Sequence[date] = ()
    tem_conteiner: bool = True


@dataclass
class CEsResolvidos:
    principal: Optional[str] = None
    ces: Dict[str, str] = field(default_factory=dict)     # CE -> papel (filhote, master, simples)
    confianca: Optional[str] = None
    motivos: List[str] = field(default_factory=list)


def normaliza_ce(valor) -> Optional[str]:
    digitos = re.sub(r'\D', '', str(valor or ''))
    return digitos if len(digitos) == 15 else None


def _papeis(candidatos: List[CandidatoCE]) -> Dict[str, str]:
    masters = {c.master for c in candidatos if c.master}
    return {c.ce: FILHOTE if c.master else MASTER if c.ce in masters else SIMPLES for c in candidatos}


def _familias(candidatos: List[CandidatoCE]) -> Dict[str, List[CandidatoCE]]:
    familias: Dict[str, List[CandidatoCE]] = {}
    for candidato in candidatos:
        familias.setdefault(candidato.master or candidato.ce, []).append(candidato)
    return familias


def _dias_da_escala(membros: List[CandidatoCE], desembarque: datetime) -> Optional[int]:
    dias = [abs((d - desembarque.date()).days) for c in membros for d in c.datas_escala if d]
    return min(dias) if dias else None


def _declarada(chave: str, membros: List[CandidatoCE], declarados: set) -> bool:
    return chave in declarados or any(c.ce in declarados for c in membros)


def _escolhe_familia(familias, desembarque, declarados, janela):
    """(chave da família, confiança, motivos) ou (None, None, motivos)."""
    dias = {chave: _dias_da_escala(membros, desembarque) for chave, membros in familias.items()}
    plausiveis = [chave for chave, d in dias.items() if d is not None and d <= janela]
    com_declaracao = [chave for chave in familias if _declarada(chave, familias[chave], declarados)]
    if len(plausiveis) == 1:
        chave = plausiveis[0]
        if chave in com_declaracao:
            return chave, ALTA, ['contêiner nos itens, escala próxima e CE declarado nos eventos']
        return chave, MEDIA, ['contêiner nos itens e escala próxima (sem CE declarado que confirme)']
    if len(plausiveis) > 1:
        apoiadas = [chave for chave in plausiveis if chave in com_declaracao]
        if len(apoiadas) == 1:
            return apoiadas[0], ALTA, ['mais de uma família possível; o CE declarado desempatou']
        chave = min(plausiveis, key=lambda k: dias[k])
        return chave, BAIXA, ['mais de uma família possível; escolhida a de escala mais próxima']
    if len(com_declaracao) == 1:
        chave = com_declaracao[0]
        return chave, MEDIA, [f'CE declarado nos eventos; escala a {dias[chave]} dias' if dias[chave] is not None
                              else 'CE declarado nos eventos; sem escala no Mercante']
    return None, None, ['nenhum CE do Mercante com o contêiner e escala próxima']


def _principal(membros: List[CandidatoCE], papeis: Dict[str, str], declarados: set) -> str:
    """Filhote primeiro (o do importador); entre vários, o declarado nos eventos."""
    ordem = sorted(membros, key=lambda c: ({FILHOTE: 0, SIMPLES: 1, MASTER: 2}[papeis[c.ce]],
                                           c.ce not in declarados, c.ce))
    return ordem[0].ce


def resolve_ces(desembarque: datetime, candidatos: Iterable[CandidatoCE], declarados: Iterable[str] = (),
                janela_dias: int = JANELA_ESCALA_DIAS) -> CEsResolvidos:
    """CEs do ciclo do desembarque `desembarque` (ver docstring do módulo).

    candidatos: CEs do Mercante ligados ao contêiner (com master, tráfego e escalas);
    declarados: CEs informados nos eventos do ciclo (desembarque, acessos, inspeções) — só corroboram.
    """
    resultado = CEsResolvidos()
    todos = [c for c in candidatos if c.tem_conteiner]
    validos = [c for c in todos if c.trafego != TRAFEGO_EXPORTACAO]
    if len(validos) < len(todos):
        resultado.motivos.append(f'{len(todos) - len(validos)} CE(s) de exportação descartado(s)')
    declarados = {ce for ce in (normaliza_ce(d) for d in declarados) if ce}
    familias = _familias(validos)
    chave, confianca, motivos = _escolhe_familia(familias, desembarque, declarados, janela_dias)
    resultado.motivos += motivos
    if chave is None:
        return resultado
    membros = familias[chave]
    papeis = _papeis(membros)
    resultado.ces = {c.ce: papeis[c.ce] for c in membros}
    resultado.principal = _principal(membros, papeis, declarados)
    resultado.confianca = confianca
    return resultado


def familia(resolvidos: CEsResolvidos, candidatos: Iterable[CandidatoCE] = ()) -> set:
    """Todos os CEs da família escolhida, para corroborar o CE declarado nos eventos: os que têm o
    contêiner, o master (mesmo sem o contêiner nos itens) e os IRMÃOS — filhotes do mesmo master sem o
    contêiner nos itens (`tem_conteiner=False`; a sombra de 01/10 mostrou inspeções no cais, minutos
    depois do desembarque, declarando um CE "de outra família")."""
    candidatos = list(candidatos)
    ces = set(resolvidos.ces)
    # masters: o dos CEs escolhidos e os próprios escolhidos que são master de algum candidato
    masters = {c.master for c in candidatos if c.ce in ces and c.master} | (
        ces & {c.master for c in candidatos if c.master})
    irmaos = {c.ce for c in candidatos if c.master and c.master in masters}
    return ces | masters | irmaos
