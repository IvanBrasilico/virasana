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

PORTO do desembarque (22/10/2026, caso CAAU5545018 — CICLO_DE_VIDA.md): o ciclo de importação acontece no porto
de descarregamento do manifesto / destino final; pode haver desembarque num porto intermediário (armazenagem no
operador ou baldeação). Com o porto (`resolve_ces(..., porto=)`):
  - o CE cuja carga SAI desse porto (origem = porto: cabotagem ou passagem embarcando ali) não é o do desembarque;
  - as datas são as das escalas NESSE porto (sem escala nele, todas); sem o porto, as de Santos (`PORTO_PADRAO`);
  - entre várias famílias plausíveis sem declaração que desempate, a de destino nesse porto vence a escala mais
    próxima;
  - os CEs descartados por SAIR do porto são a perna SEGUINTE da mesma visita (baldeação: desembarca e logo embarca
    prosseguindo viagem): não entram no ciclo, mas o CE deles declarado no embarque (ou noutro evento) não é de
    "outra família" (`seguimento`, usado por `familia`).

Casos reais (23/10/2026, aprovado pelo usuário; `test_casos_reais_ciclo.py`):
  - C6 EXPORTAÇÃO DE OUTRO PORTO em trânsito (origem noutro porto e escala no porto do desembarque, na janela; ex.:
    Vitória -> Benin, baldeação em Santos): não é importação (fica fora do ciclo), mas é a mesma carga seguindo
    viagem — entra no `seguimento`;
  - C7 CONTÊINER CONSOLIDADO: os BLs únicos do MESMO manifesto com o mesmo contêiner são UMA família (todos no
    ciclo — o NAVI-I acha o ciclo pelo card de qualquer um; principal: o declarado, senão o primeiro).
"""
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from virasana.analytic_scripts.ciclo_de_vida.linha_do_tempo import ALTA, BAIXA, MEDIA

TRAFEGO_EXPORTACAO = '7'
TRAFEGO_IMPORTACAO = '5'
PREFIXO_SANTOS = '15'      # CE emitido em Santos (os de outros portos: transbordo/cabotagem — sombra de 01/10/2026)
JANELA_ESCALA_DIAS = 15
PORTO_PADRAO = 'BRSSZ'     # porto do desembarque desconhecido: as escalas de Santos (o escopo original)
FILHOTE, MASTER, SIMPLES = 'filhote', 'master', 'simples'


@dataclass(frozen=True)
class CandidatoCE:
    ce: str
    master: Optional[str] = None
    trafego: Optional[str] = None
    datas_escala: Sequence[date] = ()      # as de `PORTO_PADRAO` (sem escala nele, todas) — vale sem `escalas`
    tem_conteiner: bool = True
    escalas: Sequence[Tuple[date, str]] = ()   # (atracação prevista, porto) de todas as escalas do manifesto
    origem: Optional[str] = None           # porto de origem da carga (Conhecimento.portoOrigemCarga)
    destino: Optional[str] = None          # porto de destino final (Conhecimento.portoDestFinal)
    manifesto: Optional[str] = None        # Conhecimento.manifestoCE (C7: BLs únicos do mesmo manifesto)


@dataclass
class CEsResolvidos:
    principal: Optional[str] = None
    ces: Dict[str, str] = field(default_factory=dict)     # CE -> papel (filhote, master, simples)
    confianca: Optional[str] = None
    motivos: List[str] = field(default_factory=list)
    seguimento: List[str] = field(default_factory=list)   # a carga seguindo viagem: saindo do porto e exportação de
    #                                                       outro porto em trânsito (C6) — corroboram, fora do ciclo


def normaliza_ce(valor) -> Optional[str]:
    digitos = re.sub(r'\D', '', str(valor or ''))
    return digitos if len(digitos) == 15 else None


def _papeis(candidatos: List[CandidatoCE]) -> Dict[str, str]:
    masters = {c.master for c in candidatos if c.master}
    return {c.ce: FILHOTE if c.master else MASTER if c.ce in masters else SIMPLES for c in candidatos}


def _chave_da_familia(candidato: CandidatoCE, masters: set) -> str:
    """O master; o próprio CE se é master; o manifesto para os BLs únicos (C7: carga consolidada — vários BLs do mesmo
    manifesto no mesmo contêiner são uma família); o próprio CE se não há manifesto."""
    if candidato.master:
        return candidato.master
    if candidato.ce in masters or not candidato.manifesto:
        return candidato.ce
    return f'manifesto {candidato.manifesto}'


def _familias(candidatos: List[CandidatoCE]) -> Dict[str, List[CandidatoCE]]:
    masters = {c.master for c in candidatos if c.master}
    familias: Dict[str, List[CandidatoCE]] = {}
    for candidato in candidatos:
        familias.setdefault(_chave_da_familia(candidato, masters), []).append(candidato)
    return familias


def datas_no_porto(escalas: Iterable[Tuple[date, str]], porto: Optional[str] = None) -> List[date]:
    """Datas das escalas no `porto` (desconhecido: PORTO_PADRAO); sem escala nele, as de todas."""
    escalas = list(escalas)
    no_porto = [dia for dia, local in escalas if local == (porto or PORTO_PADRAO)]
    return no_porto or [dia for dia, _ in escalas]


def datas_do_candidato(candidato: CandidatoCE, porto: Optional[str] = None) -> List[date]:
    return datas_no_porto(candidato.escalas, porto) if candidato.escalas else list(candidato.datas_escala)


def _dias_da_escala(membros: List[CandidatoCE], desembarque: datetime, porto: Optional[str] = None) -> Optional[int]:
    dias = [abs((d - desembarque.date()).days) for c in membros for d in datas_do_candidato(c, porto) if d]
    return min(dias) if dias else None


def _declarada(chave: str, membros: List[CandidatoCE], declarados: set) -> bool:
    return chave in declarados or any(c.ce in declarados for c in membros)


def _entre_plausiveis(plausiveis, familias, dias, com_declaracao, porto):
    """Mais de uma família com escala na janela: a declarada; senão a de destino no porto; senão a de escala mais
    próxima."""
    apoiadas = [chave for chave in plausiveis if chave in com_declaracao]
    if len(apoiadas) == 1:
        return apoiadas[0], ALTA, ['mais de uma família possível; o CE declarado desempatou']
    no_destino = [chave for chave in plausiveis if porto and any(c.destino == porto for c in familias[chave])]
    if len(no_destino) == 1:
        return no_destino[0], MEDIA, [f'mais de uma família possível; escolhida a de destino em {porto}']
    chave = min(plausiveis, key=lambda k: dias[k])
    return chave, BAIXA, ['mais de uma família possível; escolhida a de escala mais próxima']


def _escolhe_familia(familias, desembarque, declarados, janela, porto=None):
    """(chave da família, confiança, motivos) ou (None, None, motivos)."""
    dias = {chave: _dias_da_escala(membros, desembarque, porto) for chave, membros in familias.items()}
    plausiveis = [chave for chave, d in dias.items() if d is not None and d <= janela]
    com_declaracao = [chave for chave in familias if _declarada(chave, familias[chave], declarados)]
    if len(plausiveis) == 1:
        chave = plausiveis[0]
        if chave in com_declaracao:
            return chave, ALTA, ['contêiner nos itens, escala próxima e CE declarado nos eventos']
        return chave, MEDIA, ['contêiner nos itens e escala próxima (sem CE declarado que confirme)']
    if len(plausiveis) > 1:
        return _entre_plausiveis(plausiveis, familias, dias, com_declaracao, porto)
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


def _na_janela(candidato: CandidatoCE, desembarque: datetime, porto: Optional[str], janela: int) -> bool:
    dias = _dias_da_escala([candidato], desembarque, porto)
    return dias is not None and dias <= janela


def _sem_saida(validos: List[CandidatoCE], desembarque: datetime, porto: Optional[str], janela: int,
               resultado: CEsResolvidos) -> List[CandidatoCE]:
    """Tira os CEs cuja carga SAI do porto do desembarque (origem = porto): são de um embarque ali. Os com escala
    nesse porto na janela são a perna seguinte desta visita (`resultado.seguimento`); os outros, de outra operação
    (23/10/2026: uma cabotagem antiga do mesmo contêiner no seguimento fazia o CE da baldeação parecer de "outra
    família")."""
    saindo = [c for c in validos if porto and c.origem == porto]
    if saindo:
        resultado.motivos.append(f'{len(saindo)} CE(s) com a carga saindo de {porto} descartado(s)')
        resultado.seguimento = sorted(c.ce for c in saindo if _na_janela(c, desembarque, porto, janela))
    return [c for c in validos if c not in saindo]


def _em_transito(candidato: CandidatoCE, desembarque: datetime, porto: str, janela: int) -> bool:
    """Exportação de OUTRO porto com escala no porto do desembarque, na janela."""
    if candidato.trafego != TRAFEGO_EXPORTACAO or not candidato.origem or candidato.origem == porto:
        return False
    return _na_janela(candidato, desembarque, porto, janela)


def _exportacoes_em_transito(todos: List[CandidatoCE], desembarque: datetime, porto: Optional[str], janela: int,
                             resultado: CEsResolvidos):
    """C6: a exportação de outro porto em trânsito (baldeação) é a mesma carga seguindo viagem — entra no
    `seguimento`."""
    transito = [c.ce for c in todos if porto and _em_transito(c, desembarque, porto, janela)]
    if transito:
        resultado.motivos.append(f'{len(transito)} exportação(ões) de outro porto em trânsito (seguimento)')
        resultado.seguimento = sorted(set(resultado.seguimento) | set(transito))


def resolve_ces(desembarque: datetime, candidatos: Iterable[CandidatoCE], declarados: Iterable[str] = (),
                janela_dias: int = JANELA_ESCALA_DIAS, porto: Optional[str] = None) -> CEsResolvidos:
    """CEs do ciclo do desembarque `desembarque` (ver docstring do módulo).

    candidatos: CEs do Mercante ligados ao contêiner (com master, tráfego, origem, destino e escalas);
    declarados: CEs informados nos eventos do ciclo (desembarque, acessos, inspeções) — só corroboram;
    porto: porto do desembarque (`CargaCiclo.portos_dos_desembarques`); None = desconhecido (as regras de antes).
    """
    resultado = CEsResolvidos()
    todos = [c for c in candidatos if c.tem_conteiner]
    validos = [c for c in todos if c.trafego != TRAFEGO_EXPORTACAO]
    if len(validos) < len(todos):
        resultado.motivos.append(f'{len(todos) - len(validos)} CE(s) de exportação descartado(s)')
    validos = _sem_saida(validos, desembarque, porto, janela_dias, resultado)
    _exportacoes_em_transito(todos, desembarque, porto, janela_dias, resultado)
    declarados = {ce for ce in (normaliza_ce(d) for d in declarados) if ce}
    familias = _familias(validos)
    chave, confianca, motivos = _escolhe_familia(familias, desembarque, declarados, janela_dias, porto)
    resultado.motivos += motivos
    if chave is None:
        return resultado
    membros = familias[chave]
    papeis = _papeis(membros)
    resultado.ces = {c.ce: papeis[c.ce] for c in membros}
    resultado.principal = _principal(membros, papeis, declarados)
    resultado.confianca = confianca
    return resultado


def importacao_de_santos(resolvidos: CEsResolvidos, candidatos: Iterable[CandidatoCE] = ()) -> bool:
    """O CE principal é de importação (tráfego 5) e foi emitido em Santos (começa com 15).

    Só com ele uma estada longa ainda em andamento noutro operador vira TRA provisório (`linha_do_tempo`, usuário
    15/10/2026): na medição, a armazenagem noutro operador tinha CE de importação de Santos em 92% dos casos; a
    baldeação esperando o navio, quase sempre sem CE, cabotagem ou CE de outro porto."""
    principal = resolvidos.principal
    if not principal or not principal.startswith(PREFIXO_SANTOS):
        return False
    trafego = next((c.trafego for c in candidatos if c.ce == principal), None)
    return (trafego or '').strip() == TRAFEGO_IMPORTACAO


def familia(resolvidos: CEsResolvidos, candidatos: Iterable[CandidatoCE] = ()) -> set:
    """Todos os CEs da família escolhida, para corroborar o CE declarado nos eventos: os que têm o
    contêiner, o master (mesmo sem o contêiner nos itens) e os IRMÃOS — filhotes do mesmo master sem o
    contêiner nos itens (`tem_conteiner=False`; a sombra de 01/10 mostrou inspeções no cais, minutos
    depois do desembarque, declarando um CE "de outra família"); e os da perna seguinte (`seguimento`: carga
    saindo do porto do desembarque — o embarque da baldeação declara o CE dela)."""
    candidatos = list(candidatos)
    ces = set(resolvidos.ces)
    # masters: o dos CEs escolhidos e os próprios escolhidos que são master de algum candidato
    masters = {c.master for c in candidatos if c.ce in ces and c.master} | (
        ces & {c.master for c in candidatos if c.master})
    irmaos = {c.ce for c in candidatos if c.master and c.master in masters}
    return ces | masters | irmaos | set(resolvidos.seguimento)
