# coding: utf-8
"""
Pesagens do ciclo de vida no NAVI-I (P3, 20/10/2026; CICLO_DE_VIDA.md, "Fora de Santos e pesagens"):

  - no card: o peso MEDIDO do contêiner x o DECLARADO (Mercante) da pesagem principal do ciclo — a da retirada no
    operador do desembarque ou a da chegada ao seco —, o diagnóstico prévio (coerente, diferença moderada, digitação
    provável, possível fraude...), a confirmação pela outra pesagem e, na expansão, todas as pesagens do contêiner
    no ciclo (dado bruto, como o recinto transmitiu);
  - filtros: diagnóstico (`pesagem`), diferença mínima em % para mais ou para menos (`dif_pct_min`) e ordem de
    grandeza (`ordem_grandeza`: medido >= 10x ou <= 1/10 do declarado) — sobre a pesagem principal.

Sem a tabela `ciclodevida_pesagem` no banco (DDL ainda não aplicada), o card fica sem o bloco e os filtros
respondem com erro claro — o resto do NAVI-I não muda.
"""
import math
from decimal import Decimal
from typing import Dict, Iterable, List, Optional

from sqlalchemy import or_, select

from virasana.analytic_scripts.ciclo_de_vida.gravacao_ciclo import PESAGENS
from virasana.navii import tabelas_opcionais

# código do filtro -> diagnósticos da pesagem principal (pesagens_ciclo.DIAGNOSTICOS); None = qualquer comparada
FILTROS_PESAGEM = {
    'fraude': ('divergencia',),
    'digitacao': ('digitacao',),
    'divergente': ('moderada', 'digitacao', 'dois', 'divergencia'),
    'coerente': ('coerente',),
    'comparada': ('coerente', 'moderada', 'digitacao', 'dois', 'divergencia'),
}
ROTULOS_FILTRO = {
    'fraude': 'Possível fraude (divergência relevante)',
    'digitacao': 'Digitação provável',
    'divergente': 'Qualquer diferença (moderada ou maior)',
    'coerente': 'Coerente',
    'comparada': 'Com pesagem comparada',
}
CAMPOS_PESAGEM = ('pesagem', 'dif_pct_min', 'ordem_grandeza')
ORDEM_DE_GRANDEZA = 10


def tem_tabela(session) -> bool:
    return tabelas_opcionais.existe(session, PESAGENS.name)


def _numero(valor) -> Optional[float]:
    if valor is None or isinstance(valor, bool):
        return None
    try:
        numero = float(str(valor).replace(',', '.')) if isinstance(valor, str) else float(valor)
    except ValueError:
        return None
    return numero if math.isfinite(numero) else None


def normaliza(filtros: dict) -> dict:
    """`pesagem` só com um código conhecido; `dif_pct_min` número >= 0 (inteiro fica inteiro); `ordem_grandeza`
    verdadeiro ou fora."""
    if 'pesagem' in filtros and filtros['pesagem'] not in FILTROS_PESAGEM:
        del filtros['pesagem']
    if 'dif_pct_min' in filtros:
        numero = _numero(filtros['dif_pct_min'])
        if numero is None or numero < 0:
            del filtros['dif_pct_min']
        else:
            filtros['dif_pct_min'] = int(numero) if numero.is_integer() else numero
    if 'ordem_grandeza' in filtros:
        if filtros['ordem_grandeza'] in (True, 1, '1', 'true', 'True', 'sim'):
            filtros['ordem_grandeza'] = True
        else:
            del filtros['ordem_grandeza']
    return filtros


def criterios(session, filtros: dict) -> list:
    """Condições sobre a pesagem PRINCIPAL do ciclo (`PESAGENS`) para os filtros de pesagem; [] sem eles. Quem chama
    as junta à ligação do ciclo (`busca_navii._existe_ciclo`)."""
    if not any(campo in filtros for campo in CAMPOS_PESAGEM):
        return []
    if not tem_tabela(session):
        raise ValueError('Filtros de pesagem indisponíveis neste banco '
                         '(tabela das pesagens do ciclo ainda não criada).')
    condicoes = [PESAGENS.c.principal.is_(True)]
    diagnosticos = FILTROS_PESAGEM.get(filtros.get('pesagem'), FILTROS_PESAGEM['comparada'])
    condicoes.append(PESAGENS.c.diagnostico.in_(diagnosticos))
    if 'dif_pct_min' in filtros:
        limite = filtros['dif_pct_min']
        condicoes.append(or_(PESAGENS.c.diferenca_pct >= limite, PESAGENS.c.diferenca_pct <= -limite))
    if filtros.get('ordem_grandeza'):
        condicoes.append(or_(PESAGENS.c.razao >= ORDEM_DE_GRANDEZA, PESAGENS.c.razao <= 1 / ORDEM_DE_GRANDEZA))
    return condicoes


def _linha(linha, nomes_recintos: Dict[str, str]) -> dict:
    def numero(valor):
        return float(valor) if isinstance(valor, Decimal) else valor
    codigo = (linha.codigoRecinto or '').strip() or None
    return {
        'data': linha.data, 'recinto': codigo, 'nomeRecinto': nomes_recintos.get(codigo),
        'operacao': linha.operacao, 'principal': bool(linha.principal), 'placa': linha.placa,
        'semirreboque': linha.semirreboque, 'pesoBruto': numero(linha.peso_bruto),
        'capturaAutomatica': linha.captura_automatica, 'taraConjunto': numero(linha.tara_conjunto),
        'origemTaraConjunto': linha.origem_tara_conjunto, 'taraConteiner': numero(linha.tara_conteiner),
        'origemTaraConteiner': linha.origem_tara_conteiner, 'pesoMedido': numero(linha.peso_medido),
        'pesoDeclarado': numero(linha.peso_declarado), 'origemDeclarado': linha.origem_declarado,
        'diferencaKg': numero(linha.diferenca_kg), 'diferencaPct': numero(linha.diferenca_pct),
        'razao': numero(linha.razao), 'classificacao': linha.classificacao, 'diagnostico': linha.diagnostico,
        'detalhe': linha.detalhe, 'confirmacao': linha.confirmacao, 'foraDaDivisao': linha.fora_da_divisao,
        'divisao': linha.divisao,
    }


def pesagens_dos_ciclos(session, ciclos: Iterable[int], nomes_recintos: Optional[Dict[str, str]] = None
                        ) -> Dict[int, dict]:
    """{ciclo: {'principal': pesagem ou None, 'pesagens': [todas, em ordem de data]}} dos ciclos da página."""
    ciclos = sorted({c for c in ciclos if c})
    if not ciclos or not tem_tabela(session):
        return {}
    nomes_recintos = nomes_recintos or {}
    resultado: Dict[int, dict] = {}
    for linha in session.connection().execute(
            select(PESAGENS).where(PESAGENS.c.ciclo_de_vida_id.in_(ciclos))
            .order_by(PESAGENS.c.ciclo_de_vida_id, PESAGENS.c.data, PESAGENS.c.pesagem_id)):
        do_ciclo = resultado.setdefault(linha.ciclo_de_vida_id, {'principal': None, 'pesagens': []})
        pesagem = _linha(linha, nomes_recintos)
        do_ciclo['pesagens'].append(pesagem)
        if pesagem['principal']:
            do_ciclo['principal'] = pesagem
    return resultado


def acrescenta_aos_cards(session, cards: List[dict], nomes_recintos: Optional[Dict[str, str]] = None):
    """`card['pesagem']` (None: sem pesagem do contêiner no ciclo, ou sem a tabela)."""
    pesagens = pesagens_dos_ciclos(session, [card.get('ciclo_de_vida_id') for card in cards], nomes_recintos)
    for card in cards:
        card['pesagem'] = pesagens.get(card.get('ciclo_de_vida_id'))
