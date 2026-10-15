# coding: utf-8
"""
Cálculo do risco histórico por CE — explicável por construção (ver CONTEXTO.md,
"Modelo"). Para cada dimensão e valor:

  k = soma dos pesos das fichas positivas com o valor; K = soma de todas
  b = n / N                    fatia dos CEs da população com o valor (n >= 1)
  r = (k + m*b) / (K + m)      fatia das fichas positivas com o valor, suavizada:
                               com poucas fichas, puxa para a média
  vezes a média L = r / b
  peso = ln(L), limitado ao teto da dimensão; 0 se L <= 1 ou se o valor não
         cumpre o mínimo de fichas (e, no contexto, de importadores diferentes)

Pontos do CE = soma, por GRUPO, do maior peso entre os valores do CE — o total é
sempre a soma das parcelas mostradas na memória de cálculo. NCM e país + NCM
são a mesma informação (grupo "mercadoria"); rota e baldeação, quase (grupo
"logística"): cada grupo conta uma vez só.

Regras aprovadas pelo usuário depois do F0 (14/10/2026):
  - identidade (importador, embarcador, CNPJ citado): basta 1 ficha; teto 5 —
    o CNPJ citado, teto 8 (deve ser dos que mais chamam atenção);
  - contexto (rota, baldeação, NCM, país + NCM): 3 fichas de 2+ importadores
    diferentes (padrão de mercado, não de uma empresa só); teto 3.
Constantes calibradas na validação no tempo (F2).
"""
import math
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from typing import Dict, Iterable, List, Set, Tuple

from virasana.risco_historico.sinais import CITADO, DIMENSOES, DIMENSOES_CE, LIMITE_VALOR

M_PADRAO = 10.0
# Faixas por percentil dos pontos na população (com pontos > 0): Alto = 1% maiores; Médio = até 5%.
PERCENTIL_ALTO, PERCENTIL_MEDIO = 0.99, 0.95
FAIXAS = ('alto', 'medio', 'baixo', 'sem_sinal')
ROTULOS_FAIXA = {'alto': 'Alto', 'medio': 'Médio', 'baixo': 'Baixo', 'sem_sinal': 'Sem sinal'}
IDENTIDADE = ('importador', 'embarcador', CITADO)
CONTEXTO = ('rota', 'porto_baldeacao', 'pais_ncm', 'ncm_declarado')
GRUPOS = {'importador': ('importador',), 'embarcador': ('embarcador',), CITADO: (CITADO,),
          'mercadoria': ('ncm_declarado', 'pais_ncm'), 'logistica': ('rota', 'porto_baldeacao')}
GRUPO_DA_DIMENSAO = {dimensao: grupo for grupo, dimensoes in GRUPOS.items() for dimensao in dimensoes}
ROTULOS = {'importador': 'Importador', 'embarcador': 'Embarcador', CITADO: 'CNPJ citado em ficha positiva',
           'ncm_declarado': 'NCM', 'pais_ncm': 'País + NCM', 'rota': 'Rota', 'porto_baldeacao': 'Baldeação',
           'mercadoria': 'Mercadoria', 'logistica': 'Logística'}


@dataclass(frozen=True)
class Regra:
    minimo_fichas: int
    minimo_importadores: int
    teto: float


REGRAS = {'importador': Regra(1, 1, 5.0), 'embarcador': Regra(1, 1, 5.0), CITADO: Regra(1, 1, 8.0),
          **{dimensao: Regra(3, 2, 3.0) for dimensao in CONTEXTO}}


def constantes() -> dict:
    """As constantes em uso, para guardar com cada versão dos parâmetros."""
    return {'m': M_PADRAO, 'regras': {d: asdict(r) for d, r in REGRAS.items()}, 'grupos': GRUPOS}


@dataclass
class Historico:
    """O que as fichas positivas ensinam: K e, por dimensão e valor, k, as fichas e os importadores."""
    total: float = 0.0
    k: Dict[str, Dict[str, float]] = field(default_factory=lambda: {d: defaultdict(float) for d in DIMENSOES})
    fichas: Dict[str, Dict[str, List[int]]] = field(default_factory=lambda: {d: defaultdict(list) for d in DIMENSOES})
    importadores: Dict[str, Dict[str, Set[str]]] = field(
        default_factory=lambda: {d: defaultdict(set) for d in DIMENSOES})


@dataclass(frozen=True)
class Peso:
    """Memória de cálculo de um valor; `motivo` diz por que o peso é zero."""
    fichas: int
    importadores: int
    k: float
    total_k: float
    n: int
    total_n: int
    vezes: float
    peso: float
    motivo: str = ''


def valores_da_ficha(sinais: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    """Sinais de uma ficha (de `dados_risco_perdimentos.extrai`) nas dimensões do
    cálculo: sem CPF; o CNPJ citado vira raiz e não repete o importador da própria
    ficha (o CNPJ do fiscalizado digitado nas observações não conta duas vezes)."""
    importadores = {v for v in sinais.get('importador', ()) if not v.startswith('CPF')}
    valores = {dimensao: {v[:LIMITE_VALOR] for v in sinais.get(dimensao, ())} for dimensao in DIMENSOES_CE}
    valores['importador'] = importadores
    valores[CITADO] = {cnpj[:8] for cnpj in sinais.get(CITADO, ())} - importadores
    return valores


def historico(fichas: Iterable[dict]) -> Historico:
    """Fichas já com `sinais` (extrai) e `peso` (fichas_positivas). Ficha sem
    importador conhecido conta como um importador à parte."""
    resultado = Historico()
    for ficha in fichas:
        peso_ficha = float(ficha.get('peso', 1.0))
        resultado.total += peso_ficha
        valores = valores_da_ficha(ficha['sinais'])
        quem = valores['importador'] or {f'ficha {ficha["id"]}'}
        for dimensao, lista in valores.items():
            for valor in lista:
                resultado.k[dimensao][valor] += peso_ficha
                resultado.fichas[dimensao][valor].append(ficha['id'])
                resultado.importadores[dimensao][valor] |= quem
    return resultado


def vezes_a_media(k: float, total_k: float, n: int, total_n: int, m: float = M_PADRAO) -> float:
    fatia_populacao = max(n, 1) / total_n
    fatia_fichas = (k + m * fatia_populacao) / (total_k + m)
    return fatia_fichas / fatia_populacao


def _motivo_zero(regra: Regra, fichas: int, importadores: int, vezes: float) -> str:
    if fichas < regra.minimo_fichas:
        return f'menos de {regra.minimo_fichas} fichas'
    if importadores < regra.minimo_importadores:
        return f'fichas de menos de {regra.minimo_importadores} importadores'
    if vezes <= 1:
        return 'não passa da média'
    return ''


def peso(dimensao: str, fichas: int, k: float, total_k: float, n: int, total_n: int,
         importadores: int = 1, m: float = M_PADRAO) -> Peso:
    if total_k <= 0 or total_n <= 0:
        return Peso(fichas, importadores, k, total_k, n, total_n, 0.0, 0.0, 'sem fichas ou sem população')
    regra = REGRAS[dimensao]
    vezes = vezes_a_media(k, total_k, n, total_n, m)
    motivo = _motivo_zero(regra, fichas, importadores, vezes)
    # 3 casas: a precisão gravada (risco_historico_parametro.peso) — limiares e pontuação usam o mesmo número
    valor = 0.0 if motivo else round(min(regra.teto, math.log(vezes)), 3)
    return Peso(fichas, importadores, k, total_k, n, total_n, vezes, valor, motivo)


def tabela_de_pesos(hist: Historico, contagens: Dict[str, Dict[str, int]], total_n: int,
                    m: float = M_PADRAO) -> Dict[str, Dict[str, Peso]]:
    """Peso de cada valor do histórico, com a população `contagens` (sinais.conta)."""
    return {dimensao: {valor: peso(dimensao, len(hist.fichas[dimensao][valor]), k, hist.total,
                                   contagens.get(dimensao, {}).get(valor, 0), total_n,
                                   len(hist.importadores[dimensao][valor]), m)
                       for valor, k in hist.k[dimensao].items()}
            for dimensao in DIMENSOES}


def pontua(valores: Dict[str, Iterable[str]],
           pesos: Dict[str, Dict[str, Peso]]) -> Tuple[float, List[Tuple[str, str, Peso]]]:
    """(pontos, parcelas): uma parcela por grupo — o valor de maior peso entre as
    dimensões do grupo — como (dimensão, valor, Peso)."""
    parcelas = []
    for dimensoes in GRUPOS.values():
        candidatos = [(pesos[d][v].peso, d, v) for d in dimensoes for v in valores.get(d, ())
                      if v in pesos.get(d, {}) and pesos[d][v].peso > 0]
        if candidatos:
            _, dimensao, valor = max(candidatos)
            parcelas.append((dimensao, valor, pesos[dimensao][valor]))
    return round(sum(p.peso for _, _, p in parcelas), 4), parcelas


def limiares(pontos: Iterable[float]) -> Dict[str, float]:
    """Pontos mínimos de Alto e Médio: percentis dos pontos de TODOS os CEs da população."""
    ordenados = sorted(pontos)
    if not ordenados:
        return {'alto': 0.0, 'medio': 0.0}
    ultimo = len(ordenados) - 1
    return {'alto': round(ordenados[int(PERCENTIL_ALTO * ultimo)], 4),
            'medio': round(ordenados[int(PERCENTIL_MEDIO * ultimo)], 4)}


def faixa(pontos: float, limites: Dict[str, float]) -> str:
    """Faixa de um CE: sem pontos é "sem_sinal"; com pontos, alto/médio pelos limiares da versão, senão baixo."""
    if pontos <= 0:
        return 'sem_sinal'
    if pontos >= limites.get('alto', 0):
        return 'alto'
    if pontos >= limites.get('medio', 0):
        return 'medio'
    return 'baixo'
