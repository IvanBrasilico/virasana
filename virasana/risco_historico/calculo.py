# coding: utf-8
"""
Cálculo do risco histórico por CE — explicável por construção (ver CONTEXTO.md,
"Modelo"). Para cada dimensão e valor:

  k = soma dos pesos das fichas positivas com o valor; K = soma de todas
  b = n / N                    fatia dos CEs da população com o valor (n >= 1)
  r = (k + m*b) / (K + m)      fatia das fichas positivas com o valor, suavizada:
                               com poucas fichas, puxa para a média
  vezes a média L = r / b
  peso = ln(L), limitado ao teto; 0 se L <= 1 ou se o valor está em menos
         fichas que o mínimo da dimensão

Pontos do CE = soma, por dimensão, do maior peso entre os valores do CE — o
total é sempre a soma das parcelas mostradas na memória de cálculo.

Constantes PROVISÓRIAS (prévia do diagnóstico F0); a validação no tempo (F2)
calibra. O CNPJ citado em ficha positiva vale com uma ficha só e tem teto maior
(decisão do usuário, 14/10/2026: deve ser dos que mais chamam atenção).
"""
import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Set, Tuple

from virasana.risco_historico.sinais import CITADO, DIMENSOES, DIMENSOES_CE

M_PADRAO = 10.0
MINIMO_PADRAO = 2
TETO_PADRAO = 5.0
MINIMO = {CITADO: 1}
TETO = {CITADO: 8.0}


@dataclass
class Historico:
    """O que as fichas positivas ensinam: K e, por dimensão e valor, k e as fichas."""
    total: float = 0.0
    k: Dict[str, Dict[str, float]] = field(default_factory=lambda: {d: defaultdict(float) for d in DIMENSOES})
    fichas: Dict[str, Dict[str, List[int]]] = field(default_factory=lambda: {d: defaultdict(list) for d in DIMENSOES})


@dataclass(frozen=True)
class Peso:
    """Memória de cálculo de um valor."""
    fichas: int
    k: float
    total_k: float
    n: int
    total_n: int
    vezes: float
    peso: float


def valores_da_ficha(sinais: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    """Sinais de uma ficha (de `dados_risco_perdimentos.extrai`) nas dimensões do
    cálculo: sem CPF; o CNPJ citado vira raiz e não repete o importador da própria
    ficha (o CNPJ do fiscalizado digitado nas observações não conta duas vezes)."""
    importadores = {v for v in sinais.get('importador', ()) if not v.startswith('CPF')}
    valores = {dimensao: set(sinais.get(dimensao, ())) for dimensao in DIMENSOES_CE}
    valores['importador'] = importadores
    valores[CITADO] = {cnpj[:8] for cnpj in sinais.get(CITADO, ())} - importadores
    return valores


def historico(fichas: Iterable[dict]) -> Historico:
    """Fichas já com `sinais` (extrai) e `peso` (fichas_positivas)."""
    resultado = Historico()
    for ficha in fichas:
        peso_ficha = float(ficha.get('peso', 1.0))
        resultado.total += peso_ficha
        for dimensao, valores in valores_da_ficha(ficha['sinais']).items():
            for valor in valores:
                resultado.k[dimensao][valor] += peso_ficha
                resultado.fichas[dimensao][valor].append(ficha['id'])
    return resultado


def vezes_a_media(k: float, total_k: float, n: int, total_n: int, m: float = M_PADRAO) -> float:
    fatia_populacao = max(n, 1) / total_n
    fatia_fichas = (k + m * fatia_populacao) / (total_k + m)
    return fatia_fichas / fatia_populacao


def peso(dimensao: str, fichas: int, k: float, total_k: float, n: int, total_n: int,
         m: float = M_PADRAO) -> Peso:
    if total_k <= 0 or total_n <= 0:
        return Peso(fichas, k, total_k, n, total_n, 0.0, 0.0)
    vezes = vezes_a_media(k, total_k, n, total_n, m)
    valor = 0.0
    if fichas >= MINIMO.get(dimensao, MINIMO_PADRAO) and vezes > 1:
        valor = min(TETO.get(dimensao, TETO_PADRAO), math.log(vezes))
    return Peso(fichas, k, total_k, n, total_n, vezes, valor)


def tabela_de_pesos(hist: Historico, contagens: Dict[str, Dict[str, int]], total_n: int,
                    m: float = M_PADRAO) -> Dict[str, Dict[str, Peso]]:
    """Peso de cada valor do histórico, com a população `contagens` (sinais.conta)."""
    return {dimensao: {valor: peso(dimensao, len(hist.fichas[dimensao][valor]), k, hist.total,
                                   contagens.get(dimensao, {}).get(valor, 0), total_n, m)
                       for valor, k in hist.k[dimensao].items()}
            for dimensao in DIMENSOES}


def pontua(valores: Dict[str, Iterable[str]],
           pesos: Dict[str, Dict[str, Peso]]) -> Tuple[float, List[Tuple[str, str, Peso]]]:
    """(pontos, parcelas): uma parcela por dimensão — o valor de maior peso."""
    parcelas = []
    for dimensao in DIMENSOES:
        candidatos = [(pesos[dimensao][v].peso, v) for v in valores.get(dimensao, ())
                      if v in pesos.get(dimensao, {}) and pesos[dimensao][v].peso > 0]
        if candidatos:
            _, valor = max(candidatos)
            parcelas.append((dimensao, valor, pesos[dimensao][valor]))
    return round(sum(p.peso for _, _, p in parcelas), 4), parcelas
