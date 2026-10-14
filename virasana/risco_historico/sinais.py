# coding: utf-8
"""
Sinais de um CE para o risco histórico — os MESMOS valores que
`dados_risco_perdimentos` tira dos CEs das fichas (importador, embarcador,
rota, baldeação, NCM e país + NCM), para que a população e a pontuação casem com
o histórico. CPF de consignatário não vira sinal (LGPD). O CNPJ citado em ficha
positiva casa com o importador (raiz) do CE.

`sinais_do_periodo` lê os CEs criados num período (sem exportação), com o porto
de carregamento do manifesto e as NCMs, e devolve os sinais de cada um; `conta`
dá quantos CEs têm cada valor — o denominador do peso (ver calculo.py).
"""
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, Optional, Set, Tuple

from sqlalchemy import text

from virasana.analytic_scripts.dados_risco_perdimentos import (
    SQL_MANIFESTOS, _por_lotes, documento, normaliza_embarcador, posicao_ncm,
)

DIMENSOES_CE = ('importador', 'embarcador', 'rota', 'porto_baldeacao', 'pais_ncm', 'ncm_declarado')
CITADO = 'cnpj_citado'
DIMENSOES = DIMENSOES_CE + (CITADO,)
MARGEM_NCM = timedelta(days=2)     # NCMs gravados um pouco antes do CE
ATRASO_NCM = timedelta(days=7)     # e até uma semana depois

Sinais = Dict[str, Set[str]]

SQL_CES_PERIODO = text(
    'SELECT numeroCEmercante, manifestoCE, embarcador, consignatario, portoOrigemCarga, portoDestFinal '
    'FROM conhecimentosresumo WHERE create_date >= :desde AND create_date < :ate '
    "AND (tipoTrafego IS NULL OR tipoTrafego <> '7')")   # 7 = exportação
SQL_NCMS_PERIODO = text('SELECT numeroCEMercante, identificacaoNCM FROM ncmitemresumo '
                        'WHERE create_date >= :desde AND create_date < :ate')


def _texto(valor) -> str:
    return (valor or '').strip().upper()


def sinais_do_ce(ce: dict, carregamento: Optional[str], posicoes: Iterable[str]) -> Sinais:
    """Sinais de um CE (linha do conhecimentosresumo), do porto de carregamento do
    manifesto e das posições NCM (4 dígitos) declaradas."""
    sinais: Sinais = {dimensao: set() for dimensao in DIMENSOES_CE}
    importador = documento(ce.get('consignatario'))
    if importador and not importador.startswith('CPF'):
        sinais['importador'].add(importador)
    embarcador = normaliza_embarcador(ce.get('embarcador'))
    if embarcador:
        sinais['embarcador'].add(embarcador)
    origem, destino = _texto(ce.get('portoOrigemCarga')), _texto(ce.get('portoDestFinal'))
    if origem and destino:
        sinais['rota'].add(f'{origem} > {destino}')
    carregamento = _texto(carregamento)
    if carregamento and carregamento != origem:
        sinais['porto_baldeacao'].add(carregamento)
    for posicao in posicoes:
        sinais['ncm_declarado'].add(posicao)
        if origem:
            sinais['pais_ncm'].add(f'{origem[:2]} {posicao}')
    return sinais


def pontuaveis(sinais: Sinais) -> Sinais:
    """Valores do CE por dimensão do cálculo (o CNPJ citado casa com o importador)."""
    return {**sinais, CITADO: set(sinais.get('importador', ()))}


def _cronometro(tempos: dict, nome: str, inicio: float) -> float:
    agora = time.perf_counter()
    tempos[nome] = agora - inicio
    return agora


def sinais_do_periodo(conexao, desde: datetime, ate: datetime) -> Tuple[Dict[str, Sinais], Dict[str, float]]:
    """{CE: sinais} dos CEs criados em [desde, ate) que não são de exportação, e
    o tempo (s) de cada etapa. As NCMs vêm pela data de gravação delas (índice),
    não por junção com o CE."""
    tempos: Dict[str, float] = {}
    inicio = time.perf_counter()
    ces = [dict(linha._mapping) for linha in conexao.execute(SQL_CES_PERIODO, {'desde': desde, 'ate': ate})]
    inicio = _cronometro(tempos, 'CEs do período', inicio)
    manifestos = {m['numero']: m['portoCarregamento'] for m in _por_lotes(
        conexao, SQL_MANIFESTOS, 'numeros', {c['manifestoCE'] for c in ces if c['manifestoCE']})}
    inicio = _cronometro(tempos, 'portos de carregamento (manifestos)', inicio)
    numeros = {c['numeroCEmercante'] for c in ces}
    posicoes: Dict[str, Set[str]] = defaultdict(set)
    for numero, ncm in conexao.execute(SQL_NCMS_PERIODO, {'desde': desde - MARGEM_NCM, 'ate': ate + ATRASO_NCM}):
        numero, posicao = (numero or '').strip(), posicao_ncm(ncm)
        if posicao and numero in numeros:
            posicoes[numero].add(posicao)
    inicio = _cronometro(tempos, 'NCMs', inicio)
    resultado = {c['numeroCEmercante']: sinais_do_ce(c, manifestos.get(c['manifestoCE']),
                                                     posicoes.get(c['numeroCEmercante'], ()))
                 for c in ces}
    _cronometro(tempos, 'sinais (Python)', inicio)
    return resultado, tempos


def conta(sinais_por_ce: Dict[str, Sinais]) -> Dict[str, Counter]:
    """Quantos CEs têm cada valor, por dimensão (inclui a do CNPJ citado)."""
    contagens: Dict[str, Counter] = {dimensao: Counter() for dimensao in DIMENSOES}
    for sinais in sinais_por_ce.values():
        for dimensao, valores in pontuaveis(sinais).items():
            contagens[dimensao].update(valores)
    return contagens
