# coding: utf-8
"""
Sinais de um CE para o risco histórico — os MESMOS valores que
`dados_risco_perdimentos` tira dos CEs das fichas (importador, embarcador,
rota, baldeação, NCM e país + NCM), para que a população e a pontuação casem com
o histórico. CPF de consignatário não vira sinal (LGPD). O CNPJ citado em ficha
positiva casa com o importador (raiz) do CE.

  - `sinais_dos_ces`: sinais de CEs já lidos do conhecimentosresumo, com o porto
    de carregamento do manifesto e as NCMs (por CE, índice), e o maior ID de NCM
    usado (para saber se chegou NCM nova depois) — usado pela extração (lote 2);
  - `sinais_do_periodo`: os CEs alterados num período (diagnóstico F0);
  - `conta`: quantos CEs têm cada valor — o denominador do peso (calculo.py);
  - `para_json`/`de_json`: como os sinais ficam gravados em risco_historico_ce.

Atenção: `conhecimentosresumo.create_date` é a data da ÚLTIMA ALTERAÇÃO do CE
(ON UPDATE current_timestamp no banco), não a de criação.
"""
import json
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy import bindparam, text

from virasana.analytic_scripts.dados_risco_perdimentos import (
    SQL_MANIFESTOS, _por_lotes, documento, normaliza_embarcador, posicao_ncm,
)

DIMENSOES_CE = ('importador', 'embarcador', 'rota', 'porto_baldeacao', 'pais_ncm', 'ncm_declarado')
CITADO = 'cnpj_citado'
DIMENSOES = DIMENSOES_CE + (CITADO,)
LIMITE_VALOR = 255                 # tamanho de risco_historico_parametro.valor (o embarcador pode ser longo)
MARGEM_NCM = timedelta(days=2)     # NCMs gravados um pouco antes do CE
ATRASO_NCM = timedelta(days=7)     # e até uma semana depois

Sinais = Dict[str, Set[str]]

SQL_CES_PERIODO = text(
    'SELECT numeroCEmercante, manifestoCE, embarcador, consignatario, portoOrigemCarga, portoDestFinal '
    'FROM conhecimentosresumo WHERE create_date >= :desde AND create_date < :ate '
    "AND (tipoTrafego IS NULL OR tipoTrafego <> '7')")   # 7 = exportação
SQL_NCMS_PERIODO = text('SELECT numeroCEMercante, identificacaoNCM FROM ncmitemresumo '
                        'WHERE create_date >= :desde AND create_date < :ate')
SQL_NCMS_DOS_CES = text('SELECT ID, numeroCEMercante, identificacaoNCM FROM ncmitemresumo '
                        'WHERE numeroCEMercante IN :ces').bindparams(bindparam('ces', expanding=True))


def raiz_valida(raiz: str) -> bool:
    """Raiz de CNPJ (8 dígitos) que identifica um importador. Fora: CPF (LGPD) e raízes com todos os dígitos
    iguais — `00000000` é o Banco do Brasil como consignatário "à ordem", `99999999` é preenchimento inválido
    (decisão do usuário, 15/10/2026: tinham peso máximo sem dizer quem importa)."""
    return bool(re.fullmatch(r'\d{8}', raiz or '')) and len(set(raiz)) > 1


def _texto(valor) -> str:
    return (valor or '').strip().upper()


def sinais_do_ce(ce: dict, carregamento: Optional[str], posicoes: Iterable[str]) -> Sinais:
    """Sinais de um CE (linha do conhecimentosresumo), do porto de carregamento do
    manifesto e das posições NCM (4 dígitos) declaradas."""
    sinais: Sinais = {dimensao: set() for dimensao in DIMENSOES_CE}
    importador = documento(ce.get('consignatario'))
    if raiz_valida(importador):
        sinais['importador'].add(importador)
    embarcador = normaliza_embarcador(ce.get('embarcador'))
    if embarcador:
        sinais['embarcador'].add(embarcador[:LIMITE_VALOR])
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
    """Valores do CE por dimensão do cálculo (o CNPJ citado casa com o importador). Refiltra o importador:
    vale também para sinais gravados antes de uma regra nova (`raiz_valida`)."""
    importadores = {v for v in sinais.get('importador', ()) if raiz_valida(v)}
    return {**sinais, 'importador': importadores, CITADO: importadores}


def para_json(sinais: Sinais) -> str:
    return json.dumps({d: sorted(v) for d, v in sinais.items() if v and d in DIMENSOES_CE}, sort_keys=True)


def de_json(texto: Optional[str]) -> Sinais:
    dados = json.loads(texto or '{}')
    return {dimensao: set(dados.get(dimensao, ())) for dimensao in DIMENSOES_CE}


def sinais_dos_ces(conexao, ces: List[dict]) -> Dict[str, Tuple[Sinais, int]]:
    """{CE: (sinais, maior ID de NCM usado — 0 se não há NCM)} para linhas do
    conhecimentosresumo; manifestos e NCMs em lotes, pelos índices."""
    manifestos = {m['numero']: m['portoCarregamento'] for m in _por_lotes(
        conexao, SQL_MANIFESTOS, 'numeros', {c['manifestoCE'] for c in ces if c.get('manifestoCE')})}
    posicoes: Dict[str, Set[str]] = defaultdict(set)
    ultimo_ncm: Dict[str, int] = defaultdict(int)
    for ncm in _por_lotes(conexao, SQL_NCMS_DOS_CES, 'ces', {c['numeroCEmercante'] for c in ces}):
        numero, posicao = (ncm['numeroCEMercante'] or '').strip(), posicao_ncm(ncm['identificacaoNCM'])
        ultimo_ncm[numero] = max(ultimo_ncm[numero], int(ncm['ID']))
        if posicao:
            posicoes[numero].add(posicao)
    return {c['numeroCEmercante']: (sinais_do_ce(c, manifestos.get(c.get('manifestoCE')),
                                                 posicoes.get(c['numeroCEmercante'], ())),
                                    ultimo_ncm.get(c['numeroCEmercante'], 0))
            for c in ces}


def _cronometro(tempos: dict, nome: str, inicio: float) -> float:
    agora = time.perf_counter()
    tempos[nome] = agora - inicio
    return agora


def sinais_do_periodo(conexao, desde: datetime, ate: datetime) -> Tuple[Dict[str, Sinais], Dict[str, float]]:
    """{CE: sinais} dos CEs alterados em [desde, ate) que não são de exportação, e
    o tempo (s) de cada etapa. As NCMs vêm pela data de gravação delas (índice),
    não por junção com o CE. Usado pelo diagnóstico F0."""
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


def conta(sinais_por_ce: Iterable[Sinais]) -> Dict[str, Counter]:
    """Quantos CEs têm cada valor, por dimensão (inclui a do CNPJ citado)."""
    contagens: Dict[str, Counter] = {dimensao: Counter() for dimensao in DIMENSOES}
    for sinais in sinais_por_ce:
        for dimensao, valores in pontuaveis(sinais).items():
            contagens[dimensao].update(valores)
    return contagens
