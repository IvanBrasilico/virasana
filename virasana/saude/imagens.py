"""IMAGENS de escaneamento no MongoDB do virasana (GridFS, `fs.files`).

O extrator de imagens da API Recintos grava cada imagem com `metadata.recinto` e `metadata.dataescaneamento` e avança o
ponteiro `tipoEvento = '99'` do controle da extração. Medidas, todas pelo índice de `uploadDate` (gravado em UTC):
- carga parada: a imagem mais nova chegou há mais de IMAGENS_PARADAS_HORAS;
- recinto que parou de mandar imagem: nas últimas 24 h, menos de FRACAO_MINIMA do que ele mandou nas MESMAS 24 h da
  semana anterior, entre os que mandaram ao menos MINIMO_DIARIO;
- volume geral abaixo de QUEDA_GERAL da semana anterior;
- atraso: mediana de (chegada − escaneamento) por recinto acima de ATRASO_HORAS — a imagem chega tarde demais para o
  ciclo de vida, que só a procura ao refazer o contêiner (ver "revisita de imagens" no BACKLOG.md). Alguns recintos
  gravam o escaneamento com +3 h (ver `imagens_ciclo.DEFASAGEM_HORAS`): o limite é largo por isso.
Limiares provisórios: calibrar com as primeiras semanas de relatório.
"""
from collections import Counter, defaultdict
from statistics import median
from typing import Dict, List, Tuple

from virasana.saude.fontes import janelas, ponteiros_da_extracao, posicao_do_extrator
from virasana.saude.verificacao import (
    ATENCAO, FALHA, NAO_MEDIDO, OK, Resultado, como_data, ha_quanto, horas_desde, numero, pior,
)

IMAGENS = 'Imagens (MongoDB)'
IMAGENS_PARADAS_HORAS = 6
MINIMO_DIARIO = 20
FRACAO_MINIMA = 0.1
QUEDA_GERAL = 0.5
ATRASO_HORAS = 48
MINIMO_PARA_ATRASO = 10          # imagens do recinto nas últimas 24 h para medir o atraso
TEMPO_MONGO_MS = 120000          # limite de cada consulta no Mongo
SEM_RECINTO = '(sem recinto)'


def verifica_imagens(contexto) -> Resultado:
    if contexto.mongo is None:
        return Resultado(IMAGENS, NAO_MEDIDO, 'sem conexão com o Mongo do virasana')
    arquivos = contexto.mongo['fs.files']
    agora, ontem, base_inicio, base_fim = janelas(contexto.agora_utc)
    mais_nova = next(iter(arquivos.find({}, {'uploadDate': 1}).sort('uploadDate', -1).limit(1)), None)
    horas = horas_desde(agora, mais_nova and mais_nova.get('uploadDate'))
    recentes, atrasos = _ultimas_24h(arquivos, ontem, agora)
    base = _por_recinto(arquivos, base_inicio, base_fim)
    total, total_base = sum(recentes.values()), sum(base.values())
    estados, detalhes = [OK], []
    if horas is None or horas > IMAGENS_PARADAS_HORAS:
        estados.append(FALHA)
        detalhes.append(f'a imagem mais nova chegou {ha_quanto(horas)} — carga das imagens parada?')
    elif total_base >= MINIMO_DIARIO and total < QUEDA_GERAL * total_base:
        estados.append(ATENCAO)
        detalhes.append(f'{numero(total)} imagens nas últimas 24 h, {numero(total_base)} nas mesmas 24 h da semana '
                        'anterior — queda geral')
    avisos = _recintos_parados(recentes, base) + _recintos_atrasados(atrasos)
    if avisos:
        estados.append(ATENCAO)
    with contexto.engine.connect() as conexao:
        ponteiro = posicao_do_extrator(ponteiros_da_extracao(conexao), '99')
    # sem ponteiro '99' no controle (a 1ª rodada em produção, 23/10/2026, não achou nenhum), o resumo não o cita: a
    # medida da carga é o volume por recinto
    resumo = (f'mais nova {ha_quanto(horas)}; {numero(total)} nas últimas 24 h (sem. ant. {numero(total_base)})'
              + (f'; ponteiro do extrator {ha_quanto(horas_desde(contexto.agora, ponteiro))}' if ponteiro else ''))
    return Resultado(IMAGENS, pior(estados), resumo, detalhes + avisos)


def _recintos_parados(recentes: Counter, base: Dict[str, int]) -> List[str]:
    return [f'recinto {recinto}: {numero(recentes.get(recinto, 0))} imagens nas últimas 24 h, {numero(n)} nas mesmas '
            '24 h da semana anterior' for recinto, n in sorted(base.items(), key=lambda par: -par[1])
            if n >= MINIMO_DIARIO and recentes.get(recinto, 0) < FRACAO_MINIMA * n]


def _recintos_atrasados(atrasos: Dict[str, List[float]]) -> List[str]:
    avisos = []
    for recinto, horas in sorted(atrasos.items()):
        if len(horas) >= MINIMO_PARA_ATRASO and median(horas) > ATRASO_HORAS:
            avisos.append(f'recinto {recinto}: imagens chegando {median(horas):.0f} h depois do escaneamento '
                          f'(mediana de {len(horas)} imagens)')
    return avisos


def _ultimas_24h(arquivos, inicio, fim) -> Tuple[Counter, Dict[str, List[float]]]:
    """(imagens por recinto, atrasos em horas por recinto) das que chegaram em [inicio, fim)."""
    por_recinto, atrasos = Counter(), defaultdict(list)
    cursor = arquivos.find({'uploadDate': {'$gte': inicio, '$lt': fim}},
                           {'uploadDate': 1, 'metadata.recinto': 1, 'metadata.dataescaneamento': 1})
    for documento in cursor.max_time_ms(TEMPO_MONGO_MS):
        metadata = documento.get('metadata') or {}
        recinto = str(metadata.get('recinto') or SEM_RECINTO)
        por_recinto[recinto] += 1
        escaneamento = como_data(metadata.get('dataescaneamento'))
        if escaneamento:
            atrasos[recinto].append((documento['uploadDate'] - escaneamento).total_seconds() / 3600)
    return por_recinto, atrasos


def _por_recinto(arquivos, inicio, fim) -> Dict[str, int]:
    return {str(linha['_id'] or SEM_RECINTO): linha['n'] for linha in arquivos.aggregate(
        [{'$match': {'uploadDate': {'$gte': inicio, '$lt': fim}}},
         {'$group': {'_id': '$metadata.recinto', 'n': {'$sum': 1}}}], maxTimeMS=TEMPO_MONGO_MS)}
