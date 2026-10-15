# coding: utf-8
"""
Imagem de escaneamento (Mongo, GridFS `fs.files`) de cada inspeção ligada ao Ciclo de Vida v2.

Regra do usuário (01/10/2026): do Mongo só valem `metadata.numeroinformado` (contêiner) e
`metadata.dataescaneamento`. A diferença entre a hora da imagem e a da inspeção na API é FIXA POR
RECINTO (sombra de 01–02/10/2026: +3 h ou +0 h, resíduo de até 10 min em ~99%). Procura-se a imagem na
hora da inspeção + a defasagem do recinto, numa janela de ±15 min; recinto sem medição tenta +3 h e
+0 h e fica com a imagem mais próxima. Uma consulta por contêiner (índice numeroinformado +
dataescaneamento), não uma por inspeção.

Também a imagem SEM inspeção transmitida (15/10/2026): ver o bloco "imagem SEM inspeção transmitida" abaixo.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt

# medido na sombra (02/10/2026): imagem - inspeção, em horas inteiras
DEFASAGEM_HORAS = {
    '8931304': 3, '8931305': 3, '8931318': 3, '8931339': 3, '8931342': 3, '8931356': 3, '8931359': 3,
    '8933202': 3, '8933206': 3,
    '8931309': 0, '8931364': 0, '8931404': 0, '8933001': 0, '8933204': 0,
}
DEFASAGENS_SEM_MEDICAO = (3, 0)
JANELA_MINUTOS = 15


def defasagens_do(recinto: Optional[str]) -> Tuple[int, ...]:
    """Defasagens (horas) a tentar para um recinto: a medida, ou +3 h e +0 h."""
    return (DEFASAGEM_HORAS[recinto],) if recinto in DEFASAGEM_HORAS else DEFASAGENS_SEM_MEDICAO


def escolhe_imagem(imagens: Iterable[Tuple[datetime, str]], data_inspecao: datetime, recinto: Optional[str],
                   janela_minutos: int = JANELA_MINUTOS) -> Optional[str]:
    """Id da imagem mais próxima de inspeção + defasagem do recinto, dentro da janela (None se não houver)."""
    janela = timedelta(minutes=janela_minutos)
    melhor = None
    for horas in defasagens_do(recinto):
        alvo = data_inspecao + timedelta(hours=horas)
        for data, imagem_id in imagens:
            diferenca = abs(data - alvo)
            if diferenca <= janela and (melhor is None or diferenca < melhor[0]):
                melhor = (diferenca, imagem_id)
    return melhor[1] if melhor else None


def imagens_do_conteiner(db, conteiner: str, inicio: datetime, fim: datetime) -> List[Tuple[datetime, str]]:
    """[(dataescaneamento, id)] das imagens do contêiner no intervalo."""
    documentos = db['fs.files'].find(
        {'metadata.numeroinformado': conteiner, 'metadata.dataescaneamento': {'$gte': inicio, '$lte': fim}},
        {'metadata.dataescaneamento': 1})
    return [(d['metadata']['dataescaneamento'].replace(tzinfo=None), str(d['_id'])) for d in documentos]


def imagens_das_inspecoes(db, conteiner: str, inspecoes: List[ldt.Evento]) -> Dict[int, str]:
    """{id da inspeção: id da imagem} das inspeções de um contêiner (uma consulta ao Mongo)."""
    if db is None or not inspecoes:
        return {}
    folga = timedelta(hours=max(DEFASAGEM_HORAS.values()), minutes=JANELA_MINUTOS)
    imagens = imagens_do_conteiner(db, conteiner, min(e.data for e in inspecoes) - folga,
                                   max(e.data for e in inspecoes) + folga)
    escolhidas = {e.id: escolhe_imagem(imagens, e.data, e.recinto) for e in inspecoes}
    return {id_: imagem for id_, imagem in escolhidas.items() if imagem}


# ---------------------------------------------------------------------- imagem SEM inspeção transmitida
# Decisão do usuário (15/10/2026, `diagnostico_final_curta.py`): o escâner do Ecoporto escaneia o contêiner de
# passagem (e às vezes na chegada para armazenar) e ninguém transmite a inspeção à API — mas a imagem está no Mongo,
# no horário da passagem (99% dos casos, 4 min depois da entrada). Onde o contêiner esteve fora do operador do
# desembarque SEM inspeção ligada, a imagem do contêiner nesse horário vira um escaneamento do ciclo (confiança
# média). Não vale para a estada no operador do desembarque.

SEM_INSPECAO = 'imagem do escâner sem inspeção transmitida'
FOLGA_PASSAGEM_MINUTOS = 15      # passagem: da entrada − 15 min à saída + 15 min
ANTES_DA_CHEGADA_HORAS = 2       # chegada ao seco: do gate (2 h antes da entrada) ...
CHEGADA_HORAS = 24               # ... até 24 h depois da entrada (ou a saída, se antes)


@dataclass(frozen=True)
class Janela:
    recinto: str
    inicio: datetime
    fim: datetime
    passagem: bool               # passagem (escaneamento fora, antes do destino) x chegada ao recinto seco


def janelas_sem_inspecao(ciclo: ldt.CicloCalculado) -> List[Janela]:
    """Passagens e chegada ao seco do ciclo sem nenhuma inspeção ligada (de confiança alta ou média) no horário."""
    ligadas = [lig.evento.data for lig in ciclo.escaneamentos if lig.confianca != ldt.BAIXA]
    folga = timedelta(minutes=FOLGA_PASSAGEM_MINUTOS)
    janelas = [Janela(recinto, entrada - folga, saida + folga, True) for recinto, entrada, saida in ciclo.passagens]
    janelas = [j for j in janelas if not any(j.inicio <= data <= j.fim for data in ligadas)]
    if ciclo.entrada_seco:
        entrada = ciclo.entrada_seco.evento.data
        saida = ciclo.saida_seco.evento.data if ciclo.saida_seco else None
        inicio = entrada - timedelta(hours=ANTES_DA_CHEGADA_HORAS)
        fim = entrada + timedelta(hours=CHEGADA_HORAS)
        if not any(data >= inicio and (saida is None or data <= saida) for data in ligadas):
            janelas.append(Janela(ciclo.entrada_seco.evento.recinto, inicio, min(fim, saida + folga) if saida else fim,
                                  False))
    return janelas


def id_da_imagem(imagem_id: str) -> int:
    """Id NEGATIVO do escaneamento só com imagem (sem inspeção na API), derivado do id da imagem no Mongo: estável
    entre os reprocessamentos e nunca igual ao id de uma inspeção (positivo). Na tabela, `escaneamento_id < 0`."""
    return -int(imagem_id[-15:], 16)


def imagens_sem_inspecao(db, conteiner: str, janelas: List[Janela]) -> List[Tuple[Janela, datetime, str]]:
    """[(janela, horário do escaneamento — imagem menos a defasagem do recinto —, id da imagem)]: a primeira imagem
    de cada janela (uma consulta ao Mongo por contêiner)."""
    if db is None or not janelas:
        return []
    imagens = sorted(imagens_do_conteiner(db, conteiner, min(j.inicio for j in janelas),
                                          max(j.fim for j in janelas) + timedelta(hours=max(DEFASAGEM_HORAS.values()))))
    achadas, usadas = [], set()
    for janela in janelas:
        for data, imagem_id in imagens:
            horario = next((data - timedelta(hours=h) for h in defasagens_do(janela.recinto)
                            if janela.inicio <= data - timedelta(hours=h) <= janela.fim), None)
            if horario is not None and imagem_id not in usadas:
                achadas.append((janela, horario, imagem_id))
                usadas.add(imagem_id)
                break
    return achadas
