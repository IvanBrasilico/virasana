# coding: utf-8
"""
Imagem de escaneamento (Mongo, GridFS `fs.files`) de cada inspeção ligada ao Ciclo de Vida v2.

Regra do usuário (01/10/2026): do Mongo só valem `metadata.numeroinformado` (contêiner) e
`metadata.dataescaneamento`. A diferença entre a hora da imagem e a da inspeção na API é FIXA POR
RECINTO (sombra de 01–02/10/2026: +3 h ou +0 h, resíduo de até 10 min em ~99%). Procura-se a imagem na
hora da inspeção + a defasagem do recinto, numa janela de ±15 min; recinto sem medição tenta +3 h e
+0 h e fica com a imagem mais próxima. Uma consulta por contêiner (índice numeroinformado +
dataescaneamento), não uma por inspeção.
"""
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
