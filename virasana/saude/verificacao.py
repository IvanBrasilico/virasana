"""Peças comuns do health check: o resultado de cada verificação, o contexto (bancos e "agora") e o relatório.

Cada verificação é uma função `verifica_x(contexto) -> Resultado`; `executa` roda uma delas e transforma qualquer
erro em FALHA dela — uma verificação quebrada (banco fora, tabela ausente) não derruba as outras.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List, Optional

OK, ATENCAO, FALHA, NAO_MEDIDO = 'OK', 'ATENÇÃO', 'FALHA', 'NÃO MEDIDO'
GRAVIDADE = {OK: 0, NAO_MEDIDO: 0, ATENCAO: 1, FALHA: 2}
MAX_DETALHES = 15                # linhas de detalhe por verificação no relatório (o resto vira "+N")


@dataclass
class Resultado:
    componente: str
    estado: str
    resumo: str
    detalhes: List[str] = field(default_factory=list)


@dataclass
class Contexto:
    """O que as verificações leem. `mongo`/`meili` None: as verificações que dependem deles saem NÃO MEDIDO."""
    engine: object                          # SQLAlchemy: Mercante, API Recintos, ciclo de vida, risco e Fichas
    mongo: object = None                     # banco do Mongo do virasana (imagens, coleta de uso do NAVI-I)
    meili: object = None                     # cliente do Meilisearch com a chave do indexador
    agora: Optional[datetime] = None         # hora local (o MariaDB e a coleta de uso gravam a hora local)
    agora_utc: Optional[datetime] = None     # o GridFS grava uploadDate em UTC
    nucleo_desde: Optional[datetime] = None  # última troca do núcleo do ciclo de vida (ciclos anteriores: a refazer)

    def __post_init__(self):
        self.agora = self.agora or datetime.now()
        self.agora_utc = self.agora_utc or datetime.utcnow()


def pior(estados) -> str:
    return max(estados, key=lambda estado: GRAVIDADE[estado], default=OK)


def executa(componente: str, verificacao: Callable[[Contexto], Resultado], contexto: Contexto) -> Resultado:
    try:
        return verificacao(contexto)
    except Exception as erro:  # noqa: o relatório segue com as outras verificações
        return Resultado(componente, FALHA, f'erro na verificação: {type(erro).__name__}: {str(erro)[:300]}')


def como_data(valor) -> Optional[datetime]:
    """datetime do banco (o SQLite dos testes devolve texto)."""
    if valor is None or isinstance(valor, datetime):
        return valor
    return datetime.fromisoformat(str(valor)[:19])


def horas_desde(agora: datetime, momento) -> Optional[float]:
    momento = como_data(momento)
    return None if momento is None else (agora - momento).total_seconds() / 3600


def ha_quanto(horas: Optional[float]) -> str:
    if horas is None:
        return 'sem registro'
    if horas < 48:
        return f'há {horas:.1f} h'.replace('.', ',')
    return f'há {horas / 24:.1f} dias'.replace('.', ',')


def numero(valor) -> str:
    return f'{valor:,}'.replace(',', '.')


def relatorio(resultados: List[Resultado], agora: datetime) -> List[str]:
    """Primeiro uma linha por componente (o pior estado no topo do resumo), depois os detalhes de cada um."""
    geral = pior(r.estado for r in resultados)
    linhas = [f'HEALTH CHECK do AJNA — {agora:%d/%m/%Y %H:%M} — {geral}', '']
    largura = max((len(r.componente) for r in resultados), default=0)
    linhas += [f'[{r.estado:^10}] {r.componente:<{largura}}  {r.resumo}' for r in resultados]
    for r in resultados:
        if r.detalhes:
            linhas += ['', f'--- {r.componente} ({r.estado})']
            linhas += [f'  - {d}' for d in r.detalhes[:MAX_DETALHES]]
            if len(r.detalhes) > MAX_DETALHES:
                linhas.append(f'  (+{len(r.detalhes) - MAX_DETALHES})')
    return linhas


def codigo_de_saida(resultados: List[Resultado]) -> int:
    """0 = tudo OK (ou não medido), 1 = alguma ATENÇÃO, 2 = alguma FALHA — o cron decide quem avisar."""
    return GRAVIDADE[pior(r.estado for r in resultados)]
