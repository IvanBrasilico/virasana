# coding: utf-8
"""
Contêiner do ACESSO pela placa — regra PURA, sem banco (D3, 20/10/2026; CICLO_DE_VIDA.md, "Fora de Santos e
pesagens").

No Rio ICTSI (7921302) o acesso efetivado NUNCA traz o contêiner (100% dos 11.935 acessos em 14 dias — D1): a saída do
operador não se liga ao ciclo. A pesagem e a inspeção trazem o contêiner E a placa, e a pesagem é evento de gate (P0:
~95% a até 2 min de um acesso da mesma placa no mesmo recinto).

Regra (a medir com `diagnostico_acesso_pela_placa.py` antes de usar no ciclo): o acesso recebe o contêiner da pesagem
e/ou da inspeção com contêiner da MESMA placa, no MESMO recinto, a até `janela` minutos. Dois contêineres diferentes na
janela = ambíguo (dois de 20' no mesmo caminhão, ou outra operação): nenhum é dado. Contêiner + data sozinhos nunca
bastam — aqui a mesma operação vem da placa, do recinto e dos minutos.
"""
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

JANELA_MINUTOS = 5
PESAGEM, INSPECAO = 'pesagem', 'inspeção'

PELA_PESAGEM = 'pela pesagem'
PELA_INSPECAO = 'pela inspeção'
PELAS_DUAS = 'pela pesagem e pela inspeção'
AMBIGUO = 'dois contêineres na janela'
NENHUM = 'nenhuma pesagem/inspeção com contêiner na janela'
SEM_PLACA = 'acesso sem placa'


@dataclass(frozen=True)
class Marca:
    """Pesagem ou inspeção com contêiner e placa: diz qual contêiner estava no caminhão naquela hora."""
    data: datetime
    placa: str
    conteiner: str
    fonte: str          # PESAGEM ou INSPECAO


def normaliza_placa(placa) -> str:
    """Só letras e dígitos, em maiúsculas ('abc-1d23' -> 'ABC1D23')."""
    return ''.join(c for c in str(placa or '').upper() if c.isalnum())


def indexa(marcas: Iterable[Marca]) -> Dict[str, List[Marca]]:
    """{placa normalizada: marcas} — só as com placa e contêiner."""
    por_placa = defaultdict(list)
    for marca in marcas:
        placa = normaliza_placa(marca.placa)
        if placa and marca.conteiner:
            por_placa[placa].append(marca)
    return por_placa


def conteiner_do_acesso(data: datetime, placa, por_placa: Dict[str, List[Marca]],
                        janela_minutos: float = JANELA_MINUTOS) -> Tuple[Optional[str], str, Optional[float]]:
    """(contêiner ou None, motivo, minutos até a marca mais próxima) de um acesso da `placa` em `data`, pelas marcas
    do MESMO recinto (`por_placa`, de `indexa`)."""
    placa = normaliza_placa(placa)
    if not placa:
        return None, SEM_PLACA, None
    janela = timedelta(minutes=janela_minutos)
    perto = [m for m in por_placa.get(placa, ()) if abs(m.data - data) <= janela]
    if not perto:
        return None, NENHUM, None
    minutos = min(abs((m.data - data).total_seconds()) for m in perto) / 60
    conteineres = {m.conteiner for m in perto}
    if len(conteineres) > 1:
        return None, AMBIGUO, minutos
    fontes = {m.fonte for m in perto}
    motivo = PELAS_DUAS if len(fontes) > 1 else PELA_PESAGEM if PESAGEM in fontes else PELA_INSPECAO
    return conteineres.pop(), motivo, minutos
