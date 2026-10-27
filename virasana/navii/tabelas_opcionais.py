# coding: utf-8
"""
Tabelas que nem todo banco tem (homologação, testes, bancos de desenvolvimento):
risco histórico por CE, DTAs, matriz CORAD. O NAVI-I confere se existem antes de
usá-las — sem elas, a tela segue sem aquela informação. A conferência fica em
cache por alguns minutos.
"""
import time
from typing import Dict, Tuple

from sqlalchemy import inspect

VALIDADE_SEGUNDOS = 600
_cache: Dict[Tuple, Tuple[bool, float]] = {}


def existe(session, nome: str) -> bool:
    bind = session.get_bind()
    chave = (str(bind.url), id(bind), nome)   # bancos SQLite em memória têm a mesma URL
    existe_, quando = _cache.get(chave, (None, 0.0))
    if existe_ is None or time.monotonic() - quando > VALIDADE_SEGUNDOS:
        existe_ = inspect(bind).has_table(nome)
        _cache[chave] = (existe_, time.monotonic())
    return existe_


def limpa_cache():
    _cache.clear()
