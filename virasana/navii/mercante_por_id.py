# coding: utf-8
"""
Janela por ID no Mercante e limite de tempo por consulta — para quem lê o `conhecimentosresumo` (~28 M linhas).

`conhecimentosresumo.create_date` é a data da ÚLTIMA ALTERAÇÃO (ON UPDATE no banco; ver
`risco_historico/CONTEXTO.md`), não a da criação: o menor ID com `create_date` recente é um CE antigo alterado (em
18/10/2026, o ID 842.381 puxou um diagnóstico para uma varredura de mais de uma hora). A inclusão é linha nova com ID
crescente (quase sem lacunas), então a janela é por ID:

  - `criacao_do_id`: a criação estimada de um ID é a menor `create_date` do bloco de IDs a partir dele (o CE alterado
    tem data nova; os vizinhos, não);
  - `id_na_data`: o 1º ID criado a partir de uma data, por busca binária na chave primária (~25 leituras de um bloco);
  - `limita_tempo`: `max_statement_time` em cada conexão do engine no MariaDB (a consulta longa é interrompida pelo
    servidor em vez de segurar a conexão).
"""
from datetime import datetime
from typing import Callable, Optional, Tuple

from sqlalchemy import event, func, select

from virasana.integracao.mercante.mercantealchemy import Conhecimento

BLOCO_DATA = 200                 # IDs lidos para estimar a criação de um ID


def _momento(valor) -> Optional[datetime]:
    if valor is None or isinstance(valor, datetime):
        return valor
    return datetime.fromisoformat(str(valor)[:19])


def limites_dos_ids(session) -> Tuple[Optional[int], Optional[int]]:
    """(menor ID, maior ID) do `conhecimentosresumo` — pela chave primária, instantâneo."""
    return tuple(session.execute(select(func.min(Conhecimento.ID), func.max(Conhecimento.ID))).one())


def criacao_do_id(session, id_: int, bloco: int = BLOCO_DATA) -> Optional[datetime]:
    """Criação estimada do ID: a menor `create_date` dos `bloco` CEs a partir dele. None depois do último."""
    datas = [_momento(d) for (d,) in session.execute(select(Conhecimento.create_date).where(
        Conhecimento.ID >= id_).order_by(Conhecimento.ID).limit(bloco)) if d is not None]
    return min(datas) if datas else None


def id_na_data(session, data: datetime, bloco: int = BLOCO_DATA, a_partir: Optional[int] = None,
               limites: Optional[Tuple] = None) -> Optional[int]:
    """O 1º ID criado a partir de `data` (o último + 1 se nenhum; None sem CEs). `a_partir` é uma fronteira já
    conhecida: a busca não volta para antes dela. `limites` evita reler (menor, maior)."""
    menor, maior = limites or limites_dos_ids(session)
    if menor is None:
        return None
    baixo, alto = max(menor, a_partir or menor), maior + 1
    while baixo < alto:
        meio = (baixo + alto) // 2
        criacao = criacao_do_id(session, meio, bloco)
        if criacao is None or criacao >= data:
            alto = meio
        else:
            baixo = meio + 1
    return baixo


def _ouvinte_limite(segundos: float) -> Callable:
    """Ouvinte do evento 'connect': fixa o max_statement_time da conexão nova (MariaDB)."""
    def limita(conexao_dbapi, _registro):
        cursor = conexao_dbapi.cursor()
        try:
            cursor.execute('SET SESSION max_statement_time = %s', (float(segundos),))
        finally:
            cursor.close()
    return limita


def limita_tempo(engine, segundos: float) -> bool:
    """No MariaDB/MySQL, toda conexão do engine (inclusive a refeita depois de uma queda) recebe o limite; nos outros
    bancos (os testes usam SQLite), nada. Devolve se aplicou."""
    if not segundos or engine.dialect.name not in ('mysql', 'mariadb'):
        return False
    event.listen(engine, 'connect', _ouvinte_limite(segundos))
    return True
