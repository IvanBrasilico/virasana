# coding: utf-8
"""
Lote 2b do risco histórico por CE: PONTOS e FAIXA de cada CE de
`risco_historico_ce`, com os pesos da última versão dos parâmetros
(`calculo.pontua`) — sem reler o Mercante (ver CONTEXTO.md).

Fila: CEs sem versão (novos, ou com sinais regravados pela extração, que zera
`versao_id`) ou pontuados com versão anterior. A consulta usa o índice de
`versao_id` (NULL ou menor que a atual) — não varre a tabela. Versão nova
re-pontua todos os CEs, em lotes.
"""
from collections import Counter
from datetime import datetime
from typing import Callable, Tuple

from sqlalchemy import bindparam, func, or_, select

from virasana.risco_historico import calculo, parametros
from virasana.risco_historico.sinais import de_json, pontuaveis
from virasana.risco_historico.tabelas import CES


class Pontuacao:
    """Uma conexão (com a transação de quem chama). `relogio` é o "agora" — os testes o fixam."""

    def __init__(self, conexao, relogio: Callable[[], datetime] = datetime.now, lote: int = 20000):
        self.conexao, self.relogio, self.lote = conexao, relogio, lote

    def uma_volta(self) -> Tuple[Counter, bool]:
        """(estatísticas, ainda há fila?)."""
        ultima = parametros.ultima_versao(self.conexao)
        if ultima is None:
            return Counter({'sem versão dos parâmetros': 1}), False
        versao = parametros.carrega_versao(self.conexao, ultima['ID'])
        pendentes = self.conexao.execute(
            select(CES.c.ID, CES.c.sinais)
            .where(or_(CES.c.versao_id.is_(None), CES.c.versao_id < versao.id))
            .limit(self.lote)).all()
        if not pendentes:
            return Counter(), False
        agora, linhas, faixas = self.relogio(), [], Counter()
        for id_, texto in pendentes:
            pontos, _ = calculo.pontua(pontuaveis(de_json(texto)), versao.pesos)
            faixa = calculo.faixa(pontos, versao.faixas)
            faixas[faixa] += 1
            linhas.append({'b_id': id_, 'b_pontos': pontos, 'b_faixa': faixa})
        self.conexao.execute(CES.update().where(CES.c.ID == bindparam('b_id')).values(
            pontos=bindparam('b_pontos'), faixa=bindparam('b_faixa'), versao_id=versao.id, calculado_em=agora), linhas)
        estatisticas = Counter({'CEs pontuados': len(linhas)})
        estatisticas.update({f'faixa {faixa}': n for faixa, n in faixas.items()})
        return estatisticas, len(pendentes) >= self.lote

    def situacao(self) -> dict:
        """CEs por faixa e quantos esperam pontuação."""
        ultima = parametros.ultima_versao(self.conexao)
        por_faixa = dict(self.conexao.execute(select(CES.c.faixa, func.count()).group_by(CES.c.faixa)).all())
        fila = CES.c.versao_id.is_(None) if ultima is None else or_(CES.c.versao_id.is_(None),
                                                                    CES.c.versao_id < ultima['ID'])
        return {'CEs por faixa': {str(f): n for f, n in por_faixa.items()},
                'CEs na fila da pontuação': self.conexao.execute(select(func.count()).select_from(CES)
                                                                 .where(fila)).scalar()}
