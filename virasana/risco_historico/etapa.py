# coding: utf-8
"""
Etapa do risco histórico por CE no laço do serviço do ciclo de vida (F3; ver
CONTEXTO.md). A cada volta do ciclo:

  1. extração dos sinais dos CEs novos/alterados (`extracao.Extracao`) — exige a
     carga inicial (`lotes.py --sinais`): sem marcas, a etapa só avisa;
  2. parâmetros (`parametros.atualiza`): verificados no máximo uma vez por
     `intervalo_parametros` (1 h); nova versão só quando as fichas positivas
     mudam ou a última vence;
  3. pontuação dos CEs na fila (`pontuacao.Pontuacao`).

Cada passo em transação própria; extração e pontuação repetem enquanto houver
fila, até `limite_segundos`. Quem chama (o serviço) captura qualquer erro: a
etapa nunca derruba o ciclo de vida.
"""
import time
from collections import Counter
from datetime import datetime, timedelta
from typing import Callable, Optional

from virasana.risco_historico import parametros
from virasana.risco_historico.extracao import MARCA_CE, Extracao
from virasana.risco_historico.pontuacao import Pontuacao


class EtapaRisco:
    def __init__(self, engine, relogio: Callable[[], datetime] = datetime.now, limite_segundos: float = 60,
                 intervalo_parametros: timedelta = timedelta(hours=1)):
        self.engine, self.relogio = engine, relogio
        self.limite_segundos, self.intervalo_parametros = limite_segundos, intervalo_parametros
        self.parametros_vistos_em: Optional[datetime] = None

    def _repete(self, passo, prefixo: str, estatisticas: Counter, fim: float):
        """Roda `passo(conexao) -> (estatísticas, cheio)` enquanto houver fila e tempo; cada vez numa transação."""
        while True:
            with self.engine.begin() as conexao:
                parcial, cheio = passo(conexao)
            estatisticas.update({f'{prefixo}{chave}': valor for chave, valor in parcial.items()})
            if not cheio or time.monotonic() > fim:
                return

    def _sinais(self, conexao):
        extracao = Extracao(conexao, self.relogio)
        if MARCA_CE not in extracao.marcas():
            return Counter({'sem marcas: rode lotes.py --sinais': 1}), False
        return extracao.uma_volta()

    def _parametros(self, estatisticas: Counter):
        agora = self.relogio()
        if self.parametros_vistos_em and agora - self.parametros_vistos_em < self.intervalo_parametros:
            return
        with self.engine.begin() as conexao:
            versao, motivo = parametros.atualiza(conexao, agora)
        self.parametros_vistos_em = agora
        if versao:
            estatisticas[f'parâmetros: nova versão {versao} ({motivo})'] = 1

    def executa(self) -> Counter:
        estatisticas: Counter = Counter()
        fim = time.monotonic() + self.limite_segundos
        self._repete(self._sinais, 'sinais: ', estatisticas, fim)
        self._parametros(estatisticas)
        self._repete(lambda conexao: Pontuacao(conexao, self.relogio).uma_volta(), 'pontos: ', estatisticas, fim)
        return estatisticas
