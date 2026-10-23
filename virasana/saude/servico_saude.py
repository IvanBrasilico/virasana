# coding: utf-8
"""
SERVIÇO do health check (programa `saude_navi` do supervisor — `supervisor_saude.conf`; decisões do usuário,
23/10/2026: serviço próprio, uma rodada por dia às 7 h e as pedidas pela tela `/saude/` por um Supervisor).

Um laço leve: a cada INTERVALO_SEGUNDOS, grava o batimento (a tela mostra se o serviço parou) e roda o health check
(`saude_navi.roda`) quando há pedido da tela ou quando passou das HORA_AGENDADA h e ainda não houve a rodada agendada
do dia (se o serviço subir depois das 7 h, roda na hora). Cada rodada vai para o histórico (`historico.py`). Um erro
fora das verificações (ex.: o banco fora ao montar o contexto) vira uma rodada com FALHA "Health check", para a tela
mostrar; o laço segue.

Fica fora dos serviços que ele vigia (o do ciclo de vida, o indexador): se um deles parar, o aviso não para junto.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do programa virasana — SQL_URI, MONGODB_URI — e,
para medir a busca por texto, NAVII_MEILI_CHAVE_ESCRITA):
    virasana-venv/bin/python virasana/saude/servico_saude.py               # contínuo (supervisor)
    virasana-venv/bin/python virasana/saude/servico_saude.py --uma-vez     # uma volta (roda só se for a hora)
"""
import argparse
import logging
import sys
import time
from datetime import datetime, timedelta
from typing import Callable, List, Optional

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')

from virasana.saude import historico  # noqa: E402
from virasana.saude.verificacao import FALHA, Contexto, Resultado  # noqa: E402

logger = logging.getLogger('saude_navi')
HORA_AGENDADA = 7
INTERVALO_SEGUNDOS = 60
PARADO_MINUTOS = 5               # a tela diz "serviço parado" sem batimento há mais que isso
COMPONENTE_GERAL = 'Health check'


class ServicoSaude:
    """`fabrica(agora) -> Contexto`; `rodar(contexto) -> [Resultado]` (padrão: `saude_navi.roda`)."""

    def __init__(self, fabrica: Callable[[datetime], Contexto], db, relogio: Callable[[], datetime] = datetime.now,
                 hora: int = HORA_AGENDADA, rodar: Optional[Callable[[Contexto], List[Resultado]]] = None):
        self.fabrica, self.db, self.relogio, self.hora = fabrica, db, relogio, hora
        if rodar is None:
            from virasana.saude.saude_navi import roda as rodar
        self.rodar = rodar

    def marco_do_dia(self, agora: datetime) -> datetime:
        return agora.replace(hour=self.hora, minute=0, second=0, microsecond=0)

    def agendada_devida(self, agora: datetime) -> bool:
        marco = self.marco_do_dia(agora)
        return agora >= marco and self.db[historico.RODADAS].find_one(
            {'origem': 'agenda', 'inicio': {'$gte': marco}}, {'_id': 1}) is None

    def proxima(self, agora: datetime) -> datetime:
        marco = self.marco_do_dia(agora)
        return marco if self.agendada_devida(agora) or agora < marco else marco + timedelta(days=1)

    def uma_volta(self):
        """Roda se houver pedido ou se for a hora; devolve o id da rodada gravada (ou None)."""
        agora = self.relogio()
        pedidos = historico.pedidos_pendentes(self.db)
        if pedidos:
            origem, usuario = 'tela', pedidos[0].get('usuario')
            historico.marca_pedidos(self.db, [p['_id'] for p in pedidos], historico.RODANDO, agora)
        elif self.agendada_devida(agora):
            origem, usuario = 'agenda', None
        else:
            historico.batimento(self.db, agora, proxima=self.proxima(agora))
            return None
        historico.batimento(self.db, agora, rodando_desde=agora, proxima=self.proxima(agora))
        resultados = self._resultados(agora)
        fim = self.relogio()
        rodada = historico.grava_rodada(self.db, resultados, agora, fim, origem, usuario)
        if pedidos:
            historico.marca_pedidos(self.db, [p['_id'] for p in pedidos], historico.FEITO, fim, rodada)
        historico.batimento(self.db, fim, proxima=self.proxima(fim))
        estados = ', '.join(f'{r.componente}: {r.estado}' for r in resultados)
        logger.info(f'rodada {rodada} ({origem}{" por " + usuario if usuario else ""}) em '
                    f'{(fim - agora).total_seconds():.0f} s — {estados}')
        return rodada

    def _resultados(self, agora: datetime) -> List[Resultado]:
        try:
            return self.rodar(self.fabrica(agora))
        except Exception as erro:  # noqa: a tela precisa saber que a rodada falhou; o laço segue
            logger.exception('rodada do health check falhou')
            return [Resultado(COMPONENTE_GERAL, FALHA, f'a rodada falhou: {type(erro).__name__}: {str(erro)[:300]}')]

    def roda(self, intervalo: float = INTERVALO_SEGUNDOS):  # pragma: no cover - laço infinito
        logger.info(f'Serviço do health check: rodada diária às {self.hora} h e as pedidas pela tela; '
                    f'volta a cada {intervalo:.0f} s')
        while True:
            try:
                self.uma_volta()
            except Exception:  # noqa: o Mongo fora numa volta não derruba o serviço
                logger.exception('volta do serviço do health check com erro')
            time.sleep(intervalo)


def _argumentos(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    parser.add_argument('--hora', type=int, default=HORA_AGENDADA,
                        help=f'hora da rodada diária (padrão {HORA_AGENDADA})')
    parser.add_argument('--intervalo', type=float, default=INTERVALO_SEGUNDOS, help='segundos entre as voltas')
    parser.add_argument('--uma-vez', action='store_true', help='uma volta só (roda se houver pedido ou for a hora)')
    return parser.parse_args(argv)


def main(argv=None):  # pragma: no cover - depende dos bancos reais
    from virasana.saude.saude_navi import recursos
    args = _argumentos(argv)
    args.sem_mongo = False
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s', stream=sys.stdout)
    engine, mongo, meili = recursos(args)
    servico = ServicoSaude(lambda agora: Contexto(engine, mongo=mongo, meili=meili, agora=agora), mongo,
                           hora=args.hora)
    if args.uma_vez:
        servico.uma_volta()
    else:
        servico.roda(args.intervalo)


if __name__ == '__main__':  # pragma: no cover
    main()
