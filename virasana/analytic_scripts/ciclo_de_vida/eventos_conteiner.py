# coding: utf-8
"""
EVENTOS de contêineres, um a um, para conferir à mão o que o Ciclo de Vida decidiu. Somente leitura.

Para cada contêiner: todos os eventos da API Recintos no período, em ordem, com o intervalo desde o evento anterior —
desembarque/embarque (peso da balança, tara e peso do manifesto), acessos (efetivados e agendamentos, vazio, CE
declarado), inspeções (vazio, CE declarado) e pesagens (peso) — com o nome do recinto; com --mongo, as imagens do
contêiner (horário do Mongo e o horário menos +3 h). No fim, o ciclo que o núcleo ATUAL (`linha_do_tempo`, sem CE)
calcula para cada desembarque: fluxo, situação, motivo, etapas e alertas. Não mostra placa, CPF nem motorista.

Os contêineres vêm da linha de comando ou de um CSV da sombra (`sombra_ciclo_de_vida.py --csv`), filtrando as linhas
que contêm um texto (em qualquer coluna); o período começa 2 dias antes do desembarque da linha.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    EV=virasana/analytic_scripts/ciclo_de_vida/eventos_conteiner.py
    virasana-venv/bin/python $EV --conteineres MSCU1234567 TGHU7654321 --desde 2026-09-10 --saida eventos.log
    virasana-venv/bin/python $EV --csv sombra_passo1.csv --filtro "8931318 em papel de TRA" --limite 20 --mongo \\
        --saida eventos_ecoporto.log
"""
import argparse
import csv
import sys
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from sqlalchemy import create_engine

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.carga_ciclo import CargaCiclo, _bool, _dt  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ciclo_de_vida import Diagnostico  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.imagens_ciclo import imagens_do_conteiner  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import uri_do_banco  # noqa: E402

DIAS_PADRAO = 60
ANTES_DO_DESEMBARQUE_DIAS = 2

SQL_DESEMBARQUES = ('SELECT id, dataHoraOcorrencia, codigoRecinto, embarqueDesembarque, numeroConhecimento, '
                    'pesoBrutoBalanca, taraConteiner, pesoBrutoManifesto FROM apirecintos_embarquedesembarque '
                    'WHERE numeroConteiner = :conteiner AND dataHoraOcorrencia >= :desde AND dataHoraOcorrencia < :ate')
SQL_ACESSOS = ('SELECT id, dataHoraOcorrencia, codigoRecinto, operacao, direcao, vazioConteiner, numeroConhecimento '
               'FROM apirecintos_acessosveiculo WHERE numeroConteiner = :conteiner AND dataHoraOcorrencia >= :desde '
               'AND dataHoraOcorrencia < :ate')
SQL_INSPECOES = ('SELECT id, dataHoraOcorrencia, codigoRecinto, vazio, numeroConhecimento '
                 'FROM apirecintos_inspecoesnaoinvasivas WHERE numeroConteiner = :conteiner '
                 'AND dataHoraOcorrencia >= :desde AND dataHoraOcorrencia < :ate')
SQL_PESAGENS = ('SELECT id, dataHoraOcorrencia, codigoRecinto, pesoBrutoBalanca FROM apirecintos_pesagensveiculo '
                'WHERE numeroConteiner = :conteiner AND dataHoraOcorrencia >= :desde AND dataHoraOcorrencia < :ate')


def _peso(valor) -> str:
    return f'{float(valor):,.0f} kg'.replace(',', '.') if valor not in (None, '') else '-'


def _intervalo(depois: datetime, antes: Optional[datetime]) -> str:
    if antes is None:
        return ''
    horas = (depois - antes).total_seconds() / 3600
    return f'+{horas * 60:.0f} min' if horas < 1 else f'+{horas:.1f} h' if horas < 48 else f'+{horas / 24:.1f} d'


def conteineres_do_csv(caminho: str, filtro: str, limite: int) -> List[Tuple[str, datetime]]:
    """[(contêiner, desembarque)] das linhas do CSV da sombra que contêm o texto."""
    escolhidos = []
    with open(caminho, encoding='utf-8-sig') as arquivo:
        for linha in csv.DictReader(arquivo, delimiter=';'):
            if filtro in ';'.join(linha.values()):
                escolhidos.append((linha['conteiner'], datetime.fromisoformat(linha['data_desembarque'][:19])))
            if len(escolhidos) >= limite:
                break
    return escolhidos


class EventosConteiner(Diagnostico):

    def __init__(self, conexao, saida, agora: Optional[datetime] = None, dias: int = DIAS_PADRAO, mongo_db=None):
        super().__init__(conexao, saida, agora or datetime.now(), agora or datetime.now(), agora)
        self.dias, self.mongo_db = dias, mongo_db
        self.escopo, self.operadores, self.nomes = CargaCiclo(self, self.agora, self.agora).operadores()

    def nome(self, recinto: Optional[str]) -> str:
        return f'{recinto} {(self.nomes.get(recinto) or "")[:24]}'.strip() if recinto else '-'

    def eventos(self, conteiner: str, desde: datetime) -> List[tuple]:
        """[(data, ordem, texto, Evento do núcleo ou None)] em ordem."""
        params = {'conteiner': conteiner, 'desde': desde, 'ate': desde + timedelta(days=self.dias)}
        linhas = []
        for id_, data, recinto, tipo, ce, peso, tara, manifesto in self.consulta(SQL_DESEMBARQUES, params):
            evento = ldt.Evento(ldt.EMBARQUE if tipo == 'E' else ldt.DESEMBARQUE, id_, _dt(data), recinto, None, ce)
            linhas.append((evento.data, id_, f'{evento.tipo:<11} {self.nome(recinto)}  balança {_peso(peso)}, tara '
                                             f'{_peso(tara)}, manifesto {_peso(manifesto)}{self._ce(ce)}', evento))
        for id_, data, recinto, operacao, direcao, vazio, ce in self.consulta(SQL_ACESSOS, params):
            tipo = ldt.ENTRADA if direcao == 'E' else ldt.SAIDA
            evento = ldt.Evento(tipo, id_, _dt(data), recinto, _bool(vazio), ce) if operacao == 'C' else None
            rotulo = tipo if operacao == 'C' else f'agend. {tipo}'
            linhas.append((_dt(data), id_, f'{rotulo:<11} {self.nome(recinto)}{" VAZIO" if vazio else ""}'
                                           f'{self._ce(ce)}', evento))
        for id_, data, recinto, vazio, ce in self.consulta(SQL_INSPECOES, params):
            evento = ldt.Evento(ldt.INSPECAO, id_, _dt(data), recinto, _bool(vazio), ce)
            linhas.append((evento.data, id_, f'{"inspecao":<11} {self.nome(recinto)}{" vazio" if vazio else ""}'
                                             f'{self._ce(ce)}', evento))
        for id_, data, recinto, peso in self.consulta(SQL_PESAGENS, params):
            evento = ldt.Evento(ldt.PESAGEM, id_, _dt(data), recinto)
            linhas.append((evento.data, id_, f'{"pesagem":<11} {self.nome(recinto)}  {_peso(peso)}', evento))
        return sorted(linhas, key=lambda linha: (linha[0], linha[1]))

    @staticmethod
    def _ce(ce) -> str:
        return f'  CE {ce}' if (ce or '').strip() else ''

    def imagens(self, conteiner: str, desde: datetime) -> List[datetime]:
        if self.mongo_db is None:
            return []
        return sorted(data for data, _ in imagens_do_conteiner(self.mongo_db, conteiner, desde,
                                                               desde + timedelta(days=self.dias)))

    def mostra(self, conteiner: str, desde: datetime):
        self.titulo(f'{conteiner} — eventos de {desde:%d/%m/%Y} a {desde + timedelta(days=self.dias):%d/%m/%Y}')
        linhas = self.eventos(conteiner, desde)
        imagens = [(data, 0, f'{"IMAGEM":<11} Mongo {data:%d/%m %H:%M} (−3 h = {data - timedelta(hours=3):%H:%M})',
                    None) for data in self.imagens(conteiner, desde)]
        anterior = None
        for data, _, texto, _ in sorted(linhas + imagens, key=lambda linha: (linha[0], linha[1])):
            self.escreve(f'  {data:%d/%m %H:%M}  {_intervalo(data, anterior):>9}  {texto}')
            anterior = data
        eventos = [evento for _, _, _, evento in linhas if evento is not None]
        operadores = self.operadores | {e.recinto for e in eventos if e.tipo in (ldt.DESEMBARQUE, ldt.EMBARQUE)}
        for ciclo in ldt.ciclos_do_conteiner(eventos, self.escopo, operadores, self.agora):
            self._mostra_ciclo(ciclo)

    def _mostra_ciclo(self, ciclo: ldt.CicloCalculado):
        etapas = (('saída', ciclo.saida), ('entrada no seco', ciclo.entrada_seco), ('saída do seco', ciclo.saida_seco),
                  ('embarque', ciclo.embarque))
        etapas = [(nome, lig) for nome, lig in etapas if lig]
        self.escreve(f'  >> núcleo atual: desembarque {ciclo.desembarque.data:%d/%m %H:%M} em '
                     f'{self.nome(ciclo.desembarque.recinto)} -> {ciclo.fluxo}/{ciclo.situacao} — {ciclo.motivo}')
        for nome, lig in etapas:
            self.escreve(f'     {nome}: {lig.evento.data:%d/%m %H:%M} {self.nome(lig.evento.recinto)} [{lig.confianca}]'
                         + (f' {"; ".join(lig.motivos)}' if lig.motivos else ''))
        for lig in ciclo.escaneamentos:
            self.escreve(f'     escaneamento: {lig.evento.data:%d/%m %H:%M} {self.nome(lig.evento.recinto)} '
                         f'[{lig.confianca}] {"; ".join(lig.motivos)}')
        if ciclo.alertas:
            self.escreve(f'     alertas: {"; ".join(ciclo.alertas)}')


def _escolhidos(args) -> List[Tuple[str, datetime]]:
    if args.csv:
        return [(c, d - timedelta(days=ANTES_DO_DESEMBARQUE_DIAS))
                for c, d in conteineres_do_csv(args.csv, args.filtro or '', args.limite)]
    desde = datetime.fromisoformat(args.desde) if args.desde else datetime.now() - timedelta(days=DIAS_PADRAO)
    return [(c, desde) for c in args.conteineres or []]


def main():  # pragma: no cover - depende do banco real
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    parser.add_argument('--conteineres', nargs='+', help='números dos contêineres')
    parser.add_argument('--desde', help='início do período (AAAA-MM-DD; padrão: 60 dias atrás)')
    parser.add_argument('--csv', help='CSV da sombra: os contêineres das linhas que contêm --filtro')
    parser.add_argument('--filtro', help='texto procurado nas linhas do CSV')
    parser.add_argument('--limite', type=int, default=20, help='máximo de contêineres do CSV')
    parser.add_argument('--dias', type=int, default=DIAS_PADRAO, help='dias de eventos a mostrar')
    parser.add_argument('--mongo', action='store_true', help='mostra as imagens do contêiner no Mongo')
    parser.add_argument('--saida', help='grava neste arquivo (padrão: só na tela)')
    args = parser.parse_args()
    mongo_db = None
    if args.mongo:
        from pymongo import MongoClient
        from ajna_commons.flask.conf import DATABASE, MONGODB_URI
        mongo_db = MongoClient(host=MONGODB_URI)[DATABASE]
    saida = open(args.saida, 'w', encoding='utf-8') if args.saida else sys.stdout
    try:
        with create_engine(uri_do_banco(args)).connect() as conexao:
            mostrador = EventosConteiner(conexao, saida, dias=args.dias, mongo_db=mongo_db)
            for conteiner, desde in _escolhidos(args):
                mostrador.mostra(conteiner, desde)
    finally:
        if args.saida:
            saida.close()
    if args.saida:
        print(f'Eventos gravados em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
