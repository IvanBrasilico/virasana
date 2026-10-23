# coding: utf-8
"""
AUDITORIA dos ciclos de vida GRAVADOS pelas invariantes (`invariantes_ciclo.py`) — só leitura (22/10/2026).

Para cada ciclo v2 com desembarque no período: as invariantes que ele viola (CE principal de exportação ou com a carga
saindo do porto, contêiner fora dos itens do principal, família/confiança incoerentes, etapas fora de ordem, concluído
sem o evento que conclui). O porto do desembarque é o da escala gravada no ciclo (escalasresumo); sem ela, o mais
visto no recinto. Relatório por regra e por operador, com amostras; o CSV traz todos — as violações são os candidatos
naturais a casos reais do corpus de regressão (`virasana/tests/dados/casos_ciclo/casos.txt`).

Consultas: um DIA de desembarques por vez, numa conexão nova (o progresso vai para a tela): os ciclos pelo índice de
data_desembarque, só as colunas usadas; família, CEs e itens por chave, em lotes. (23/10/2026: a 1ª versão juntava a
família de 30 dias de uma vez, em tempo quadrático — mais de 1 h parada, e o servidor derrubou a conexão.)

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    A=virasana/analytic_scripts/ciclo_de_vida/audita_ciclos.py
    virasana-venv/bin/python $A --dias 7 --saida auditoria_ciclos.log --csv auditoria_ciclos.csv

Sai com código 1 quando há violação (0 sem nenhuma). O health check (`virasana/saude/saude_navi.py`) audita os
desembarques dos últimos 2 dias com a mesma classe.
"""
import argparse
import csv
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from sqlalchemy import select  # noqa: E402

from virasana.analytic_scripts.ciclo_de_vida import gravacao_ciclo as gc  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.carga_ciclo import ConsultaSQL  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.caso_ciclo import Saida, _com_familia  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.ces_do_ciclo import normaliza_ce  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.invariantes_ciclo import violacoes  # noqa: E402

DIAS_PADRAO = 7
AMOSTRAS = 5
COLUNAS_CSV = ('desembarque_id', 'conteiner', 'data_desembarque', 'recinto', 'porto', 'principal', 'confianca',
               'situacao', 'fluxo', 'violacoes')


def _t(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def regra(mensagem: str) -> str:
    """A regra da mensagem, sem o porto (para agrupar)."""
    return re.sub(r'saindo de \w+', 'saindo do porto', mensagem)


COLUNAS_CICLO = ('ID', 'desembarque_id', 'numeroConteiner', 'numeroCEmercante', 'confianca_ce', 'situacao', 'fluxo',
                 'escala', 'codigoRecintoDesembarque', 'data_desembarque', 'data_saida', 'data_entrada_seco',
                 'data_saida_seco', 'data_embarque')


class Auditoria:
    """As invariantes nos ciclos v2 de [desde, ate), um dia por vez, cada dia numa conexão nova do `engine`."""

    def __init__(self, engine, desde: datetime, ate: datetime, progresso: Callable[[str], None] = print):
        self.engine, self.desde, self.ate, self.progresso = engine, desde, ate, progresso
        self.total = 0

    def ciclos(self, inicio: datetime, fim: datetime) -> Dict[int, dict]:
        linhas = [dict(linha._mapping) for linha in self.conexao.execute(
            select(*[gc.CICLOS.c[coluna] for coluna in COLUNAS_CICLO])
            .where(gc.CICLOS.c.versao_heuristica == gc.VERSAO, gc.CICLOS.c.data_desembarque >= inicio,
                   gc.CICLOS.c.data_desembarque < fim))]
        familias = [{'ciclo_de_vida_id': ciclo, 'numeroCEmercante': ce} for ciclo, ce in self.diag._por_lotes(
            'SELECT ciclo_de_vida_id, numeroCEmercante FROM ciclodevida_ciclo_ce WHERE ciclo_de_vida_id IN :ids',
            'ids', [linha['ID'] for linha in linhas])]
        return _com_familia(linhas, familias)

    def mercante(self, ciclos: Dict[int, dict]):
        """(info dos CEs principais: tráfego e origem; itens (CE, contêiner) desses CEs)."""
        ces = {normaliza_ce(c['numeroCEmercante']) for c in ciclos.values()} - {None}
        info = {normaliza_ce(ce): {'trafego': _t(trafego), 'origem': _t(origem).upper()}
                for ce, trafego, origem in self.diag._por_lotes(
                    'SELECT numeroCEmercante, tipoTrafego, portoOrigemCarga FROM conhecimentosresumo '
                    'WHERE numeroCEmercante IN :ces', 'ces', ces)}
        itens = {(normaliza_ce(ce), _t(conteiner)) for ce, conteiner in self.diag._por_lotes(
            'SELECT numeroCEmercante, codigoConteiner FROM itensresumo WHERE numeroCEmercante IN :ces', 'ces', ces)}
        return info, itens

    def portos(self, ciclos: Dict[int, dict]) -> Dict[int, Optional[str]]:
        """{desembarque: porto} — o da escala gravada no ciclo; sem ela, o mais visto no recinto."""
        da_escala = {_t(escala): _t(porto) for escala, porto in self.diag._por_lotes(
            'SELECT numeroDaEscala, portoDaEscala FROM escalasresumo WHERE numeroDaEscala IN :escalas', 'escalas',
            {_t(c['escala']) for c in ciclos.values()} - {''})}
        por_recinto = defaultdict(Counter)
        for ciclo in ciclos.values():
            if da_escala.get(_t(ciclo['escala'])):
                por_recinto[_t(ciclo['codigoRecintoDesembarque'])][da_escala[_t(ciclo['escala'])]] += 1
        do_recinto = {recinto: contagem.most_common(1)[0][0] for recinto, contagem in por_recinto.items()}
        return {d: da_escala.get(_t(c['escala'])) or do_recinto.get(_t(c['codigoRecintoDesembarque']))
                for d, c in ciclos.items()}

    def executa(self) -> List[dict]:
        achados, inicio = [], self.desde
        while inicio < self.ate:
            fim = min(inicio + timedelta(days=1), self.ate)
            with self.engine.connect() as self.conexao:
                self.diag = ConsultaSQL(self.conexao)
                do_dia = self._audita(inicio, fim)
            achados += do_dia
            self.progresso(f'{inicio:%Y-%m-%d}: {self.total} ciclos auditados, {len(achados)} com violação')
            inicio = fim
        return achados

    def _audita(self, inicio: datetime, fim: datetime) -> List[dict]:
        ciclos = self.ciclos(inicio, fim)
        info, itens = self.mercante(ciclos)
        portos = self.portos(ciclos)
        self.total += len(ciclos)
        achados = []
        for desembarque, ciclo in sorted(ciclos.items()):
            erros = violacoes(ciclo, info, itens, portos.get(desembarque))
            if erros:
                achados.append({'desembarque_id': desembarque, 'conteiner': _t(ciclo['numeroConteiner']),
                                'data_desembarque': ciclo['data_desembarque'],
                                'recinto': _t(ciclo['codigoRecintoDesembarque']), 'porto': portos.get(desembarque),
                                'principal': _t(ciclo['numeroCEmercante']), 'confianca': ciclo['confianca_ce'],
                                'situacao': ciclo['situacao'], 'fluxo': ciclo['fluxo'], 'violacoes': ' | '.join(erros)})
        return achados


def relatorio(achados: List[dict], total: int, saida: Saida, caminho_csv: Optional[str] = None):
    saida(f'Ciclos v2 auditados: {total}; com violação: {len(achados)}')
    saida.titulo('Por regra')
    por_regra = Counter(regra(m) for a in achados for m in a['violacoes'].split(' | '))
    for mensagem, quantidade in por_regra.most_common():
        saida(f'  {mensagem}: {quantidade}')
    saida.titulo('Por operador do desembarque e regra')
    por_operador = Counter((a['recinto'], regra(m)) for a in achados for m in a['violacoes'].split(' | '))
    for (recinto, mensagem), quantidade in sorted(por_operador.items()):
        saida(f'  {recinto} {mensagem}: {quantidade}')
    saida.titulo(f'Amostras ({AMOSTRAS} por regra)')
    for mensagem in por_regra:
        for achado in [a for a in achados if mensagem in map(regra, a['violacoes'].split(' | '))][:AMOSTRAS]:
            saida(f'  [{mensagem}] {achado["conteiner"]} desembarque {achado["desembarque_id"]} em '
                  f'{achado["data_desembarque"]} no {achado["recinto"]} ({achado["porto"]}): CE {achado["principal"]} '
                  f'({achado["confianca"]}); {achado["situacao"]}/{achado["fluxo"]}')
    if caminho_csv:
        with open(caminho_csv, 'w', encoding='utf-8', newline='') as arquivo:
            escritor = csv.DictWriter(arquivo, fieldnames=COLUNAS_CSV, delimiter=';')
            escritor.writeheader()
            escritor.writerows(achados)
        saida(f'  todas em {caminho_csv}')


def _argumentos(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dias', type=int, default=DIAS_PADRAO, help='dias de desembarques até --ate (padrão: 7)')
    parser.add_argument('--ate', help='fim do período (AAAA-MM-DD, exclusivo; padrão: agora)')
    parser.add_argument('--saida', help='grava o relatório neste arquivo (padrão: só na tela)')
    parser.add_argument('--csv', help='grava todas as violações neste CSV (;)')
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    return parser.parse_args(argv)


def main(argv=None):  # pragma: no cover - depende do banco real
    from sqlalchemy import create_engine
    from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import uri_do_banco
    from virasana.navii.mercante_por_id import limita_tempo
    args = _argumentos(argv)
    ate = datetime.strptime(args.ate, '%Y-%m-%d') if args.ate else datetime.now()
    engine = create_engine(uri_do_banco(args))
    limita_tempo(engine, 300)
    arquivo = open(args.saida, 'w', encoding='utf-8') if args.saida else None
    try:
        auditoria = Auditoria(engine, ate - timedelta(days=args.dias), ate)
        achados = auditoria.executa()
        relatorio(achados, auditoria.total, Saida(arquivo), args.csv)
    finally:
        if arquivo:
            arquivo.close()
    sys.exit(1 if achados else 0)   # cron e health check: violação = código 1


if __name__ == '__main__':  # pragma: no cover
    main()
