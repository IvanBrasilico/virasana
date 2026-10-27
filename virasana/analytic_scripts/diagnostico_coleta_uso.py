# coding: utf-8
"""
Conferência da COLETA DE USO do NAVI-I (F1 das estatísticas de uso; `navii/coleta_uso.py`, coleção Mongo
`navii_uso_pesquisas`): antes de existir a aba, mostra se os números fazem sentido — e é o protótipo das contas da F2.

  1. VOLUME: pesquisas, usuários, sessões de análise (mesma aba do navegador, pausa <= 30 min), por origem e janela,
     sem período, por dia;
  2. CARDS: enviados, vistos e examinados — somados por pesquisa e DISTINTOS por item (o card: um contêiner num CE;
     a diferença é o que o refinamento repete); profundidade (vistos / total da pesquisa), vistos / enviados,
     segundos por card visto, interações por tipo;
  3. TRANSIÇÕES entre pesquisas da mesma aba: repetida, mais critérios, menos critérios, mudança, outra janela/período;
  4. POR USUÁRIO (CPF mascarado): pesquisas, sessões, minutos de sessão, cards distintos vistos e examinados,
     vistos por hora de sessão, Fichas abertas;
  5. AÇÕES: Fichas abertas pelo card, por resultado.

Minutos de sessão: do início da 1ª pesquisa à última gravação (`atualizado`) de cada sessão — aproximação, os cards
vistos chegam em lotes de 20 s. As contas por pesquisa (vistos, sessões, transições) são as da aba
(`navii/estatisticas_uso.py`). Somente leitura.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    python virasana/analytic_scripts/diagnostico_coleta_uso.py --dias 7 --saida coleta_uso.log
"""
import argparse
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import List

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.diagnostico_fichas_repressao import mascara_cpf  # noqa: E402
from virasana.analytic_scripts.diagnostico_risco_perdimentos import Diagnostico  # noqa: E402
from virasana.navii import coleta_uso  # noqa: E402
from virasana.navii.estatisticas_uso import examinados, minutos, sessoes, transicao, vistos  # noqa: E402


def quartis(valores: List[float], unidade: str = '') -> str:
    """'mediana X, p75 Y, p90 Z' ou '-'."""
    if not valores:
        return '-'
    valores = sorted(valores)

    def p(fracao):
        return valores[min(len(valores) - 1, int(round(fracao * (len(valores) - 1))))]
    return f'mediana {statistics.median(valores):.1f}{unidade}, p75 {p(.75):.1f}{unidade}, p90 {p(.9):.1f}{unidade}'


def pct(parte: float, total: float) -> str:
    return f'{100 * parte / total:.0f}%' if total else '-'


class DiagnosticoColeta(Diagnostico):
    """Lê as pesquisas do período e escreve as seções (as tabelas do diagnóstico do risco histórico)."""

    SECOES = ('secao_volume', 'secao_cards', 'secao_transicoes', 'secao_usuarios', 'secao_acoes')

    def __init__(self, col, saida, inicio: datetime, fim: datetime):  # noqa: sem conexão SQL (só o Mongo)
        self.saida, self.col, self.inicio, self.fim = saida, col, inicio, fim
        self.docs: List[dict] = []
        self.sessoes: List[List[dict]] = []

    def carrega(self):
        self.docs = list(self.col.find({'inicio': {'$gte': self.inicio, '$lt': self.fim}}))
        self.sessoes = sessoes(self.docs) if self.docs else []

    def roda(self):
        self.carrega()
        self.escreve(f'Período: {self.inicio:%d/%m/%Y %H:%M} a {self.fim:%d/%m/%Y %H:%M} — {len(self.docs)} pesquisas')
        for nome in self.SECOES:
            getattr(self, nome)()

    def secao_volume(self):
        self.titulo('1. VOLUME')
        self.tabela(['medida', 'quantidade'], [
            ('pesquisas', len(self.docs)), ('usuários', len({d['usuario'] for d in self.docs})),
            ('sessões de análise', len(self.sessoes)),
            ('pesquisas sem período (link do CE/DTA)', sum(1 for d in self.docs if not d.get('periodo'))),
            ('páginas carregadas', sum(d.get('paginas', 0) for d in self.docs))])
        for nome, campo in (('origem', 'origem'), ('janela', 'janela')):
            self.escreve()
            contagem = Counter(d.get(campo) for d in self.docs)
            self.tabela([nome, 'pesquisas', '%'], [(k, n, pct(n, len(self.docs))) for k, n in contagem.most_common()])
        self.escreve()
        por_dia = defaultdict(list)
        for doc in self.docs:
            por_dia[f'{doc["inicio"]:%Y-%m-%d}'].append(doc)
        self.tabela(['dia', 'pesquisas', 'usuários'],
                    [(dia, len(lista), len({d['usuario'] for d in lista})) for dia, lista in sorted(por_dia.items())])

    def secao_cards(self):
        self.titulo('2. CARDS: enviados, vistos e examinados; profundidade e atenção')
        enviados = sum(d.get('n_cards', 0) for d in self.docs)
        somados_vistos = sum(len(vistos(d)) for d in self.docs)
        somados_examinados = sum(len(examinados(d)) for d in self.docs)
        distintos_vistos = set().union(*(vistos(d) for d in self.docs))
        distintos_examinados = set().union(*(examinados(d) for d in self.docs))
        self.tabela(['cards', 'somados por pesquisa', 'distintos (item)'], [
            ('enviados', enviados, '-'), ('vistos', somados_vistos, len(distintos_vistos)),
            ('examinados', somados_examinados, len(distintos_examinados))])
        self.escreve(f'Vistos / enviados: {pct(somados_vistos, enviados)}; repetição pelo refinamento: '
                     f'{somados_vistos - len(distintos_vistos)} card(s) visto(s) de novo.')
        profundidades = [100 * len(vistos(d)) / d['total'] for d in self.docs if d.get('total')]
        segundos = [v['s'] for d in self.docs for v in (d.get('vistos') or {}).values() if v.get('s')]
        self.escreve(f'Profundidade (vistos / total da pesquisa): {quartis(profundidades, "%")}')
        self.escreve(f'Segundos por card visto: {quartis(segundos, " s")}')
        interacoes = Counter(x for d in self.docs for v in (d.get('vistos') or {}).values() for x in v.get('x', []))
        self.escreve()
        self.tabela(['interação', 'cards'], interacoes.most_common())

    def secao_transicoes(self):
        self.titulo('3. TRANSIÇÕES: a pesquisa seguinte da mesma aba em relação à anterior')
        por_id = {d['_id']: d for d in self.docs}
        contagem = Counter(transicao(por_id[d['anterior']], d) for d in self.docs if d.get('anterior') in por_id)
        self.tabela(['transição', 'pesquisas'], contagem.most_common())

    def secao_usuarios(self):
        self.titulo('4. POR USUÁRIO (CPF mascarado)')
        linhas = []
        por_usuario = defaultdict(list)
        for sessao in self.sessoes:
            por_usuario[sessao[0]['usuario']].append(sessao)
        for usuario, lista in sorted(por_usuario.items()):
            docs = [d for sessao in lista for d in sessao]
            minutos_sessao = sum(minutos(sessao) for sessao in lista)
            distintos = set().union(*(vistos(d) for d in docs))
            linhas.append((mascara_cpf(usuario), len(docs), len(lista), f'{minutos_sessao:.0f}', len(distintos),
                           len(set().union(*(examinados(d) for d in docs))),
                           f'{60 * len(distintos) / minutos_sessao:.0f}' if minutos_sessao else '-',
                           sum(1 for d in docs for a in d.get('acoes', []) if a.get('t') == 'ficha')))
        self.tabela(['usuário', 'pesquisas', 'sessões', 'minutos', 'vistos', 'examinados', 'vistos/hora', 'Fichas'],
                    linhas)

    def secao_acoes(self):
        self.titulo('5. AÇÕES: Fichas abertas pelo card')
        acoes = [a for d in self.docs for a in d.get('acoes', []) if a.get('t') == 'ficha']
        self.tabela(['resultado', 'Fichas'], Counter(a.get('resultado') for a in acoes).most_common())


def main():  # pragma: no cover - depende do Mongo real
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dias', type=int, default=7, help='pesquisas dos últimos N dias (padrão 7)')
    parser.add_argument('--saida', default=datetime.now().strftime('coleta_uso_%Y%m%d_%H%M%S.log'))
    args = parser.parse_args()
    from virasana.db import mongodb
    fim = datetime.now()
    with open(args.saida, 'w', encoding='utf-8') as saida:
        DiagnosticoColeta(mongodb[coleta_uso.COLECAO], saida, fim - timedelta(days=args.dias), fim).roda()
    print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
