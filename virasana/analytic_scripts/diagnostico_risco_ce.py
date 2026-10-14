# coding: utf-8
"""
Diagnóstico F0 do risco histórico por CE (ver virasana/risco_historico/CONTEXTO.md).

Mede, só lendo, o que o cálculo vai encontrar em produção, usando o MESMO código
do cálculo (fichas_positivas, sinais, calculo):
  1. estrutura das tabelas do Mercante usadas (índices, charset) — MariaDB;
  2. tipo de operação x resultado (quanto é FMA, quanto é exportação);
  3. valor dos TGs por ficha (limiar de alto valor);
  4. fichas positivas (definição do usuário) e os sinais delas;
  5. população: CEs criados nos últimos N dias — tempo de cada etapa (com
     estimativa para 365 dias), quantos valores do histórico aparecem e PRÉVIA
     dos pesos e dos pontos com as constantes provisórias de calculo.py;
  6. incremento: CEs por dia, marca inicial, atraso entre a emissão e a chegada
     do CE, retificações e NCMs que chegam depois do CE.

Somente leitura. Importador, CNPJ citado e embarcador nunca saem no log (só
contagens); rotas, portos e NCMs saem.

Uso (na raiz do repositório virasana):
    python virasana/analytic_scripts/diagnostico_risco_ce.py
    python virasana/analytic_scripts/diagnostico_risco_ce.py --dias-populacao 90
"""
import argparse
import sys
import time
from collections import Counter
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy import create_engine

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts import dados_risco_perdimentos as drp  # noqa: E402
from virasana.analytic_scripts.diagnostico_risco_perdimentos import TIPOS_RESULTADO, Diagnostico  # noqa: E402
from virasana.navii.fichas_ovr import TIPOS_OPERACAO  # noqa: E402
from virasana.risco_historico import calculo, fichas_positivas, sinais  # noqa: E402

TABELAS_MERCANTE = ['conhecimentosresumo', 'ncmitemresumo', 'manifestosresumo', 'itensresumo']
FAIXAS_TG = [(0, 'até 10 mil'), (10e3, '10 a 50 mil'), (50e3, '50 a 100 mil'), (100e3, '100 a 500 mil'),
             (500e3, '500 mil a 1 mi'), (1e6, '1 a 10 mi'), (10e6, 'acima de 10 mi')]
FAIXAS_PESO = [(0, '0 (não conta)'), (1e-9, 'até 1'), (1, '1 a 2'), (2, '2 a 3'), (3, '3 a 5'), (5, '5 ou mais')]
FAIXAS_HORAS = [(-1e9, 'antes'), (0, 'até 1 h'), (1, '1 h a 1 dia'), (24, '1 a 7 dias'), (168, '7 a 30 dias'),
                (720, 'mais de 30 dias')]
FAIXAS_DIAS_EMISSAO = [(-1e9, 'antes da emissão'), (0, 'no dia'), (1, '1 dia'), (2, '2 a 7 dias'),
                       (8, '8 a 30 dias'), (31, 'mais de 30 dias')]
CONTEXTO = ('rota', 'porto_baldeacao', 'pais_ncm', 'ncm_declarado')
IDENTIDADE = ('importador', 'embarcador', sinais.CITADO)
PERCENTIS = (0.5, 0.9, 0.95, 0.99, 0.999)


def faixa(valor: float, faixas) -> str:
    """Rótulo da última faixa cujo limite inferior é <= valor."""
    rotulo = faixas[0][1]
    for limite, nome in faixas:
        if valor >= limite:
            rotulo = nome
    return rotulo


def distribuicao(valores, faixas) -> List[tuple]:
    contagem = Counter(faixa(v, faixas) for v in valores)
    return [(nome, contagem[nome]) for _, nome in faixas]


def percentil(ordenados: List[float], q: float) -> float:
    return ordenados[int(q * (len(ordenados) - 1))] if ordenados else 0.0


def _data(valor) -> Optional[datetime]:
    if valor is None or isinstance(valor, datetime):
        return valor
    try:
        return datetime.fromisoformat(str(valor)[:19])
    except ValueError:
        return None


def _nome_operacao(tipo) -> str:
    if tipo is None:
        return '(sem tipo)'
    return f'{tipo} {TIPOS_OPERACAO[tipo]}' if 0 <= tipo < len(TIPOS_OPERACAO) else str(tipo)


class DiagnosticoRiscoCE(Diagnostico):
    """Reaproveita a saída (escreve/titulo/tabela/consulta) do diagnóstico do risco histórico."""

    def __init__(self, conexao, saida, agora: datetime, dias_populacao: int = 30, dias_incremento: int = 30,
                 limiar: float = fichas_positivas.TG_ALTO_VALOR):
        self.conexao, self.saida, self.agora = conexao, saida, agora
        self.mysql = conexao.dialect.name == 'mysql'
        self.params: Dict = {}
        self.dias_populacao, self.dias_incremento, self.limiar = dias_populacao, dias_incremento, limiar
        self.hist: Optional[calculo.Historico] = None

    # ---------------------------------------------------------------- 1
    def secao_estrutura(self):
        self.titulo('1. ESTRUTURA DAS TABELAS DO MERCANTE (índices, charset)')
        if not self.mysql:
            self.escreve('(SHOW CREATE TABLE só no MariaDB/MySQL)')
            return
        lista = ', '.join(f"'{t}'" for t in TABELAS_MERCANTE)
        self.tabela(['tabela', 'linhas (estimativa)', 'dados_mb', 'indices_mb'], self.consulta(
            f'SELECT TABLE_NAME, TABLE_ROWS, ROUND(DATA_LENGTH/1048576), ROUND(INDEX_LENGTH/1048576) '  # nosec
            f'FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ({lista})'))
        for tabela in TABELAS_MERCANTE:
            # nomes de tabela vêm da constante TABELAS_MERCANTE
            for linha in self.consulta(f'SHOW CREATE TABLE {tabela}'):  # nosec B608
                self.escreve()
                self.escreve(linha[1])

    # ---------------------------------------------------------------- 2
    def secao_operacao(self):
        self.titulo('2. TIPO DE OPERAÇÃO x RESULTADO (todas as fichas com resultado)')
        linhas = self.consulta(
            'SELECT o.tipooperacao, r.tipo_resultado, COUNT(DISTINCT r.ovr_id), SUM(r.valor) '
            'FROM ovr_resultados r JOIN ovr_ovrs o ON o.id = r.ovr_id '
            'GROUP BY o.tipooperacao, r.tipo_resultado ORDER BY o.tipooperacao, r.tipo_resultado')
        excluidos, positivos = fichas_positivas.TIPOS_OPERACAO_EXCLUIDOS, fichas_positivas.RESULTADOS_POSITIVOS
        self.tabela(['tipo de operação', 'resultado', 'fichas', 'valor', 'conta?'], [
            (_nome_operacao(tipo), f'{resultado} {TIPOS_RESULTADO.get(resultado, "?")}', fichas, round(valor or 0, 2),
             'sim' if tipo not in excluidos and resultado in positivos else '')
            for tipo, resultado, fichas, valor in linhas])

    # ---------------------------------------------------------------- 3
    def secao_tgs(self):
        self.titulo(f'3. VALOR DOS TGs POR FICHA (limiar de alto valor: R$ {self.limiar:,.0f})')
        # só constantes inteiras entram no texto do SQL
        positivos = ', '.join(str(int(r)) for r in fichas_positivas.RESULTADOS_POSITIVOS)
        linhas = self.consulta(
            f'SELECT o.id, o.tipooperacao, SUM(t.valor), '  # nosec B608
            f'CASE WHEN EXISTS (SELECT 1 FROM ovr_resultados r WHERE r.ovr_id = o.id '
            f'AND r.tipo_resultado IN ({positivos})) THEN 1 ELSE 0 END '
            f'FROM ovr_tgovr t JOIN ovr_ovrs o ON o.id = t.ovr_id GROUP BY o.id, o.tipooperacao')
        excluidos = fichas_positivas.TIPOS_OPERACAO_EXCLUIDOS
        grupos = {'todas': [], 'importação sem FMA': [], '  com resultado positivo': [],
                  '  sem resultado positivo': [], 'FMA': []}
        for _, tipo, valor, com_resultado in linhas:
            valor = float(valor or 0)
            grupos['todas'].append(valor)
            if tipo == 0:
                grupos['FMA'].append(valor)
            elif tipo not in excluidos:
                grupos['importação sem FMA'].append(valor)
                grupos['  com resultado positivo' if com_resultado else '  sem resultado positivo'].append(valor)
        colunas = list(grupos)
        dist = {nome: dict(distribuicao(valores, FAIXAS_TG)) for nome, valores in grupos.items()}
        self.tabela(['faixa (R$)'] + colunas, [[nome] + [dist[c][nome] for c in colunas] for _, nome in FAIXAS_TG])
        self.escreve()
        self.escreve('Fichas com TG: ' + ', '.join(f'{nome.strip()} {len(v)}' for nome, v in grupos.items()))

    # ---------------------------------------------------------------- 4
    def secao_universo(self):
        self.titulo('4. FICHAS POSITIVAS (resultado 2/3/6/7 ou TG de alto valor; sem FMA nem exportação)')
        fichas = fichas_positivas.busca(self.conexao, limiar=self.limiar)
        por_ano: Dict[str, Counter] = {}
        for ficha in fichas:
            ano = str(ficha['datahora'])[:4] if ficha['datahora'] else '?'
            por_ano.setdefault(ano, Counter())[ficha['motivo']] += 1
        self.tabela(['ano', 'só resultado', 'só TG alto', 'ambos', 'total'],
                    [(ano, c['resultado'], c['tg'], c['ambos'], sum(c.values())) for ano, c in sorted(por_ano.items())])
        motivos = Counter(f['motivo'] for f in fichas)
        self.escreve()
        self.escreve(f'Total: {len(fichas)} fichas (só resultado {motivos["resultado"]}, só TG alto {motivos["tg"]}, '
                     f'ambos {motivos["ambos"]}); soma dos pesos {sum(f["peso"] for f in fichas):.0f}')
        extracao = drp.extrai(self.conexao, fichas_positivas.RESULTADOS_POSITIVOS, fichas=fichas)
        self.escreve(f'Descartadas por terem só CEs de exportação: {extracao["descartadas_exportacao"]}; '
                     f'com CE: {sum(1 for f in extracao["fichas"] if f["ces"])} de {len(extracao["fichas"])}; '
                     f'CEs: {extracao["ces"]} ({extracao["ces_no_mercante"]} no Mercante)')
        self.hist = calculo.historico(extracao['fichas'])
        self.escreve(f'K (soma dos pesos das fichas que ensinam): {self.hist.total:.0f}')
        self.escreve()
        linhas = []
        for dimensao in sinais.DIMENSOES:
            fichas_dim = self.hist.fichas[dimensao]
            com_sinal = len({ficha for ids in fichas_dim.values() for ficha in ids})
            repetidos = sum(1 for ids in fichas_dim.values() if len(ids) >= 2)
            linhas.append((dimensao, com_sinal, len(fichas_dim), repetidos))
        self.tabela(['dimensão', 'fichas com sinal', 'valores', 'em 2+ fichas'], linhas)

    # ---------------------------------------------------------------- 5
    def secao_populacao(self):
        desde = self.agora - timedelta(days=self.dias_populacao)
        self.titulo(f'5. POPULAÇÃO: CEs criados desde {desde:%Y-%m-%d} ({self.dias_populacao} dias, sem exportação)')
        sinais_ces, tempos = sinais.sinais_do_periodo(self.conexao, desde, self.agora)
        contagens, total_n = sinais.conta(sinais_ces), len(sinais_ces)
        escala = 365 / self.dias_populacao
        self.tabela(['etapa', 'segundos', 'estimativa p/ 365 dias'],
                    [(nome, f'{s:.1f}', f'{s * escala:.0f}') for nome, s in tempos.items()])
        self.escreve()
        self.escreve(f'N (CEs na população): {total_n}')
        if not total_n or self.hist is None:
            return
        pesos = calculo.tabela_de_pesos(self.hist, contagens, total_n)
        self._presenca(contagens, pesos)
        self._previa_contexto(pesos)
        self._previa_identidade(pesos)
        self._previa_pontos(sinais_ces, pesos)

    def _presenca(self, contagens, pesos):
        self.escreve()
        self.tabela(['dimensão', 'valores na população', 'do histórico presentes', 'com peso > 0'], [
            (d, len(contagens[d]), sum(1 for v in pesos[d] if contagens[d].get(v)),
             sum(1 for v, p in pesos[d].items() if p.peso > 0 and contagens[d].get(v))) for d in sinais.DIMENSOES])

    def _previa_contexto(self, pesos):
        for dimensao in CONTEXTO:
            melhores = sorted(((p.peso, p.vezes, v, p) for v, p in pesos[dimensao].items() if p.n),
                              reverse=True)[:10]
            self.escreve()
            self.escreve(f'Prévia — maiores pesos de {dimensao} (presentes na população):')
            self.tabela(['valor', 'descrição', 'fichas', 'k', 'CEs', '% dos CEs', 'x média', 'peso'], [
                (v, drp.descreve(dimensao, v, {})[:50], p.fichas, f'{p.k:.0f}', p.n,
                 f'{100 * p.n / p.total_n:.3f}', f'{p.vezes:.1f}', f'{p.peso:.2f}') for _, _, v, p in melhores])

    def _previa_identidade(self, pesos):
        self.escreve()
        self.escreve('Prévia — pesos dos valores de identidade presentes na população (valores não exibidos):')
        dist = {d: dict(distribuicao([p.peso for p in pesos[d].values() if p.n], FAIXAS_PESO)) for d in IDENTIDADE}
        self.tabela(['peso'] + list(IDENTIDADE), [[nome] + [dist[d][nome] for d in IDENTIDADE]
                                                  for _, nome in FAIXAS_PESO])

    def _previa_pontos(self, sinais_ces, pesos):
        pontos, por_dimensao = [], Counter()
        for sinais_ce in sinais_ces.values():
            total, parcelas = calculo.pontua(sinais.pontuaveis(sinais_ce), pesos)
            pontos.append(total)
            por_dimensao.update(dimensao for dimensao, _, _ in parcelas)
        ordenados = sorted(pontos)
        self.escreve()
        self.escreve(f'Prévia dos pontos: {sum(1 for p in pontos if p > 0)} de {len(pontos)} CEs com algum sinal')
        self.tabela(['percentil', 'pontos'], [(f'{100 * q:g}%', f'{percentil(ordenados, q):.2f}') for q in PERCENTIS]
                    + [('máximo', f'{ordenados[-1]:.2f}')])
        self.escreve()
        self.tabela(['dimensão', 'CEs em que conta'], [(d, por_dimensao[d]) for d in sinais.DIMENSOES])

    # ---------------------------------------------------------------- 6
    def secao_incremento(self):
        desde = self.agora - timedelta(days=self.dias_incremento)
        self.titulo(f'6. INCREMENTO: CEs criados desde {desde:%Y-%m-%d} ({self.dias_incremento} dias)')
        params = {'desde': desde}
        dias = self.consulta(
            'SELECT SUBSTR(CAST(create_date AS CHAR), 1, 10) AS dia, COUNT(*), '
            "SUM(CASE WHEN tipoTrafego = '7' THEN 1 ELSE 0 END), MIN(ID), MAX(ID) FROM conhecimentosresumo "
            'WHERE create_date >= :desde GROUP BY SUBSTR(CAST(create_date AS CHAR), 1, 10) ORDER BY 1', params)
        self.tabela(['dia', 'CEs', 'de exportação', 'menor ID', 'maior ID'], dias)
        if not dias:
            return
        total = sum(int(d[1]) for d in dias)
        self.escreve()
        self.escreve(f'{total} CEs em {len(dias)} dias (média {total / len(dias):.0f} por dia)')
        primeiro = self.numero_p('SELECT MIN(ID) FROM conhecimentosresumo WHERE create_date >= :desde', params)
        fora = self.numero_p('SELECT COUNT(*) FROM conhecimentosresumo WHERE ID >= :marca AND create_date < :desde',
                             {**params, 'marca': primeiro})
        self.escreve(f'Marca inicial (primeiro ID desde {desde:%Y-%m-%d}): {primeiro}; '
                     f'IDs depois dela criados ANTES da data (fora de ordem): {fora}')
        self._atrasos(desde)

    def numero_p(self, sql, params) -> int:
        linhas = self.consulta(sql, params)
        return int(linhas[0][0] or 0) if linhas else 0

    def _atrasos(self, desde):
        ces = self.consulta('SELECT numeroCEmercante, dataEmissao, create_date, last_modified, tipoTrafego '
                            'FROM conhecimentosresumo WHERE create_date >= :desde', {'desde': desde})
        ncms = dict(self.consulta('SELECT numeroCEMercante, MIN(create_date) FROM ncmitemresumo '
                                  'WHERE create_date >= :desde GROUP BY numeroCEMercante',
                                  {'desde': desde - sinais.MARGEM_NCM}))
        ncms = {(numero or '').strip(): _data(data) for numero, data in ncms.items()}
        emissao, retificacao, ncm, sem_ncm = [], [], [], 0
        for numero, data_emissao, criado, modificado, trafego in ces:
            criado, modificado, emitido = _data(criado), _data(modificado), _data(data_emissao)
            if emitido and criado:
                emissao.append((criado.date() - emitido.date()).days)
            if modificado and criado:
                retificacao.append((modificado - criado).total_seconds() / 3600)
            if trafego != '7':
                if ncms.get(numero) and criado:
                    ncm.append((ncms[numero] - criado).total_seconds() / 3600)
                else:
                    sem_ncm += 1
        self.escreve()
        self.escreve('Chegada do CE ao AJNA (create_date) depois da emissão (dataEmissao):')
        self.tabela(['atraso', 'CEs'], distribuicao(emissao, FAIXAS_DIAS_EMISSAO))
        self.escreve()
        self.escreve(f'Retificações (last_modified depois da criação): {len(retificacao)} de {len(ces)} CEs')
        self.tabela(['depois da criação', 'CEs'], distribuicao(retificacao, FAIXAS_HORAS))
        self.escreve()
        self.escreve(f'Primeira NCM do CE (sem exportação) depois da criação do CE; sem NCM: {sem_ncm}')
        self.tabela(['depois da criação', 'CEs'], distribuicao(ncm, FAIXAS_HORAS))


def _argumentos(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--dias-populacao', type=int, default=30, help='CEs dos últimos N dias (padrão 30)')
    parser.add_argument('--dias-incremento', type=int, default=30, help='seção 6: últimos N dias (padrão 30)')
    parser.add_argument('--limiar-tg', type=float, default=fichas_positivas.TG_ALTO_VALOR,
                        help='TG de alto valor, em R$ (padrão 100000)')
    parser.add_argument('--sem-populacao', action='store_true', help='pula a seção 5 (a mais pesada)')
    parser.add_argument('--saida', default=datetime.now().strftime('risco_ce_diagnostico_%Y%m%d_%H%M%S.log'))
    return parser.parse_args(argv)


def roda(conexao, saida, args, agora: datetime, url: str = ''):
    diag = DiagnosticoRiscoCE(conexao, saida, agora, args.dias_populacao, args.dias_incremento, args.limiar_tg)
    diag.escreve(f'Diagnóstico F0 do risco histórico por CE — {agora:%Y-%m-%d %H:%M:%S}')
    diag.escreve(f'Banco: {url}')
    secoes = [diag.secao_estrutura, diag.secao_operacao, diag.secao_tgs, diag.secao_universo]
    secoes += [] if args.sem_populacao else [diag.secao_populacao]
    for secao in secoes + [diag.secao_incremento]:
        inicio = time.perf_counter()
        secao()
        diag.escreve(f'   [seção em {time.perf_counter() - inicio:.1f} s]')
        saida.flush()


def main(argv=None):  # pragma: no cover - depende do banco real
    args = _argumentos(argv)
    uri = args.uri
    if not uri:
        from ajna_commons.flask.conf import SQL_URI as uri
    engine = create_engine(uri)
    with engine.connect() as conexao, open(args.saida, 'w', encoding='utf-8') as saida:
        roda(conexao, saida, args, datetime.now(), engine.url.render_as_string(hide_password=True))
    print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
