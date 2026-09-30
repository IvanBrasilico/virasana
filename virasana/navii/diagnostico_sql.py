# coding: utf-8
"""
Diagnóstico de SQL do NAVI-I: estrutura das tabelas + EXPLAIN das buscas.

Grava em um arquivo de log, para avaliar desenho de tabelas e índices:

  1. Versão do MySQL.
  2. `SHOW CREATE TABLE` e volume aproximado (information_schema) de cada
     tabela usada pelo NAVI-I.
  3. Para uma lista de buscas simuladas (combinações de janela e filtros da
     tela): o SQL gerado e o `EXPLAIN` da query da página (ORDER BY/LIMIT), da
     contagem e das queries de enriquecimento dos cards.

As queries são montadas pelas MESMAS funções da tela (`monta_query_navii` e
as de enriquecimento em `busca_navii.py`) — o log sempre reflete o código atual.

Somente leitura: roda apenas SHOW/SELECT em information_schema/EXPLAIN. O
EXPLAIN simples não executa a consulta (atenção: MySQL <= 5.6 materializa
subqueries do FROM durante o EXPLAIN). `--analyze` EXECUTA as consultas de
verdade para medir tempos e linhas reais (`ANALYZE FORMAT=JSON` no MariaDB,
`EXPLAIN ANALYZE` no MySQL >= 8.0.18) — use com critério, de preferência em
homologação ou fora do horário de pico.

Uso (a partir da raiz do repo virasana):

    python virasana/navii/diagnostico_sql.py
    python virasana/navii/diagnostico_sql.py --saida navii_diag.log --inicio 2026-09-01 --fim 2026-09-15
    python virasana/navii/diagnostico_sql.py --amostras   # inclui amostras de formato de dados
"""
import argparse
import sys
from datetime import datetime

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from virasana.navii.busca_navii import (
    _periodo_navii, busca_ciclos_navii, busca_itens_navii,
    busca_manifestos_navii, busca_nomes_empresas, busca_nomes_recintos,
    monta_query_navii,
)
from virasana.integracao.mercante.mercantealchemy import Item

TABELAS = [
    'itensresumo', 'conhecimentosresumo', 'ncmitemresumo', 'manifestosresumo',
    'escalamanifestoresumo', 'escalasresumo',
    'ciclodevida_ciclo_de_vida_importacao',
    'ciclodevida_ciclo_de_vida_importacao_escaneamentos',
    'ovr_recintos', 'laudo_empresas', 'laudo_ncm', 'Enumerado',
]

# Buscas simuladas: (descrição, filtros). Cada uma roda nas duas janelas.
# Os valores são ilustrativos — ajuste para códigos que existam na base para
# que o otimizador estime cardinalidades realistas.
CENARIOS = [
    ('sem filtros', {}),
    ('somente com imagem de escaneamento', {'com_imagem': True}),
    ('somente entregues ao importador', {'concluido': True}),
    ('NCM por prefixo (capítulo)', {'ncm': '8471'}),
    ('NCM completo', {'ncm': '84713012'}),
    ('produto (texto livre)', {'produto': 'brinquedo'}),
    ('NCM + produto', {'ncm': '9503', 'produto': 'brinquedo'}),
    ('porto de origem', {'porto_origem': 'CNSHA'}),
    ('porto de destino', {'porto_destino': 'BRSSZ'}),
    ('porto de baldeação', {'porto_baldeacao': 'SGSIN'}),
    ('recinto', {'recinto': '8931356'}),
    ('empresa (texto livre)', {'empresa': 'comercio'}),
    ('empresa por CNPJ', {'empresa': '40260836000140'}),
    ('até 1 NCM distinto', {'ncms_max': 1}),
    ('recinto + com imagem', {'recinto': '8931356', 'com_imagem': True}),
    ('origem + NCM + até 3 NCMs', {'porto_origem': 'CNSHA', 'ncm': '8471', 'ncms_max': 3}),
    ('recinto + empresa + entregue', {'recinto': '8931356', 'empresa': 'comercio', 'concluido': True}),
]

# Valores fictícios para o EXPLAIN das queries de enriquecimento da página
# (20 CEs por página na tela).
CES_FICTICIOS = ['%015d' % n for n in range(152600000000001, 152600000000021)]
MANIFESTOS_FICTICIOS = ['%013d' % n for n in range(1526000000001, 1526000000021)]


class SessaoCapturadora:
    """Finge ser a sessão para as funções de enriquecimento: captura a query
    montada (sem executar) e devolve lista vazia."""

    def __init__(self, session):
        self._session = session
        self.capturadas = []

    def query(self, *entidades):
        capturadas = self.capturadas
        q = self._session.query(*entidades)

        class QueryCapturada:
            def __init__(self, query):
                self._query = query

            def __getattr__(self, nome):
                metodo = getattr(self._query, nome)

                def encadeia(*args, **kwargs):
                    return QueryCapturada(metodo(*args, **kwargs))
                return encadeia

            def all(self):
                capturadas.append(self._query.statement)
                return []

        return QueryCapturada(q)

    def execute(self, statement, *args, **kwargs):
        self.capturadas.append(statement)

        class ResultadoVazio:
            def all(self):
                return []

        return ResultadoVazio()


class Diagnostico:
    def __init__(self, engine, saida, analyze=False):
        self.engine = engine
        self.conn = engine.connect()
        self.saida = saida
        self.analyze = analyze
        self.comando_analyze = 'EXPLAIN ANALYZE '

    def escreve(self, texto=''):
        self.saida.write(texto + '\n')

    def titulo(self, texto, nivel='='):
        self.escreve()
        self.escreve(nivel * 100)
        self.escreve(texto)
        self.escreve(nivel * 100)

    def tabela(self, resultado):
        """Escreve um resultado (cursor) como tabela de texto."""
        colunas = list(resultado.keys())
        linhas = [['' if v is None else str(v) for v in linha] for linha in resultado]
        larguras = [max([len(c)] + [len(linha[i]) for linha in linhas]) for i, c in enumerate(colunas)]
        self.escreve(' | '.join(c.ljust(larguras[i]) for i, c in enumerate(colunas)))
        self.escreve('-+-'.join('-' * largura for largura in larguras))
        for linha in linhas:
            self.escreve(' | '.join(v.ljust(larguras[i]) for i, v in enumerate(linha)))

    def executa(self, sql, params=None):
        """Executa SQL já no formato do driver e grava o resultado como tabela."""
        try:
            if params:
                resultado = self.conn.exec_driver_sql(sql, params)
            else:
                resultado = self.conn.exec_driver_sql(sql)
            self.tabela(resultado)
        except Exception as err:  # noqa: o log deve seguir mesmo se um item falhar
            self.escreve(f'!! ERRO executando [{sql[:80]}...]: {err}')

    def compila(self, statement):
        """(sql, params) no formato do driver do banco, com as listas de IN expandidas."""
        compiled = statement.compile(dialect=self.engine.dialect, compile_kwargs={'render_postcompile': True})
        if compiled.positional:
            return compiled.string, tuple(compiled.params[nome] for nome in compiled.positiontup)
        return compiled.string, dict(compiled.params)

    @staticmethod
    def sql_legivel(sql, params):
        """SQL com os valores embutidos — só para leitura no log (o EXPLAIN roda
        com parâmetros de verdade, como a aplicação)."""
        def literal(valor):
            if valor is None:
                return 'NULL'
            if isinstance(valor, bool):
                return '1' if valor else '0'
            if isinstance(valor, (int, float)):
                return str(valor)
            if isinstance(valor, datetime):
                valor = valor.strftime('%Y-%m-%d %H:%M:%S')
            return "'" + str(valor).replace("'", "''") + "'"

        if isinstance(params, dict):
            return sql % {nome: literal(valor) for nome, valor in params.items()}
        if '?' in sql:  # paramstyle qmark (ex.: SQLite)
            partes = sql.split('?')
            valores = [literal(valor) for valor in params] + ['']
            return ''.join(parte + valor for parte, valor in zip(partes, valores))
        return sql % tuple(literal(valor) for valor in params)

    def explain(self, descricao, statement):
        self.titulo(descricao, '-')
        try:
            sql, params = self.compila(statement)
            self.escreve(self.sql_legivel(sql, params))
        except Exception as err:  # noqa
            self.escreve(f'!! ERRO compilando a query: {err}')
            return
        self.escreve()
        self.escreve('EXPLAIN:')
        self.executa('EXPLAIN ' + sql, params)
        if self.analyze:
            self.escreve()
            self.escreve(self.comando_analyze.strip() + ' (executa a consulta):')
            self.executa(self.comando_analyze + sql, params)

    # ---------------------------------------------------------------- seções

    def secao_versao(self):
        self.titulo('1. VERSÃO DO BANCO')
        self.executa('SELECT VERSION() AS versao, DATABASE() AS banco, NOW() AS agora')
        try:
            versao = self.conn.exec_driver_sql('SELECT VERSION()').scalar() or ''
        except Exception:  # noqa
            versao = ''
        if 'mariadb' in versao.lower():
            self.comando_analyze = 'ANALYZE FORMAT=JSON '

    def secao_tabelas(self):
        self.titulo('2. ESTRUTURA DAS TABELAS (SHOW CREATE TABLE)')
        self.escreve('Volume aproximado (information_schema — estimativa do InnoDB, não é COUNT exato):')
        self.executa(
            'SELECT TABLE_NAME, ENGINE, TABLE_ROWS, '
            'ROUND(DATA_LENGTH/1024/1024) AS dados_mb, ROUND(INDEX_LENGTH/1024/1024) AS indices_mb, '
            'TABLE_COLLATION '
            'FROM information_schema.TABLES '
            'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN :tabelas '
            'ORDER BY TABLE_ROWS DESC'.replace(':tabelas', '(' + ', '.join(f"'{t}'" for t in TABELAS) + ')')
        )
        for tabela in TABELAS:
            self.titulo(tabela, '-')
            try:
                linha = self.conn.exec_driver_sql(f'SHOW CREATE TABLE `{tabela}`').fetchone()
                self.escreve(linha[1])
            except Exception as err:  # noqa
                self.escreve(f'!! ERRO: {err}')

    def secao_amostras(self):
        self.titulo('3. AMOSTRAS DE FORMATO DOS DADOS (opcional: --amostras)')
        self.escreve('Formato das datas da escala (VARCHAR) — a busca compara como texto YYYY-MM-DD:')
        self.executa('SELECT numeroDaEscala, dataPrevistaAtracacao, dataEfetivaPrimeiraAtracacao, embarcacao '
                     'FROM escalasresumo ORDER BY ID DESC LIMIT 5')
        self.escreve()
        self.escreve('Códigos de recinto (qual coluna casa com o ciclo de vida / com o terminal do manifesto):')
        self.executa('SELECT id, nome, cod_siscomex, cod_carga, cod_dte FROM ovr_recintos LIMIT 40')
        self.escreve()
        self.executa('SELECT codigoRecintoDesembarque, codigoRecintoArmazenagem, numeroCEmercante, numeroConteiner, '
                     'data_desembarque, concluido FROM ciclodevida_ciclo_de_vida_importacao ORDER BY ID DESC LIMIT 5')
        self.escreve()
        self.executa('SELECT numero, codigoTerminalDescarregamento, portoCarregamento, portoDescarregamento '
                     'FROM manifestosresumo ORDER BY ID DESC LIMIT 5')
        self.escreve()
        self.executa('SELECT numeroCEmercante, codigoConteiner, tipoItemCarga, pesoBruto '
                     'FROM itensresumo ORDER BY ID DESC LIMIT 5')

    def secao_buscas(self, session, periodos):
        self.titulo('4. BUSCAS SIMULADAS (SQL gerado pela tela + EXPLAIN)')
        for tipo_janela in ('efetiva', 'prevista'):
            for descricao, filtros in CENARIOS:
                filtros = dict(filtros)
                inicio, fim = periodos.get(tipo_janela) or (None, None)
                if inicio and fim:
                    filtros.update(data_inicio=inicio, data_fim=fim)
                periodo = _periodo_navii(tipo_janela, filtros)
                self.titulo(f'[{tipo_janela}] {descricao} — período {periodo[0]} a {periodo[1]} — filtros={filtros}')
                q = monta_query_navii(session, tipo_janela, filtros)
                self.explain('PÁGINA (20 cards, ORDER BY Item.ID DESC)',
                             q.order_by(Item.ID.desc()).limit(20).statement)
                self.explain('PÁGINA SEGUINTE (cursor)',
                             q.filter(Item.ID < 10 ** 9).order_by(Item.ID.desc()).limit(20).statement)
                self.explain('CONTAGEM TOTAL',
                             q.with_entities(func.count(Item.ID)).statement)

    def secao_enriquecimento(self, session):
        self.titulo('5. QUERIES DE ENRIQUECIMENTO DOS CARDS (por página, valores fictícios)')
        capturadora = SessaoCapturadora(session)
        itens_ficticios = [Item(numeroCEmercante=ce, codigoConteiner='ABCD1234567') for ce in CES_FICTICIOS]
        chamadas = [
            ('busca_ciclos_navii', lambda: busca_ciclos_navii(capturadora, itens_ficticios)),
            ('busca_manifestos_navii', lambda: busca_manifestos_navii(capturadora, MANIFESTOS_FICTICIOS)),
            ('busca_itens_navii', lambda: busca_itens_navii(capturadora, CES_FICTICIOS)),
            ('busca_nomes_empresas', lambda: busca_nomes_empresas(capturadora, CES_FICTICIOS)),
            ('busca_nomes_recintos (cod_siscomex)', lambda: busca_nomes_recintos(capturadora, ['8931356', '8931359'])),
            ('busca_nomes_recintos (cod_carga)',
             lambda: busca_nomes_recintos(capturadora, ['8931356', '8931359'], campo='cod_carga')),
        ]
        for nome, chamada in chamadas:
            capturadora.capturadas.clear()
            try:
                chamada()
            except Exception as err:  # noqa
                self.escreve(f'!! ERRO capturando {nome}: {err}')
                continue
            for i, statement in enumerate(capturadora.capturadas, 1):
                self.explain(f'{nome} — query {i}', statement)
        self.escreve()
        self.escreve('Obs.: busca_itens_navii só consulta Enumerado/laudo_ncm quando há itens/NCMs na página;')
        self.escreve('e os escaneamentos do ciclo (lazy="selectin") são 1 query por página, por ciclo_de_vida_id.')


def main():
    from ajna_commons.flask.conf import SQL_URI

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', default=SQL_URI, help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--saida', default=datetime.now().strftime('navii_diagnostico_%Y%m%d_%H%M%S.log'),
                        help='arquivo de log a gravar')
    parser.add_argument('--inicio', help="início do período das buscas simuladas ('YYYY-MM-DD'); "
                                         'padrão: a janela sugerida pela tela')
    parser.add_argument('--fim', help="fim do período das buscas simuladas ('YYYY-MM-DD')")
    parser.add_argument('--amostras', action='store_true',
                        help='inclui 5 linhas de amostra das tabelas (formato de datas/códigos)')
    parser.add_argument('--analyze', action='store_true',
                        help='mede tempos/linhas reais (ANALYZE FORMAT=JSON no MariaDB, EXPLAIN ANALYZE '
                             'no MySQL >= 8.0.18) — EXECUTA as consultas')
    args = parser.parse_args()
    if bool(args.inicio) != bool(args.fim):
        parser.error('--inicio e --fim precisam vir juntos')

    engine = create_engine(args.uri)
    session = sessionmaker(bind=engine)()
    periodo = (args.inicio, args.fim) if args.inicio else None
    with open(args.saida, 'w', encoding='utf-8') as saida:
        diag = Diagnostico(engine, saida, analyze=args.analyze)
        diag.escreve(f'Diagnóstico de SQL do NAVI-I — gerado em {datetime.now():%Y-%m-%d %H:%M:%S}')
        diag.escreve(f'Banco: {engine.url.render_as_string(hide_password=True)}')
        diag.secao_versao()
        diag.secao_tabelas()
        if args.amostras:
            diag.secao_amostras()
        diag.secao_buscas(session, {'efetiva': periodo, 'prevista': periodo})
        diag.secao_enriquecimento(session)
    print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
