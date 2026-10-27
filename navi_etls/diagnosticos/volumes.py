# coding: utf-8
"""
VOLUMES DAS ENTRADAS DE DADOS (NAVI-ETL, F0.4) — só leitura; roda na estação ou no servidor com o venv do virasana.

Mede o que chega pelas entradas que o NAVI-ETL vai copiar (`navi_etls/INVENTARIO.md`) e o espaço que ocupa — para
as metas de desempenho da modernização (F3) e para as melhorias anotadas no `PLANO.md` (o XML lido na hora do BSON;
o Mercante sem as tabelas RAW):

  E. ESPAÇO: por tabela, linhas (estimativa do InnoDB) e MB de dados e índices, em grupos (Mercante RAW × resumo,
     API Recintos, DUE, CNPJ, DTA); `fs.files` e `fs.chunks` no Mongo;
  R. API RECINTOS: eventos por dia (gravação, `create_date`) e por recinto; LOTES (rajadas de gravação: o commit de
     um POST) por dia e eventos por lote, nos últimos --dias-detalhe dias; atraso da transmissão à gravação (último
     dia); rejeições por dia e motivo; ponteiros do controle da extração;
  I. IMAGENS: `fs.files` por dia (`uploadDate`, em UTC) e por ORIGEM — Avatar (`metadata.idcov`), XML do Avatar
     (`text/xml`), API Recintos (`metadata.datahoratransmissao`), outra; tamanho; por recinto; ENVIOS do Avatar
     (rajadas: um BSON) e arquivos por envio; imagens do Avatar com `metadata.xml`; o que o passo do XML do
     periódico procura de novo a cada volta (a imagem da API Recintos não tem XML);
  M. MERCANTE: arquivos baixados do Aniita por dia e tipo; linhas gravadas nas tabelas RAW por dia e movimento
     (I/A/E); o que ainda falta resumir (`controleresumo`);
  D. DUE, CNPJ E DTA: DUEs por mês de registro (as tabelas não têm data de gravação); colunas de data da
     `navii_dtas` e linhas por mês.

Nada grava. Consultas com `max_statement_time` (MariaDB) e `maxTimeMS` (Mongo), por data indexada ou pela chave.
Uma seção que falha (tabela que não existe, tempo esgotado) não derruba as outras.

Uso (na raiz do repositório virasana, com SQL_URI e MONGODB_URI no ambiente, como os outros diagnósticos):
    virasana-venv/Scripts/python.exe navi_etls/diagnosticos/volumes.py --dias 30 --saida volumes.log
"""
import argparse
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import bindparam, create_engine, event, inspect, text

DIAS, DIAS_DETALHE, LIMITE = 30, 3, 300
TOP = 15
INTERVALO_LOTE = 3                # s: gravações seguidas com até 3 s entre si numa tabela = um lote (um POST)
INTERVALO_ENVIO = 30              # s: arquivos do Avatar com até 30 s entre si = um envio (um BSON)
AMOSTRA_ATRASO = 200000           # eventos lidos (do último dia) para o atraso da transmissão à gravação
DIAS_XML = 5                      # a janela do passo do XML no periódico (periodic_updates: `cincodias`)
MESES = 12                        # DUE e DTA por mês
MB = 1024 * 1024
EVENTOS = {'1': 'apirecintos_acessosveiculo', '3': 'apirecintos_pesagensveiculo',
           '4': 'apirecintos_embarquedesembarque', '25': 'apirecintos_inspecoesnaoinvasivas'}
REJEITADOS, CONTROLE = 'apirecintos_registros_rejeitados', 'apirecintos_controle_extracao'
RAW_MERCANTE = ('conhecimentosEmbarque', 'manifestosCarga', 'itensCarga', 'NCMItemCarga', 'ConteinerVazio',
                'EscalaManifesto', 'exclusoesEscala', 'escala')
RESUMO_MERCANTE = ('conhecimentosresumo', 'itensresumo', 'ncmitemresumo', 'manifestosresumo', 'escalasresumo',
                   'manifestosescalasresumo', 'conteinervazioresumo', 'escalamanifestoresumo')
ARQUIVOS, CONTROLE_RESUMO = 'arquivosbaixados', 'controleresumo'
DUE, DTA = 'pucomex_due', 'navii_dtas'
GRUPOS = (('Mercante RAW', RAW_MERCANTE), ('Mercante resumo', RESUMO_MERCANTE),
          ('Mercante controle', (ARQUIVOS, CONTROLE_RESUMO)),
          ('API Recintos', tuple(EVENTOS.values()) + (REJEITADOS, CONTROLE)),
          ('DUE', (DUE, 'pucomex_due_itens', 'pucomex_due_conteiner')), ('CNPJ', ('laudo_empresas',)),
          ('DTA', (DTA,)))
ORIGENS = ('Avatar', 'XML do Avatar', 'API Recintos', 'outra')
MOVIMENTOS = ('I', 'A', 'E')
SECOES = ('espaco', 'recintos', 'imagens', 'mercante', 'due')
IDENTIFICADOR = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


# ---------------------------------------------------------------------- funções puras
def br(valor, casas: int = 0) -> str:
    """Número no formato brasileiro (1.234,5); '-' para None."""
    if valor is None:
        return '-'
    return f'{valor:,.{casas}f}'.replace(',', '_').replace('.', ',').replace('_', '.')


def percentil(ordenados: Sequence[float], fracao: float) -> Optional[float]:
    """O valor na posição `fracao` (0 a 1) de uma lista ordenada (o mais próximo); None se vazia."""
    if not ordenados:
        return None
    return ordenados[min(len(ordenados) - 1, max(0, round(fracao * (len(ordenados) - 1))))]


def distribuicao(valores: Iterable[float]) -> Tuple[int, Optional[float], Optional[float], Optional[float]]:
    """(quantidade, mediana, p95, máximo)."""
    ordenados = sorted(valores)
    maximo = ordenados[-1] if ordenados else None
    return len(ordenados), percentil(ordenados, 0.5), percentil(ordenados, 0.95), maximo


def como_data(valor) -> Optional[datetime]:
    """datetime do que o banco devolve (o SQLite dos testes devolve texto)."""
    if valor is None or isinstance(valor, datetime):
        return valor
    if isinstance(valor, date):
        return datetime(valor.year, valor.month, valor.day)
    return datetime.fromisoformat(str(valor))


def dia(valor) -> str:
    data = como_data(valor)
    return f'{data:%Y-%m-%d}' if data else '?'


def rajadas(pares: Iterable[Tuple[datetime, int]], intervalo: float) -> List[Tuple[datetime, int]]:
    """(início, quantidade) das rajadas: momentos em ordem com até `intervalo` s entre um e o seguinte.

    `pares` = (momento, quantidade naquele momento) — p. ex. o COUNT por segundo de `create_date`."""
    grupos: List[List] = []
    anterior = None
    for momento, quantidade in sorted((como_data(momento), quantidade) for momento, quantidade in pares):
        if anterior is None or (momento - anterior).total_seconds() > intervalo:
            grupos.append([momento, 0])
        grupos[-1][1] += quantidade
        anterior = momento
    return [(inicio, total) for inicio, total in grupos]


def origem(documento: dict) -> str:
    """De onde veio um arquivo do GridFS, pelo `metadata` que cada entrada grava."""
    metadata = documento.get('metadata') or {}
    if metadata.get('contentType') == 'text/xml':
        return 'XML do Avatar'
    if 'idcov' in metadata:
        return 'Avatar'
    if 'datahoratransmissao' in metadata:
        return 'API Recintos'
    return 'outra'


def tipo_do_arquivo(nome: str) -> str:
    """MERCANTE_CONHECIMENTO_2026-01-14-13-12-00.XML -> CONHECIMENTO."""
    partes = (nome or '').split('_')
    return partes[1] if len(partes) > 2 and partes[0].upper() == 'MERCANTE' else (nome or '?')[:20]


def nome_do_banco(uri: str) -> str:
    """O banco do Mongo pela URI, como o ajna_commons (o último trecho do caminho; 'test' sem ele)."""
    resto = uri.split('?')[0].split('//', 1)[-1]
    return (resto.split('/', 1)[1] if '/' in resto else '') or 'test'


def monta(modelo: str, **nomes: str) -> str:
    """O SQL com os nomes de tabela e de coluna no lugar de `{nome}`. Eles vêm das listas fixas do topo ou da
    estrutura da própria tabela, nunca de fora; mesmo assim, só passa identificador simples (letras, dígitos e _)."""
    for nome in nomes.values():
        if not IDENTIFICADOR.match(nome):
            raise ValueError(f'identificador inválido no SQL: {nome!r}')
    return modelo.format(**nomes)


def mascara(uri: Optional[str]) -> str:
    return re.sub(r'(//[^:/@\s]+:)\S*@', r'\1***@', uri or '')


# ---------------------------------------------------------------------- relatório
class Relatorio:

    def __init__(self, saida):
        self.saida = saida

    def escreve(self, texto: str = ''):
        print(texto, file=self.saida, flush=True)

    def titulo(self, texto: str):
        self.escreve()
        self.escreve('=' * 110)
        self.escreve(texto)
        self.escreve('=' * 110)

    def tabela(self, cabecalho: Sequence[str], linhas: Sequence[Sequence]):
        if not linhas:
            self.escreve('(nada)')
            return
        texto = [[str(celula) for celula in linha] for linha in linhas]
        larguras = [max(len(celula) for celula in coluna) for coluna in zip(cabecalho, *texto)]
        self.escreve(' | '.join(celula.ljust(largura) for celula, largura in zip(cabecalho, larguras)))
        self.escreve('-+-'.join('-' * largura for largura in larguras))
        for linha in texto:
            self.escreve(' | '.join(celula.ljust(largura) for celula, largura in zip(linha, larguras)))


def _ouvinte_limite(segundos: float) -> Callable:
    """Ouvinte do 'connect': limita o tempo de cada consulta (MariaDB; no MySQL, max_execution_time)."""
    def limita(conexao_dbapi, _registro):
        cursor = conexao_dbapi.cursor()
        try:
            cursor.execute('SET SESSION max_statement_time = %s', (float(segundos),))
        except Exception:  # não é MariaDB: tenta o nome do MySQL
            try:
                cursor.execute('SET SESSION max_execution_time = %s', (int(segundos * 1000),))
            except Exception:  # sem limite: avisa e segue
                print('aviso: o banco não aceitou limite de tempo por consulta', file=sys.stderr)
        finally:
            cursor.close()
    return limita


def engine_sql(uri: str, limite: float):
    engine = create_engine(uri, pool_pre_ping=True)
    if limite and engine.dialect.name in ('mysql', 'mariadb'):
        event.listen(engine, 'connect', _ouvinte_limite(limite))
    return engine


# ---------------------------------------------------------------------- diagnóstico
class Volumes:

    def __init__(self, engine, db, saida, ate: datetime, dias: int = DIAS, dias_detalhe: int = DIAS_DETALHE,
                 limite: float = LIMITE):
        self.engine, self.db, self.r = engine, db, Relatorio(saida)
        self.ate, self.dias = ate, dias
        self.desde = ate - timedelta(days=dias)
        self.desde_detalhe = ate - timedelta(days=dias_detalhe)
        self.max_ms = int(limite * 1000) if limite else None
        self.tabelas = set(inspect(engine).get_table_names()) if engine is not None else set()
        self.tempos: Dict[str, float] = {}

    # ------------------------------------------------------------------ acesso
    def sql(self, modelo: str, nomes: Optional[Dict[str, str]] = None, **parametros) -> list:
        """Linhas de uma consulta; `nomes` = tabelas e colunas do modelo (ver `monta`), `parametros` = valores."""
        with self.engine.connect() as conexao:
            return conexao.execute(text(monta(modelo, **(nomes or {}))), parametros).fetchall()

    def existe(self, tabela: str) -> bool:
        return tabela in self.tabelas

    def _janela(self) -> dict:
        return {'desde': self.desde, 'ate': self.ate}

    def _por_dia_sql(self, tabela: str, extra: str = '') -> List[tuple]:
        """(dia, [extra,] quantidade) da tabela na janela, pela data de gravação."""
        if extra:
            return self.sql('SELECT DATE(create_date), {extra}, COUNT(*) FROM {tabela} WHERE create_date >= :desde '
                            'AND create_date < :ate GROUP BY DATE(create_date), {extra}',
                            {'tabela': tabela, 'extra': extra}, **self._janela())
        return self.sql('SELECT DATE(create_date), COUNT(*) FROM {tabela} WHERE create_date >= :desde '
                        'AND create_date < :ate GROUP BY DATE(create_date)', {'tabela': tabela}, **self._janela())

    def roda_secao(self, nome: str, funcao: Callable):
        """Uma seção; o erro é relatado e as outras continuam."""
        inicio = time.time()
        try:
            funcao()
        except Exception as erro:  # uma seção não derruba o diagnóstico
            self.r.escreve(f'!! ERRO em {nome}: {type(erro).__name__}: {str(erro)[:300]}')
        self.tempos[nome] = time.time() - inicio

    # ------------------------------------------------------------------ E. espaço
    def _medidas(self, nomes: Sequence[str]) -> Dict[str, Tuple]:
        """{tabela: (linhas, bytes de dados, bytes de índices)} das que existem."""
        if self.engine.dialect.name in ('mysql', 'mariadb'):
            consulta = text('SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH FROM information_schema.TABLES '
                            'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN :nomes'
                            ).bindparams(bindparam('nomes', expanding=True))
            with self.engine.connect() as conexao:
                return {nome: (linhas, dados, indices)
                        for nome, linhas, dados, indices in conexao.execute(consulta, {'nomes': list(nomes)})}
        # outros bancos (os testes): só a contagem
        return {nome: (self.sql('SELECT COUNT(*) FROM {tabela}', {'tabela': nome})[0][0], None, None)
                for nome in nomes if self.existe(nome)}

    def espaco(self):
        self.r.titulo('E. ESPAÇO — por tabela (linhas: estimativa do InnoDB; MB de dados e de índices) e no Mongo')
        medidas = self._medidas([nome for _, grupo in GRUPOS for nome in grupo])
        linhas, totais = [], []
        for grupo, tabelas in GRUPOS:
            soma = [0, 0]
            for nome in tabelas:
                if nome not in medidas:
                    linhas.append((grupo, nome, 'não existe', '', '', ''))
                    continue
                quantidade, dados, indices = medidas[nome]
                total = (dados or 0) + (indices or 0)
                soma[0] += quantidade or 0
                soma[1] += total
                linhas.append((grupo, nome, br(quantidade), br((dados or 0) / MB, 1), br((indices or 0) / MB, 1),
                               br(total / MB, 1)))
            totais.append((grupo, br(soma[0]), br(soma[1] / MB, 1)))
        self.r.tabela(['grupo', 'tabela', 'linhas', 'dados (MB)', 'índices (MB)', 'total (MB)'], linhas)
        self.r.escreve()
        self.r.tabela(['grupo', 'linhas', 'total (MB)'], totais)
        self._espaco_mongo()

    def _espaco_mongo(self):
        if self.db is None:
            return
        linhas = []
        for colecao in ('fs.files', 'fs.chunks'):
            try:
                estatistica = self.db.command('collstats', colecao)
                linhas.append((colecao, br(estatistica.get('count')), br(estatistica.get('size', 0) / MB),
                               br(estatistica.get('storageSize', 0) / MB),
                               br(estatistica.get('totalIndexSize', 0) / MB)))
            except Exception as erro:  # sem permissão ou sem o comando (mongomock)
                linhas.append((colecao, f'(sem estatística: {type(erro).__name__})', '', '', ''))
        self.r.escreve()
        self.r.tabela(['coleção', 'documentos', 'dados (MB)', 'em disco (MB)', 'índices (MB)'], linhas)

    # ------------------------------------------------------------------ R. API Recintos
    def recintos(self):
        self.r.titulo(f'R. API RECINTOS — eventos gravados de {self.desde:%d/%m/%Y} a {self.ate:%d/%m/%Y}')
        tabelas = {tipo: tabela for tipo, tabela in EVENTOS.items() if self.existe(tabela)}
        faltam = [tabela for tabela in EVENTOS.values() if tabela not in tabelas.values()]
        if faltam:
            self.r.escreve(f'(não existem: {", ".join(faltam)})')
        self._eventos_por_dia(tabelas)
        self._eventos_por_recinto(tabelas)
        self._lotes(tabelas)
        self._atraso(tabelas)
        self._rejeicoes()
        self._ponteiros()

    def _eventos_por_dia(self, tabelas: Dict[str, str]):
        por_dia = defaultdict(Counter)
        for tipo, tabela in tabelas.items():
            for data, quantidade in self._por_dia_sql(tabela):
                por_dia[dia(data)][tipo] += quantidade
        linhas = [[data] + [br(por_dia[data][tipo]) for tipo in tabelas] + [br(sum(por_dia[data].values()))]
                  for data in sorted(por_dia)]
        totais = Counter()
        for contagem in por_dia.values():
            totais.update(contagem)
        if por_dia:
            linhas.append(['média por dia'] + [br(totais[tipo] / len(por_dia)) for tipo in tabelas] +
                          [br(sum(totais.values()) / len(por_dia))])
        self.r.escreve()
        self.r.escreve('Eventos por dia de gravação (create_date), por tipo de evento:')
        self.r.tabela(['dia'] + [f'tipo {tipo}' for tipo in tabelas] + ['total'], linhas)

    def _eventos_por_recinto(self, tabelas: Dict[str, str]):
        por_recinto = defaultdict(Counter)
        for tipo, tabela in tabelas.items():
            for recinto, quantidade in self.sql('SELECT codigoRecinto, COUNT(*) FROM {tabela} '
                                                'WHERE create_date >= :desde AND create_date < :ate '
                                                'GROUP BY codigoRecinto', {'tabela': tabela}, **self._janela()):
                por_recinto[recinto or '(vazio)'][tipo] += quantidade
        ordem = sorted(por_recinto, key=lambda recinto: -sum(por_recinto[recinto].values()))
        linhas = [[recinto] + [br(por_recinto[recinto][tipo]) for tipo in tabelas] +
                  [br(sum(por_recinto[recinto].values()))] for recinto in ordem[:TOP]]
        self.r.escreve()
        self.r.escreve(f'Eventos por recinto na janela ({len(por_recinto)} recintos; os {TOP} maiores):')
        self.r.tabela(['recinto'] + [f'tipo {tipo}' for tipo in tabelas] + ['total'], linhas)

    def _lotes(self, tabelas: Dict[str, str]):
        linhas = []
        for tipo, tabela in tabelas.items():
            pares = self.sql('SELECT create_date, COUNT(*) FROM {tabela} '
                             'WHERE create_date >= :desde AND create_date < :ate GROUP BY create_date',
                             {'tabela': tabela}, desde=self.desde_detalhe, ate=self.ate)
            lotes = rajadas(pares, INTERVALO_LOTE)
            por_dia = Counter(dia(inicio) for inicio, _ in lotes)
            quantidade, mediana, p95, maximo = distribuicao(total for _, total in lotes)
            linhas.append((f'tipo {tipo}', br(quantidade), br(quantidade / len(por_dia), 1) if por_dia else '-',
                           br(mediana), br(p95), br(maximo)))
        self.r.escreve()
        self.r.escreve(f'Lotes (rajadas de gravação com até {INTERVALO_LOTE} s entre si: aproximadamente um POST) '
                       f'desde {self.desde_detalhe:%d/%m/%Y %H:%M}:')
        self.r.tabela(['tipo', 'lotes', 'lotes por dia', 'eventos por lote: mediana', 'p95', 'máximo'], linhas)

    def _atraso(self, tabelas: Dict[str, str]):
        linhas = []
        for tipo, tabela in tabelas.items():
            pares = self.sql('SELECT dataHoraTransmissao, create_date FROM {tabela} '
                             'WHERE create_date >= :desde ORDER BY create_date DESC LIMIT :limite',
                             {'tabela': tabela}, desde=self.ate - timedelta(days=1), limite=AMOSTRA_ATRASO)
            minutos = [(como_data(gravado) - como_data(transmitido)).total_seconds() / 60
                       for transmitido, gravado in pares if transmitido and gravado]
            quantidade, mediana, p95, maximo = distribuicao(minutos)
            negativos = sum(1 for valor in minutos if valor < 0)
            linhas.append((f'tipo {tipo}', br(quantidade), br(mediana, 1), br(p95, 1), br(maximo, 1), br(negativos)))
        self.r.escreve()
        self.r.escreve('Atraso da transmissão (dataHoraTransmissao) à gravação (create_date), em minutos, no último '
                       'dia (negativo: relógio ou fuso diferentes):')
        self.r.tabela(['tipo', 'eventos', 'mediana', 'p95', 'máximo', 'negativos'], linhas)

    def _rejeicoes(self):
        if not self.existe(REJEITADOS):
            self.r.escreve(f'({REJEITADOS} não existe)')
            return
        por_motivo, por_dia = Counter(), Counter()
        for data, motivo, quantidade in self._por_dia_sql(REJEITADOS, 'motivo'):
            por_motivo[motivo] += quantidade
            por_dia[dia(data)] += quantidade
        self.r.escreve()
        self.r.escreve('Rejeições (apirecintos_registros_rejeitados) por motivo e por dia:')
        self.r.tabela(['motivo', 'rejeições'], [(motivo, br(n)) for motivo, n in por_motivo.most_common()])
        self.r.tabela(['dia', 'rejeições'], [(data, br(por_dia[data])) for data in sorted(por_dia)])

    def _ponteiros(self):
        if not self.existe(CONTROLE):
            self.r.escreve(f'({CONTROLE} não existe)')
            return
        por_tipo = defaultdict(list)
        for recinto, tipo, ultima in self.sql('SELECT codigoRecinto, tipoEvento, ultimaDataPesquisada '
                                              'FROM {tabela}', {'tabela': CONTROLE}):
            if ultima:
                por_tipo[str(tipo)].append(((self.ate - como_data(ultima)).total_seconds() / 3600, recinto))
        linhas = []
        for tipo in sorted(por_tipo, key=lambda valor: (len(valor), valor)):
            atrasos = sorted(por_tipo[tipo], reverse=True)
            _, mediana, _, maximo = distribuicao(horas for horas, _ in atrasos)
            linhas.append((tipo, len(atrasos), br(mediana, 1), br(maximo, 1),
                           '; '.join(f'{recinto} ({br(horas)} h)' for horas, recinto in atrasos[:5])))
        self.r.escreve()
        self.r.escreve('Ponteiros do controle da extração (tipo 99 = imagens), atraso em horas até o fim da janela:')
        self.r.tabela(['tipo', 'recintos', 'atraso: mediana', 'máximo', 'os mais atrasados'], linhas)

    # ------------------------------------------------------------------ I. imagens
    def _le_fs_files(self) -> List[tuple]:
        """(uploadDate, origem, tamanho, recinto, tem XML) dos arquivos da janela (só os campos que precisa)."""
        projecao = {'uploadDate': 1, 'length': 1, 'metadata.idcov': 1, 'metadata.datahoratransmissao': 1,
                    'metadata.contentType': 1, 'metadata.recinto': 1, 'metadata.xml.container': 1}
        cursor = self.db['fs.files'].find({'uploadDate': {'$gte': self.desde, '$lt': self.ate}}, projecao)
        if self.max_ms:
            cursor = cursor.max_time_ms(self.max_ms)
        arquivos = []
        for documento in cursor.batch_size(5000):
            metadata = documento.get('metadata') or {}
            arquivos.append((documento.get('uploadDate'), origem(documento), documento.get('length') or 0,
                             metadata.get('recinto') or '(vazio)', 'xml' in metadata))
        return arquivos

    def imagens(self):
        self.r.titulo(f'I. IMAGENS — fs.files por dia (uploadDate, UTC) de {self.desde:%d/%m/%Y} a {self.ate:%d/%m/%Y}')
        if self.db is None:
            self.r.escreve('(sem MONGODB_URI)')
            return
        arquivos = self._le_fs_files()
        self._imagens_por_dia(arquivos)
        self._tamanhos(arquivos)
        self._imagens_por_recinto(arquivos)
        self._envios_do_avatar(arquivos)
        self._xml_das_imagens(arquivos)
        self._pendentes_do_xml()

    def _imagens_por_dia(self, arquivos: List[tuple]):
        por_dia, bytes_dia = defaultdict(Counter), Counter()
        for enviado, de_onde, tamanho, _, _ in arquivos:
            por_dia[dia(enviado)][de_onde] += 1
            bytes_dia[dia(enviado)] += tamanho
        linhas = [[data] + [br(por_dia[data][nome]) for nome in ORIGENS] +
                  [br(sum(por_dia[data].values())), br(bytes_dia[data] / MB)] for data in sorted(por_dia)]
        self.r.escreve()
        self.r.escreve('Arquivos por dia e origem (Avatar: metadata.idcov; API Recintos: '
                       'metadata.datahoratransmissao):')
        self.r.tabela(['dia'] + list(ORIGENS) + ['total', 'MB'], linhas)

    def _tamanhos(self, arquivos: List[tuple]):
        linhas = []
        for nome in ORIGENS:
            tamanhos = [tamanho for _, de_onde, tamanho, _, _ in arquivos if de_onde == nome]
            quantidade, mediana, p95, maximo = distribuicao(tamanhos)
            linhas.append((nome, br(quantidade), br(sum(tamanhos) / MB), br((mediana or 0) / 1024),
                           br((p95 or 0) / 1024), br((maximo or 0) / 1024)))
        self.r.escreve()
        self.r.escreve('Tamanho por origem (KB por arquivo):')
        self.r.tabela(['origem', 'arquivos', 'MB', 'mediana (KB)', 'p95 (KB)', 'máximo (KB)'], linhas)

    def _imagens_por_recinto(self, arquivos: List[tuple]):
        por_recinto = defaultdict(Counter)
        for _, de_onde, _, recinto, _ in arquivos:
            if de_onde != 'XML do Avatar':
                por_recinto[recinto][de_onde] += 1
        ordem = sorted(por_recinto, key=lambda recinto: -sum(por_recinto[recinto].values()))
        nomes = [nome for nome in ORIGENS if nome != 'XML do Avatar']
        linhas = [[recinto] + [br(por_recinto[recinto][nome]) for nome in nomes] +
                  [br(sum(por_recinto[recinto].values()))] for recinto in ordem[:TOP]]
        self.r.escreve()
        self.r.escreve(f'Imagens por recinto (metadata.recinto; {len(por_recinto)} recintos; os {TOP} maiores):')
        self.r.tabela(['recinto'] + nomes + ['total'], linhas)

    def _envios_do_avatar(self, arquivos: List[tuple]):
        envios = rajadas(((enviado, 1) for enviado, de_onde, _, _, _ in arquivos
                          if de_onde in ('Avatar', 'XML do Avatar') and enviado), INTERVALO_ENVIO)
        por_dia = Counter(dia(inicio) for inicio, _ in envios)
        quantidade, mediana, p95, maximo = distribuicao(total for _, total in envios)
        self.r.escreve()
        self.r.escreve(f'Envios do Avatar (rajadas de arquivos com até {INTERVALO_ENVIO} s entre si: '
                       'aproximadamente um BSON; imagem e XML contam):')
        self.r.tabela(['envios', 'envios por dia', 'arquivos por envio: mediana', 'p95', 'máximo'],
                      [(br(quantidade), br(quantidade / len(por_dia), 1) if por_dia else '-', br(mediana), br(p95),
                        br(maximo))])

    def _xml_das_imagens(self, arquivos: List[tuple]):
        por_dia = defaultdict(Counter)
        for enviado, de_onde, _, _, tem_xml in arquivos:
            if de_onde == 'Avatar':
                por_dia[dia(enviado)]['imagens'] += 1
                por_dia[dia(enviado)]['com XML'] += tem_xml
        linhas = [(data, br(contagem['imagens']), br(contagem['com XML']),
                   br(100 * contagem['com XML'] / contagem['imagens'], 1))
                  for data, contagem in sorted(por_dia.items())]
        self.r.escreve()
        self.r.escreve('Imagens do Avatar com metadata.xml (gravado hoje pelo passo do XML do periódico):')
        self.r.tabela(['dia', 'imagens do Avatar', 'com XML', '%'], linhas)

    def _conta(self, filtro: dict) -> int:
        if self.max_ms:
            return self.db['fs.files'].count_documents(filtro, maxTimeMS=self.max_ms)
        return self.db['fs.files'].count_documents(filtro)

    def _pendentes_do_xml(self):
        """O que o passo do XML (xmli.dados_xml_grava_fsfiles) procura a cada volta: imagens sem metadata.xml com
        escaneamento nos últimos DIAS_XML dias — a da API Recintos nunca terá XML e volta todas as vezes."""
        base = {'metadata.xml': None, 'metadata.contentType': 'image/jpeg',
                'metadata.dataescaneamento': {'$gt': self.ate - timedelta(days=DIAS_XML)}}
        filtros = (('API Recintos', {'metadata.datahoratransmissao': {'$exists': True}}),
                   ('Avatar', {'metadata.idcov': {'$exists': True}}),
                   ('outra', {'metadata.datahoratransmissao': {'$exists': False},
                              'metadata.idcov': {'$exists': False}}))
        linhas = [(nome, br(self._conta({**base, **filtro}))) for nome, filtro in filtros]
        self.r.escreve()
        self.r.escreve(f'Candidatas do passo do XML a cada volta do periódico (sem metadata.xml, escaneadas nos '
                       f'últimos {DIAS_XML} dias; o passo lê até 10.000 por volta, a cada ~5 min):')
        self.r.tabela(['origem', 'imagens'], linhas)

    # ------------------------------------------------------------------ M. Mercante
    def mercante(self):
        self.r.titulo(f'M. MERCANTE — arquivos do Aniita, tabelas RAW e resumo, de {self.desde:%d/%m/%Y} a '
                      f'{self.ate:%d/%m/%Y}')
        self._arquivos_baixados()
        self._raw_por_dia()
        self._falta_resumir()

    def _arquivos_baixados(self):
        if not self.existe(ARQUIVOS):
            self.r.escreve(f'({ARQUIVOS} não existe)')
            return
        por_dia = defaultdict(Counter)
        for criado, nome in self.sql('SELECT create_date, nome FROM {tabela} '
                                     'WHERE create_date >= :desde AND create_date < :ate', {'tabela': ARQUIVOS},
                                     **self._janela()):
            por_dia[dia(criado)][tipo_do_arquivo(nome)] += 1
        tipos = sorted({tipo for contagem in por_dia.values() for tipo in contagem})
        linhas = [[data] + [br(por_dia[data][tipo]) for tipo in tipos] + [br(sum(por_dia[data].values()))]
                  for data in sorted(por_dia)]
        self.r.escreve()
        self.r.escreve('Arquivos XML baixados do Aniita por dia (arquivosbaixados.create_date) e tipo:')
        self.r.tabela(['dia'] + tipos + ['total'], linhas)

    def _raw_por_dia(self):
        linhas = []
        for tabela in RAW_MERCANTE:
            if not self.existe(tabela):
                linhas.append((tabela, 'não existe', '', '', '', '', ''))
                continue
            movimentos, dias = Counter(), set()
            for data, movimento, quantidade in self._por_dia_sql(tabela, 'tipoMovimento'):
                movimentos[movimento] += quantidade
                dias.add(dia(data))
            total = sum(movimentos.values())
            linhas.append((tabela, *(br(movimentos[movimento]) for movimento in MOVIMENTOS), br(total),
                           br(total / len(dias)) if dias else '-',
                           br(100 * (movimentos['A'] + movimentos['E']) / total, 1) if total else '-'))
        self.r.escreve()
        self.r.escreve('Linhas gravadas nas tabelas RAW na janela, por movimento (I inclusão, A atualização, '
                       'E exclusão):')
        self.r.tabela(['tabela RAW', 'I', 'A', 'E', 'total', 'por dia', '% A+E'], linhas)

    def _falta_resumir(self):
        if not self.existe(CONTROLE_RESUMO):
            self.r.escreve(f'({CONTROLE_RESUMO} não existe)')
            return
        controle = {(str(nome), str(movimento)): maxid for nome, movimento, maxid in
                    self.sql('SELECT nomeclasse, tipomovimento, maxid FROM {tabela}', {'tabela': CONTROLE_RESUMO})}
        linhas = []
        for tabela in RAW_MERCANTE:
            if not self.existe(tabela):
                continue
            maior = self.sql('SELECT MAX(id) FROM {tabela}', {'tabela': tabela})[0][0]
            faltam = [self.sql('SELECT COUNT(*) FROM {tabela} WHERE id > :maxid AND tipoMovimento = :movimento',
                               {'tabela': tabela}, maxid=controle.get((tabela, movimento)) or 0,
                               movimento=movimento)[0][0]
                      for movimento in MOVIMENTOS]
            linhas.append((tabela, br(maior), *(br(controle.get((tabela, movimento))) for movimento in MOVIMENTOS),
                           *(br(valor) for valor in faltam)))
        self.r.escreve()
        self.r.escreve('O que falta resumir (linhas RAW acima do maxid do controleresumo, por movimento):')
        self.r.tabela(['tabela RAW', 'maior id', 'maxid I', 'maxid A', 'maxid E', 'faltam I', 'faltam A', 'faltam E'],
                      linhas)

    # ------------------------------------------------------------------ D. DUE, CNPJ, DTA
    def due(self):
        self.r.titulo('D. DUE, CNPJ E DTA (linhas totais na seção E)')
        desde_meses = self.ate - timedelta(days=31 * MESES)
        if self.existe(DUE):
            registros = self.sql('SELECT data_registro_due FROM {tabela} WHERE data_registro_due >= :desde',
                                 {'tabela': DUE}, desde=desde_meses.date())
            por_mes = Counter(f'{como_data(valor):%Y-%m}' for (valor,) in registros if valor)
            self.r.escreve()
            self.r.escreve(f'DUEs por mês de registro (data_registro_due), últimos {MESES} meses:')
            self.r.tabela(['mês', 'DUEs'], [(mes, br(por_mes[mes])) for mes in sorted(por_mes)])
        self._dta(desde_meses)

    def _dta(self, desde_meses: datetime):
        if not self.existe(DTA):
            self.r.escreve(f'({DTA} não existe)')
            return
        colunas = inspect(self.engine).get_columns(DTA)
        datas = [coluna['name'] for coluna in colunas if re.search('DATE|TIME', str(coluna['type']).upper())]
        self.r.escreve()
        self.r.escreve(f'{DTA}: {len(colunas)} colunas; de data: {", ".join(datas) or "nenhuma"}')
        for coluna in datas[:3]:
            # a coluna vem da estrutura da própria tabela
            registros = self.sql('SELECT {coluna} FROM {tabela} WHERE {coluna} >= :desde',
                                 {'tabela': DTA, 'coluna': coluna}, desde=desde_meses)
            por_mes = Counter(f'{como_data(valor):%Y-%m}' for (valor,) in registros if valor)
            self.r.escreve(f'{DTA} por mês de {coluna}:')
            self.r.tabela(['mês', 'linhas'], [(mes, br(por_mes[mes])) for mes in sorted(por_mes)])


def roda(volumes: Volumes, secoes: Sequence[str] = SECOES, sql_uri: str = '', mongo_uri: str = ''):
    volumes.r.titulo('VOLUMES DAS ENTRADAS DE DADOS (NAVI-ETL, F0.4) — só leitura')
    volumes.r.escreve(f'janela: {volumes.desde:%d/%m/%Y %H:%M} a {volumes.ate:%d/%m/%Y %H:%M} ({volumes.dias} dias); '
                      f'detalhe (lotes) desde {volumes.desde_detalhe:%d/%m/%Y %H:%M}')
    volumes.r.escreve(f'SQL: {mascara(sql_uri) or "(sem SQL_URI)"} | '
                      f'Mongo: {mascara(mongo_uri) or "(sem MONGODB_URI)"}')
    sql = {'espaco', 'recintos', 'mercante', 'due'}
    for secao in secoes:
        if secao in sql and volumes.engine is None:
            volumes.r.escreve(f'({secao}: sem SQL_URI)')
            continue
        volumes.roda_secao(secao, getattr(volumes, secao))
    volumes.r.titulo('TEMPOS')
    volumes.r.tabela(['seção', 'segundos'], [(nome, br(segundos, 1)) for nome, segundos in volumes.tempos.items()])


def _argumentos(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dias', type=int, default=DIAS, help=f'janela em dias até --ate (padrão {DIAS})')
    parser.add_argument('--dias-detalhe', type=int, default=DIAS_DETALHE,
                        help=f'dias dos lotes da API Recintos (padrão {DIAS_DETALHE})')
    parser.add_argument('--ate', help='fim da janela (AAAA-MM-DD, exclusivo; padrão: agora)')
    parser.add_argument('--secoes', nargs='+', choices=SECOES, default=list(SECOES), help='padrão: todas')
    parser.add_argument('--limite', type=float, default=LIMITE,
                        help=f'segundos por consulta, 0 = sem limite (padrão {LIMITE})')
    parser.add_argument('--sql-uri', help='padrão: SQL_URI do ambiente')
    parser.add_argument('--mongo-uri', help='padrão: MONGODB_URI do ambiente')
    parser.add_argument('--saida', help='grava o relatório neste arquivo (padrão: só na tela)')
    return parser.parse_args(argv)


def main(argv=None):  # pragma: no cover - depende das bases reais
    args = _argumentos(argv)
    ate = datetime.strptime(args.ate, '%Y-%m-%d') if args.ate else datetime.now()
    sql_uri = args.sql_uri or os.environ.get('SQL_URI', '')
    mongo_uri = args.mongo_uri or os.environ.get('MONGODB_URI', '')
    engine = engine_sql(sql_uri, args.limite) if sql_uri else None
    db = None
    if mongo_uri:
        from pymongo import MongoClient
        db = MongoClient(mongo_uri)[nome_do_banco(mongo_uri)]
    saida = open(args.saida, 'w', encoding='utf-8') if args.saida else sys.stdout
    try:
        roda(Volumes(engine, db, saida, ate, args.dias, args.dias_detalhe, args.limite), args.secoes, sql_uri,
             mongo_uri)
    finally:
        if args.saida:
            saida.close()


if __name__ == '__main__':  # pragma: no cover
    main()
