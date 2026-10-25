"""Diagnóstico de volumes das entradas de dados (navi_etls/diagnosticos/volumes.py), em SQLite e mongomock.

Janela até 24/10/2026 00:00 (30 dias); D = 23/10, D-1 = 22/10.
  - acessos (tipo 1): lote de 10 eventos em D 10:00:00–10:00:02 (8931359), lote de 5 em D 12:00 (8931404), lote de 3
    em D-1 09:00 (8931359) e 1 evento de setembro (fora da janela); transmissão 30 min antes da gravação;
    pesagens (tipo 3): 2 em D; embarques e inspeções: tabelas que não existem;
  - rejeições: 2 de placa e 1 de contêiner; ponteiros: tipo 1 do 8931359 (14 h de atraso) e 99 do 8931404 (72 h);
  - Mercante: 2 arquivos de CONHECIMENTO e 1 de ITEM em D; RAW conhecimentosEmbarque I, I, A, E (D-1), I com o
    controle em I=1 e A=3 (faltam 2 I e 1 E); itensCarga vazia;
  - DUE: 2 em out/2026, 1 em set/2026 e 1 de 2024 (fora dos 12 meses); DTA com duas colunas de data;
  - fs.files: envio do Avatar em D 08:00 (3 pares jpg + XML, 2 imagens com metadata.xml) e outro em D 15:00 (1 par
    sem XML); 2 imagens da API Recintos e 1 de outra origem, todas escaneadas em D-1 e sem XML.
"""
import io
import os
import sys
import unittest
from datetime import date, datetime, timedelta

import mongomock
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'diagnosticos'))
import volumes as vol  # noqa: E402

ATE = datetime(2026, 10, 24)
D, D1 = datetime(2026, 10, 23), datetime(2026, 10, 22)
DDL = [
    'CREATE TABLE apirecintos_acessosveiculo (id INTEGER PRIMARY KEY, codigoRecinto VARCHAR(7), '
    'dataHoraTransmissao DATETIME, create_date DATETIME)',
    'CREATE TABLE apirecintos_pesagensveiculo (id INTEGER PRIMARY KEY, codigoRecinto VARCHAR(7), '
    'dataHoraTransmissao DATETIME, create_date DATETIME)',
    'CREATE TABLE apirecintos_registros_rejeitados (id INTEGER PRIMARY KEY, motivo VARCHAR(30), create_date DATETIME)',
    'CREATE TABLE apirecintos_controle_extracao (id INTEGER PRIMARY KEY, codigoRecinto VARCHAR(7), '
    'tipoEvento VARCHAR(2), ultimaDataPesquisada DATETIME)',
    'CREATE TABLE arquivosbaixados (ID INTEGER PRIMARY KEY, nome VARCHAR(50), filename_date DATETIME, '
    'create_date DATETIME)',
    'CREATE TABLE "conhecimentosEmbarque" (id INTEGER PRIMARY KEY, "tipoMovimento" TEXT, create_date DATETIME)',
    'CREATE TABLE "itensCarga" (id INTEGER PRIMARY KEY, "tipoMovimento" TEXT, create_date DATETIME)',
    'CREATE TABLE controleresumo (ID INTEGER PRIMARY KEY, nomeclasse CHAR(50), tipomovimento CHAR(1), maxid BIGINT)',
    'CREATE TABLE conhecimentosresumo (ID INTEGER PRIMARY KEY, "numeroCEmercante" VARCHAR(15))',
    'CREATE TABLE pucomex_due (numero_due CHAR(14) PRIMARY KEY, data_registro_due DATE)',
    'CREATE TABLE navii_dtas (id INTEGER PRIMARY KEY, nr_ce VARCHAR(15), data_registro DATE, '
    'data_hora_atualizacao DATETIME)',
    'CREATE TABLE laudo_empresas (id INTEGER PRIMARY KEY, cnpj VARCHAR(15), nome VARCHAR(255))',
]


def evento(con, tabela, recinto, gravado):
    con.execute(text(f'INSERT INTO {tabela} (codigoRecinto, dataHoraTransmissao, create_date) '
                     'VALUES (:recinto, :transmitido, :gravado)'),
                {'recinto': recinto, 'transmitido': gravado - timedelta(minutes=30), 'gravado': gravado})


def eventos_api_recintos(con):
    acessos = 'apirecintos_acessosveiculo'
    for segundo, quantidade in ((0, 4), (1, 4), (2, 2)):
        for _ in range(quantidade):
            evento(con, acessos, '8931359', D + timedelta(hours=10, seconds=segundo))
    for _ in range(5):
        evento(con, acessos, '8931404', D + timedelta(hours=12))
    for _ in range(3):
        evento(con, acessos, '8931359', D1 + timedelta(hours=9))
    evento(con, acessos, '8931359', datetime(2026, 9, 1))
    for _ in range(2):
        evento(con, 'apirecintos_pesagensveiculo', '8931359', D + timedelta(hours=11))


def rejeicoes_e_ponteiros(con):
    for motivo in ('placa_invalida', 'placa_invalida', 'conteiner_invalido'):
        con.execute(text('INSERT INTO apirecintos_registros_rejeitados (motivo, create_date) VALUES (:m, :d)'),
                    {'m': motivo, 'd': D + timedelta(hours=10)})
    ponteiros = (('8931359', '1', D + timedelta(hours=10)), ('8931404', '99', datetime(2026, 10, 21)))
    for recinto, tipo, ultima in ponteiros:
        con.execute(text('INSERT INTO apirecintos_controle_extracao (codigoRecinto, tipoEvento, '
                         'ultimaDataPesquisada) VALUES (:r, :t, :u)'), {'r': recinto, 't': tipo, 'u': ultima})


def mercante(con):
    for nome in ('MERCANTE_CONHECIMENTO_2026-10-23-10-00-00.XML', 'MERCANTE_CONHECIMENTO_2026-10-23-10-10-00.XML',
                 'MERCANTE_ITEM_2026-10-23-10-00-00.XML'):
        con.execute(text('INSERT INTO arquivosbaixados (nome, filename_date, create_date) VALUES (:n, :d, :d)'),
                    {'n': nome, 'd': D + timedelta(hours=10)})
    for movimento, quando in (('I', D), ('I', D), ('A', D), ('E', D1), ('I', D)):
        con.execute(text('INSERT INTO "conhecimentosEmbarque" ("tipoMovimento", create_date) VALUES (:m, :d)'),
                    {'m': movimento, 'd': quando + timedelta(hours=8)})
    for movimento, maxid in (('I', 1), ('A', 3)):
        con.execute(text('INSERT INTO controleresumo (nomeclasse, tipomovimento, maxid) VALUES '
                         "('conhecimentosEmbarque', :m, :x)"), {'m': movimento, 'x': maxid})


def due_cnpj_dta(con):
    for numero, registro in (('26BR0000000001', date(2026, 10, 2)), ('26BR0000000002', date(2026, 10, 9)),
                             ('26BR0000000003', date(2026, 9, 15)), ('24BR0000000004', date(2024, 1, 10))):
        con.execute(text('INSERT INTO pucomex_due VALUES (:n, :r)'), {'n': numero, 'r': registro})
    for ce, registro in (('152605000000001', date(2026, 10, 1)), ('152605000000002', date(2026, 10, 5))):
        con.execute(text('INSERT INTO navii_dtas (nr_ce, data_registro, data_hora_atualizacao) VALUES '
                         '(:c, :r, :r)'), {'c': ce, 'r': registro})
    con.execute(text("INSERT INTO laudo_empresas (cnpj, nome) VALUES ('11222333', 'EMPRESA')"))


def monta_sql():
    engine = create_engine('sqlite://')
    with engine.begin() as con:
        for ddl in DDL:
            con.execute(text(ddl))
        for parte in (eventos_api_recintos, rejeicoes_e_ponteiros, mercante, due_cnpj_dta):
            parte(con)
    return engine


def monta_mongo():
    db = mongomock.MongoClient().db
    arquivos = []
    escaneado = D1 + timedelta(hours=20)
    for envio, pares, com_xml in ((D + timedelta(hours=8), 3, 2), (D + timedelta(hours=15), 1, 0)):
        for par in range(pares):
            enviado = envio + timedelta(seconds=2 * par)
            metadata = {'contentType': 'image/jpeg', 'idcov': str(par), 'recinto': 'BTP',
                        'dataescaneamento': escaneado}
            if par < com_xml:
                metadata['xml'] = {'container': 'MSCU1234566', 'alerta': False}
            arquivos.append({'uploadDate': enviado, 'length': 300 * 1024, 'metadata': metadata})
            arquivos.append({'uploadDate': enviado + timedelta(seconds=1), 'length': 5 * 1024,
                             'metadata': {'contentType': 'text/xml', 'idcov': str(par), 'recinto': 'BTP'}})
    for minuto in (0, 30):
        arquivos.append({'uploadDate': D + timedelta(hours=11, minutes=minuto), 'length': 200 * 1024,
                         'metadata': {'contentType': 'image/jpeg', 'datahoratransmissao': D, 'recinto': '8931359',
                                      'dataescaneamento': escaneado}})
    arquivos.append({'uploadDate': D + timedelta(hours=13), 'length': 100 * 1024,
                     'metadata': {'contentType': 'image/jpeg', 'recinto': 'OUTRO', 'dataescaneamento': escaneado}})
    arquivos.append({'uploadDate': datetime(2026, 9, 1), 'length': 1, 'metadata': {'idcov': 'velho'}})  # fora
    db['fs.files'].insert_many(arquivos)
    return db


class TestVolumes(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.saida = io.StringIO()
        cls.volumes = vol.Volumes(monta_sql(), monta_mongo(), cls.saida, ATE, dias=30, dias_detalhe=3, limite=0)
        vol.roda(cls.volumes, sql_uri='mysql+pymysql://ajna:segredo@servidor/mercante',
                 mongo_uri='mongodb://ajna:segredo@servidor/ajna')
        cls.log = cls.saida.getvalue()

    def linha(self, secao_inicio, *comeca):
        """As células da primeira linha de tabela cujas primeiras células são `comeca`, depois de `secao_inicio`."""
        for linha in self.log.split(secao_inicio, 1)[1].splitlines():
            celulas = [celula.strip() for celula in linha.split('|')]
            if celulas[:len(comeca)] == list(comeca):
                return celulas
        raise AssertionError(f'linha {comeca} não encontrada depois de {secao_inicio!r}')

    def test_sem_erros_e_sem_senha(self):
        self.assertNotIn('!! ERRO', self.log)
        self.assertNotIn('segredo', self.log)
        self.assertIn('mysql+pymysql://ajna:***@servidor/mercante', self.log)
        for titulo in ('E. ESPAÇO', 'R. API RECINTOS', 'I. IMAGENS', 'M. MERCANTE', 'D. DUE, CNPJ E DTA', 'TEMPOS'):
            with self.subTest(titulo=titulo):
                self.assertIn(titulo, self.log)

    def test_espaco(self):
        self.assertEqual(self.linha('E. ESPAÇO', 'API Recintos', 'apirecintos_acessosveiculo')[2], '19')
        self.assertIn('não existe', self.linha('E. ESPAÇO', 'API Recintos', 'apirecintos_embarquedesembarque'))
        self.assertEqual(self.linha('E. ESPAÇO', 'Mercante RAW', 'conhecimentosEmbarque')[2], '5')

    def test_eventos_por_dia_e_recinto(self):
        self.assertEqual(self.linha('Eventos por dia', '2026-10-23')[1:], ['15', '2', '17'])
        self.assertEqual(self.linha('Eventos por dia', '2026-10-22')[1:], ['3', '0', '3'])
        self.assertNotIn('2026-09-01', self.log.split('Eventos por dia')[1].split('Eventos por recinto')[0])
        self.assertEqual(self.linha('Eventos por recinto', '8931359')[1:], ['13', '2', '15'])

    def test_lotes_e_atraso(self):
        self.assertEqual(self.linha('Lotes (rajadas', 'tipo 1')[1:], ['3', '1,5', '5', '10', '10'])  # 10, 5 e 3
        self.assertEqual(self.linha('Atraso da transmissão', 'tipo 1')[1:4], ['15', '30,0', '30,0'])

    def test_rejeicoes_e_ponteiros(self):
        self.assertEqual(self.linha('Rejeições', 'placa_invalida')[1], '2')
        self.assertEqual(self.linha('Ponteiros do controle', '99')[1:4], ['1', '72,0', '72,0'])

    def test_imagens(self):
        self.assertEqual(self.linha('Arquivos por dia e origem', '2026-10-23')[1:6], ['4', '4', '2', '1', '11'])
        self.assertEqual(self.linha('Envios do Avatar', '2')[:5], ['2', '2,0', '2', '6', '6'])  # 6 e 2 arquivos
        self.assertEqual(self.linha('Imagens do Avatar com metadata.xml', '2026-10-23')[1:], ['4', '2', '50,0'])
        self.assertEqual(self.linha('Candidatas do passo do XML', 'API Recintos')[1], '2')
        self.assertEqual(self.linha('Candidatas do passo do XML', 'Avatar')[1], '2')
        self.assertEqual(self.linha('Candidatas do passo do XML', 'outra')[1], '1')

    def test_mercante(self):
        self.assertEqual(self.linha('Arquivos XML baixados', '2026-10-23')[1:], ['2', '1', '3'])
        raw = self.linha('Linhas gravadas nas tabelas RAW', 'conhecimentosEmbarque')
        self.assertEqual(raw[1:5] + raw[6:], ['3', '1', '1', '5', '40,0'])
        self.assertEqual(self.linha('O que falta resumir', 'conhecimentosEmbarque')[5:], ['2', '0', '1'])

    def test_due_e_dta(self):
        self.assertEqual(self.linha('DUEs por mês', '2026-10')[1], '2')
        self.assertEqual(self.linha('DUEs por mês', '2026-09')[1], '1')
        self.assertNotIn('2024-01', self.log)
        self.assertIn('de data: data_registro, data_hora_atualizacao', self.log)


class TestFuncoes(unittest.TestCase):

    def test_rajadas(self):
        base = datetime(2026, 10, 23, 10)
        pares = [(base, 4), (base + timedelta(seconds=2), 1), (base + timedelta(seconds=10), 3)]
        self.assertEqual(vol.rajadas(pares, 3), [(base, 5), (base + timedelta(seconds=10), 3)])
        self.assertEqual(vol.rajadas([('2026-10-23 10:00:00', 2)], 3), [(base, 2)])   # texto do SQLite

    def test_origem_tipo_banco_e_numeros(self):
        self.assertEqual([vol.origem(d) for d in ({'metadata': {'contentType': 'text/xml', 'idcov': '1'}},
                                                  {'metadata': {'idcov': '1'}},
                                                  {'metadata': {'datahoratransmissao': 1}}, {})],
                         ['XML do Avatar', 'Avatar', 'API Recintos', 'outra'])
        self.assertEqual(vol.tipo_do_arquivo('MERCANTE_ESCALA_2026-01-14-13-12-00.XML'), 'ESCALA')
        self.assertEqual(vol.nome_do_banco('mongodb://u:s@host:27017/ajna?authSource=admin'), 'ajna')
        self.assertEqual(vol.nome_do_banco('mongodb://host'), 'test')
        self.assertEqual((vol.br(1234567), vol.br(1234.56, 1), vol.br(None)), ('1.234.567', '1.234,6', '-'))
        self.assertEqual(vol.distribuicao([]), (0, None, None, None))

    def test_sql_so_com_identificador_simples(self):
        self.assertEqual(vol.monta('SELECT COUNT(*) FROM {tabela}', tabela='itensCarga'),
                         'SELECT COUNT(*) FROM itensCarga')
        with self.assertRaises(ValueError):
            vol.monta('SELECT COUNT(*) FROM {tabela}', tabela='x; DROP TABLE y')


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
