"""Health check (virasana/saude/, 23/10/2026): peças comuns e as FONTES — carga do Mercante, da API Recintos e das
imagens no MongoDB. SQLite em memória e mongomock; "agora" fixo (23/10/2026 07:00; no Mongo, em UTC, 10:00).

API Recintos: em cada tabela, o recinto 8931359 (BTP) transmite 60 eventos nas últimas 24 h e 60 nas mesmas 24 h da
semana anterior; nos acessos, o 8931356 (EMBRAPORT) transmitiu 60 na semana anterior e nada agora (parou), e o
8931404 só 10 (abaixo do mínimo: não conta).
"""
from virasana.navii.busca_navii import tabela_recintos  # 1º: ajusta o sys.path (ajna_commons, bhadrasana2)
import unittest  # noqa: I100
from datetime import datetime, timedelta
from unittest import mock

import mongomock
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from virasana.integracao.mercante.mercantealchemy import Conhecimento, Item
from virasana.saude import fontes, imagens
from virasana.saude.verificacao import (
    ATENCAO, FALHA, NAO_MEDIDO, OK, Contexto, Resultado, codigo_de_saida, executa, ha_quanto, relatorio,
)

AGORA = datetime(2026, 10, 23, 7, 0)
AGORA_UTC = AGORA + timedelta(hours=3)
BTP, EMBRAPORT, PEQUENO = '8931359', '8931356', '8931404'


def banco():
    engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
    Conhecimento.__table__.create(engine)
    Item.__table__.create(engine)
    tabela_recintos.create(engine)
    with engine.begin() as c:
        for _, tabela, _ in fontes.TABELAS_API:
            c.execute(text(f'CREATE TABLE {tabela} (id INTEGER PRIMARY KEY, codigoRecinto TEXT, '
                           'dataHoraTransmissao DATETIME)'))
        c.execute(text(f'CREATE TABLE {fontes.CONTROLE} (id INTEGER PRIMARY KEY, codigoRecinto TEXT, tipoEvento TEXT, '
                       'ultimaDataPesquisada DATETIME)'))
        c.execute(text(f'CREATE TABLE {fontes.REJEITADOS} (id INTEGER PRIMARY KEY, nomeTabela TEXT, motivo TEXT, '
                       'create_date DATETIME)'))
        c.execute(tabela_recintos.insert(), [{'id': 1, 'nome': 'BTP', 'cod_siscomex': BTP},
                                             {'id': 2, 'nome': 'EMBRAPORT', 'cod_siscomex': EMBRAPORT}])
    return engine


def eventos(engine, tabela, recinto, quantos, inicio):
    """`quantos` eventos transmitidos a partir de `inicio`, um a cada 10 min."""
    with engine.begin() as c:
        c.execute(text(f'INSERT INTO {tabela} (codigoRecinto, dataHoraTransmissao) VALUES (:r, :d)'),
                  [{'r': recinto, 'd': inicio + timedelta(minutes=10 * i)} for i in range(quantos)])


def ponteiro(engine, recinto, tipo, data):
    with engine.begin() as c:
        c.execute(text(f'INSERT INTO {fontes.CONTROLE} (codigoRecinto, tipoEvento, ultimaDataPesquisada) '
                       'VALUES (:r, :t, :d)'), {'r': recinto, 't': tipo, 'd': data})


def cenario_api(engine, recente=True):
    """BTP normal em todas as tabelas; EMBRAPORT parou nos acessos; PEQUENO abaixo do mínimo."""
    # 60 eventos, um a cada 10 min: os de agora vão até 1 h atrás
    ontem, semana = AGORA - timedelta(hours=11), AGORA - timedelta(days=8) + timedelta(hours=1)
    if not recente:   # tudo velho: o extrator parou há 2 dias
        ontem = AGORA - timedelta(days=2, hours=12)
    for tipo, tabela, _ in fontes.TABELAS_API:
        eventos(engine, tabela, BTP, 60, semana)
        eventos(engine, tabela, BTP, 60, ontem)
        ponteiro(engine, BTP, tipo, ontem + timedelta(hours=10, minutes=30))
    eventos(engine, 'apirecintos_acessosveiculo', EMBRAPORT, 60, semana)
    eventos(engine, 'apirecintos_acessosveiculo', PEQUENO, 10, semana)
    ponteiro(engine, EMBRAPORT, '1', AGORA - timedelta(hours=1))


class TestPecasComuns(unittest.TestCase):

    def test_erro_vira_falha_da_verificacao(self):
        def quebra(contexto):
            raise RuntimeError('MySQL server has gone away')
        resultado = executa('Mercante (carga)', quebra, Contexto(None, agora=AGORA))
        self.assertEqual((resultado.estado, resultado.resumo),
                         (FALHA, 'erro na verificação: RuntimeError: MySQL server has gone away'))

    def test_relatorio_e_codigo_de_saida(self):
        resultados = [Resultado('A', OK, 'tudo certo'), Resultado('B', ATENCAO, 'algo', ['detalhe 1']),
                      Resultado('C', NAO_MEDIDO, 'sem chave')]
        linhas = relatorio(resultados, AGORA)
        self.assertEqual(linhas[0], 'HEALTH CHECK do AJNA — 23/10/2026 07:00 — ATENÇÃO')
        self.assertIn('[ ATENÇÃO  ] B  algo', linhas)
        self.assertIn('  - detalhe 1', linhas)
        self.assertEqual(codigo_de_saida(resultados), 1)
        self.assertEqual(codigo_de_saida(resultados + [Resultado('D', FALHA, 'x')]), 2)
        self.assertEqual(codigo_de_saida([Resultado('C', NAO_MEDIDO, 'x')]), 0)

    def test_ha_quanto(self):
        self.assertEqual((ha_quanto(None), ha_quanto(3.25), ha_quanto(72)), ('sem registro', 'há 3,2 h', 'há 3,0 dias'))


class TestMercante(unittest.TestCase):

    def resultado(self, horas_ce, horas_item=1):
        engine = banco()
        with engine.begin() as c:
            if horas_ce is not None:
                c.execute(Conhecimento.__table__.insert(), {'numeroCEmercante': '1', 'create_date':
                                                            AGORA - timedelta(hours=horas_ce)})
            c.execute(Item.__table__.insert(), {'numeroCEmercante': '1', 'create_date':
                                                AGORA - timedelta(hours=horas_item)})
        return fontes.verifica_mercante(Contexto(engine, agora=AGORA))

    def test_em_dia_atrasado_e_parado(self):
        self.assertEqual(self.resultado(2).estado, OK)
        self.assertEqual(self.resultado(10).estado, ATENCAO)
        self.assertEqual(self.resultado(30).estado, FALHA)
        self.assertEqual(self.resultado(None).estado, FALHA)   # nenhum CE
        self.assertEqual(self.resultado(2).resumo, 'CE mais novo carregado há 2,0 h; item há 1,0 h')


class TestApiRecintos(unittest.TestCase):

    def test_recinto_que_parou_de_transmitir(self):
        engine = banco()
        cenario_api(engine)
        resultado = fontes.verifica_api_recintos(Contexto(engine, agora=AGORA))
        self.assertEqual(resultado.estado, ATENCAO)
        self.assertEqual(len(resultado.detalhes), 1)
        detalhe = resultado.detalhes[0]
        self.assertIn(f'acessos: recinto {EMBRAPORT} EMBRAPORT — 0 nas últimas 24 h, 60 nas mesmas 24 h da semana '
                      'anterior', detalhe)
        self.assertIn('ponteiro da extração há 1,0 h', detalhe)   # o extrator pergunta: o recinto não manda
        self.assertIn('acessos 60 (sem. ant. 130)', resultado.resumo)

    def test_extrator_parado(self):
        engine = banco()
        cenario_api(engine, recente=False)
        resultado = fontes.verifica_api_recintos(Contexto(engine, agora=AGORA))
        self.assertEqual(resultado.estado, FALHA)
        self.assertTrue(any('pesagens: nada transmitido nem pesquisado há' in d and 'extrator da API parado' in d
                            for d in resultado.detalhes))

    def test_salto_de_rejeicoes(self):
        engine = banco()
        cenario_api(engine)
        with engine.begin() as c:
            c.execute(text(f'INSERT INTO {fontes.REJEITADOS} (nomeTabela, motivo, create_date) VALUES (:t, :m, :d)'),
                      [{'t': 'apirecintos_pesagensveiculo', 'm': 'tamanho', 'd': AGORA - timedelta(hours=2)}] * 150
                      + [{'t': 'apirecintos_pesagensveiculo', 'm': 'tamanho', 'd': AGORA - timedelta(days=7, hours=2)}]
                      * 10)
        resultado = fontes.verifica_api_recintos(Contexto(engine, agora=AGORA))
        self.assertIn('rejeições em apirecintos_pesagensveiculo (tamanho): 150 nas últimas 24 h, 10 nas mesmas 24 h '
                      'da semana anterior', resultado.detalhes)


def fs_files(*documentos):
    mongo = mongomock.MongoClient().db
    if documentos:
        mongo['fs.files'].insert_many(list(documentos))
    return mongo


def imagem(recinto, chegada_utc, horas_de_atraso=1):
    return {'uploadDate': chegada_utc,
            'metadata': {'recinto': recinto, 'dataescaneamento': chegada_utc - timedelta(hours=horas_de_atraso)}}


class TestImagens(unittest.TestCase):

    def contexto(self, mongo):
        engine = banco()
        ponteiro(engine, '0', '99', AGORA - timedelta(minutes=30))
        return Contexto(engine, mongo=mongo, agora=AGORA, agora_utc=AGORA_UTC)

    def documentos(self):
        ontem, semana = AGORA_UTC - timedelta(hours=20), AGORA_UTC - timedelta(days=7, hours=20)
        normal = [imagem('BTP', ontem + timedelta(minutes=i)) for i in range(30)]
        normal += [imagem('BTP', semana + timedelta(minutes=i)) for i in range(30)]
        parou = [imagem('EMBRAPORT', semana + timedelta(minutes=i)) for i in range(30)]
        atrasado = [imagem('ECOPORTO', ontem + timedelta(minutes=i), horas_de_atraso=60) for i in range(12)]
        return normal + parou + atrasado + [imagem('BTP', AGORA_UTC - timedelta(hours=1))]

    def test_recinto_parado_e_imagens_atrasadas(self):
        resultado = imagens.verifica_imagens(self.contexto(fs_files(*self.documentos())))
        self.assertEqual(resultado.estado, ATENCAO)
        self.assertEqual(resultado.detalhes, [
            'recinto EMBRAPORT: 0 imagens nas últimas 24 h, 30 nas mesmas 24 h da semana anterior',
            'recinto ECOPORTO: imagens chegando 60 h depois do escaneamento (mediana de 12 imagens)'])
        self.assertEqual(resultado.resumo, 'mais nova há 1,0 h; 43 nas últimas 24 h (sem. ant. 60); ponteiro do '
                                           'extrator há 0,5 h')

    def test_carga_parada(self):
        velha = imagem('BTP', AGORA_UTC - timedelta(hours=10))
        resultado = imagens.verifica_imagens(self.contexto(fs_files(velha)))
        self.assertEqual(resultado.estado, FALHA)
        self.assertEqual(resultado.detalhes[0], 'a imagem mais nova chegou há 10,0 h — carga das imagens parada?')

    def test_sem_mongo_nao_mede(self):
        self.assertEqual(imagens.verifica_imagens(Contexto(banco(), agora=AGORA)).estado, NAO_MEDIDO)

    def test_horario_do_mongo_em_utc(self):
        """A imagem de 2 h atrás no horário local está, no Mongo, 5 h depois (UTC): não é "parada"."""
        recente = imagem('BTP', AGORA_UTC - timedelta(hours=2))
        with mock.patch.object(imagens, 'IMAGENS_PARADAS_HORAS', 3):
            self.assertEqual(imagens.verifica_imagens(self.contexto(fs_files(recente))).estado, OK)


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
