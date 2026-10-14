"""Testes do F1 do risco histórico por CE: tabelas (x DDL), extração incremental
dos sinais, parâmetros em versões e a linha de comando (virasana/risco_historico/).

Cenário de test_risco_historico_ce (agora = 14/10/2026): CEs P1..P10 e PX
(exportação) emitidos há poucos dias, IDs 6..16; PZ (ID 17) emitido há 60 dias;
P0 e os CEs das fichas, antigos.
"""
import re
import unittest
from argparse import Namespace
from datetime import timedelta
from pathlib import Path
from unittest import mock

from sqlalchemy import create_engine, select, text
from sqlalchemy.pool import StaticPool

from virasana.risco_historico import extracao as ext
from virasana.risco_historico import lotes, parametros, sinais
from virasana.risco_historico.tabelas import CES, MARCAS, PARAMETROS, VERSOES, metadata
from virasana.tests.test_risco_historico_ce import AGORA, CNPJ_A, CNPJ_C, insere, monta_banco

RECENTE = AGORA - timedelta(days=5)


def banco():
    engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
    monta_banco(engine)
    metadata.create_all(engine)
    with engine.begin() as con:
        insere(con, 'laudo_empresas', [{'cnpj': CNPJ_A, 'nome': 'IMPORTADORA ALFA LTDA'}])
    return engine


def extracao(conexao, **opcoes):
    return ext.Extracao(conexao, relogio=lambda: AGORA, **opcoes)


def sinais_de(conexao, numero):
    return sinais.de_json(conexao.execute(select(CES.c.sinais).where(CES.c.numeroCEmercante == numero)).scalar())


class TestTabelas(unittest.TestCase):

    def test_tabelas_batem_com_a_ddl(self):
        ddl = (Path(ext.__file__).parent / 'ddl_risco_historico.sql').read_text(encoding='utf-8')
        ddl = '\n'.join(linha for linha in ddl.splitlines() if not linha.lstrip().startswith('--'))
        blocos = dict(re.findall(r'CREATE TABLE IF NOT EXISTS (\w+) \((.*?)\n\);', ddl, re.S))
        for tabela in (CES, VERSOES, PARAMETROS, MARCAS):
            colunas = set(re.findall(r'^\s+(\w+) [A-Z]', blocos[tabela.name], re.M)) - {
                'PRIMARY', 'UNIQUE', 'KEY', 'CONSTRAINT', 'REFERENCES'}
            self.assertEqual(colunas, set(tabela.c.keys()), tabela.name)


class TestExtracao(unittest.TestCase):

    def setUp(self):
        self.conexao = banco().connect()

    def tearDown(self):
        self.conexao.close()

    def test_marca_inicial_pela_emissao_e_carga(self):
        e = extracao(self.conexao)
        self.assertEqual(e.inicia(desde_dias=30), {'marca inicial (ID)': 6, 'IDs a carregar': 12,
                                                   'CEs emitidos no período': 11, 'menor ID entre eles': 6,
                                                   'IDs mais baixos ignorados': 0})
        self.assertIsNone(e.inicia(desde_dias=30))                      # já iniciada: não mexe
        estatisticas, cheio = e.uma_volta()
        self.assertFalse(cheio)
        self.assertEqual(estatisticas['CEs novos lidos'], 12)          # IDs 6..17
        self.assertEqual(estatisticas['exportação ignorada'], 1)       # PX
        self.assertEqual(estatisticas['CEs novos gravados'], 11)       # P1..P10 e PZ
        p1 = sinais_de(self.conexao, 'P1')
        self.assertEqual(p1['importador'], {'11222333'})
        self.assertEqual(p1['porto_baldeacao'], {'CNSHA'})
        self.assertEqual(p1['ncm_declarado'], {'9503'})
        linha = self.conexao.execute(select(CES).where(CES.c.numeroCEmercante == 'P1')).one()
        self.assertEqual((linha.ce_id, linha.ncm_id, linha.dataEmissao), (6, 3, (RECENTE - timedelta(days=2))
                                                                          .strftime('%Y-%m-%d')))
        estatisticas, cheio = e.uma_volta()                             # nada novo (só relê a sobreposição)
        self.assertFalse(cheio)
        self.assertEqual((estatisticas['CEs novos lidos'], estatisticas['CEs alterados re-extraídos']), (0, 0))
        self.assertEqual(e.situacao(), {'CEs na tabela': 11, 'IDs do Mercante ainda não lidos': 0})

    def test_ce_antigo_com_emissao_recente_nao_puxa_a_marca(self):
        self.conexao.execute(text("UPDATE conhecimentosresumo SET dataEmissao = :d WHERE ID = 2"),   # CE2, antigo
                             {'d': RECENTE.strftime('%Y-%m-%d')})
        with mock.patch.object(ext, 'QUANTIL_INICIAL', 0.1):
            inicio = extracao(self.conexao).inicia(desde_dias=30)
        self.assertEqual((inicio['marca inicial (ID)'], inicio['menor ID entre eles'], inicio['IDs mais baixos ignorados']),
                         (6, 2, 1))

    def test_lotes_e_fila(self):
        e = extracao(self.conexao, lote_ces=5)
        e.inicia()
        self.assertTrue(e.uma_volta()[1])                               # 5 de 12: ainda há fila
        self.assertEqual(e.marcas()[ext.MARCA_CE][0], 10)

    def test_inicio_recusado(self):
        with self.assertRaisesRegex(ext.ErroInicio, 'nenhum CE emitido'):
            extracao(self.conexao).inicia(desde_dias=1)
        with mock.patch.object(ext, 'LIMITE_CES_POR_DIA', 1):
            with self.assertRaisesRegex(ext.ErroInicio, '12 IDs a carregar'):
                extracao(self.conexao).inicia(desde_dias=1, desde_id=6)
        with self.assertRaisesRegex(ext.ErroInicio, 'maior que o último ID do Mercante'):
            extracao(self.conexao).inicia(desde_id=999)                  # erro de digitação
        with self.assertRaisesRegex(ext.ErroInicio, 'sem marcas'):
            extracao(self.conexao).uma_volta()

    def test_desde_id_com_marcas_gravadas_e_reinicio(self):
        e = extracao(self.conexao)
        e.inicia(desde_id=16)
        e.uma_volta()
        with self.assertRaisesRegex(ext.ErroInicio, 'use --reinicia'):
            e.inicia(desde_id=6)                                         # não é ignorado em silêncio
        self.assertEqual(e.inicia(desde_id=6, reinicia=True)['marca inicial (ID)'], 6)
        self.assertEqual(e.uma_volta()[0]['CEs novos gravados'], 11)     # P1..P10 e PZ (PZ refeito)

    def _altera(self, ids, consignatario, quando):
        self.conexao.execute(text('UPDATE conhecimentosresumo SET consignatario = :c, create_date = :d '
                                  'WHERE ID IN (%s)' % ','.join(str(i) for i in ids)),  # nosec - teste
                             {'c': consignatario, 'd': quando})

    def test_retificacao_pela_data_da_ultima_alteracao(self):
        e = extracao(self.conexao)
        e.inicia()
        e.uma_volta()
        self._altera([8], CNPJ_C, RECENTE + timedelta(days=1))          # P3 retificado
        estatisticas, _ = e.uma_volta()
        self.assertEqual(estatisticas['CEs alterados re-extraídos'], 1)
        self.assertEqual(sinais_de(self.conexao, 'P3')['importador'], {'11444777'})
        self.assertEqual(e.uma_volta()[0]['CEs alterados re-extraídos'], 0)   # sobreposição relida, sem refazer

    def _ate_esvaziar(self, e):
        total = 0
        for _ in range(20):
            estatisticas, cheio = e.uma_volta()
            total += estatisticas['CEs alterados re-extraídos']
            if not cheio:
                return total
        self.fail('a fila de alterados não esvaziou')

    def test_muitos_alterados_no_mesmo_instante_em_paginas(self):
        e = extracao(self.conexao, lote_alterados=2)
        e.inicia()
        e.uma_volta()
        self._altera(range(10, 15), CNPJ_A, RECENTE + timedelta(days=2))     # P5..P9, mesmo instante
        self.assertEqual(self._ate_esvaziar(e), 5)
        self.assertEqual(sinais_de(self.conexao, 'P9')['importador'], {'11222333'})
        with mock.patch.object(ext, 'PAGINAS_ALTERADOS', 1):               # uma página por volta: continua dali
            self._altera(range(6, 11), CNPJ_C, RECENTE + timedelta(days=3))
            self.assertEqual(self._ate_esvaziar(e), 5)

    def test_ncm_que_chega_depois(self):
        e = extracao(self.conexao)
        e.inicia()
        e.uma_volta()
        insere(self.conexao, 'ncmitemresumo', [
            {'ID': 5, 'numeroCEMercante': 'P4', 'identificacaoNCM': '61083000', 'create_date': AGORA},
            {'ID': 6, 'numeroCEMercante': 'P0', 'identificacaoNCM': '61083000', 'create_date': AGORA}])  # fora
        estatisticas, _ = e.uma_volta()
        self.assertEqual((estatisticas['NCMs novas lidas'], estatisticas['CEs re-extraídos por NCM nova']), (2, 1))
        self.assertEqual(sinais_de(self.conexao, 'P4')['ncm_declarado'], {'6108'})
        self.assertEqual(e.marcas()[ext.MARCA_NCM][0], 6)


class TestParametros(unittest.TestCase):

    def setUp(self):
        self.conexao = banco().connect()
        e = extracao(self.conexao)
        e.inicia()
        e.uma_volta()

    def tearDown(self):
        self.conexao.close()

    def test_versao_com_memoria_de_calculo(self):
        versao, motivo = parametros.atualiza(self.conexao, AGORA)
        self.assertEqual((versao, motivo), (1, 'primeira versão'))
        v = parametros.ultima_versao(self.conexao)
        self.assertEqual((v['fichas'], float(v['total_k']), v['total_n']), (5, 5.0, 11))
        linha = self.conexao.execute(select(PARAMETROS).where(PARAMETROS.c.dimensao == 'importador',
                                                              PARAMETROS.c.valor == '11222333')).one()
        self.assertEqual((linha.fichas, linha.n, linha.descricao, linha.fichas_ids), (2, 1, 'IMPORTADORA ALFA LTDA',
                                                                                       '2,1'))
        self.assertGreater(float(linha.peso), 0)
        rota = self.conexao.execute(select(PARAMETROS).where(PARAMETROS.c.dimensao == 'rota',
                                                             PARAMETROS.c.valor == 'CNNGB > BRSSZ')).one()
        self.assertEqual((float(rota.peso), rota.motivo), (0.0, 'menos de 3 fichas'))
        pesos = parametros.pesos_da_versao(self.conexao, versao)
        self.assertAlmostEqual(pesos['importador']['11222333'].peso, float(linha.peso))
        self.assertEqual(parametros.atualiza(self.conexao, AGORA), (None, 'parâmetros em dia'))

    def test_quando_ha_nova_versao(self):
        parametros.atualiza(self.conexao, AGORA)
        self.assertEqual(parametros.atualiza(self.conexao, AGORA + timedelta(days=31))[1], 'versão vencida')
        insere(self.conexao, 'ovr_resultados', [{'id': 99, 'ovr_id': 6, 'tipo_resultado': 2}])
        self.assertEqual(parametros.atualiza(self.conexao, AGORA + timedelta(days=32)), (3, 'fichas positivas mudaram'))
        self.assertEqual(parametros.atualiza(self.conexao, AGORA + timedelta(days=32), forca=True), (4, 'forçada'))

    def test_populacao_na_janela(self):
        self.conexao.execute(CES.update().where(CES.c.numeroCEmercante == 'PZ').values(
            criado_em=AGORA - timedelta(days=100)))
        total, contagens = parametros.populacao(self.conexao, AGORA - timedelta(days=90))
        self.assertEqual(total, 10)
        self.assertEqual(contagens[sinais.CITADO]['11444777'], 1)

    def test_populacao_vazia(self):
        self.conexao.execute(CES.delete())
        self.assertEqual(parametros.atualiza(self.conexao, AGORA)[0], None)
        self.assertIn('população vazia', parametros.atualiza(self.conexao, AGORA)[1])

    def test_para_tela(self):
        self.assertIsNone(parametros.para_tela(self.conexao))
        parametros.atualiza(self.conexao, AGORA)
        dados = parametros.para_tela(self.conexao)
        grupos = {g['chave']: g for g in dados['grupos']}
        # 44555666 (ficha 5) não tem CE na população, mas conta para os CEs que chegarem
        self.assertEqual([lin['valor'] for lin in grupos['importador']['linhas']], ['11222333', '44555666'])
        self.assertTrue(all(lin['peso'] > 0 for g in dados['grupos'] for lin in g['linhas']))
        todos = {g['chave']: g for g in parametros.para_tela(self.conexao, todos=True)['grupos']}
        self.assertIn('menos de 3 fichas', {lin['motivo'] for lin in todos['logistica']['linhas']})
        self.assertEqual(dados['versao']['constantes']['m'], 10.0)


class TestLinhaDeComando(unittest.TestCase):

    def args(self, **opcoes):
        return Namespace(**{'desde_dias': 30, 'desde_id': None, 'lote': 5, 'simula': False, 'uma_vez': False,
                            'reinicia': False, **opcoes})

    def test_voltas_ate_alcancar(self):
        engine = banco()
        self.assertEqual(lotes.sinais(engine, self.args(), relogio=lambda: AGORA), 3)   # 5 + 5 + 2 IDs
        with engine.connect() as conexao:
            self.assertEqual(conexao.execute(select(CES.c.ID)).fetchall().__len__(), 11)

    def test_desde_id_so_na_primeira_volta(self):
        engine = banco()
        self.assertEqual(lotes.sinais(engine, self.args(desde_id=6), relogio=lambda: AGORA), 3)
        self.assertEqual(lotes.sinais(engine, self.args(desde_id=6, reinicia=True), relogio=lambda: AGORA), 3)
        with self.assertRaises(ext.ErroInicio):
            lotes.sinais(engine, self.args(desde_id=7), relogio=lambda: AGORA)

    def test_simula_nao_grava_nada(self):
        engine = banco()
        self.assertEqual(lotes.sinais(engine, self.args(simula=True), relogio=lambda: AGORA), 1)
        with engine.connect() as conexao:
            self.assertEqual(conexao.execute(select(CES.c.ID)).fetchall(), [])
            self.assertEqual(conexao.execute(select(MARCAS.c.nome)).fetchall(), [])

    def test_banco(self):
        self.assertEqual(lotes.uri_do_banco(Namespace(uri=None, homologacao=True), {'SQL_URI_HOM': 'mysql://h'}),
                         'mysql://h')
        self.assertEqual(lotes.uri_do_banco(Namespace(uri='sqlite://', homologacao=False)), 'sqlite://')


if __name__ == '__main__':
    unittest.main()
