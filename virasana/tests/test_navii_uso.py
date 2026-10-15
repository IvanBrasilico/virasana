# coding: utf-8
"""Log de uso do NAVI-I (selo NAVII_USO / NAVII_LENTA para o grep) — uso_navii.py e as rotas."""
import logging
import unittest
from unittest import mock

from flask import Flask

from virasana.navii import uso_navii
from virasana.navii.views_navii import bp_navii
from virasana.tests.test_navii_busca_sqlite import CE_EXTRATO, PERIODO, NaviiSqliteTestCase, monta_extrato_cenario

LOGGER = 'virasana.navii.uso'


class TestLinha(unittest.TestCase):

    def test_campos_chave_valor_sem_espacos_soltos(self):
        texto = uso_navii.linha('pesquisa', '12345', 2.5, janela='efetiva', filtros={'produto': 'NOTEBOOK 14'},
                                total=None, t_consulta=1.23456)
        self.assertEqual(texto, 'NAVII_USO acao=pesquisa usuario=12345 janela=efetiva '
                                'filtros={"produto":"NOTEBOOK 14"} t_consulta=1.235 t_total=2.50')
        self.assertNotIn('NAVII_LENTA', texto)

    def test_lenta_e_usuario_ausente(self):
        texto = uso_navii.linha('extrato', None, uso_navii.SEGUNDOS_LENTA, ce='texto com espaço')
        self.assertTrue(texto.startswith('NAVII_USO acao=extrato usuario=- ce="texto com espaço"'))
        self.assertTrue(texto.endswith(' NAVII_LENTA'))

    def test_quebra_de_linha_nao_abre_outra_linha_no_log(self):
        self.assertNotIn('\n', uso_navii.linha('extrato', 'u', ce='1\nNAVII_USO acao=falsa'))

    def test_nivel(self):
        with self.assertLogs(LOGGER, level='INFO') as log:
            uso_navii.registra('tela', 'u', 0.1)
            uso_navii.registra('pesquisa', 'u', uso_navii.SEGUNDOS_LENTA + 1)
        self.assertEqual([r.levelno for r in log.records], [logging.INFO, logging.WARNING])


class TestRotasRegistramUso(NaviiSqliteTestCase):

    def setUp(self):
        super().setUp()
        app = Flask(__name__)
        app.config.update(LOGIN_DISABLED=True, db_session=self.session, TESTING=True)
        app.register_blueprint(bp_navii)
        self.client = app.test_client()

    def test_pesquisa_primeira_pagina_e_seguinte(self):
        with self.assertLogs(LOGGER, level='INFO') as log:
            primeira = self.client.get('/navii/api/ces', query_string={**PERIODO, 'limit': 2}).get_json()
            self.client.get('/navii/api/ces', query_string={**PERIODO, 'limit': 2,
                                                             'cursor_id': primeira['next_cursor']})
        um, dois = log.output
        self.assertIn('NAVII_USO acao=pesquisa usuario=- janela=efetiva pagina=1 ', um)
        self.assertIn(' total=4 n_cards=2 t_consulta=', um)
        self.assertIn('"data_inicio":"' + PERIODO['data_inicio'] + '"', um)
        self.assertIn(' pagina=seguinte ', dois)
        self.assertNotIn(' total=', dois)
        self.assertNotIn('t_contagem=', dois)

    def test_pesquisa_invalida_tambem_fica_no_log(self):
        with self.assertLogs(LOGGER, level='INFO') as log:
            self.assertEqual(self.client.get('/navii/api/ces', query_string={**PERIODO, 'tipo_janela': 'x'})
                             .status_code, 400)
        self.assertIn('acao=pesquisa', log.output[0])
        self.assertIn(' erro=', log.output[0])

    def test_lenta_vai_em_warning(self):
        with mock.patch.object(uso_navii, 'SEGUNDOS_LENTA', 0), self.assertLogs(LOGGER, level='INFO') as log:
            self.client.get('/navii/api/ces', query_string=PERIODO)
        self.assertEqual(log.records[0].levelno, logging.WARNING)
        self.assertTrue(log.output[0].endswith('NAVII_LENTA'))

    def test_tela_e_extrato(self):
        monta_extrato_cenario(self.session)
        with mock.patch('virasana.navii.views_navii.render_template', return_value='ok'), \
                self.assertLogs(LOGGER, level='INFO') as log:
            self.client.get('/navii/', query_string={'ce': CE_EXTRATO})
            self.client.get('/navii/extrato', query_string={'CE': CE_EXTRATO})
        tela, extrato = log.output
        self.assertIn('NAVII_USO acao=tela usuario=- link={"ce":["' + CE_EXTRATO + '"]}', tela)
        self.assertIn(f'NAVII_USO acao=extrato usuario=- ce={CE_EXTRATO} conteineres=2 t_total=', extrato)


if __name__ == '__main__':
    unittest.main()
