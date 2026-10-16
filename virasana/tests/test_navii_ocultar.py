"""Testes ADVERSARIAIS dos filtros OCULTAR do NAVI-I (NCM e porto de origem; pedido do usuário, 16/10/2026).

Regras (é um sistema de análise de risco: o filtro nunca pode sonegar informação):
  - OCULTAR NCM esconde o card só quando TODOS os NCMs do contêiner batem com algum código oculto; basta um NCM
    não oculto — ou vazio/NULL, que é desconhecido — para o card aparecer. Sem NCM declarado, o card aparece.
  - OCULTAR origem esconde só o card cujo porto bate; porto vazio/NULL aparece (armadilha do NOT ... NULL no SQL).
  - Código oculto igual ou prefixo de um código do filtro positivo do mesmo campo é ignorado, com aviso (filtrar
    prevalece); o contrário é refinamento e vale.
  - Curinga ('%', '_') nunca vira LIKE que esconde tudo; prefixo de 1 caractere é ignorado, com aviso.

Cenário (manifesto M1, escala prevista em 10/01/2026 — dentro de PERIODO; além do cenário base):
  CEX1 (origem INNSA): K1 [9797]; K2 [9797, 8471]; K3 []; K4 [NULL]; K5 ['']; K6 [9797, NULL];
                       K7 [97970000, 97971000]; K8 ['979' — mais curto que o código oculto]
  CEX2 (origem INMAA): K9 [8471]; K10 [9797]        — outro contêiner do mesmo CE não contamina
  CEX3 (origem NULL):  K1 [8471]                    — o MESMO contêiner K1 em outro CE: a chave é CE + contêiner
  CEX4 (origem ''):    K11 [97971000]
  CEX5 (origem CNSHA): K12 ['9797.00.00' — gravado com pontos]
"""
import json
import logging
import re
import unittest

from flask import Flask
from sqlalchemy.dialects import mysql

from virasana.navii import filtros_salvos
from virasana.navii.busca_navii import (
    busca_ces_navii, conta_conteineres_navii, monta_query_navii, normaliza_filtros,
)
from virasana.navii.views_navii import _filtros_da_requisicao, bp_navii
from virasana.tests.test_navii_busca_sqlite import PERIODO, NaviiSqliteTestCase, cria_ce
from virasana.tests import test_navii_filtros_salvos as tela_filtros

ARTE, NOTEBOOK = '97970000', '84713000'


def monta_cenario_ocultar(session):
    cria_ce(session, 'CEX1', 'M1', {
        'K1': [(ARTE, 'arte')], 'K2': [(ARTE, 'arte'), (NOTEBOOK, 'notebook')], 'K3': [],
        'K4': [(None, 'sem NCM')], 'K5': [('', 'NCM vazio')], 'K6': [(ARTE, 'arte'), (None, 'sem NCM')],
        'K7': [(ARTE, 'arte'), ('97971000', 'outra arte')], 'K8': [('979', 'NCM truncado')],
    }, porto_origem='INNSA')
    cria_ce(session, 'CEX2', 'M1', {'K9': [(NOTEBOOK, 'notebook')], 'K10': [(ARTE, 'arte')]}, porto_origem='INMAA')
    cria_ce(session, 'CEX3', 'M1', {'K1': [(NOTEBOOK, 'notebook')]}, porto_origem=None)
    cria_ce(session, 'CEX4', 'M1', {'K11': [('97971000', 'outra arte')]}, porto_origem='')
    cria_ce(session, 'CEX5', 'M1', {'K12': [('9797.00.00', 'com pontos')]}, porto_origem='CNSHA')
    session.commit()


class OcultarTestCase(NaviiSqliteTestCase):

    def setUp(self):
        super().setUp()
        monta_cenario_ocultar(self.session)
        self.base = self.cards()

    def cards(self, tipo_janela='prevista', **filtros):
        """{(CE, contêiner)} de TODAS as páginas (de 3 em 3: a paginação também é exercitada) — e confere que a
        contagem bate com os cards."""
        filtros = {**PERIODO, **filtros}
        vistos, cursor = [], None
        while True:
            pagina = busca_ces_navii(self.session, tipo_janela, filtros, cursor_id=cursor, limit=3)
            vistos += [(r.item.numeroCEmercante, r.item.codigoConteiner) for r in pagina]
            if len(pagina) < 3:
                break
            cursor = pagina[-1].item.ID
        self.assertEqual(len(vistos), len(set(vistos)), 'card repetido entre páginas')
        self.assertEqual(conta_conteineres_navii(self.session, tipo_janela, filtros), len(vistos),
                         f'contagem diferente dos cards com {filtros}')
        return set(vistos)


class TestOcultarNcm(OcultarTestCase):

    def test_oculta_so_quando_todos_os_ncms_batem(self):
        ocultos = {('CEX1', 'K1'), ('CEX1', 'K7'), ('CEX2', 'K10'), ('CEX4', 'K11'), ('CEX5', 'K12')}
        self.assertTrue(ocultos <= self.base)
        self.assertEqual(self.cards(ncm_ocultar='9797'), self.base - ocultos)

    def test_nunca_oculta_sem_a_informacao(self):
        visiveis = self.cards(ncm_ocultar='9797')
        for card, motivo in ((('CEX1', 'K2'), 'misto: 8471 não está oculto'),
                             (('CEX1', 'K3'), 'sem NCM declarado'),
                             (('CEX1', 'K4'), 'NCM NULL (desconhecido)'),
                             (('CEX1', 'K5'), "NCM ''"),
                             (('CEX1', 'K6'), '9797 + NCM NULL: o desconhecido pode não ser 9797'),
                             (('CEX1', 'K8'), "'979' não começa com 9797"),
                             (('CEX2', 'K9'), 'outro contêiner do mesmo CE'),
                             (('CEX3', 'K1'), 'o mesmo contêiner, noutro CE, com 8471')):
            self.assertIn(card, visiveis, motivo)

    def test_misto_com_todos_os_codigos_ocultos_some(self):
        # K2 tem 9797 e 8471: cada NCM bate com ALGUM código oculto -> todos batem
        visiveis = self.cards(ncm_ocultar=['9797', '8471'])
        self.assertNotIn(('CEX1', 'K2'), visiveis)
        self.assertNotIn(('CEX3', 'K1'), visiveis)
        self.assertIn(('CEX1', 'K6'), visiveis)      # o NULL continua desconhecido

    def test_refinamento_do_filtro_positivo(self):
        # filtrar 9797 e ocultar 97971: K11 (só 97971000) some; K7 (97970000 + 97971000) fica
        self.assertEqual(self.cards(ncm='9797', ncm_ocultar='97971'),
                         {('CEX1', 'K1'), ('CEX1', 'K2'), ('CEX1', 'K6'), ('CEX1', 'K7'), ('CEX2', 'K10'),
                          ('CEX5', 'K12')})

    def test_conflito_com_o_filtro_positivo_filtrar_prevalece(self):
        so_positivo = self.cards(ncm='97971000')
        self.assertEqual(so_positivo, {('CEX1', 'K7'), ('CEX4', 'K11')})
        self.assertEqual(self.cards(ncm='97971000', ncm_ocultar='9797'), so_positivo)   # 9797 cobre 97971000
        self.assertEqual(self.cards(ncm='9797', ncm_ocultar='9797'), self.cards(ncm='9797'))   # igual
        avisos = []
        filtros = normaliza_filtros({'ncm': '97971000', 'ncm_ocultar': '9797, 8471'}, avisos)
        self.assertEqual(filtros['ncm_ocultar'], ['8471'])                # só o que conflita sai
        self.assertEqual(len(avisos), 1)
        self.assertIn('9797', avisos[0])
        self.assertIn('97971000', avisos[0])

    def test_curinga_e_prefixo_curto_nao_escondem_tudo(self):
        for valor in ('%', '_', '%%', '.', ' ', '9', '9%', '%9'):
            avisos = []
            filtros = normaliza_filtros({'ncm_ocultar': valor}, avisos)
            self.assertNotIn('ncm_ocultar', filtros, valor)
            self.assertEqual(self.cards(ncm_ocultar=valor), self.base, valor)
        avisos = []
        normaliza_filtros({'ncm_ocultar': '9'}, avisos)
        self.assertIn('capítulo', avisos[0])

    def test_normalizacao(self):
        self.assertEqual(normaliza_filtros({'ncm_ocultar': '97.97; 9797 , 8471.30'})['ncm_ocultar'],
                         ['9797', '847130'])
        self.assertEqual(self.cards(ncm_ocultar='97.97'), self.cards(ncm_ocultar='9797'))

    def test_janela_efetiva_e_demais_filtros(self):
        base = self.cards('efetiva')
        # C4 (CE3) só tem 61091000; C1 (CE1: 84713000 e 84716000), K9 e o K1 do CEX3 só têm 8471 (os do cenário
        # novo, sem ciclo, entram na janela efetiva pela escala prevista)
        self.assertEqual(self.cards('efetiva', ncm_ocultar='6109'), base - {('CE3', 'C4')})
        self.assertEqual(self.cards('efetiva', ncm_ocultar='8471'),
                         base - {('CE1', 'C1'), ('CEX2', 'K9'), ('CEX3', 'K1')})
        self.assertEqual(self.cards('efetiva', ncm_ocultar='8471', com_imagem=True), set())


class TestOcultarOrigem(OcultarTestCase):

    def cards_do(self, *ces, **filtros):
        return {card for card in self.cards(**filtros) if card[0] in ces}

    def test_oculta_pais_e_mantem_porto_vazio(self):
        visiveis = self.cards(porto_origem_ocultar='in')
        self.assertFalse({c for c in visiveis if c[0] in ('CEX1', 'CEX2')})   # Índia: INNSA e INMAA
        self.assertEqual(self.cards_do('CEX3', 'CEX4', porto_origem_ocultar='IN'),
                         {('CEX3', 'K1'), ('CEX4', 'K11')})                  # origem NULL e '' aparecem
        self.assertEqual(visiveis, {c for c in self.base if c[0] not in ('CEX1', 'CEX2')})

    def test_porto_completo_e_exato(self):
        visiveis = self.cards(porto_origem_ocultar='INNSA')
        self.assertFalse({c for c in visiveis if c[0] == 'CEX1'})
        self.assertEqual(self.cards_do('CEX2', porto_origem_ocultar='INNSA'), {('CEX2', 'K9'), ('CEX2', 'K10')})

    def test_refinamento_e_conflito(self):
        self.assertEqual(self.cards(porto_origem='IN', porto_origem_ocultar='INNSA'),
                         {('CEX2', 'K9'), ('CEX2', 'K10')})
        avisos = []
        filtros = normaliza_filtros({'porto_origem': 'INNSA', 'porto_origem_ocultar': 'IN'}, avisos)
        self.assertNotIn('porto_origem_ocultar', filtros)
        self.assertIn('INNSA', avisos[0])
        self.assertEqual(self.cards(porto_origem='INNSA', porto_origem_ocultar='IN'), self.cards(porto_origem='INNSA'))

    def test_curinga_e_prefixo_curto(self):
        for valor in ('%', '_', 'I%', 'I', '%N', ' '):
            self.assertNotIn('porto_origem_ocultar', normaliza_filtros({'porto_origem_ocultar': valor}), valor)
            self.assertEqual(self.cards(porto_origem_ocultar=valor), self.base, valor)
        avisos = []
        normaliza_filtros({'porto_origem_ocultar': 'I'}, avisos)
        self.assertIn('sigla do país', avisos[0])

    def test_ncm_e_origem_juntos_escondem_cada_um_o_seu(self):
        so_ncm, so_origem = self.base - self.cards(ncm_ocultar='9797'), self.base - self.cards(porto_origem_ocultar='CN')
        self.assertTrue(so_ncm and so_origem)
        self.assertEqual(self.cards(ncm_ocultar='9797', porto_origem_ocultar='CN'), self.base - so_ncm - so_origem)

    def test_janela_efetiva(self):
        base = self.cards('efetiva')
        self.assertEqual(self.cards('efetiva', porto_origem_ocultar='CNNGB'), base - {('CE3', 'C4')})


class TestOcultarSql(OcultarTestCase):

    def sql(self, **filtros):
        q = monta_query_navii(self.session, 'prevista', {**PERIODO, **filtros})
        return str(q.statement.compile(dialect=mysql.dialect()))

    def test_sem_join_nem_in_subquery(self):
        """Mesma regra dos demais filtros (CONTEXTO.md): por candidato, com LIMIT; nada de JOIN/IN (subquery)."""
        sql = self.sql(ncm_ocultar='9797', porto_origem_ocultar='IN')
        self.assertNotIn('NOT IN (SELECT', sql)
        self.assertNotIn('JOIN ncmitemresumo', sql)
        self.assertEqual(sql.count('LIMIT'), self.sql().count('LIMIT') + 2)
        self.assertIn('`portoOrigemCarga` IS NULL', sql)


class TestOcultarTelaEApi(OcultarTestCase):

    def setUp(self):
        super().setUp()
        self.app = Flask(__name__)
        self.app.config.update(LOGIN_DISABLED=True, db_session=self.session, TESTING=True)
        self.app.register_blueprint(bp_navii)
        self.client = self.app.test_client()

    def get(self, **params):
        return self.client.get('/navii/api/ces', query_string={**PERIODO, 'tipo_janela': 'prevista', **params})

    def test_api_filtra_e_devolve_avisos(self):
        dados = self.get(ncm_ocultar='9797', limit=50).get_json()
        self.assertEqual(dados['avisos'], [])
        self.assertEqual(dados['total'], len(self.cards(ncm_ocultar='9797')))
        dados = self.get(ncm='97971000', ncm_ocultar='9797').get_json()
        self.assertEqual(dados['total'], 2)
        self.assertEqual(len(dados['avisos']), 1)
        self.assertIn('ignorado', dados['avisos'][0])

    def test_parametros_repetidos_e_log_de_uso(self):
        with self.assertLogs('virasana.navii.uso', logging.INFO) as log:
            self.get(ncm_ocultar=['9797', '8471'], porto_origem_ocultar='IN')
        self.assertIn('ncm_ocultar', log.output[0])
        self.assertIn('porto_origem_ocultar', log.output[0])
        with self.app.test_request_context('/?ncm_ocultar=9797&ncm_ocultar=8471&porto_origem_ocultar=in'):
            self.assertEqual(normaliza_filtros(_filtros_da_requisicao()),
                             {'ncm_ocultar': ['9797', '8471'], 'porto_origem_ocultar': ['IN']})

    def test_filtros_salvos(self):
        col = filtros_salvos.colecao(tela_filtros.banco_mongo())
        salvo = filtros_salvos.salva(col, 'ana', 'sem arte', 'efetiva',
                                     {'ncm': '8471', 'ncm_ocultar': '97.97, 9', 'porto_origem_ocultar': ' in '})
        self.assertEqual(salvo['filtros'], {'ncm': ['8471'], 'ncm_ocultar': ['9797'], 'porto_origem_ocultar': ['IN']})
        conflito = filtros_salvos.salva(col, 'ana', 'conflito', 'efetiva', {'ncm': '84713000', 'ncm_ocultar': '8471'})
        self.assertEqual(conflito['filtros'], {'ncm': ['84713000']})


class TestOcultarPagina(unittest.TestCase):
    """A tela renderizada (o JS não roda nos testes: aqui, o que o servidor entrega a ele)."""

    def setUp(self):
        tela_filtros.TestApi.setUp(self)   # mesma app dos testes da tela (login por cabeçalho, Mongo simulado)

    def pagina(self, url='/navii/'):
        return self.client.get(url, headers={'X-Usuario': 'ana'}).get_data(as_text=True)

    def test_seletor_filtrar_ocultar_so_em_ncm_e_origem(self):
        html = self.pagina()
        self.assertEqual(re.findall(r'class="modo-lista"[^>]*data-campo="(\w+)"', html), ['ncm', 'porto_origem'])
        self.assertIn('var MINIMO_OCULTAR = 2;', html)   # a mesma regra do servidor, sem número repetido no JS
        for id_ in ('menu-codigo', 'toast-navii', 'avisos-busca', 'aviso-ncm', 'aviso-porto_origem'):
            self.assertIn(f'id="{id_}"', html)
        self.assertIn('TODOS os NCMs do contêiner', html)

    def test_link_com_codigos_ocultos_chega_normalizado(self):
        html = self.pagina('/navii/?ncm_ocultar=97.97&ncm_ocultar=9&porto_origem_ocultar=in&ncm=8471')
        inicial = json.loads(re.search(r'var FILTROS_INICIAIS = (.*?);\n', html).group(1))
        self.assertEqual(inicial['filtros'], {'ncm': ['8471'], 'ncm_ocultar': ['9797'], 'porto_origem_ocultar': ['IN']})


if __name__ == '__main__':
    unittest.main()
