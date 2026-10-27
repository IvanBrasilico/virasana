"""Testes ADVERSARIAIS da coleta de uso do NAVI-I (navii/coleta_uso.py + /navii/api/ces e POST /navii/api/uso) —
F1 das estatísticas de uso. Mongo = mongomock; a busca é o cenário SQLite do NAVI-I (test_navii_busca_sqlite).

O que não pode acontecer: o navegador forjar card que o servidor não enviou, mexer na pesquisa de outro usuário,
inflar segundos (reenvio, valor absurdo, NaN, booleano), ligar a pesquisa à de outro usuário — e a coleta derrubar a
tela (sem Mongo, Mongo fora, setor ilegível).
"""
import math
import os
import unittest
from datetime import datetime
from unittest import mock

import mongomock
from flask import Flask
from flask_login import LoginManager, UserMixin

from virasana.navii import coleta_uso, tabelas_opcionais
from virasana.navii.acompanhamento_fichas import USUARIOS
from virasana.navii.views_navii import bp_navii
from virasana.tests.test_navii_busca_sqlite import cria_sessao, monta_cenario

ANA, BETO = '11111111111', '22222222222'
AGORA = datetime(2026, 10, 17, 10)


def card(item, conteiner='C1', ce='CE1', desembarque=None, prevista=None):
    return {'item_id': item, 'numeroConteiner': conteiner, 'numeroCEmercante': ce,
            'linhaDoTempo': {'desembarque': {'data': desembarque}}, 'dataPrevistaAtracacao': prevista}


class ColetaTestCase(unittest.TestCase):

    def setUp(self):
        self.col = coleta_uso.colecao(mongomock.MongoClient()['virasana'])

    def pesquisa(self, usuario=ANA, cards=(), **campos):
        argumentos = dict(setor='2', aba='aba-0001-teste', anterior=None, origem='tela', janela='efetiva',
                          periodo=('2026-01-01', '2026-01-31'), filtros={'ncm': ['8471']}, total=len(cards),
                          tempos={'consulta': 0.1}, agora=AGORA)
        argumentos.update(campos)
        return coleta_uso.registra_pesquisa(self.col, usuario, cards=list(cards), **argumentos)

    def documento(self, pesquisa):
        return self.col.find_one({'_id': coleta_uso._id(pesquisa)})


class TestPesquisa(ColetaTestCase):

    def test_guarda_o_que_o_servidor_sabe(self):
        id_ = self.pesquisa(cards=[card(10, desembarque=datetime(2026, 1, 11, 8)),
                                   card(11, 'C2', prevista='2026-01-10 00:00:00')],
                            filtros={'ncm': ['8471'], 'data_inicio': '2026-01-01', 'data_fim': '2026-01-31'})
        doc = self.documento(id_)
        self.assertEqual((doc['usuario'], doc['setor'], doc['origem'], doc['janela'], doc['total'], doc['n_cards']),
                         (ANA, '2', 'tela', 'efetiva', 2, 2))
        self.assertEqual(doc['filtros'], {'ncm': ['8471']})                       # as datas ficam em `periodo`
        self.assertEqual(doc['periodo'], {'inicio': '2026-01-01', 'fim': '2026-01-31'})
        self.assertEqual(doc['cards'], [{'i': 10, 'c': 'C1', 'ce': 'CE1', 'p': 0, 'd': '2026-01-11', 'pr': None},
                                        {'i': 11, 'c': 'C2', 'ce': 'CE1', 'p': 1, 'd': None, 'pr': '2026-01-10'}])
        self.assertEqual((doc['vistos'], doc['acoes'], doc['inicio']), ({}, [], AGORA))

    def test_valores_do_navegador_sao_conferidos(self):
        doc = self.documento(self.pesquisa(origem='<script>', aba='x"; drop', periodo=(None, None)))
        self.assertEqual((doc['origem'], doc['aba'], doc['periodo']), ('outra', None, None))
        self.assertIsNone(self.documento(self.pesquisa(aba='curta'))['aba'])     # menos de 8 caracteres

    def test_anterior_so_se_for_do_mesmo_usuario(self):
        da_ana = self.pesquisa()
        self.assertIsNone(self.documento(self.pesquisa(BETO, anterior=da_ana))['anterior'])
        self.assertIsNone(self.documento(self.pesquisa(anterior='lixo'))['anterior'])
        self.assertIsNone(self.documento(self.pesquisa(anterior='5f0000000000000000000000'))['anterior'])
        self.assertEqual(str(self.documento(self.pesquisa(anterior=da_ana))['anterior']), da_ana)

    def test_paginas_seguintes_continuam_a_posicao_e_tem_teto(self):
        id_ = self.pesquisa(cards=[card(1), card(2)])
        self.assertTrue(coleta_uso.registra_pagina(self.col, id_, ANA, [card(3), card(4)]))
        self.assertFalse(coleta_uso.registra_pagina(self.col, id_, BETO, [card(99)]))      # de outro usuário
        self.assertFalse(coleta_uso.registra_pagina(self.col, 'lixo', ANA, [card(99)]))
        with mock.patch.object(coleta_uso, 'MAX_CARDS', 5):
            coleta_uso.registra_pagina(self.col, id_, ANA, [card(5), card(6), card(7)])
        doc = self.documento(id_)
        self.assertEqual([(c['i'], c['p']) for c in doc['cards']], [(1, 0), (2, 1), (3, 2), (4, 3), (5, 4)])
        self.assertEqual((doc['n_cards'], doc['paginas']), (7, 3))               # enviados: todos, mesmo além do teto


class TestVistos(ColetaTestCase):

    def setUp(self):
        super().setUp()
        self.id = self.pesquisa(cards=[card(1), card(10), card(11), card(12)])   # 1: True não pode virar o item 1

    def vistos(self, *lote, usuario=ANA, pesquisa=None):
        return coleta_uso.registra_vistos(self.col, pesquisa or self.id, usuario, list(lote))

    def test_so_cards_enviados_e_so_o_dono(self):
        self.assertEqual(self.vistos({'item': 10, 's': 3.2}, {'item': '11', 's': 2}, {'item': 99, 's': 5}), 2)
        self.assertIsNone(self.vistos({'item': 10, 's': 50}, usuario=BETO))     # pesquisa de outro usuário
        self.assertIsNone(self.vistos({'item': 10, 's': 50}, pesquisa='lixo'))
        self.assertEqual(self.documento(self.id)['vistos'], {'10': {'s': 3.2}, '11': {'s': 2.0}})

    def test_valores_absurdos(self):
        aceitos = self.vistos({'item': 10, 's': True}, {'item': 11, 's': math.nan}, {'item': 12, 's': -4},
                              {'item': True, 's': 3}, 'lixo', {'item': 12, 's': '9'}, {'item': 10, 's': math.inf})
        self.assertEqual(aceitos, 0)
        self.assertEqual(self.vistos({'item': 10, 's': 10 ** 9}), 1)
        self.assertEqual(self.documento(self.id)['vistos'], {'10': {'s': coleta_uso.MAX_SEGUNDOS}})
        self.assertEqual(coleta_uso.registra_vistos(self.col, self.id, ANA, {'item': 10}), 0)   # lote não é lista

    def test_reenviar_nao_soma_nem_diminui_e_interacoes_sem_repetir(self):
        self.vistos({'item': 10, 's': 5, 'x': ['imagem', 'imagem', 'hack']})
        self.vistos({'item': 10, 's': 3, 'x': ['extrato']})
        self.vistos({'item': 10, 's': 8, 'x': ['imagem']})
        visto = self.documento(self.id)['vistos']['10']
        self.assertEqual((visto['s'], sorted(visto['x'])), (8.0, ['extrato', 'imagem']))

    def test_interacao_sem_segundos_conta_como_examinado(self):
        self.assertEqual(self.vistos({'item': 11, 's': 0, 'x': ['ficha']}), 1)
        self.assertEqual(self.documento(self.id)['vistos'], {'11': {'x': ['ficha']}})

    def test_lote_tem_teto(self):
        with mock.patch.object(coleta_uso, 'MAX_LOTE', 2):
            self.assertEqual(self.vistos({'item': 10, 's': 1}, {'item': 11, 's': 1}, {'item': 12, 's': 1}), 2)


class TestAcaoESetor(ColetaTestCase):

    def test_acao_so_na_pesquisa_do_usuario_e_com_teto(self):
        id_ = self.pesquisa()
        self.assertTrue(coleta_uso.registra_acao(self.col, id_, ANA, {'t': 'ficha', 'ficha': 7}, agora=AGORA))
        self.assertFalse(coleta_uso.registra_acao(self.col, id_, BETO, {'t': 'ficha', 'ficha': 8}))
        self.assertFalse(coleta_uso.registra_acao(self.col, None, ANA, {'t': 'ficha', 'ficha': 9}))
        with mock.patch.object(coleta_uso, 'MAX_ACOES', 2):
            for ficha in (10, 11):
                coleta_uso.registra_acao(self.col, id_, ANA, {'t': 'ficha', 'ficha': ficha})
        self.assertEqual([a['ficha'] for a in self.documento(id_)['acoes']], [10, 11])

    def test_setor_do_fichas_com_cache(self):
        tabelas_opcionais.limpa_cache()
        coleta_uso.limpa_cache()
        session = cria_sessao()
        self.assertIsNone(coleta_uso.setor_do_usuario(session, ANA))            # banco sem as tabelas do Fichas
        coleta_uso.limpa_cache()
        tabelas_opcionais.limpa_cache()
        USUARIOS.create(session.get_bind())
        session.execute(USUARIOS.insert(), [{'cpf': ANA, 'nome': 'Ana', 'setor_id': '2 '}])
        self.assertEqual(coleta_uso.setor_do_usuario(session, ANA), '2')
        session.execute(USUARIOS.update().values(setor_id='9'))
        self.assertEqual(coleta_uso.setor_do_usuario(session, ANA), '2')         # em cache
        coleta_uso.limpa_cache()
        self.assertEqual(coleta_uso.setor_do_usuario(session, ANA), '9')
        self.assertIsNone(coleta_uso.setor_do_usuario(session, BETO))


class Usuario(UserMixin):
    def __init__(self, id_):
        self.id = id_


class TestApi(unittest.TestCase):

    def setUp(self):
        tabelas_opcionais.limpa_cache()
        coleta_uso.limpa_cache()
        self.session = cria_sessao()
        monta_cenario(self.session)
        self.session.commit()
        self.mongo = mongomock.MongoClient()['virasana']
        self.app = Flask(__name__, template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                                                'templates'))
        self.app.config.update(TESTING=True, db_session=self.session, mongodb=self.mongo, SECRET_KEY='x')
        LoginManager(self.app).request_loader(
            lambda req: Usuario(req.headers['X-Usuario']) if req.headers.get('X-Usuario') else None)
        self.app.register_blueprint(bp_navii)
        self.client = self.app.test_client()

    def ces(self, usuario=ANA, **parametros):
        consulta = {'tipo_janela': 'efetiva', 'data_inicio': '2026-01-01', 'data_fim': '2026-03-31', 'limit': 2,
                    **parametros}
        return self.client.get('/navii/api/ces', query_string=consulta, headers={'X-Usuario': usuario}).get_json()

    def uso(self, corpo, usuario=ANA):
        return self.client.post('/navii/api/uso', json=corpo, headers={'X-Usuario': usuario} if usuario else {})

    def documento(self, pesquisa):
        return self.mongo[coleta_uso.COLECAO].find_one({'_id': coleta_uso._id(pesquisa)})

    def test_pesquisa_paginas_e_vistos(self):
        primeira = self.ces(aba='aba-0001-teste', origem='salvo', ncm='84.71')     # guardado normalizado
        id_ = primeira['pesquisa']
        doc = self.documento(id_)
        self.assertEqual((doc['usuario'], doc['origem'], doc['aba'], doc['total'], doc['filtros']),
                         (ANA, 'salvo', 'aba-0001-teste', primeira['total'], {'ncm': ['8471']}))
        self.assertEqual(doc['periodo'], {'inicio': '2026-01-01', 'fim': '2026-03-31'})
        self.assertEqual([c['i'] for c in doc['cards']], [c['item_id'] for c in primeira['cards']])
        self.assertIsNone(doc['setor'])                                           # sem cadastro no Fichas
        segunda = self.ces(ncm='8471', cursor_id=primeira['next_cursor'], pesquisa=id_)
        self.assertEqual(segunda['pesquisa'], id_)
        doc = self.documento(id_)
        self.assertEqual([c['p'] for c in doc['cards']], list(range(len(primeira['cards']) + len(segunda['cards']))))
        item = primeira['cards'][0]['item_id']
        self.assertEqual(self.uso({'pesquisa': id_, 'vistos': [{'item': item, 's': 2.5, 'x': ['imagem']},
                                                               {'item': 987654, 's': 9}]}).get_json(), {'aceitos': 1})
        self.assertEqual(self.documento(id_)['vistos'], {str(item): {'s': 2.5, 'x': ['imagem']}})

    def test_ligacoes_entre_pesquisas_e_de_outro_usuario(self):
        da_ana = self.ces()['pesquisa']
        do_beto = self.ces(BETO, anterior=da_ana)['pesquisa']
        self.assertIsNone(self.documento(do_beto)['anterior'])
        self.assertEqual(self.uso({'pesquisa': da_ana, 'vistos': []}, usuario=BETO).status_code, 404)
        pagina = self.ces(BETO, cursor_id=999999, pesquisa=da_ana)                 # página na pesquisa da Ana
        self.assertIsNone(pagina['pesquisa'])
        self.assertEqual(self.documento(da_ana)['paginas'], 1)
        self.assertEqual(str(self.documento(self.ces(anterior=da_ana)['pesquisa'])['anterior']), da_ana)

    def test_entradas_invalidas(self):
        self.assertEqual(self.uso({'pesquisa': 'x'}, usuario=None).status_code, 401)
        self.assertEqual(self.client.post('/navii/api/uso', data='lixo', headers={'X-Usuario': ANA}).status_code, 400)
        self.assertEqual(self.uso(['lista']).status_code, 400)
        self.assertEqual(self.uso({'pesquisa': 'lixo', 'vistos': []}).status_code, 404)

    def test_sem_mongo_ou_mongo_fora_a_tela_segue(self):
        del self.app.config['mongodb']
        dados = self.ces()
        self.assertTrue(dados['cards'])
        self.assertIsNone(dados['pesquisa'])
        self.assertEqual(self.uso({'pesquisa': 'x', 'vistos': []}).get_json(), {'aceitos': 0})
        quebrado = mock.MagicMock()
        quebrado.__getitem__.side_effect = RuntimeError('Mongo fora')
        self.app.config['mongodb'] = quebrado
        dados = self.ces()
        self.assertTrue(dados['cards'])
        self.assertIsNone(dados['pesquisa'])

    def test_setor_ilegivel_nao_perde_a_pesquisa(self):
        with mock.patch.object(coleta_uso, 'setor_do_usuario', side_effect=RuntimeError('banco')):
            id_ = self.ces()['pesquisa']
        self.assertIsNotNone(id_)
        self.assertIsNone(self.documento(id_)['setor'])

    def test_tela_recebe_a_url_do_uso(self):
        html = self.client.get('/navii/', headers={'X-Usuario': ANA}).get_data(as_text=True)
        self.assertIn('var USO_URL = "/navii/api/uso"', html)


if __name__ == '__main__':
    unittest.main()
