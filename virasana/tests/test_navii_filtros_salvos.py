"""Testes do autocompletar (sugestões) e dos filtros salvos do NAVI-I.

O MongoDB é substituído por uma coleção falsa em memória com só o que
`virasana.navii.filtros_salvos` usa (find/update_one/delete_one/create_index).
"""
import copy
import os
import unittest
from types import SimpleNamespace

from flask import Flask
from flask_login import LoginManager, UserMixin

from virasana.navii import filtros_salvos, referencias
from virasana.navii.busca_navii import sugere_recintos, tabela_recintos
from virasana.navii.views_navii import bp_navii
from virasana.tests.test_navii_busca_sqlite import cria_sessao


class ColecaoFalsa:
    def __init__(self):
        self.documentos = []

    def create_index(self, *args, **kwargs):
        pass

    @staticmethod
    def _casa(documento, filtro):
        return all(documento.get(k) == v for k, v in filtro.items())

    def find(self, filtro, projecao=None):
        excluir = {k for k, v in (projecao or {}).items() if not v}
        return [{k: copy.deepcopy(v) for k, v in d.items() if k not in excluir}
                for d in self.documentos if self._casa(d, filtro)]

    def update_one(self, filtro, atualizacao, upsert=False):
        for documento in self.documentos:
            if self._casa(documento, filtro):
                documento.update(copy.deepcopy(atualizacao['$set']))
                return
        if upsert:
            self.documentos.append({**filtro, **copy.deepcopy(atualizacao['$set']),
                                    **atualizacao.get('$setOnInsert', {})})

    def delete_one(self, filtro):
        for i, documento in enumerate(self.documentos):
            if self._casa(documento, filtro):
                del self.documentos[i]
                return SimpleNamespace(deleted_count=1)
        return SimpleNamespace(deleted_count=0)


class BancoFalso(dict):
    def __missing__(self, nome):
        self[nome] = ColecaoFalsa()
        return self[nome]


def codigos(sugestoes):
    return [s['codigo'] for s in sugestoes]


class TestSugestoesDeReferencia(unittest.TestCase):

    def test_porto_pelo_codigo_pais_primeiro(self):
        resultado = codigos(referencias.sugere_portos('cn'))
        self.assertEqual(resultado[0], 'CN')
        self.assertTrue(all(c.startswith('CN') for c in resultado))
        self.assertLessEqual(len(resultado), 15)

    def test_porto_pelo_nome_sem_acento(self):
        self.assertIn('BRSSZ', codigos(referencias.sugere_portos('santos')))
        self.assertIn('BR', codigos(referencias.sugere_portos('BRASIL')))
        sugestao = next(s for s in referencias.sugere_portos('BRSSZ') if s['codigo'] == 'BRSSZ')
        self.assertEqual(sugestao['descricao'], 'Santos, Brasil')

    def test_ncm_por_codigo_e_por_descricao(self):
        self.assertEqual(codigos(referencias.sugere_ncm('8471'))[0], '8471')
        self.assertEqual(codigos(referencias.sugere_ncm('84'))[0], '84')
        self.assertEqual(codigos(referencias.sugere_ncm('8471.3')), [])  # só capítulo/posição
        self.assertIn('8471', codigos(referencias.sugere_ncm('maquinas automaticas')))

    def test_termo_vazio(self):
        self.assertEqual(referencias.sugere_portos('  '), [])
        self.assertEqual(referencias.sugere_ncm(''), [])


class TestSugestoesDeRecinto(unittest.TestCase):

    def setUp(self):
        self.session = cria_sessao()
        self.session.execute(tabela_recintos.insert(), [
            {'id': 1, 'nome': 'BTP', 'cod_siscomex': '8931356', 'cod_carga': 'T01'},
            {'id': 2, 'nome': 'EADI SANTOS', 'cod_siscomex': '8932000', 'cod_carga': ''},
            {'id': 3, 'nome': 'BTP (operador)', 'cod_siscomex': '', 'cod_carga': 'T02'},
        ])

    def test_por_codigo_e_por_nome(self):
        self.assertEqual(sugere_recintos(self.session, '8931'), [{'codigo': '8931356', 'descricao': 'BTP'}])
        self.assertEqual(codigos(sugere_recintos(self.session, 'santos')), ['8932000'])
        # linha de operador (sem cod_siscomex) não é sugerida
        self.assertEqual(codigos(sugere_recintos(self.session, 'btp')), ['8931356'])

    def test_termo_vazio_ou_curinga(self):
        self.assertEqual(sugere_recintos(self.session, ''), [])
        self.assertEqual(sugere_recintos(self.session, '%'), [])


class TestFiltrosSalvos(unittest.TestCase):

    def setUp(self):
        self.col = filtros_salvos.colecao(BancoFalso())

    def test_salva_lista_e_substitui_pelo_nome(self):
        filtros_salvos.salva(self.col, 'ana', 'China', 'efetiva', {'porto_origem': 'cn, CNSHA'})
        filtros_salvos.salva(self.col, 'ana', 'agro', 'prevista', {'ncm': ['10', '12']})
        filtros_salvos.salva(self.col, 'ana', 'China', 'prevista', {'porto_origem': ['CN']})
        salvos = filtros_salvos.lista(self.col, 'ana')
        self.assertEqual([s['nome'] for s in salvos], ['agro', 'China'])
        self.assertEqual(salvos[1]['tipo_janela'], 'prevista')
        self.assertEqual(salvos[1]['filtros'], {'porto_origem': ['CN']})
        self.assertIn('criado', salvos[1])

    def test_cada_usuario_ve_os_seus(self):
        filtros_salvos.salva(self.col, 'ana', 'meu', 'efetiva', {})
        self.assertEqual(filtros_salvos.lista(self.col, 'beto'), [])
        self.assertFalse(filtros_salvos.apaga(self.col, 'beto', 'meu'))
        self.assertTrue(filtros_salvos.apaga(self.col, 'ana', 'meu'))
        self.assertEqual(filtros_salvos.lista(self.col, 'ana'), [])

    def test_so_campos_conhecidos_sem_datas_e_normalizados(self):
        salvo = filtros_salvos.salva(self.col, 'ana', '  eletrônicos   da China ', 'efetiva', {
            'ncm': '8471.30, 8517', 'porto_escala': 'brssz', 'produto': ' notebook ', 'empresa': '',
            'ncms_max': '3', 'concluido': 1, 'com_imagem': False,
            'data_inicio': '2026-01-01', 'data_fim': '2026-01-31', 'qualquer': 'x',
        })
        self.assertEqual(salvo['nome'], 'eletrônicos da China')
        self.assertEqual(salvo['filtros'], {
            'ncm': ['847130', '8517'], 'porto_escala': ['BRSSZ'], 'produto': 'notebook',
            'ncms_max': 3, 'concluido': True,
        })

    def test_validacoes(self):
        for nome, janela, filtros in (('', 'efetiva', {}), ('x' * 61, 'efetiva', {}),
                                      ('ok', 'outra', {}), ('ok', 'efetiva', {'ncms_max': 'muitos'})):
            with self.subTest(nome=nome, janela=janela):
                with self.assertRaises(filtros_salvos.FiltroInvalido):
                    filtros_salvos.salva(self.col, 'ana', nome, janela, filtros)

    def test_limite_por_usuario(self):
        for i in range(filtros_salvos.MAXIMO_POR_USUARIO):
            filtros_salvos.salva(self.col, 'ana', f'f{i}', 'efetiva', {})
        with self.assertRaises(filtros_salvos.FiltroInvalido):
            filtros_salvos.salva(self.col, 'ana', 'mais um', 'efetiva', {})
        filtros_salvos.salva(self.col, 'ana', 'f0', 'prevista', {})  # substituir continua valendo


class Usuario(UserMixin):
    def __init__(self, id_):
        self.id = id_


class TestApi(unittest.TestCase):

    def setUp(self):
        self.session = cria_sessao()
        self.session.execute(tabela_recintos.insert(), [{'id': 1, 'nome': 'BTP', 'cod_siscomex': '8931356'}])
        self.mongo = BancoFalso()
        pasta_templates = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')
        app = Flask(__name__, template_folder=pasta_templates)
        app.config.update(LOGIN_DISABLED=True, TESTING=True, db_session=self.session, mongodb=self.mongo)
        login_manager = LoginManager(app)

        @login_manager.request_loader
        def carrega_usuario(req):
            usuario = req.headers.get('X-Usuario')
            return Usuario(usuario) if usuario else None

        app.register_blueprint(bp_navii)
        self.client = app.test_client()

    def test_sugestoes(self):
        for campo, termo, esperado in (('porto', 'BRSSZ', 'BRSSZ'), ('ncm', '8471', '8471'),
                                       ('recinto', 'btp', '8931356')):
            with self.subTest(campo=campo):
                resposta = self.client.get('/navii/api/sugestoes', query_string={'campo': campo, 'q': termo})
                self.assertEqual(codigos(resposta.get_json()['sugestoes'])[0], esperado)
        self.assertEqual(self.client.get('/navii/api/sugestoes?campo=xyz&q=a').status_code, 400)

    def test_ciclo_salvar_listar_apagar(self):
        ana = {'X-Usuario': 'ana'}
        resposta = self.client.post('/navii/api/filtros', headers=ana, json={
            'nome': 'China', 'tipo_janela': 'efetiva', 'filtros': {'porto_origem': ['CN'], 'ncm': ['8471']}})
        self.assertEqual(resposta.status_code, 200)
        salvos = resposta.get_json()['filtros_salvos']
        self.assertEqual(salvos[0]['nome'], 'China')
        self.assertEqual(salvos[0]['filtros'], {'porto_origem': ['CN'], 'ncm': ['8471']})
        self.assertIsInstance(salvos[0]['atualizado'], str)
        self.assertEqual(self.client.get('/navii/api/filtros', headers={'X-Usuario': 'beto'})
                         .get_json()['filtros_salvos'], [])
        self.assertEqual(self.client.delete('/navii/api/filtros?nome=China', headers={'X-Usuario': 'beto'})
                         .status_code, 404)
        resposta = self.client.delete('/navii/api/filtros?nome=China', headers=ana)
        self.assertEqual(resposta.get_json()['filtros_salvos'], [])

    def test_erros(self):
        self.assertEqual(self.client.get('/navii/api/filtros').status_code, 401)  # sem usuário
        resposta = self.client.post('/navii/api/filtros', headers={'X-Usuario': 'ana'},
                                    json={'nome': '', 'tipo_janela': 'efetiva'})
        self.assertEqual(resposta.status_code, 400)
        self.assertIn('nome', resposta.get_json()['erro'])

    def test_pagina_renderiza(self):
        html = self.client.get('/navii/', headers={'X-Usuario': 'ana'}).get_data(as_text=True)
        self.assertIn('NAVI-I', html)
        self.assertIn('f-salvos', html)


if __name__ == '__main__':
    unittest.main()
