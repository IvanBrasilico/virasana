"""Modos de exibição dos cards do NAVI-I (22/10/2026, pedido do usuário): detalhado (NCMs recolhíveis),
simplificado (imagem maior, só o começo do card) e galeria (só as imagens; dados ao passar o mouse).

O comportamento é do navegador (classes na lista + CSS, linhas justificadas em JS): conferido no navegador com o
servidor de demonstração. Aqui, a tela traz o seletor acessível nas duas abas e as peças de que os modos dependem.
"""
from virasana.tests.test_navii_busca_sqlite import NaviiSqliteTestCase  # 1º: ajusta o sys.path
import os  # noqa: I100
import re

from flask import Flask

from virasana.navii.views_navii import bp_navii


class TestModosDeExibicao(NaviiSqliteTestCase):

    def setUp(self):
        super().setUp()
        app = Flask(__name__, template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates'))
        app.config.update(LOGIN_DISABLED=True, db_session=self.session, TESTING=True)
        app.register_blueprint(bp_navii)
        self.cliente = app.test_client()

    def tela(self, url='/navii/'):
        resposta = self.cliente.get(url)
        self.assertEqual(resposta.status_code, 200)
        return resposta.get_data(as_text=True)

    def test_seletor_com_os_tres_modos_acessivel(self):
        html = self.tela()
        seletor = re.search(r'<div class="seletor-visao" id="seletor-visao" role="group"\s+'
                            r'aria-label="Modo de exibição dos cards">(.*?)</div>', html, re.S)
        self.assertIsNotNone(seletor)
        botoes = re.findall(r'data-visao="(\w+)" aria-pressed="(true|false)"', seletor.group(1))
        self.assertEqual(botoes, [('detalhado', 'true'), ('simples', 'false'), ('galeria', 'false')])
        # cada botão com o nome à vista (no celular, só para o leitor de tela) e uma dica do que mostra
        self.assertEqual(re.findall(r'<span>(\w+)</span>', seletor.group(1)), ['Detalhado', 'Simplificado', 'Galeria'])
        self.assertEqual(seletor.group(1).count('title="'), 3)

    def test_tamanho_da_galeria_so_aparece_na_galeria(self):
        html = self.tela()
        self.assertRegex(html, r'<label class="tamanho-galeria" id="tamanho-galeria"[^>]*\bhidden>')
        self.assertIn('id="f-tamanho-galeria" min="6" max="18"', html)

    def test_aba_de_texto_tambem_tem_os_modos(self):
        self.assertIn('id="seletor-visao"', self.tela('/navii/texto'))

    def test_pecas_dos_modos_no_script(self):
        html = self.tela()
        # NCMs recolhíveis no card detalhado; o card guarda as preferências no navegador
        self.assertIn("'<details class=\"secao-conteineres\">'", html)
        for chave in ("'navii_visao'", "'navii_tamanho_galeria'"):
            self.assertIn(chave, html)
        # "Ver detalhes" conta como interação "detalhes" na coleta de uso (coleta_uso.INTERACOES)
        self.assertIn("['button.btn-detalhes-card', 'detalhes']", html)
        # galeria: linhas justificadas no JS; os modos valem só para o card não expandido
        self.assertIn('function montaLinhasGaleria()', html)
        self.assertIn('.visao-galeria .card-navii:not(.expandido) .bloco-dados', html)
        self.assertIn('.visao-simples .card-navii:not(.expandido) .card-content', html)
