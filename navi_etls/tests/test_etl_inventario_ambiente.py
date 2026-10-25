"""Inventário do ambiente (navi_etls/diagnosticos/inventario_ambiente.py), com um sistema simulado."""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'diagnosticos'))
import inventario_ambiente as inv  # noqa: E402


class SistemaFalso:
    """Programas que "existem" e a saída de cada um (o 1º elemento do comando)."""

    def __init__(self, programas, saidas=None):
        self.programas, self.saidas, self.chamados = set(programas), saidas or {}, []

    def existe(self, nome):
        return f'/usr/bin/{nome}' if nome in self.programas else None

    def executa(self, comando, tempo=0):
        self.chamados.append(list(comando))
        if comando[0] not in self.programas:
            return None
        return self.saidas.get(comando[0], f'{comando[0]} ok')


def os_release(conteudo):
    caminho = os.path.join(tempfile.mkdtemp(), 'os-release')
    with open(caminho, 'w', encoding='utf-8') as arquivo:
        arquivo.write(conteudo)
    return caminho


class TestInventario(unittest.TestCase):

    def test_linux_com_dnf_e_rpm(self):
        sistema = SistemaFalso({'python3', 'dnf', 'rpm', 'httpd', 'apachectl'}, {
            'rpm': 'python3-3.9.18-1.el9\nbash-5.1.8-6.el9\nmariadb-connector-c-3.2.6-1.el9\nhttpd-2.4.57-5.el9',
            'apachectl': ' core_module (static)\n proxy_module (shared)\n proxy_http_module (shared)'})
        relatorio = inv.Inventario(sistema.executa, sistema.existe,
                                   os_release('NAME="Rocky Linux"\nVERSION="9.4 (Blue Onyx)"\nID="rocky"\n')
                                   ).gera(venvs=['venv-que-nao-existe'])
        self.assertIn('| os-release NAME | Rocky Linux |', relatorio)
        self.assertIn('**Python**\n\n```\ndnf ok\n```', relatorio)       # o oferecido pela distribuição
        instalados = relatorio.split('## 4.')[1].split('## 5.')[0]
        self.assertIn('python3-3.9.18', instalados)
        self.assertIn('mariadb-connector-c', instalados)
        self.assertNotIn('bash', instalados)                               # só os que interessam
        apache = relatorio.split('módulos de proxy do Apache')[1]
        self.assertIn('proxy_http_module', apache)
        self.assertNotIn('core_module', apache)
        self.assertIn('**cliente MariaDB**\n\nnão encontrado', relatorio)
        self.assertIn('**venv venv-que-nao-existe**\n\nnão encontrado', relatorio)
        self.assertIn(['dnf', '-q', 'list', '--available', '--showduplicates', 'python3', 'python3.1?',
                       'python3.1?-devel'], sistema.chamados)

    def test_debian_com_apt(self):
        sistema = SistemaFalso({'apt-cache', 'dpkg-query'}, {'dpkg-query': 'python3.11 3.11.2-6\nvim 2:9.0'})
        relatorio = inv.Inventario(sistema.executa, sistema.existe, os_release('ID=debian\n')).gera()
        self.assertIn('**Python**\n\n```\napt-cache ok\n```', relatorio)
        self.assertIn('python3.11 3.11.2-6', relatorio)
        self.assertNotIn('vim', relatorio)
        self.assertIn(['apt-cache', 'policy', 'python3', 'python3.9', 'python3.10', 'python3.11', 'python3.12',
                       'python3.13', 'python3.14'], sistema.chamados)

    def test_senha_mascarada_e_bancos(self):
        self.assertEqual(inv.mascara('mysql+pymysql://ajna:segredo@host:3306/mercante'),
                         'mysql+pymysql://ajna:***@host:3306/mercante')
        self.assertEqual(inv.mascara('http://usuario:p@ss@proxy:3128'), 'http://usuario:***@proxy:3128')
        self.assertEqual(inv.dados_mongo(None), [('MONGODB_URI', None)])
        sqlite = dict(inv.dados_sql('sqlite://'))
        self.assertEqual(sqlite['driver'], 'sqlite+pysqlite')
        self.assertTrue(str(sqlite['versão']).startswith('(erro'))        # SQLite não tem VERSION(): segue
        self.assertIsNone(inv.filtra_interessa(None))
        self.assertEqual(inv.celula('3.9.22 | packaged by conda-forge |'), '3.9.22 \\| packaged by conda-forge \\|')
        self.assertEqual((inv.celula(None), inv.celula('')), ('-', '-'))
        self.assertEqual(inv.le_os_release(os.path.join(tempfile.mkdtemp(), 'nao-existe')), {})


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
