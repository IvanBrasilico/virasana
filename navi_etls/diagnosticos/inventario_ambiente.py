# coding: utf-8
"""
INVENTÁRIO DO AMBIENTE (NAVI-ETL, F0.1) — só leitura; roda no Windows de desenvolvimento e no servidor Linux.

Antes de escolher as versões da modernização (regra do usuário, 24/10/2026: as mais novas OFICIAIS da distribuição,
nada instalado "por fora"), registra o que cada máquina tem e o que a distribuição oferece:

  1. sistema operacional (versão e build no Windows; /etc/os-release e kernel no Linux);
  2. Python: este interpretador, os do PATH (e os do lançador `py`, no Windows);
  3. o que os repositórios da distribuição OFERECEM (dnf/yum ou apt): Python e MariaDB;
  4. pacotes do sistema instalados que interessam (python3*, mariadb/mysql, mongo, httpd/apache2, supervisor, gcc);
  5. serviços e ferramentas: Apache (versão e módulos de proxy), supervisor, clientes MariaDB/MySQL, mongosh, gcc;
  6. venvs (--venv, quantos quiser): versão do Python e `pip freeze`;
  7. bancos (--banco; SQL_URI e MONGODB_URI do ambiente): versão e configurações do MariaDB, versão do MongoDB;
  8. acesso ao PyPI (--pypi): configuração do pip (proxy, espelho) e uma consulta de versão.

Senha nunca sai no relatório (URIs e proxies mascarados). Programa que não existe aparece como "não encontrado".

Uso (na raiz do repositório virasana):
    virasana-venv/bin/python navi_etls/diagnosticos/inventario_ambiente.py --venv virasana-venv --banco --pypi --grava
    (--grava salva em navi_etls/ambiente/<máquina>_<data>.md; sem ele, só na tela ou no --saida)
"""
import argparse
import os
import platform
import re
import shutil
# subprocess: só os comandos fixos abaixo, sem shell
import subprocess  # nosec B404
import sys
from datetime import datetime
from typing import Callable, Dict, List, Optional, Sequence, Tuple

TEMPO = 120                       # s por comando
PASTA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ambiente')
INTERESSA = re.compile(r'^(python3|mariadb|mysql|mongo|httpd|apache2|supervisor|gcc|libmariadb|libmysql)', re.I)
PYTHONS = ('python3', 'python', 'python3.9', 'python3.10', 'python3.11', 'python3.12', 'python3.13', 'python3.14')
FERRAMENTAS = (('Apache (httpd)', ['httpd', '-v']), ('Apache (apache2)', ['apache2', '-v']),
               ('supervisor', ['supervisord', '--version']), ('cliente MariaDB', ['mariadb', '--version']),
               ('cliente MySQL', ['mysql', '--version']), ('mongosh', ['mongosh', '--version']),
               ('gcc', ['gcc', '--version']), ('git', ['git', '--version']))
CONSULTAS_SQL = (('versão', 'SELECT VERSION()'), ('comentário da versão', 'SELECT @@version_comment'),
                 ('compilado para', 'SELECT @@version_compile_os'), ('InnoDB', 'SELECT @@innodb_version'),
                 ('charset do servidor', 'SELECT @@character_set_server'),
                 ('collation do servidor', 'SELECT @@collation_server'), ('sql_mode', 'SELECT @@sql_mode'),
                 ('max_allowed_packet', 'SELECT @@max_allowed_packet'),
                 ('max_statement_time (MariaDB)', 'SELECT @@max_statement_time'),
                 ('lower_case_table_names', 'SELECT @@lower_case_table_names'),
                 ('fuso', 'SELECT @@time_zone'), ('fuso do sistema', 'SELECT @@system_time_zone'))
VARIAVEIS_PIP = ('http_proxy', 'https_proxy', 'no_proxy', 'pip_index_url', 'pip_extra_index_url', 'pip_trusted_host')


def mascara(texto: Optional[str]) -> str:
    """Tira a senha de URIs e proxies (`esquema://usuario:senha@host` -> `esquema://usuario:***@host`)."""
    return re.sub(r'(//[^:/@\s]+:)\S*@', r'\1***@', texto or '')


def celula(valor) -> str:
    """Valor de uma célula da tabela Markdown: mascarado, sem quebra de linha nem `|` (que abriria outra coluna)."""
    if valor is None or valor == '':
        return '-'
    return mascara(str(valor)).replace('\n', ' ').replace('|', '\\|')


def le_os_release(caminho: str = '/etc/os-release') -> Dict[str, str]:
    """Os campos do /etc/os-release ({} se não existe, como no Windows)."""
    try:
        with open(caminho, encoding='utf-8') as arquivo:
            linhas = arquivo.read().splitlines()
    except OSError:
        return {}
    campos = {}
    for linha in linhas:
        if '=' in linha and not linha.startswith('#'):
            chave, valor = linha.split('=', 1)
            campos[chave.strip()] = valor.strip().strip('"')
    return campos


def executa(comando: Sequence[str], tempo: int = TEMPO) -> Optional[str]:
    """Saída (stdout e stderr) de um comando fixo; None se o programa não existe."""
    if not shutil.which(comando[0]):
        return None
    try:
        # o comando é uma das listas fixas deste módulo, sem shell
        opcoes = {'capture_output': True, 'timeout': tempo, 'encoding': 'utf-8', 'errors': 'replace'}
        resultado = subprocess.run(list(comando), **opcoes)  # nosec B603
    except (OSError, subprocess.TimeoutExpired) as erro:
        return f'(erro: {erro})'
    return (resultado.stdout + resultado.stderr).strip()


def filtra_interessa(saida: Optional[str]) -> Optional[str]:
    """Só as linhas de pacotes que interessam (python3*, mariadb, mysql, mongo, Apache, supervisor, gcc)."""
    if saida is None:
        return None
    return '\n'.join(sorted(linha for linha in saida.splitlines() if INTERESSA.match(linha.strip())))


class Inventario:
    """Monta o relatório em Markdown. `executa`, `existe` e `os_release` são trocados nos testes."""

    def __init__(self, executa: Callable = executa, existe: Callable = shutil.which,
                 os_release: str = '/etc/os-release'):
        self.executa, self.existe, self.os_release = executa, existe, os_release
        self.partes: List[str] = []

    # ------------------------------------------------------------------ escrita
    def secao(self, titulo: str):
        self.partes += ['', f'## {titulo}', '']

    def campos(self, linhas: Sequence[Tuple[str, object]]):
        self.partes += ['| campo | valor |', '|---|---|']
        self.partes += [f'| {nome} | {celula(valor)} |' for nome, valor in linhas]
        self.partes.append('')

    def bloco(self, titulo: str, saida: Optional[str]):
        self.partes.append(f'**{titulo}**')
        if saida is None:
            self.partes += ['', 'não encontrado', '']
            return
        self.partes += ['', '```', mascara(saida) or '(sem saída)', '```', '']

    # ------------------------------------------------------------------ seções
    def sistema(self):
        self.secao('1. Sistema operacional')
        linhas = [('máquina', platform.node()), ('plataforma', platform.platform()),
                  ('sistema', platform.system()), ('versão', platform.version()), ('arquitetura', platform.machine())]
        if platform.system() == 'Windows':
            linhas += [('Windows', ' '.join(platform.win32_ver())), ('edição', platform.win32_edition())]
        release = le_os_release(self.os_release)
        linhas += [(f'os-release {chave}', release.get(chave)) for chave in ('NAME', 'VERSION', 'ID', 'ID_LIKE')
                   if release.get(chave)]
        if release:
            linhas.append(('kernel', platform.release()))
        self.campos(linhas)
        if platform.system() == 'Windows':
            self.bloco('ver', self.executa(['cmd', '/c', 'ver']))

    def pythons(self):
        self.secao('2. Python instalado')
        self.campos([('este interpretador', sys.version.replace('\n', ' ')), ('executável', sys.executable)])
        for nome in PYTHONS:
            caminho = self.existe(nome)
            if caminho:
                self.bloco(f'{nome} ({caminho})', self.executa([nome, '--version']))
        if self.existe('py'):
            self.bloco('lançador py -0p (Windows)', self.executa(['py', '-0p']))

    def oferta(self):
        """O que os repositórios OFICIAIS da distribuição oferecem (a regra do usuário para escolher versões)."""
        self.secao('3. Oferecido pelos repositórios da distribuição')
        gerenciador = next((nome for nome in ('dnf', 'yum') if self.existe(nome)), None)
        if gerenciador:
            self.bloco('Python', self.executa([gerenciador, '-q', 'list', '--available', '--showduplicates',
                                               'python3', 'python3.1?', 'python3.1?-devel']))
            self.bloco('MariaDB e conector C', self.executa([gerenciador, '-q', 'list', '--available',
                                                             'mariadb*', 'mariadb-connector-c*']))
            self.bloco('módulos (AppStream)', self.executa([gerenciador, '-q', 'module', 'list', 'python*',
                                                            'mariadb', 'mysql']))
        elif self.existe('apt-cache'):
            self.bloco('Python', self.executa(['apt-cache', 'policy', 'python3'] +
                                              [f'python3.{menor}' for menor in range(9, 15)]))
            self.bloco('MariaDB e conector C', self.executa(['apt-cache', 'policy', 'mariadb-server',
                                                             'mariadb-client', 'libmariadb-dev']))
        else:
            self.partes.append('(sem dnf, yum ou apt: no Windows, a "distribuição" é o instalador oficial do '
                               'python.org)')

    def instalados(self):
        self.secao('4. Pacotes do sistema instalados (os que interessam)')
        if self.existe('rpm'):
            self.bloco('rpm -qa', filtra_interessa(self.executa(['rpm', '-qa'])))
        elif self.existe('dpkg-query'):
            self.bloco('dpkg-query', filtra_interessa(self.executa(['dpkg-query', '-W', '-f',
                                                                    '${Package} ${Version}\n'])))
        else:
            self.partes.append('(sem rpm nem dpkg)')

    def ferramentas(self):
        self.secao('5. Serviços e ferramentas')
        for titulo, comando in FERRAMENTAS:
            self.bloco(titulo, self.executa(comando))
        modulos = self.executa(['apachectl', '-M'])
        if modulos is not None:
            modulos = '\n'.join(linha for linha in modulos.splitlines() if 'proxy' in linha or 'wsgi' in linha)
        self.bloco('módulos de proxy do Apache (apachectl -M)', modulos)

    def venvs(self, caminhos: Sequence[str]):
        self.secao('6. Venvs')
        if not caminhos:
            self.partes.append('(nenhum --venv)')
        for caminho in caminhos:
            python = next((os.path.join(caminho, *partes) for partes in (('Scripts', 'python.exe'), ('bin', 'python'))
                           if os.path.exists(os.path.join(caminho, *partes))), None)
            if not python:
                self.bloco(f'venv {caminho}', None)
                continue
            self.bloco(f'venv {caminho}: versão', self.executa([python, '--version']))
            self.bloco(f'venv {caminho}: pip freeze', self.executa([python, '-m', 'pip', 'freeze']))

    def bancos(self):
        self.secao('7. Bancos (SQL_URI e MONGODB_URI do ambiente)')
        self.campos(dados_sql(os.environ.get('SQL_URI')) + dados_mongo(os.environ.get('MONGODB_URI')))

    def pypi(self):
        self.secao('8. Acesso ao PyPI')
        self.campos([(nome, os.environ.get(nome) or os.environ.get(nome.upper())) for nome in VARIAVEIS_PIP])
        self.bloco('pip config list', self.executa([sys.executable, '-m', 'pip', 'config', 'list']))
        self.bloco('pip index versions fastapi', self.executa([sys.executable, '-m', 'pip', 'index', 'versions',
                                                               'fastapi']))

    def gera(self, venvs: Sequence[str] = (), banco: bool = False, pypi: bool = False) -> str:
        self.partes = [f'# Inventário do ambiente — {platform.node()} — {datetime.now():%d/%m/%Y %H:%M}']
        self.sistema()
        self.pythons()
        self.oferta()
        self.instalados()
        self.ferramentas()
        self.venvs(venvs)
        if banco:
            self.bancos()
        if pypi:
            self.pypi()
        return '\n'.join(self.partes) + '\n'


def dados_sql(uri: Optional[str]) -> List[Tuple[str, object]]:
    """Versão e configurações do MariaDB/MySQL (cada consulta com o próprio erro, sem derrubar as outras)."""
    if not uri:
        return [('SQL_URI', None)]
    from sqlalchemy import create_engine, text
    engine = create_engine(uri)
    linhas: List[Tuple[str, object]] = [('SQL_URI', uri), ('driver', f'{engine.dialect.name}+{engine.dialect.driver}')]
    try:
        with engine.connect() as conexao:
            for nome, consulta in CONSULTAS_SQL:
                try:
                    linhas.append((nome, conexao.execute(text(consulta)).scalar()))
                except Exception as erro:  # variável que não existe nesta versão
                    linhas.append((nome, f'(erro: {type(erro).__name__})'))
    except Exception as erro:  # sem conexão: registra e segue
        linhas.append(('conexão', f'(erro: {type(erro).__name__}: {str(erro)[:200]})'))
    return linhas


def dados_mongo(uri: Optional[str]) -> List[Tuple[str, object]]:
    if not uri:
        return [('MONGODB_URI', None)]
    import pymongo
    linhas: List[Tuple[str, object]] = [('MONGODB_URI', uri), ('pymongo', pymongo.version)]
    try:
        cliente = pymongo.MongoClient(uri, serverSelectionTimeoutMS=10000)
        linhas.append(('MongoDB', cliente.server_info().get('version')))
    except Exception as erro:  # sem conexão: registra e segue
        return linhas + [('conexão', f'(erro: {type(erro).__name__}: {str(erro)[:200]})')]
    for nome, comando, chave in (('featureCompatibilityVersion', {'getParameter': 1, 'featureCompatibilityVersion': 1},
                                  'featureCompatibilityVersion'),
                                 ('motor de armazenamento', {'serverStatus': 1}, 'storageEngine')):
        try:
            linhas.append((nome, cliente.admin.command(comando).get(chave)))
        except Exception as erro:  # sem permissão no admin
            linhas.append((nome, f'(erro: {type(erro).__name__})'))
    return linhas


def _argumentos(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--venv', action='append', default=[], help='pasta de um venv (pode repetir)')
    parser.add_argument('--banco', action='store_true', help='pergunta a versão ao MariaDB e ao MongoDB')
    parser.add_argument('--pypi', action='store_true', help='confere o acesso ao PyPI (rede)')
    parser.add_argument('--grava', action='store_true', help=f'grava em {PASTA}/<máquina>_<data>.md')
    parser.add_argument('--saida', help='grava neste arquivo')
    return parser.parse_args(argv)


def main(argv=None):  # pragma: no cover - depende da máquina
    args = _argumentos(argv)
    relatorio = Inventario().gera(args.venv, args.banco, args.pypi)
    caminho = args.saida
    if args.grava and not caminho:
        os.makedirs(PASTA, exist_ok=True)
        caminho = os.path.join(PASTA, f'{platform.node()}_{datetime.now():%Y%m%d}.md')
    if caminho:
        with open(caminho, 'w', encoding='utf-8') as arquivo:
            arquivo.write(relatorio)
        print(f'gravado em {caminho}')
    else:
        print(relatorio)


if __name__ == '__main__':  # pragma: no cover
    main()
