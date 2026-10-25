# coding: utf-8
"""
Verificações de qualidade dos módulos novos de análise de risco (NAVII).

Roda, com o Python atual (o venv do projeto), na raiz do repositório virasana:

  1. pytest      — os testes dos módulos novos (dados simulados, sem tocar nas bases);
  2. flake8      — estilo, aspas (flake8-quotes) e complexidade ciclomática <= 10;
  3. bandit      — análise de segurança (mesmos testes ignorados do tox);
  4. radon       — complexidade: falha a partir da nota D (> 20); nota C só é listada.

Sai com código diferente de zero se algum passo falhar. As regras valem só para
estes módulos — o código legado continua com a configuração do tox.ini.

Uso:
    python qualidade_navii.py                                # tudo
    python qualidade_navii.py --rapido                       # sem pytest
    python qualidade_navii.py --alterados                    # só o que os arquivos alterados (git) alcançam
    python qualidade_navii.py --alterados --desde ORIG_HEAD  # idem, também o que mudou desde um commit
    tox -e navii [-- --alterados]                            # o mesmo num ambiente isolado (requirements-gate.txt)

--alterados é para o ciclo de desenvolvimento: roda os testes afetados pelos arquivos alterados e o
flake8/bandit/radon só nos alterados do escopo do gate. Afetados por um .py: os testes que executaram alguma linha
dele na última rodada inteira (mapa gravado pelo pytest-cov em MAPA) e os que o importam direto; .py fora do mapa
(novo, ou sem mapa): os que o importam direta ou indiretamente. Template, dado ou arquivo estático vale pelos .py
que citam o nome dele. Mudança no próprio gate, no tox.ini, no setup.py, num conftest.py ou um .py removido valem
o gate inteiro. Antes de dar a tarefa por concluída (e antes do deploy): o gate inteiro.
"""
import argparse
import ast
import importlib.util
import json
import os
# subprocess: só chama as ferramentas abaixo, com argumentos fixos
import subprocess  # nosec B404
import sys
from datetime import datetime

MODULOS = [
    'virasana/navii',
    'virasana/entregas',
    'virasana/apreensoes',
    'virasana/risco_historico',
    'virasana/completude',
    'virasana/saude',
    'virasana/analytic_scripts/dados_transporte_apreensoes.py',
    'virasana/analytic_scripts/dados_risco_perdimentos.py',
    'virasana/analytic_scripts/diagnostico_risco_perdimentos.py',
    'virasana/analytic_scripts/diagnostico_fichas_repressao.py',
    'virasana/analytic_scripts/diagnostico_estatisticas_uso.py',
    'virasana/analytic_scripts/diagnostico_coleta_uso.py',
    'virasana/analytic_scripts/diagnostico_texto_navii.py',
    'virasana/analytic_scripts/diagnostico_risco_ce.py',
    'virasana/analytic_scripts/declaracao',
    'virasana/analytic_scripts/entregas_cnpj.py',
    'virasana/analytic_scripts/ciclo_de_vida/diagnostico_ciclo_de_vida.py',
    'virasana/analytic_scripts/ciclo_de_vida/diagnostico_ces_ciclo_de_vida.py',
    'virasana/analytic_scripts/ciclo_de_vida/linha_do_tempo.py',
    'virasana/analytic_scripts/ciclo_de_vida/diagnostico_operador_tra.py',
    'virasana/analytic_scripts/ciclo_de_vida/diagnostico_final_curta.py',
    'virasana/analytic_scripts/ciclo_de_vida/eventos_conteiner.py',
    'virasana/analytic_scripts/ciclo_de_vida/ces_do_ciclo.py',
    'virasana/analytic_scripts/ciclo_de_vida/heuristica_ciclo.py',
    'virasana/analytic_scripts/ciclo_de_vida/sombra_ciclo_de_vida.py',
    'virasana/analytic_scripts/ciclo_de_vida/dados_conteiner.py',
    'virasana/analytic_scripts/ciclo_de_vida/carga_ciclo.py',
    'virasana/analytic_scripts/ciclo_de_vida/imagens_ciclo.py',
    'virasana/analytic_scripts/ciclo_de_vida/gravacao_ciclo.py',
    'virasana/analytic_scripts/ciclo_de_vida/servico_ciclo_de_vida.py',
    'virasana/analytic_scripts/ciclo_de_vida/acompanha_ciclo_de_vida.py',
    'virasana/analytic_scripts/ciclo_de_vida/estagios_ciclo.py',
    'virasana/analytic_scripts/ciclo_de_vida/diagnostico_imagens_ciclo.py',
    'virasana/analytic_scripts/ciclo_de_vida/pesagens_ciclo.py',
    'virasana/analytic_scripts/ciclo_de_vida/diagnostico_pesagens.py',
    'virasana/analytic_scripts/ciclo_de_vida/diagnostico_ciclo_outros_portos.py',
    'virasana/analytic_scripts/ciclo_de_vida/acesso_pela_placa.py',
    'virasana/analytic_scripts/ciclo_de_vida/diagnostico_acesso_pela_placa.py',
    'virasana/analytic_scripts/ciclo_de_vida/caso_ciclo.py',
    'virasana/analytic_scripts/ciclo_de_vida/diagnostico_porto_desembarque.py',
    'virasana/analytic_scripts/ciclo_de_vida/invariantes_ciclo.py',
    'virasana/analytic_scripts/ciclo_de_vida/audita_ciclos.py',
    'virasana/navii/pesagem_navii.py',
    'navi_etls/diagnosticos',            # NAVI-ETL, F0 (até o gate próprio da F2)
]

TESTES = [
    'virasana/tests/test_navii.py',
    'virasana/tests/test_navii_busca_sqlite.py',
    'virasana/tests/test_navii_referencias.py',
    'virasana/tests/test_navii_filtros_salvos.py',
    'virasana/tests/test_navii_ocultar.py',
    'virasana/tests/test_navii_sem_desembarque.py',
    'virasana/tests/test_navii_ficha_card.py',
    'virasana/tests/test_navii_acompanhamento.py',
    'virasana/tests/test_navii_coleta_uso.py',
    'virasana/tests/test_navii_estatisticas_uso.py',
    'virasana/tests/test_navii_estatisticas_tela.py',
    'virasana/tests/test_navii_resultado_fichas.py',
    'virasana/tests/test_navii_mercante_por_id.py',
    'virasana/tests/test_navii_texto.py',
    'virasana/tests/test_navii_indexador_texto.py',
    'virasana/tests/test_navii_texto_tela.py',
    'virasana/tests/test_navii_filtro_peso.py',
    'virasana/tests/test_navii_filtro_trafego.py',
    'virasana/tests/test_navii_imagem_fallback.py',
    'virasana/tests/test_pesagens_ciclo.py',
    'virasana/tests/test_diagnostico_pesagens.py',
    'virasana/tests/test_diagnostico_ciclo_outros_portos.py',
    'virasana/tests/test_navii_fichas_ovr.py',
    'virasana/tests/test_navii_uso.py',
    'virasana/tests/test_entregas_cnpj.py',
    'virasana/tests/test_dados_transporte_apreensoes.py',
    'virasana/tests/test_apreensoes_transporte_tela.py',
    'virasana/tests/test_diagnostico_risco_perdimentos.py',
    'virasana/tests/test_diagnostico_fichas_repressao.py',
    'virasana/tests/test_diagnostico_estatisticas_uso.py',
    'virasana/tests/test_diagnostico_coleta_uso.py',
    'virasana/tests/test_diagnostico_texto_navii.py',
    'virasana/tests/test_dados_risco_perdimentos.py',
    'virasana/tests/test_risco_historico_tela.py',
    'virasana/tests/test_risco_historico_ce.py',
    'virasana/tests/test_risco_historico_lotes.py',
    'virasana/tests/test_completude_tela.py',
    'virasana/tests/test_diagnostico_ciclo_de_vida.py',
    'virasana/tests/test_diagnostico_ces_ciclo_de_vida.py',
    'virasana/tests/test_linha_do_tempo.py',
    'virasana/tests/test_sombra_ciclo_de_vida.py',
    'virasana/tests/test_servico_ciclo_de_vida.py',
    'virasana/tests/test_acompanha_ciclo_de_vida.py',
    'virasana/tests/test_estagios_ciclo.py',
    'virasana/tests/test_diagnostico_imagens_ciclo.py',
    'virasana/tests/test_diagnostico_operador_tra.py',
    'virasana/tests/test_diagnostico_final_curta.py',
    'virasana/tests/test_eventos_conteiner.py',
    'virasana/tests/test_imagens_sem_inspecao.py',
    'virasana/tests/test_prefixo_proxy.py',
    'virasana/tests/test_acesso_pela_placa.py',
    'virasana/tests/test_diagnostico_acesso_pela_placa.py',
    'virasana/tests/test_navii_pesagem.py',
    'virasana/tests/test_navii_diagnostico_card.py',
    'virasana/tests/test_caso_ciclo.py',
    'virasana/tests/test_navii_atracacao_prevista.py',
    'virasana/tests/test_diagnostico_porto_desembarque.py',
    'virasana/tests/test_caso_ciclo_caau.py',
    'virasana/tests/test_ces_porto_desembarque.py',
    'virasana/tests/test_casos_reais_ciclo.py',
    'virasana/tests/test_audita_ciclos.py',
    'virasana/tests/test_qualidade_navii.py',
    'virasana/tests/test_navii_visoes.py',
    'virasana/tests/test_saude_fontes.py',
    'virasana/tests/test_saude_ciclo.py',
    'virasana/tests/test_saude_historico.py',
    'virasana/tests/test_saude_tela.py',
    'virasana/tests/test_regras_declaracao.py',
    'virasana/tests/test_diagnostico_declaracao.py',
    'navi_etls/tests/test_etl_volumes.py',
    'navi_etls/tests/test_etl_inventario_ambiente.py',
]

FLAKE8 = ['--max-line-length', '120', '--max-complexity', '10',
          # D (docstrings) e T000 (TODO) seguem o tox; W503/W504 são excludentes entre si
          '--ignore', 'D,T000,E402,W503,W504']
BANDIT_IGNORADOS = 'B301,B403,B311'  # os do tox.ini (B410 não existe no bandit atual)

# com --alterados, mudança num destes arquivos vale o gate inteiro (valem para todos os testes)
GERAIS = ('qualidade_navii.py', 'tox.ini', 'setup.py', 'pyproject.toml', 'requirements-gate.txt', 'conftest.py')
TEMPLATES = 'virasana/templates'
# gravados pela rodada inteira (no .gitignore; fora do padrão .coverage.*, que o pytest-cov combina)
COBERTURA = '.cobertura_gate'
MAPA = '.mapa_testes_gate.json'


def _existentes(caminhos):
    return [c for c in caminhos if os.path.exists(c)]


def _git(*argumentos):
    """Linhas da saída de um comando git (caminhos com acento sem escape)."""
    # git com argumentos montados aqui, sem entrada externa além do commit de --desde
    return subprocess.run(['git', '-c', 'core.quotepath=false', *argumentos],  # nosec B603 B607
                          capture_output=True, encoding='utf-8', check=True).stdout.splitlines()


def alterados(desde=None):
    """{caminho: removido?}: o que não foi comitado (os novos incluídos) e, com `desde`, o que mudou desde ele."""
    caminhos = {}
    if desde:
        for linha in _git('diff', '--name-status', desde):
            campos = linha.split('\t')
            caminhos[campos[-1]] = campos[0].startswith('D')
    for linha in _git('status', '--porcelain', '--untracked-files=all'):
        caminhos[linha[3:].split(' -> ')[-1].strip('"')] = 'D' in linha[:2]
    return caminhos


def _arquivos_do_import(nome):
    """Arquivos de um import do virasana: os __init__.py dos pacotes do caminho e o módulo."""
    partes = nome.split('.')
    candidatos = ['/'.join(partes[:fim]) + '/__init__.py' for fim in range(1, len(partes) + 1)]
    return [c for c in candidatos + ['/'.join(partes) + '.py'] if os.path.isfile(c)]


def _importados(caminho):
    """Nomes absolutos dos módulos do virasana importados por um arquivo .py."""
    try:
        with open(caminho, encoding='utf-8') as arquivo:
            arvore = ast.parse(arquivo.read())
    except (OSError, SyntaxError, UnicodeDecodeError):
        return []
    pacote = os.path.dirname(caminho).split('/')
    nomes = []
    for no in ast.walk(arvore):
        if isinstance(no, ast.Import):
            nomes += [apelido.name for apelido in no.names]
        elif isinstance(no, ast.ImportFrom):
            base = no.module or ''
            if no.level:  # import relativo: a partir do pacote do arquivo
                base = '.'.join(pacote[:len(pacote) - no.level + 1] + ([base] if base else []))
            nomes += [base] + [f'{base}.{apelido.name}' for apelido in no.names]
    return [nome for nome in nomes if nome.split('.')[0] == 'virasana']


def _alcance(inicio, diretos):
    """Arquivos do virasana que `inicio` alcança pelos imports, ele incluído (`diretos`: cache dos imports)."""
    vistos, fila = set(), [inicio]
    while fila:
        arquivo = fila.pop()
        if arquivo in vistos:
            continue
        vistos.add(arquivo)
        if arquivo not in diretos:
            diretos[arquivo] = {alvo for nome in _importados(arquivo) for alvo in _arquivos_do_import(nome)}
        fila.extend(diretos[arquivo] - vistos)
    return vistos


def _textos(arquivos):
    """{caminho: conteúdo} dos .py alcançados e dos templates — onde se procura quem cita um arquivo não .py."""
    caminhos = set(arquivos)
    for raiz, _, nomes in os.walk(TEMPLATES):
        caminhos |= {os.path.join(raiz, nome).replace(os.sep, '/') for nome in nomes}
    textos = {}
    for caminho in caminhos:
        try:
            with open(caminho, encoding='utf-8') as arquivo:
                textos[caminho] = arquivo.read()
        except (OSError, UnicodeDecodeError):
            continue
    return textos


def _citam(caminho, textos):
    """Arquivos que citam o nome de `caminho` ou, se nenhum cita, o da pasta dele (corpus lido com glob)."""
    for chave in (os.path.basename(caminho), os.path.basename(os.path.dirname(caminho))):
        citantes = {outro for outro, texto in textos.items() if chave and chave in texto}
        if citantes:
            return citantes
    return set()


def _py_que_citam(caminho, textos):
    """Os .py que citam um arquivo não .py, direto ou por templates que o citam (include, extends, script)."""
    resultado, fila, vistos = set(), [caminho], set()
    while fila:
        for citante in _citam(fila.pop(), textos) - vistos:
            vistos.add(citante)
            if citante.endswith('.py'):
                resultado.add(citante)
            else:
                fila.append(citante)
    return resultado


def _no_escopo(caminho, modulos):
    return any(caminho == modulo or caminho.startswith(modulo.rstrip('/') + '/') for modulo in modulos)


def _le_mapa():
    """({arquivo: testes que o executaram}, quando foi gravado) da última rodada inteira; (None, None) sem mapa."""
    try:
        with open(MAPA, encoding='utf-8') as entrada:
            conteudo = json.load(entrada)
        return conteudo['arquivos'], conteudo['gravado']
    except (OSError, ValueError, KeyError):
        return None, None


def _grava_mapa():
    """Grava em MAPA, a partir dos contextos do pytest-cov, os testes que executaram cada arquivo."""
    from coverage import CoverageData
    cobertura = CoverageData(basename=COBERTURA)
    cobertura.read()
    mapa = {}
    for arquivo in cobertura.measured_files():
        # contexto "virasana/tests/test_x.py::Classe::test_y|run"; o vazio é a importação (coleta)
        testes = {contexto.split('::')[0] for contextos in cobertura.contexts_by_lineno(arquivo).values()
                  for contexto in contextos if '::' in contexto}
        mapa[os.path.relpath(arquivo).replace(os.sep, '/')] = sorted(testes)
    if not mapa:  # o pytest não chegou a medir nada: fica o mapa anterior
        print('Sem dados de cobertura: o mapa dos testes por arquivo não foi gravado.')
        return
    with open(MAPA, 'w', encoding='utf-8') as saida:
        json.dump({'gravado': datetime.now().isoformat(timespec='minutes'), 'arquivos': mapa}, saida)
    print(f'Mapa dos testes por arquivo (para o --alterados) gravado em {MAPA}.')


def _afetados(arquivos, testes, alcance, diretos, mapa):
    """Testes afetados por arquivos .py: pelo mapa, os que executaram o arquivo e os que o importam direto (uma
    constante usada no teste); arquivo fora do mapa (novo, ou sem mapa), os que o alcançam pelos imports."""
    afetados = set()
    for arquivo in arquivos:
        if mapa is not None and arquivo in mapa:
            afetados |= set(mapa[arquivo]) | {t for t in testes if arquivo == t or arquivo in diretos.get(t, ())}
        else:
            afetados |= {t for t in testes if arquivo in alcance[t]}
    return afetados & set(testes)


def _gerais(mudancas):
    """As mudanças que valem para todos os testes: os arquivos GERAIS e um .py removido."""
    return [c for c, removido in mudancas.items()
            if os.path.basename(c) in GERAIS or (removido and c.endswith('.py'))]


def selecao(modulos, testes, desde=None):
    """(módulos, testes, parcial?) do gate afetados pelos arquivos alterados; o gate inteiro se mudou algo geral."""
    mudancas = alterados(desde)
    gerais = _gerais(mudancas)
    if gerais:
        print(f'Alterado(s) {", ".join(sorted(gerais))}: vale o gate inteiro.')
        return modulos, testes, False
    diretos = {}
    alcance = {teste: _alcance(teste, diretos) for teste in testes}
    textos = _textos(set().union(*alcance.values()))
    mapa, gravado = _le_mapa()
    por_mudanca = {c: _afetados({c} if c.endswith('.py') else _py_que_citam(c, textos), testes, alcance, diretos,
                                mapa)
                   for c in mudancas}
    escolhidos = [teste for teste in testes if any(teste in afetados for afetados in por_mudanca.values())]
    _mostra_selecao(mudancas, por_mudanca, escolhidos, len(testes), gravado)
    return [c for c in mudancas if c.endswith('.py') and _no_escopo(c, modulos)], escolhidos, True


def _mostra_selecao(mudancas, por_mudanca, escolhidos, total, gravado):
    print(f'--alterados: {len(mudancas)} arquivo(s) alterado(s); {len(escolhidos)} de {total} arquivos de teste',
          f'(mapa da rodada inteira de {gravado})' if gravado else
          '(SEM mapa: seleção pelos imports, mais larga — rode o gate inteiro uma vez)')
    for teste in escolhidos:
        print('   ', teste)
    sem_teste = sorted(c for c, afetados in por_mudanca.items() if not afetados)
    if sem_teste:
        print('Nenhum teste do gate alcança:', ', '.join(sem_teste))
    fora = sorted(c for c in mudancas if os.path.basename(c).startswith('test_') and c.endswith('.py')
                  and c not in por_mudanca[c])
    if fora:
        print('Teste fora da lista TESTES do gate (teste novo entra lá):', ', '.join(fora))


def passo(nome, comando, ignorar_avisos=(), ambiente=None) -> bool:
    """Roda uma ferramenta e mostra a saída (sem as linhas de aviso conhecidas)."""
    print(f'\n=== {nome}: {" ".join(comando[2:])[:150]}', flush=True)
    # comando montado aqui, sem entrada externa
    execucao = subprocess.run(comando, capture_output=True, text=True, env=ambiente)  # nosec B603
    for linha in (execucao.stdout + execucao.stderr).splitlines():
        if not any(aviso in linha for aviso in ignorar_avisos):
            print(linha)
    ok = execucao.returncode == 0
    print(f'--- {nome}: {"OK" if ok else "FALHOU"}', flush=True)
    return ok


def radon_ate_c(modulos) -> bool:
    """Lista as funções nota C e falha se houver nota D ou pior."""
    print('\n=== radon cc (nota C: listada; D ou pior: falha)', flush=True)
    saida = subprocess.run([sys.executable, '-m', 'radon', 'cc', '-s', '-n', 'C', *modulos],  # nosec B603
                           capture_output=True, text=True).stdout
    print(saida.rstrip() or '(nenhuma função acima de B)')
    ruins = [linha for linha in saida.splitlines() if ' - D ' in linha or ' - E ' in linha or ' - F ' in linha]
    print(f'--- radon: {"FALHOU" if ruins else "OK"}', flush=True)
    return not ruins


def _pytest(testes, grava_mapa=False) -> bool:
    """O pytest dos testes; na rodada inteira, com o pytest-cov por teste, para gravar o mapa do --alterados."""
    if not testes:
        print('\n=== pytest: nenhum teste do gate alcança os arquivos alterados', flush=True)
        return True
    comando = [sys.executable, '-m', 'pytest', '-q', '--disable-warnings', '-p', 'no:cacheprovider']
    if not (grava_mapa and importlib.util.find_spec('pytest_cov')):
        return passo('pytest', comando + testes)
    ok = passo('pytest', comando + ['--cov=virasana', '--cov-context=test', '--cov-report=', *testes],
               ambiente={**os.environ, 'COVERAGE_FILE': COBERTURA})
    _grava_mapa()
    return ok


def _estaticas(modulos) -> list:
    """flake8, bandit e radon nos módulos (sem módulo, nada: sem caminho, as ferramentas varreriam o repositório)."""
    if not modulos:
        print('\n=== flake8/bandit/radon: nenhum arquivo alterado no escopo do gate', flush=True)
        return []
    # o bandit avisa "nosec encountered" em cada linha de um SQL multilinha marcado com nosec
    return [passo('flake8', [sys.executable, '-m', 'flake8', *FLAKE8, *modulos]),
            passo('bandit', [sys.executable, '-m', 'bandit', '-q', '-r', *modulos, '-s', BANDIT_IGNORADOS],
                  ignorar_avisos=('nosec encountered',)),
            radon_ate_c(modulos)]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--rapido', action='store_true', help='pula o pytest')
    parser.add_argument('--alterados', action='store_true',
                        help='só os testes que alcançam os arquivos alterados (git) e as verificações estáticas deles')
    parser.add_argument('--desde', metavar='COMMIT',
                        help='com --alterados, também o que mudou desde este commit (ex.: ORIG_HEAD, após o git pull)')
    args = parser.parse_args()
    modulos, testes, parcial = _existentes(MODULOS), _existentes(TESTES), False
    if args.alterados or args.desde:
        modulos, testes, parcial = selecao(modulos, testes, args.desde)
    resultados = [] if args.rapido else [_pytest(testes, grava_mapa=not parcial)]
    resultados += _estaticas(modulos)
    print('\nRESULTADO:', 'tudo OK' if all(resultados) else 'há falhas (ver acima)')
    if parcial:
        print('PARCIAL (--alterados): o gate inteiro é `python qualidade_navii.py` (ou `tox -e navii`).')
    sys.exit(0 if all(resultados) else 1)


if __name__ == '__main__':  # pragma: no cover
    main()
