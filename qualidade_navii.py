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
    python qualidade_navii.py            # tudo
    python qualidade_navii.py --rapido   # sem pytest
"""
import argparse
# subprocess: só chama as ferramentas abaixo, com argumentos fixos
import subprocess  # nosec B404
import sys

MODULOS = [
    'virasana/navii',
    'virasana/entregas',
    'virasana/apreensoes',
    'virasana/risco_historico',
    'virasana/completude',
    'virasana/analytic_scripts/dados_transporte_apreensoes.py',
    'virasana/analytic_scripts/dados_risco_perdimentos.py',
    'virasana/analytic_scripts/diagnostico_risco_perdimentos.py',
    'virasana/analytic_scripts/diagnostico_fichas_repressao.py',
    'virasana/analytic_scripts/diagnostico_estatisticas_uso.py',
    'virasana/analytic_scripts/diagnostico_coleta_uso.py',
    'virasana/analytic_scripts/diagnostico_texto_navii.py',
    'virasana/analytic_scripts/diagnostico_risco_ce.py',
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
]

FLAKE8 = ['--max-line-length', '120', '--max-complexity', '10',
          # D (docstrings) e T000 (TODO) seguem o tox; W503/W504 são excludentes entre si
          '--ignore', 'D,T000,E402,W503,W504']
BANDIT_IGNORADOS = 'B301,B403,B311'  # os do tox.ini (B410 não existe no bandit atual)


def _existentes(caminhos):
    import os
    return [c for c in caminhos if os.path.exists(c)]


def passo(nome, comando, ignorar_avisos=()) -> bool:
    """Roda uma ferramenta e mostra a saída (sem as linhas de aviso conhecidas)."""
    print(f'\n=== {nome}: {" ".join(comando[2:])[:150]}', flush=True)
    # comando montado aqui, sem entrada externa
    execucao = subprocess.run(comando, capture_output=True, text=True)  # nosec B603
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


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--rapido', action='store_true', help='pula o pytest')
    args = parser.parse_args()
    modulos, testes = _existentes(MODULOS), _existentes(TESTES)
    resultados = []
    if not args.rapido:
        resultados.append(passo('pytest', [sys.executable, '-m', 'pytest', '-q', '--disable-warnings',
                                           '-p', 'no:cacheprovider', *testes]))
    resultados.append(passo('flake8', [sys.executable, '-m', 'flake8', *FLAKE8, *modulos]))
    # o bandit avisa "nosec encountered" em cada linha de um SQL multilinha marcado com nosec
    resultados.append(passo('bandit', [sys.executable, '-m', 'bandit', '-q', '-r', *modulos,
                                       '-s', BANDIT_IGNORADOS], ignorar_avisos=('nosec encountered',)))
    resultados.append(radon_ate_c(modulos))
    print('\nRESULTADO:', 'tudo OK' if all(resultados) else 'há falhas (ver acima)')
    sys.exit(0 if all(resultados) else 1)


if __name__ == '__main__':  # pragma: no cover
    main()
