#!/usr/bin/env python3
"""
Pesquisa CEs de importação para o CNPJ. Destes CEs, localiza as saídas dos
contêineres carregados pelos gates dos recintos (Acesso Veículo), para ter as
placas e possibilitar a localização das entregas. Grava uma planilha Excel.

A mesma consulta está na tela /entregas_cnpj/ (virasana/entregas); a lógica
fica em `virasana/entregas/busca_entregas.py`.

Uso (a partir da raiz do repo virasana):
    python virasana/analytic_scripts/entregas_cnpj.py <cnpj ou raiz> [<inicio> <fim>]
    python virasana/analytic_scripts/entregas_cnpj.py 12345678000190 2026-05-01 2026-05-31

Sem datas, usa os últimos 30 dias.
"""
import sys
from datetime import datetime, timedelta

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')

from virasana.entregas.busca_entregas import ParametrosInvalidos, busca_entregas, planilha_xlsx  # noqa: E402


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if not argv:
        print(__doc__)
        return 1
    cnpj = argv[0]
    try:
        inicio = datetime.strptime(argv[1], '%Y-%m-%d').date()
        fim = datetime.strptime(argv[2], '%Y-%m-%d').date()
    except (IndexError, ValueError):
        fim = datetime.now().date()
        inicio = fim - timedelta(days=30)
        print(f'Datas não informadas ou inválidas: usando {inicio} a {fim}.')

    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine

    print(f'CNPJ: {cnpj}  Período de saída: {inicio} a {fim}')
    engine = create_engine(SQL_URI)
    try:
        with engine.connect() as conexao:
            resultado = busca_entregas(conexao, cnpj, inicio, fim)
    except ParametrosInvalidos as err:
        print(f'[ERRO] {err}')
        return 1
    finally:
        engine.dispose()

    saida = f"entregas_{resultado['cnpj']}_{inicio}_{fim}.xlsx"
    with open(saida, 'wb') as arquivo:
        arquivo.write(planilha_xlsx(resultado))
    print(f"{resultado['qtd_ces']} CEs de importação, {resultado['qtd_conteineres']} contêineres; "
          f"{len(resultado['linhas'])} saídas localizadas; "
          f"{len(resultado['conteineres_sem_saida'])} contêineres sem saída no período.")
    print(f'Planilha gravada em {saida}')
    return 0


if __name__ == '__main__':  # pragma: no-cover
    sys.exit(main())
