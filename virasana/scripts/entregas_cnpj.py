#!/usr/bin/env python3
"""
Pesquisa CEs de importação para o CNPJ. Destes CEs, localiza as entregas para ter as placas e
possibilitar a localização das entregas

Uso:
    python entregas_cnpj.py 2026-05-01 2026-05-31 0000000001
"""

import sys
from datetime import datetime, timedelta, time

import pandas as pd

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')
sys.path.append('../ajna_docs/commons')


def consultar_contagem_ovrs(inicio: datetime, fim: datetime, engine) -> int:
    """
    Consultar via SQL usando pandas e o engine para contar registros.

    Args:
        inicio: Data de início (datetime)
        fim: Data de fim (datetime)
        engine: Engine SQLAlchemy já criada

    Returns:
        int: Quantidade de registros encontrados
    """
    # Formata as datas para o formato do MySQL/MariaDB
    inicio_str = inicio.strftime('%Y-%m-%d %H:%M:%S')
    fim_str = fim.strftime('%Y-%m-%d %H:%M:%S')

    # SQL com substituição direta (SAFE porque os valores são validados e formatados)
    query = f"""
           SELECT count(*) as contagem 
           FROM ovr_ovrs 
           WHERE datahora BETWEEN '{inicio_str}' AND '{fim_str}'
       """

    # Usa pandas para executar o SQL
    df = pd.read_sql_query(query, con=engine)

    return int(df['contagem'].iloc[0])


if __name__ == '__main__':  # pragma: no-cover
    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Se tem argumentos, mas não são 2 - mostra erro
    if len(sys.argv) != 3:
        print(f"\n[ERRO] Número incorreto de argumentos: {len(sys.argv) - 1}")
        print("       Esperado: 0 ou 2 argumentos (inicio e fim)")
        show_usage_message()

    # Obtém as datas
    inicio_arg = None
    fim_arg = None
    try:
        inicio_arg = sys.argv[1]
        fim_arg = sys.argv[2]
    except Exception as err:
        print(f'Argumentos incorretos: {err}')

    inicio, fim = get_date_range(inicio_arg, fim_arg)

    # Executa a consulta
    print(f"\n[INFO] Executando consulta...")
    print(f"       Período: {inicio.strftime('%Y-%m-%d')} até {fim.strftime('%Y-%m-%d')}")

    try:
        contagem = consultar_contagem_ovrs(inicio, fim, engine)
        print(f"\n[RESULTADO] Quantidade de registros: {contagem}")
        print(f"            Período: {inicio.strftime('%Y-%m-%d')} até {fim.strftime('%Y-%m-%d')}")
    except Exception as e:
        print(f"\n[ERRO] Falha na consulta SQL: {e}")
        session.close()
        engine.dispose()
        sys.exit(1)

    # Fecha recursos
    session.close()
    engine.dispose()
    print("\n[INFO] Consulta realizada com sucesso.")
