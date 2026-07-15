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

sys.path.append('../scripts')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')
sys.path.append('../ajna_docs/commons')


def df_ces(engine, cnpj, inicio, fim) -> pd.DataFrame:
    # Formata as datas para o formato do MySQL/MariaDB
    inicio_str = inicio.strftime('%Y-%m-%d %H:%M:%S')
    fim_str = fim.strftime('%Y-%m-%d %H:%M:%S')

    query = f"""
           SELECT *  FROM conhecimentos_resumo 
           WHERE  BETWEEN '{inicio_str}' AND '{fim_str}'
       """

    # Usa pandas para executar o SQL
    df = pd.read_sql_query(query, con=engine)

    return df


if __name__ == '__main__':  # pragma: no-cover
    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    print('Uso: python entregas_cnpj.py <cnpj>0000000001 <inicio>(2026-06-01) <fim>(2026-05-31) ')
    # Obtém CNPJ
    try:
        cnpj = sys.argv[1]
    except:
        print('Obrigatório informar CNPJ. Saindo...')
        sys.exit(0)
    # Obtém datas
    try:
        inicio = datetime.strptime(sys.argv[2], '%Y-%m-%d')
        fim = datetime.strptime(sys.argv[3], '%Y-%m-%d')
    except Exception as err:
        print(err)
        print('Erro ao puxar datas, usando hoje e 30 dias atrás.')
        # Define o fim às 00h de hoje e inicio 30 dias antes
        fim = datetime.combine(datetime.now(), time.min)
        inicio = fim - timedelta(days=30)
    # Executa a consulta
    print(f"\n[INFO] Executando consulta...")
    print(f"CNPJ:{cnpj}  Período: {inicio.strftime('%Y-%m-%d')} até {fim.strftime('%Y-%m-%d')}")
    try:
        ces = consultar_ces(engine, cnpj, inicio, fim)
        print(f"\n[RESULTADO] Quantidade de registros: {contagem}")
        print("\n[INFO] Consulta realizada com sucesso.")
    except Exception as e:
        print(f"\n[ERRO] Falha na consulta SQL: {e}")
        session.close()
        engine.dispose()
        sys.exit(1)
    finally:
        session.close()
        engine.dispose()
