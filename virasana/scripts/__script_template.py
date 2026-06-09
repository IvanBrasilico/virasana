#!/usr/bin/env python3
"""
Script para consultar contagem de registros na tabela ovr_ovrs
entre duas datas usando SQL via pandas e o engine já criado.

Uso:
    python script_ovrs_contagem.py                           # Usa default: hoje - 30 dias até hoje
    python script_ovrs_contagem.py 2026-05-01 2026-05-31     # Datas customizadas (YYYY-MM-DD)
"""

import sys
from datetime import datetime, timedelta, time

import pandas as pd

# Paths existentes
sys.path.append('.')
sys.path.append('../ajna_docs/commons')
from ajna_commons.flask.log import logger

sys.path.append('../bhadrasana2')
sys.path.append('../ajna_docs/commons')


def show_usage_message():
    """Exibe mensagem padrão explicando o que o script faz."""
    print("=" * 70)
    print("SCRIPT DE CONSULTA DE CONTAGEM - TABELA ovr_ovrs")
    print("=" * 70)
    print()
    print("Descrição:")
    print("  Este script consulta via SQL usando pandas e o engine já criado")
    print("  para contar registros na tabela 'ovr_ovrs' entre duas datas.")
    print()
    print("Uso:")
    print("  python script_ovrs_contagem.py                           # Default: hoje - 30 dias até hoje")
    print("  python script_ovrs_contagem.py 2026-05-01 2026-05-31     # Datas customizadas (YYYY-MM-DD)")
    print()
    print("Parâmetros:")
    print("  inicio  (opcional) - Data de início no formato YYYY-MM-DD")
    print("  fim     (opcional) - Data de fim no formato YYYY-MM-DD")
    print()
    print("Default:")
    print("  Se nenhum argumento ser passado, o script usa:")
    print("    - Data de início: hoje - 30 dias")
    print("    - Data de fim: hoje (às 00h)")
    print()
    print("SQL executado:")
    print("  SELECT count(*) FROM ovr_ovrs WHERE datahora BETWEEN :inicio AND :fim")
    print("=" * 70)


def get_date_range(inicio_arg=None, fim_arg=None):
    """
    Retorna o intervalo de datas para a consulta.

    Args:
        inicio_arg: Data de início como string (YYYY-MM-DD) ou None
        fim_arg: Data de fim como string (YYYY-MM-DD) ou None

    Returns:
        tuple: (datetime_inicio, datetime_fim)
    """
    # Define o fim às 00h de hoje
    fim = datetime.combine(datetime.now(), time.min)
    inicio = fim - timedelta(days=30)

    if inicio_arg is None:
        # Nenhum argumento passado - usa default
        print(f"\n[INFO] Início não passado. Usando default:")
        print(f"       - Data de início: {inicio.strftime('%Y-%m-%d')} (hoje - 30 dias)")
    else:
        # Converte strings para datetime
        try:
            inicio = datetime.strptime(inicio_arg, '%Y-%m-%d')
            inicio = datetime.combine(inicio, time.min)
        except ValueError:
            print(f"\n[ERRO] Data de início inválida: {inicio_arg}")
            print("       Formato esperado: YYYY-MM-DD (ex: 2026-05-01)")
            sys.exit(1)

    if fim_arg is None:
        # Nenhum argumento passado - usa default
        print(f"\n[INFO] Fim não passado. Usando default:")
        print(f"       - Data de início: {fim.strftime('%Y-%m-%d')} (hoje)")
    else:
        try:
            datetime_fim = datetime.strptime(fim_arg, '%Y-%m-%d')
            datetime_fim = datetime.combine(datetime_fim, time.min)
        except ValueError:
            print(f"\n[ERRO] Data de fim inválida: {fim_arg}")
            print("       Formato esperado: YYYY-MM-DD (ex: 2026-05-31)")
            sys.exit(1)

    return inicio, fim


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
