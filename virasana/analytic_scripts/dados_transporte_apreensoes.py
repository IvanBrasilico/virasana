"""Recupera dados de transporte de apreensões

Recuperar lista de apreensões de drogas do Fichas e através do número do contêiner tentar recuperar dados de
Acesso Veículo no Gate. Recuperar se possível placa, motorista e transportadora.

TODO: Testar se dá para recuperar também do texto da RVF e/ou dos anexos.

"""
import sys
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
from ajna_commons.flask.conf import SQL_URI

SQL_CTRS_APREENSOES = '''select distinct ficha.datahora, rvf.numerolote from ovr_ovrs ficha
inner join ovr_verificacoesfisicas rvf on ficha.id = rvf.ovr_id
inner join ovr_apreensoes_rvf a on a.rvf_id = rvf.id
where numerolote is not null and numerolote !=''
order by ficha.datahora'''

SQL_ACESSOS = '''
SELECT placa, cnpjTransportador, cpfMotorista, nomeMotorista,
numeroConteiner, dataHoraOcorrencia, tipoOperacao, operacao, direcao
FROM dbmercante.apirecintos_acessosveiculo 
WHERE numeroConteiner = "%s" AND dataHoraOcorrencia between "%s" and "%s"
ORDER BY dataHoraOcorrencia DESC
'''


# AND tipoOperacao = "I" AND operacao = "C" AND direcao = "E"


def main():
    connection = create_engine(SQL_URI)
    df_ctrs = pd.read_sql(SQL_CTRS_APREENSOES, connection)
    cont = 0
    for row in df_ctrs.itertuples():
        print(row)
        datainicial = row[1] - timedelta(days=30)
        numeroConteiner = row[2]
        datafinal = datainicial + timedelta(days=60)
        print(SQL_ACESSOS % (numeroConteiner, datainicial, datafinal))
        df_acessos = pd.read_sql(SQL_ACESSOS, con=connection, params=(numeroConteiner, datainicial, datafinal))
        if df_acessos.empty:
            print("SEM RESULTADOS")
        else:
            cont += 1
            print(df_acessos.head())
    print(f'{cont} acessos veículo encontrados para {len(df_ctrs)} apreensões')
    for row in df_ctrs.itertuples():
        print(row[1], row[2])


if __name__ == '__main__':
    main()
