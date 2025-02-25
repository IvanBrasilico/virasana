import sys
from datetime import datetime, timedelta, time

import pandas as pd

sys.path.append('.')
from virasana.integracao.due.due_alchemy import Due, DueItem
from virasana.integracao.due.manager_conteineres_dues import get_conteineres_escaneados_sem_due
from virasana.integracao.due.rd_baixa_dues_novas import dues_rd


def exporta_ctrs(session):
    # Obtém a data de ontem
    lista_final = get_conteineres_escaneados_sem_due(session, datainicio=inicio_ontem,
                                                     datafim=fim_ontem, codigoRecinto='8931359')
    df = pd.DataFrame(lista_final[1:], columns=lista_final[0])
    df.to_csv('escaneamentos_sem_due.csv', index=False)


def insert_from_dataframe(session, classe, df):
    try:
        df = df.fillna('').drop_duplicates()
        _list = [classe(**row.to_dict()) for _, row in df.iterrows()]
        session.bulk_save_objects(_list)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Erro ao inserir dados: {e}")
    finally:
        session.close()

def importa_dues():
    # Passo 5: Inserir DUEs e itensDUE a partir dos dfs acima
    # Passo 6: Atualizar metadata e Acesso com número da DUE* (TODO)
    # Passo 7: Inserir nomes das empresas na tabela XXXXXX (TODO)
    # *Falta gerar o csv da ligação do contêiner com a DUE na parte do RD
    df_dues = pd.read_csv('dues.csv')
    insert_from_dataframe(session, Due, df_dues)
    df_itens_dues = pd.read_csv('itens_dues.csv')
    insert_from_dataframe(session, DueItem, df_itens_dues)
    df_cnpjs = pd.read_csv('cnpjs_nomes.csv')


if __name__ == '__main__':  # pragma: no-cover
    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    ontem = datetime.now() - timedelta(days=1)
    # Define o início e o fim do dia de ontem
    inicio_ontem = datetime.combine(ontem, time.min)
    fim_ontem = datetime.combine(ontem, time.max)
    operacao = input('Qual a operação desejada? (E)exportar lista, (I)mportar DUEs, (R)eceita Data?')

    if operacao == 'E':
        exporta_ctrs()
    elif operacao == 'I':
        importa_dues()
    elif operacao == 'R':
        # Passo 4: Recuperar do Receita Data CÓDIGO FAKE DE EXEMPLO!!!
        # Abaixo apenas exemplo, hoje é preciso rodar no Jupyter Notebook do RD na mão
        df = pd.read_csv('escaneamentos_sem_due.csv')
        dues_rd(inicio_ontem, fim_ontem, df['numero_conteiner'].unique(), None)
        df_dues_conteiner = pd.read_csv('dues_conteiner.csv')
        merge_df = df.merge(df_dues_conteiner, left_on='numero_container', right_on='id_conteiner', how='inner')
        merge_df.to_csv('escaneamentos_sem_due_atualizado.csv', index=False)
