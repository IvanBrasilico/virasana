import pandas as pd

def get_dues(inicio, fim, con):
    SQL_DUES = f''' SELECT numero_due, data_criacao_due, data_registro_due, ni_declarante, 
     cnpj_estabelecimento_exportador, telefone_contato, email_contato, nome_contato,
     codigo_iso3_pais_importador, nome_pais_importador,
     duev_nm_tp_doc_fiscal_c, duev_cetp_nm_c, 
     codigo_recinto_despacho, codigo_recinto_embarque, codigo_unidade_embarque
    FROM coana.due d
    WHERE data_registro_due BETWEEN "{inicio}" and "{fim}"
    and codigo_unidade_embarque = "0817800" '''
    df_dues = pd.read_sql(SQL_DUES, con)
    print(f'{len(df_dues)} registradas de {inicio} a {fim}')
    return df_dues


def get_nomes_cnpjs(lista_cnpjs_base8, con):
    SQL_CNPJs = '''SELECT DISTINCT b_cd_cnpj_emph as cnpj, nm_cnpj_emph_empresarial as nome
     FROM cnpj.wd_cnpj_esth WHERE b_cd_cnpj_emph IN (%s)'''

    sql_cnpjs = SQL_CNPJs % ' ,'.join(lista_cnpjs_base8)
    # print(sql_cnpjs)
    df_cnpjs = pd.read_sql(sql_cnpjs, con)
    print(f'{len(df_cnpjs)} cnpjs recuperados.')
    df_cnpjs.head()
    return df_cnpjs


def get_itens_dues(lista_dues, con):
    SQL_ITENS_DUE = '''SELECT nr_due, due_nr_item, descricao_item,
     descricao_complementar_item, nfe_nr_item, nfe_ncm, 
     unidade_comercial, qt_unidade_comercial, valor_total_due_itens,
     nfe_nm_importador, pais_destino_item
    FROM coana.due_itens WHERE nr_due IN ("%s") '''
    sql_itens_due = SQL_ITENS_DUE % '" ,"'.join(lista_dues)
    df_itens_dues = pd.read_sql(sql_itens_due, con)
    print(f'{len(df_itens_dues)} itens de DUEs recuperados.')
    return df_itens_dues


def dues_rd(inicio, fim, lista_conteineres, con):
    # Puxa DUEs de um período - mudar para puxar período e lista de contêineres
    df_dues = get_dues(inicio, fim)
    cnpjs_base = [str(int(cnpj[:8])) for cnpj in df_dues['cnpj_estabelecimento_exportador'].unique()]
    print(f'{len(cnpjs_base)} CNPJs a recuperar nome. Amostra:{cnpjs_base[:3]}')
    df_cnpjs = get_nomes_cnpjs(cnpjs_base)
    df_itens_dues = get_itens_dues(df_dues['numero_due'].unique())
    df_dues.to_csv('dues.csv', index=False)
    df_cnpjs.to_csv('cnpjs_nomes.csv', index=False)
    df_itens_dues.to_csv('itens_dues.csv', index=False)



if __name__ == '__main__':
    def handler_dl():
        raise NotImplemented('Not implemented... this code needs to run on DataLake')

    from datetime import date, timedelta

    hoje = date.today()
    ontem = hoje - timedelta(days=1)
    con = handler_dl()
    df = pd.read_csv('escaneamentos_sem_due.csv')
    dues_rd(ontem, hoje, df['numero_conteiner'].unique(), con)
