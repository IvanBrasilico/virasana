
# coding: utf-8

# # Notebook para acesso ao Receita Data - dados de viajantes

# In[1]:


import getpass
import pandas as pd
from datalake_serpro import handler_dl


# In[2]:


user = getpass.getuser() + '@DATALAKE.SERPRO'
senha = getpass.getpass(f'Senha para kinit {user}:')


# In[3]:


commands = f'echo "{senha}" | /opt/anaconda3/bin/kinit {user}'
get_ipython().system(' {commands}')


# In[4]:


get_ipython().system(' /opt/anaconda3/bin/klist')


# # Rodar

# In[ ]:


cpfs = pd.read_csv('viagem.csv', header=None, names=['cpf'])


# In[ ]:


def get_nomes_cpfs(lista_cpfs, con):
    sql_cpfs = 'SELECT DISTINCT b_cd_cnpf_cpf as cpf, nm_cnpf_cpfa_nome as nome ' +                ' FROM cnpf.wd_cnpf_cpfa where b_cd_cnpf_cpf in (%s)'
    lista = []
    for row in lista_cpfs:
        try:
            item = str(int(row))
            lista.append(item)
        except:
            pass
    df_cpfs = pd.read_sql(sql_cpfs % ', '.join(lista), con)
    return df_cpfs


# In[ ]:


with handler_dl(DEBUG=True, timeout=300) as con:
    df_cpfs_nomes = get_nomes_cpfs(cpfs.cpf.values, con)
    df_cpfs_nomes.cpf = df_cpfs_nomes.cpf.apply(lambda x: str(int(x)).zfill(11))


# In[ ]:


df_cpfs_nomes


# In[ ]:


def get_viagens_cpf(cpf, con):
    sql_vu = 'SELECT DISTINCT cast(codigo_vu as string) as codigo_vu ' +              'FROM coana.edbv_viajante_unico_geral WHERE cpf_vu = "%s"'
    sql_viagens = 'SELECT cast(codigo_vu as string) as codigo_vu, ' +                   'data_chegada,  codigo_local_embarque, codigo_local_destino, codigo_reserva, numero_voo ' +                    ' FROM coana.edbv_dados_voos WHERE codigo_vu = %s'
    df_vu = pd.read_sql(sql_vu % cpf, con)
    if len(df_vu) == 0:
        return None
    df_viagens = pd.read_sql(sql_viagens % df_vu.iloc[0].codigo_vu, con)
    df_final = df_vu.merge(df_viagens, right_on='codigo_vu', left_on='codigo_vu')
    return df_final


# In[ ]:


def get_codigo_vu_nome(nome, con):
    nomes = [nome for nome in nome.split() if len(nome) > 2]
    primeiro_nome = nomes[0]
    segundo_nome = nomes[1]
    sobrenome = nomes[-1]
    sobre_sobrenome = nomes[-2]
    print(primeiro_nome, segundo_nome, sobrenome, sobre_sobrenome)
    sql_viajante_nomes = 'SELECT cast(codigo_vu as string) as codigo_vu, vuvg_nm_responsavel, lista_nomes_viajantes '+                      'FROM coana.edbv_viajante_unico_geral WHERE cpf_vu is NULL '
    sql_viajante_nomes+= ' and vung_nm_viajante like "%s"' % (primeiro_nome + '%')
    sql_viajante_nomes+= ' and lista_nomes_viajantes like "%s"' % ('%' + segundo_nome + '%')
    print(sql_viajante_nomes)
    df_nomes = pd.read_sql(sql_viajante_nomes, con)
    print(len(df_nomes))
    index = None
    for row in df_nomes.itertuples():
        if sobrenome in row[3]:
            index = row[0]
            codigo_vu = row[1]
            # print(row)
            break
    if index is None and sobre_sobrenome != segundo_nome:
        for row in df_nomes.itertuples():
            if sobrenome in row[3]:
                index = row[0]
                codigo_vu = row[1]
                break
    if index is None:
        return None
    # print(codigo_vu, df_nomes.iloc[index])
    return codigo_vu


# In[ ]:


def get_viagens_codigo_vu(codigo_vu, con):
    sql_vu = 'SELECT DISTINCT cast(codigo_vu as string) as codigo_vu ' +              'FROM coana.edbv_viajante_unico_geral WHERE codigo_vu = %s'
    sql_viagens = 'SELECT cast(codigo_vu as string) as codigo_vu, ' +                   'data_chegada,  codigo_local_embarque, codigo_local_destino, codigo_reserva, numero_voo ' +                    ' FROM coana.edbv_dados_voos WHERE codigo_vu = %s'
    df_vu = pd.read_sql(sql_vu % codigo_vu, con)
    df_viagens = pd.read_sql(sql_viagens % df_vu.iloc[0].codigo_vu, con)
    df_final = df_vu.merge(df_viagens, right_on='codigo_vu', left_on='codigo_vu')
    return df_final


# In[ ]:


# Colocando tudo junto
semcpf = 0
comcpf = 0
cpfs = list(df_cpfs_nomes.cpf.values)
nomes = list(df_cpfs_nomes.nome.values)


# In[ ]:


for r in range(len(nomes)):
    cpf = cpfs[r]
    nome = nomes[r]
    print(cpf, nome)
    with handler_dl(DEBUG=True, timeout=300) as con:
        viagens = get_viagens_cpf(cpf, con)
        if viagens is not None:
            comcpf +=1
        else:
            codigo_vu = get_codigo_vu_nome(nome, con)
            if codigo_vu:
                viagens = get_viagens_codigo_vu(codigo_vu, con)
            if viagens is not None:
                semcpf +=1
    cpfs.pop(r)
    nomes.pop(r)
    if viagens is not None:
        viagens['cpf'] = cpf
        if r == 0:
            viagens.to_csv('viagens_nome.csv')
        else:
            viagens.to_csv('viagens_nome.csv', header=False, mode='a')


# In[ ]:


print(comcpf, semcpf)

