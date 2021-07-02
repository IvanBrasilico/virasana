
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

# In[5]:


def get_viagens_cpfs(lista_cpfs):
    sql_vu = 'SELECT DISTINCT cpf_vu as cpf, codigo_vu FROM coana.edbv_viajante_unico_geral WHERE cpf_vu in ("%s")'
    sql_viagens = 'SELECT * FROM coana.edbv_dados_voos WHERE codigo_vu in (%s)'
    df_vu = pd.read_sql(sql_vu % '", "'.join(lista_cpfs), con)
    lista_vus = ['%d' % codigo_vu for codigo_vu in df_vu.codigo_vu.values]
    df_viagens = pd.read_sql(sql_viagens % ', '.join(lista_vus), con)
    df_final = df_vu.merge(df_viagens, right_on='codigo_vu', left_on='codigo_vu')
    return df_final


# In[6]:


def get_nomes_cnpjs(lista_cpfs):
    sql_cnpjs ='SELECT DISTINCT b_cd_cnpj_emph as cnpj, nm_cnpj_emph_empresarial as nome ' +                'FROM cnpj.wd_cnpj_esth where b_cd_cnpj_emph in (%s)'
    lista_cnpjs = []
    for row in lista_cpfs:
        try:
            item = str(int(row[:8]))
            lista_cnpjs.append(item)
        except:
            pass
    df_cnpjs = pd.read_sql(sql_cnpjs % ', '.join(lista_cnpjs), con)
    return df_cnpjs


# In[7]:


def get_nomes_cpfs(lista_cpfs):
    sql_cpfs = 'SELECT DISTINCT b_cd_cnpf_cpf as cpf, nm_cnpf_cpfa_nome as nome, ' +                'ed_cnpf_cpfa_logr as endereco, ed_cnpf_cpfa_compl as complemento, ' +                'ed_cnpf_cpfa_cep as cep ' +                ' FROM cnpf.wd_cnpf_cpfa where b_cd_cnpf_cpf in (%s)'
    lista = []
    for row in lista_cpfs:
        try:
            item = str(int(row))
            lista.append(item)
        except:
            pass
    df_cpfs = pd.read_sql(sql_cpfs % ', '.join(lista), con)
    return df_cpfs


# In[8]:


def get_dsis_cpfs(lista_cpfs):
    sql_dsis = '''
    SELECT nm_dsi as numero,
    nr_impdr_expdr as consignatario,
    dt_dia_regis as data_registro, 
    nr_rep_legal as despachante,
    max(nx_esp_produto_dsi) as descricao
    FROM coana.importacao_dsi WHERE nr_impdr_expdr in ("%s")
    GROUP BY nm_dsi, nr_impdr_expdr, dt_dia_regis, nr_rep_legal
    '''
    lista = []
    for row in lista_cpfs:
        try:
            item = str(int(row))
            lista.append(item)
        except:
            pass
    df_cpfs = pd.read_sql(sql_dsis % '", "'.join(lista), con)
    return df_cpfs


# In[9]:


cpfs = pd.read_csv('viagem.csv', header=None, names=['cpf'])
cpfs2 = pd.read_csv('pessoa.csv', header=None, names=['cpf'])
cpfs3 = pd.read_csv('empresa.csv', header=None, names=['cpf'])
cpfs_dsi = pd.read_csv('dsi.csv', header=None, names=['cpf'])


# In[10]:


#with handler_dl(DEBUG=True, timeout=300) as con:
#    df_viagens = get_viagens_cpfs(cpfs.cpf.values)


# In[11]:


#df_viagens.to_csv('viagens.csv')


# In[12]:


with handler_dl(DEBUG=True, timeout=300) as con:
    df_cnpjs = get_nomes_cnpjs(cpfs3.cpf.values)

df_cnpjs.to_csv('cnpjs.csv')


# In[13]:


with handler_dl(DEBUG=True, timeout=300) as con:
    df_cpfs = get_nomes_cpfs(cpfs2.cpf.values)

df_cpfs.to_csv('cpfs.csv')


# In[14]:


with handler_dl(DEBUG=True, timeout=300) as con:
    df_dsis = get_dsis_cpfs(cpfs_dsi.cpf.values)

df_dsis.to_csv('dsis.csv')

