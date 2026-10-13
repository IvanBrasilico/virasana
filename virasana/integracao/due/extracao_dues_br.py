import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')
sys.path.insert(0, '.')

from ajna_commons.flask.conf import SQL_URI


# ============================================================================
# CONFIGURACAO
# ============================================================================

ANO_MES = '2026-08'

ARQUIVO_DUES = f'pucomex_due_{ANO_MES}.csv'
ARQUIVO_ITENS = f'pucomex_due_itens_{ANO_MES}.csv'
ARQUIVO_CONTEINERES = f'pucomex_due_conteiner_{ANO_MES}.csv'

TABELA_DUES = 'dbmercante.pucomex_due'
TABELA_ITENS = 'dbmercante.pucomex_due_itens'
TABELA_CONTEINERES = 'dbmercante.pucomex_due_conteiner'

TAMANHO_LOTE = 500


# ============================================================================
# LIMPEZA E CONVERSAO
# ============================================================================

def limpa_texto(valor):
    if pd.isna(valor):
        return None

    valor = str(valor).strip()

    if valor == '' or valor.lower() in ('nan', 'none', 'null', 'nat'):
        return None

    return valor


def so_digitos(valor):
    valor = limpa_texto(valor)
    if valor is None:
        return None
    valor = ''.join(c for c in valor if c.isdigit())
    return valor or None


def ajusta_cnpj(valor):
    valor = so_digitos(valor)
    if valor is None:
        return None
    return valor.zfill(14)[:14]


def ajusta_due(valor):
    valor = limpa_texto(valor)
    return valor[:14] if valor else None


def ajusta_conteiner(valor):
    valor = limpa_texto(valor)
    if valor is None:
        return None
    valor = ''.join(valor.upper().split())
    return valor[:11] if valor else None


def ajusta_inteiro(valor):
    valor = limpa_texto(valor)
    if valor is None:
        return None
    try:
        return int(float(valor.replace(',', '.')))
    except Exception:
        return None


def ajusta_decimal(valor):
    valor = limpa_texto(valor)
    if valor is None:
        return None
    try:
        return float(valor.replace(',', '.'))
    except Exception:
        return None


def ajusta_data(valor):
    valor = limpa_texto(valor)
    if valor is None:
        return None
    convertido = pd.to_datetime(valor, errors='coerce')
    return None if pd.isna(convertido) else convertido.date()


def ajusta_datetime(valor):
    valor = limpa_texto(valor)
    if valor is None:
        return None
    convertido = pd.to_datetime(valor, errors='coerce')
    return None if pd.isna(convertido) else convertido.to_pydatetime()


def trunca(valor, tamanho):
    valor = limpa_texto(valor)
    return None if valor is None else valor[:tamanho]


def corrige_mojibake(valor):
    if not isinstance(valor, str):
        return valor
    try:
        return valor.encode('latin1').decode('utf-8')
    except Exception:
        return valor


def le_csv(arquivo):
    if not os.path.exists(arquivo):
        raise FileNotFoundError(f'Arquivo nao encontrado: {arquivo}')

    df = pd.read_csv(
        arquivo,
        dtype=str,
        encoding='utf-8-sig',
        sep=None,
        engine='python',
        keep_default_na=False
    )
    df.columns = [str(col).strip().lower() for col in df.columns]
    return df


def divide_lotes(lista, tamanho=TAMANHO_LOTE):
    for inicio in range(0, len(lista), tamanho):
        yield lista[inicio:inicio + tamanho]


# ============================================================================
# PREPARACAO DOS CSVs
# ============================================================================

def prepara_dues(df):
    if 'numero_due' not in df.columns:
        raise ValueError('O CSV de DU-Es precisa ter a coluna numero_due.')

    df = df.copy()
    df['numero_due'] = df['numero_due'].apply(ajusta_due)

    for col in ['ni_declarante', 'cnpj_estabelecimento_exportador']:
        if col in df.columns:
            df[col] = df[col].apply(ajusta_cnpj)

    for col in ['data_criacao_due', 'data_registro_due']:
        if col in df.columns:
            df[col] = df[col].apply(ajusta_data)

    for col in ['data_carga_apresentada_despacho', 'data_desembaraco']:
        if col in df.columns:
            df[col] = df[col].apply(ajusta_datetime)

    limites = {
        'telefone_contato': 100,
        'email_contato': 100,
        'nome_contato': 100,
        'codigo_iso3_pais_importador': 100,
        'nome_pais_importador': 100,
        'duev_nm_tp_doc_fiscal_c': 100,
        'duev_cetp_nm_c': 100,
        'codigo_recinto_despacho': 7,
        'codigo_recinto_embarque': 7,
        'codigo_unidade_embarque': 7,
        'lista_id_conteiner': 650,
        'data_inicial_entrada_recinto': 30,
        'data_primeira_recepcao_nota_recinto': 30,
        'data_ultima_recepcao_nota_recinto': 30,
        'data_final_manifestacao_carga': 30,
    }

    for col, tamanho in limites.items():
        if col in df.columns:
            df[col] = df[col].apply(corrige_mojibake)
            df[col] = df[col].apply(lambda x: trunca(x, tamanho))

    df = df[df['numero_due'].notna()].copy()
    return df.drop_duplicates(subset=['numero_due'], keep='last')


def prepara_itens(df):
    obrigatorias = {'nr_due', 'due_nr_item'}
    ausentes = obrigatorias - set(df.columns)
    if ausentes:
        raise ValueError(f'Colunas ausentes no CSV de itens: {sorted(ausentes)}')

    df = df.copy()
    df['nr_due'] = df['nr_due'].apply(ajusta_due)
    df['due_nr_item'] = df['due_nr_item'].apply(ajusta_inteiro)

    if 'nfe_nr_item' in df.columns:
        df['nfe_nr_item'] = df['nfe_nr_item'].apply(ajusta_inteiro)

    for col in ['qt_unidade_comercial', 'valor_total_due_itens']:
        if col in df.columns:
            df[col] = df[col].apply(ajusta_decimal)

    if 'nfe_dt_emissao' in df.columns:
        df['nfe_dt_emissao'] = df['nfe_dt_emissao'].apply(ajusta_datetime)

    limites = {
        'descricao_item': 1000,
        'descricao_complementar_item': 1000,
        'nfe_ncm': 100,
        'unidade_comercial': 100,
        'nfe_nm_importador': 100,
        'pais_destino_item': 100,
        'nfe_nr_chave': 44,
    }

    for col, tamanho in limites.items():
        if col in df.columns:
            df[col] = df[col].apply(corrige_mojibake)
            df[col] = df[col].apply(lambda x: trunca(x, tamanho))

    df = df[df['nr_due'].notna() & df['due_nr_item'].notna()].copy()
    return df.drop_duplicates(subset=['nr_due', 'due_nr_item'], keep='last')


def prepara_conteineres(df):
    obrigatorias = {'numero_due', 'numero_conteiner'}
    ausentes = obrigatorias - set(df.columns)
    if ausentes:
        raise ValueError(f'Colunas ausentes no CSV de conteineres: {sorted(ausentes)}')

    df = df.copy()
    df['numero_due'] = df['numero_due'].apply(ajusta_due)
    df['numero_conteiner'] = df['numero_conteiner'].apply(ajusta_conteiner)
    df = df[df['numero_due'].notna() & df['numero_conteiner'].notna()].copy()

    return df.drop_duplicates(
        subset=['numero_due', 'numero_conteiner'],
        keep='last'
    )


# ============================================================================
# SQL GENERICO
# ============================================================================

def get_colunas_tabela(con, tabela):
    result = con.execute(text(f'SHOW COLUMNS FROM {tabela}')).fetchall()
    return [row[0] for row in result]


def monta_sql_insert(tabela, colunas):
    cols = ', '.join(colunas)
    vals = ', '.join(f':{col}' for col in colunas)
    return text(f'INSERT INTO {tabela} ({cols}) VALUES ({vals})')


def monta_sql_update(tabela, colunas, chaves):
    atualizaveis = [col for col in colunas if col not in chaves]
    if not atualizaveis:
        return None

    sets = ', '.join(f'{col} = :{col}' for col in atualizaveis)
    where = ' AND '.join(f'{chave} = :{chave}' for chave in chaves)
    return text(f'UPDATE {tabela} SET {sets} WHERE {where}')


def dataframe_para_registros(df, colunas):
    registros = []
    for _, row in df[colunas].iterrows():
        dados = {}
        for col in colunas:
            valor = row[col]
            dados[col] = None if pd.isna(valor) else valor
        registros.append(dados)
    return registros


def importa_insert_update(con, df, tabela, chaves, ignoradas=None):
    ignoradas = set(ignoradas or [])
    colunas_tabela = get_colunas_tabela(con, tabela)
    colunas = [
        col for col in df.columns
        if col in colunas_tabela and col not in ignoradas
    ]

    for chave in chaves:
        if chave not in colunas:
            raise ValueError(f'A chave {chave} nao esta disponivel para {tabela}.')

    nao_gravadas = [
        col for col in df.columns
        if col not in colunas and col not in ignoradas
    ]

    print()
    print(f'Tabela: {tabela}')
    print('Colunas que serao gravadas:')
    print(colunas)
    if nao_gravadas:
        print('Colunas do CSV que nao serao gravadas:')
        print(nao_gravadas)

    where = ' AND '.join(f'{chave} = :{chave}' for chave in chaves)
    sql_existe = text(f'SELECT 1 FROM {tabela} WHERE {where} LIMIT 1')
    sql_insert = monta_sql_insert(tabela, colunas)
    sql_update = monta_sql_update(tabela, colunas, chaves)

    inseridos = 0
    atualizados = 0
    registros = dataframe_para_registros(df, colunas)

    for indice, dados in enumerate(registros, start=1):
        dados_chave = {chave: dados[chave] for chave in chaves}
        existe = con.execute(sql_existe, dados_chave).fetchone()

        if existe:
            if sql_update is not None:
                con.execute(sql_update, dados)
            atualizados += 1
        else:
            con.execute(sql_insert, dados)
            inseridos += 1

        if indice % 500 == 0:
            print(f'{tabela}: processados {indice} de {len(registros)}...')

    print(f'{tabela}: inseridos={inseridos}; atualizados={atualizados}')
    return inseridos, atualizados


# ============================================================================
# CONTEINERES
# ============================================================================

def sincroniza_conteineres(con, df_dues, df_conteineres):
    """Substitui os vinculos de conteiner apenas para as DU-Es deste pacote."""
    numeros_dues = df_dues['numero_due'].dropna().drop_duplicates().tolist()

    sql_delete = text(
        f'DELETE FROM {TABELA_CONTEINERES} WHERE numero_due = :numero_due'
    )
    sql_insert = text(
        f'INSERT INTO {TABELA_CONTEINERES} '
        '(numero_due, numero_conteiner) '
        'VALUES (:numero_due, :numero_conteiner)'
    )

    print()
    print(f'Sincronizando {TABELA_CONTEINERES}...')

    for indice, numero_due in enumerate(numeros_dues, start=1):
        con.execute(sql_delete, {'numero_due': numero_due})
        if indice % 1000 == 0:
            print(f'Vinculos antigos removidos para {indice} DU-Es...')

    registros = dataframe_para_registros(
        df_conteineres,
        ['numero_due', 'numero_conteiner']
    )

    for indice, lote in enumerate(divide_lotes(registros), start=1):
        if lote:
            con.execute(sql_insert, lote)
        print(f'Lote {indice}: {len(lote)} vinculos inseridos.')

    print(
        f'{len(numeros_dues)} DU-Es sincronizadas; '
        f'{len(registros)} vinculos atuais gravados.'
    )


# ============================================================================
# VALIDACOES DOS TRES CSVs
# ============================================================================

def valida_arquivos_csv():
    arquivos = [ARQUIVO_DUES, ARQUIVO_ITENS, ARQUIVO_CONTEINERES]
    ausentes = [arquivo for arquivo in arquivos if not os.path.exists(arquivo)]

    if ausentes:
        raise FileNotFoundError(
            'Os seguintes arquivos nao foram encontrados no diretorio atual: '
            + ', '.join(ausentes)
        )


def valida_relacionamentos(df_dues, df_itens, df_conteineres):
    numeros_dues = set(df_dues['numero_due'])
    itens_orfaos = set(df_itens['nr_due']) - numeros_dues
    conteineres_orfaos = set(df_conteineres['numero_due']) - numeros_dues

    if itens_orfaos:
        raise ValueError(
            f'Ha {len(itens_orfaos)} DU-Es no CSV de itens sem DUE principal.'
        )

    if conteineres_orfaos:
        raise ValueError(
            f'Ha {len(conteineres_orfaos)} DU-Es no CSV de conteineres '
            'sem DUE principal.'
        )


# ============================================================================
# IMPORTACAO COMPLETA
# ============================================================================

def importa_arquivos_dues(engine):
    valida_arquivos_csv()

    print(f'Lendo {ARQUIVO_DUES}...')
    df_dues = prepara_dues(le_csv(ARQUIVO_DUES))

    print(f'Lendo {ARQUIVO_ITENS}...')
    df_itens = prepara_itens(le_csv(ARQUIVO_ITENS))

    print(f'Lendo {ARQUIVO_CONTEINERES}...')
    df_conteineres = prepara_conteineres(le_csv(ARQUIVO_CONTEINERES))

    print()
    print('Quantidades apos preparacao:')
    print(f'DU-Es: {len(df_dues)}')
    print(f'Itens: {len(df_itens)}')
    print(f'Vinculos DU-E x conteiner: {len(df_conteineres)}')

    if df_dues.empty:
        raise ValueError('O CSV principal de DU-Es esta vazio.')

    valida_relacionamentos(df_dues, df_itens, df_conteineres)
    print('Validacoes de relacionamento aprovadas.')

    # Uma unica transacao para as tres tabelas.
    # Se ocorrer erro, toda a importacao desta execucao sera desfeita.
    with engine.begin() as con:
        ins_dues, upd_dues = importa_insert_update(
            con,
            df_dues,
            TABELA_DUES,
            chaves=['numero_due'],
            ignoradas=['createdate']
        )

        ins_itens, upd_itens = importa_insert_update(
            con,
            df_itens,
            TABELA_ITENS,
            chaves=['nr_due', 'due_nr_item'],
            ignoradas=['createdate']
        )

        sincroniza_conteineres(con, df_dues, df_conteineres)

    print()
    print('Importacao finalizada com sucesso.')
    print(f'DU-Es inseridas: {ins_dues}')
    print(f'DU-Es atualizadas: {upd_dues}')
    print(f'Itens inseridos: {ins_itens}')
    print(f'Itens atualizados: {upd_itens}')
    print(f'Vinculos de conteiner gravados: {len(df_conteineres)}')


if __name__ == '__main__':
    print('Importacao mensal de DU-Es')
    print(f'Mes dos arquivos: {ANO_MES}')
    print('Arquivos esperados no diretorio atual:')
    print(f'- {ARQUIVO_DUES}')
    print(f'- {ARQUIVO_ITENS}')
    print(f'- {ARQUIVO_CONTEINERES}')
    print()

    engine = create_engine(SQL_URI, pool_pre_ping=True)

    opcao = input(
        'Digite I para importar os tres CSVs ou qualquer outra tecla para cancelar: '
    )

    if opcao.strip().lower() == 'i':
        importa_arquivos_dues(engine)
    else:
        print('Importacao cancelada.')
