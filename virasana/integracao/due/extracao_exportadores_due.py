import os
import sys
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')

sys.path.insert(0, '.')

from ajna_commons.flask.conf import SQL_URI


ARQUIVO_NI_DECLARANTES = 'ni_declarantes_due_ultimos_30_dias.csv'
ARQUIVO_NARCOS_EXPORTADORES = 'narcos_exportadores_extract.csv'
TABELA_DESTINO = 'dbmercante.narcos_exportadores'


def exporta_ni_declarantes_due(con):
    """
    Exporta todos os ni_declarante distintos da tabela dbmercante.pucomex_due
    cujo createdate esteja entre hoje e 30 dias atrás.
    """

    data_inicial = date.today() - timedelta(days=30)
    data_final_exclusiva = date.today() + timedelta(days=1)

    sql = text("""
        SELECT DISTINCT ni_declarante
        FROM dbmercante.pucomex_due
        WHERE createdate >= :data_inicial
          AND createdate < :data_final_exclusiva
          AND ni_declarante IS NOT NULL
          AND ni_declarante <> ''
        ORDER BY ni_declarante;
    """)

    print("Executando consulta de exportação...")
    print(f"Data inicial: {data_inicial}")
    print(f"Data final exclusiva: {data_final_exclusiva}")

    df = pd.read_sql(
        sql,
        con,
        params={
            "data_inicial": data_inicial,
            "data_final_exclusiva": data_final_exclusiva,
        }
    )

    df.to_csv(
        ARQUIVO_NI_DECLARANTES,
        index=False,
        encoding='utf-8-sig'
    )

    print(f"Arquivo gerado: {ARQUIVO_NI_DECLARANTES}")
    print(f"Total de declarantes distintos encontrados: {len(df)}")

    if not df.empty:
        print(df.head())


def limpa_texto(valor):
    """
    Trata valores vindos do CSV:
    - transforma NaN em None;
    - remove .0 de campos que vieram como float textual;
    - remove espaços extras.
    """

    if pd.isna(valor):
        return None

    valor = str(valor).strip()

    if valor == '' or valor.lower() in ('nan', 'none', 'null', 'nat'):
        return None

    if valor.endswith('.0'):
        valor = valor[:-2]

    return valor


def so_digitos(valor):
    valor = limpa_texto(valor)

    if valor is None:
        return None

    valor = ''.join([c for c in valor if c.isdigit()])

    if valor == '':
        return None

    return valor


def ajusta_cnpj(valor):
    valor = so_digitos(valor)

    if valor is None:
        return None

    if len(valor) <= 14:
        return valor.zfill(14)

    return valor[:14]


def ajusta_cnpj_raiz(valor):
    valor = so_digitos(valor)

    if valor is None:
        return None

    if len(valor) >= 8:
        return valor[:8]

    return valor.zfill(8)


def ajusta_cpf(valor):
    valor = so_digitos(valor)

    if valor is None:
        return None

    if len(valor) <= 11:
        return valor.zfill(11)

    return valor[:11]


def ajusta_inteiro(valor):
    valor = limpa_texto(valor)

    if valor is None:
        return None

    try:
        return int(float(valor))
    except Exception:
        return None


def trunca(valor, tamanho):
    valor = limpa_texto(valor)

    if valor is None:
        return None

    return valor[:tamanho]


def corrige_mojibake(valor):
    """
    Corrige textos que chegaram com problema típico de encoding,
    por exemplo: ANÃ”NIMA em vez de ANÔNIMA.
    Se não conseguir corrigir, mantém o valor original.
    """

    if not isinstance(valor, str):
        return valor

    try:
        return valor.encode('latin1').decode('utf-8')
    except Exception:
        return valor


def le_csv_narcos_exportadores(arquivo):
    """
    Lê o arquivo narcos_exportadores_extract.csv.

    O sep=None permite ler tanto CSV separado por ; quanto por ,
    usando detecção automática do pandas.
    """

    if not os.path.exists(arquivo):
        print(f"Arquivo não encontrado: {arquivo}")
        return pd.DataFrame()

    df = pd.read_csv(
        arquivo,
        dtype=str,
        encoding='utf-8-sig',
        sep=None,
        engine='python'
    )

    df.columns = [
        col.strip().lower()
        for col in df.columns
    ]

    return df


def prepara_dataframe_narcos(df):
    """
    Prepara o DataFrame para importação na tabela dbmercante.narcos_exportadores.
    """

    if df.empty:
        return df

    renomes = {
        'mesano_abertura': 'mesano_abertura',
        'mesano_abertura_pj': 'mesano_abertura',
        'data_extracao': 'data_extracao',
    }

    df = df.rename(columns=renomes)

    if 'data_extracao' in df.columns:
        df['dt_atualizacao'] = pd.to_datetime(
            df['data_extracao'],
            errors='coerce'
        ).dt.date

        df['dt_atualizacao'] = df['dt_atualizacao'].fillna(date.today())
    else:
        df['dt_atualizacao'] = date.today()

    if 'ni_declarante' in df.columns:
        df['ni_declarante'] = df['ni_declarante'].apply(ajusta_cnpj)

    if 'cnpj_raiz' in df.columns:
        df['cnpj_raiz'] = df['cnpj_raiz'].apply(ajusta_cnpj_raiz)

    if 'cnpj_matriz_encontrado' in df.columns:
        df['cnpj_matriz_encontrado'] = df['cnpj_matriz_encontrado'].apply(ajusta_cnpj)

    for col in [
        'cpf_contador_pf_estab',
        'cpf_resp_pj',
        'cpf_socio_credito_selecionado'
    ]:
        if col in df.columns:
            df[col] = df[col].apply(ajusta_cpf)

    for col in [
        'divergencia_uf',
        'tds_socios_declaram_irpf'
    ]:
        if col in df.columns:
            df[col] = df[col].apply(ajusta_inteiro)

    for col in [
        'numero_socios_ativ',
        'menor_idade_socio',
        'maior_idade_socio',
        'tds_socios_ativos_vivos',
        'qtde_socios_bfamilia_2425'
    ]:
        if col in df.columns:
            df[col] = df[col].apply(limpa_texto)

    colunas_texto_com_acentos = [
        'nome_empresarial_pj',
        'desc_nat_juridica_pj',
        'desc_sit_cadastral_pj',
        'desc_tipo_estab_matriz_estab',
        'desc_nivel1_secao_cnae_estudo_pj',
        'faixa_tempo_empresa',
        'faixa_ult_mov_socio',
        'status_match_cnpj'
    ]

    for col in colunas_texto_com_acentos:
        if col in df.columns:
            df[col] = df[col].apply(corrige_mojibake)

    limites = {
        'ni_declarante': 14,
        'cnpj_raiz': 8,
        'cnpj_matriz_encontrado': 14,
        'status_match_cnpj': 35,
        'nome_empresarial_pj': 140,
        'desc_nat_juridica_pj': 67,
        'faixa_capital_social': 8,
        'desc_sit_cadastral_pj': 13,
        'desc_tipo_estab_matriz_estab': 6,
        'desc_nivel1_secao_cnae_estudo_pj': 65,
        'cpf_contador_pf_estab': 11,
        'cpf_resp_pj': 11,
        'mesano_abertura': 7,
        'faixa_tempo_empresa': 7,
        'uf_domic_empresa': 2,
        'uf_domic_responsavel': 2,
        'uf_cfc_contador': 4,
        'dt_ult_mov_socio': 7,
        'faixa_ult_mov_socio': 18,
        'numero_socios_ativ': 4,
        'menor_idade_socio': 4,
        'maior_idade_socio': 4,
        'tds_socios_ativos_vivos': 4,
        'cpf_socio_sem_irpf': 154,
        'socios_bfamilia_2425': 255,
        'qtde_socios_bfamilia_2425': 255,
        'cpf_socio_credito_selecionado': 11,
        'cred_liq_24': 9,
        'cred_liq_25': 9,
    }

    for col, tamanho in limites.items():
        if col in df.columns:
            df[col] = df[col].apply(lambda x: trunca(x, tamanho))

    if 'ni_declarante' not in df.columns:
        raise ValueError("O arquivo precisa ter a coluna ni_declarante.")

    df = df[df['ni_declarante'].notna()].copy()

    df = df.drop_duplicates(
        subset=['ni_declarante'],
        keep='last'
    ).copy()

    return df


def get_colunas_tabela(con):
    sql = text(f"SHOW COLUMNS FROM {TABELA_DESTINO}")
    result = con.execute(sql).fetchall()
    return [row[0] for row in result]


def monta_sql_update(colunas_gravacao):
    colunas_update = [
        col for col in colunas_gravacao
        if col != 'ni_declarante'
    ]

    if not colunas_update:
        raise ValueError("Não há colunas para atualizar além de ni_declarante.")

    sets = ', '.join([
        f"{col} = :{col}"
        for col in colunas_update
    ])

    sql = f"""
        UPDATE {TABELA_DESTINO}
        SET {sets}
        WHERE ni_declarante = :ni_declarante
    """

    return text(sql)


def monta_sql_insert(colunas_gravacao):
    cols = ', '.join(colunas_gravacao)
    vals = ', '.join([f":{col}" for col in colunas_gravacao])

    sql = f"""
        INSERT INTO {TABELA_DESTINO}
        ({cols})
        VALUES
        ({vals})
    """

    return text(sql)


def importa_narcos_exportadores(engine, arquivo=ARQUIVO_NARCOS_EXPORTADORES):
    """
    Importa o arquivo narcos_exportadores_extract.csv para dbmercante.narcos_exportadores.

    Regra:
    - se ni_declarante já existe, atualiza;
    - se ni_declarante não existe, insere.
    """

    print(f"Lendo arquivo: {arquivo}")

    df = le_csv_narcos_exportadores(arquivo)

    if df.empty:
        print("Arquivo vazio ou não encontrado. Importação cancelada.")
        return

    print(f"Linhas lidas no CSV: {len(df)}")
    print("Colunas encontradas no CSV:")
    print(list(df.columns))

    df = prepara_dataframe_narcos(df)

    print()
    print(f"Linhas após preparação: {len(df)}")

    if df.empty:
        print("Nenhuma linha válida para importar.")
        return

    print(df.head())

    with engine.begin() as con:
        colunas_tabela = get_colunas_tabela(con)

        colunas_ignoradas = {
            'data_extracao'
        }

        colunas_gravacao = [
            col for col in df.columns
            if col in colunas_tabela and col not in colunas_ignoradas
        ]

        if 'ni_declarante' not in colunas_gravacao:
            raise ValueError("A coluna ni_declarante é obrigatória para o upsert.")

        print()
        print("Colunas existentes na tabela e que serão gravadas:")
        print(colunas_gravacao)

        colunas_nao_gravadas = [
            col for col in df.columns
            if col not in colunas_gravacao and col not in colunas_ignoradas
        ]

        if colunas_nao_gravadas:
            print()
            print("Atenção: estas colunas existem no CSV, mas não existem na tabela e não serão gravadas:")
            print(colunas_nao_gravadas)

        sql_existe = text(f"""
            SELECT 1
            FROM {TABELA_DESTINO}
            WHERE ni_declarante = :ni_declarante
            LIMIT 1
        """)

        sql_update = monta_sql_update(colunas_gravacao)
        sql_insert = monta_sql_insert(colunas_gravacao)

        total_insert = 0
        total_update = 0

        for index, row in df.iterrows():
            dados = {}

            for col in colunas_gravacao:
                valor = row[col]

                if pd.isna(valor):
                    valor = None

                dados[col] = valor

            existe = con.execute(
                sql_existe,
                {'ni_declarante': dados['ni_declarante']}
            ).fetchone()

            if existe:
                con.execute(sql_update, dados)
                total_update += 1
            else:
                con.execute(sql_insert, dados)
                total_insert += 1

            if (index + 1) % 100 == 0:
                print(f"Processados {index + 1} registros...")

        print()
        print("Importação finalizada.")
        print(f"Registros inseridos: {total_insert}")
        print(f"Registros atualizados: {total_update}")
        print(f"Total processado: {total_insert + total_update}")


if __name__ == '__main__':
    engine = create_engine(SQL_URI)

    opcao = input(
        'Escolha (E)xportar ou (I)mportar dados. '
        'Os arquivos serão criados/procurados no diretório atual: '
    )

    if opcao.lower() == 'i':
        print()
        print("Importando narcos_exportadores_extract.csv para dbmercante.narcos_exportadores...")
        importa_narcos_exportadores(engine)
        print("Finalizado.")

    else:
        print()
        print("Exportando ni_declarante distintos de DUEs dos últimos 30 dias...")
        exporta_ni_declarantes_due(engine)
        print("Finalizado.")