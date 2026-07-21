import sys

sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')

from bhadrasana.models.apirecintos import EmbarqueDesembarque, AcessoVeiculo
from virasana.integracao.due.due_alchemy import Due

from datetime import datetime, timedelta
import os
import pandas as pd

RECINTO_PADRAO = '8931356'


def parse_dt(valor):
    if valor is None:
        return None
    if isinstance(valor, datetime):
        return valor
    if isinstance(valor, str):
        for fmt in (
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M',
                '%Y-%m-%dT%H:%M',
                '%Y-%m-%d',
        ):
            try:
                return datetime.strptime(valor, fmt)
            except ValueError:
                pass
    raise ValueError(f'Data inválida: {valor}')


def stats_timedelta(series):
    s = pd.to_timedelta(series.dropna())
    if len(s) == 0:
        return {'count': 0, 'mean': None, 'min': None, 'max': None, 'std': None}
    return {
        'count': int(len(s)),
        'mean': s.mean(),
        'min': s.min(),
        'max': s.max(),
        'std': s.std(ddof=1) if len(s) > 1 else pd.Timedelta(0),
    }


def print_stats(titulo, stats):
    print(f'\n{titulo}')
    print(f"count: {stats['count']}")
    print(f"mean : {stats['mean']}")
    print(f"min  : {stats['min']}")
    print(f"max  : {stats['max']}")
    print(f"std  : {stats['std']}")


def split_conteiners(lista_id_conteiner):
    if not lista_id_conteiner:
        return []
    return [x.strip() for x in str(lista_id_conteiner).split(',') if x.strip()]


def buscar_dues(session, recinto, data_inicio, data_fim):
    return (
        session.query(Due)
        .filter(
            Due.codigo_recinto_embarque == recinto,
            Due.codigo_recinto_despacho == recinto,
            Due.data_desembaraco.isnot(None),
            Due.data_desembaraco >= data_inicio,
            Due.data_desembaraco < data_fim,
        )
        .all()
    )

def buscar_dues_externas(session, recinto, data_inicio, data_fim):
    return (
        session.query(Due)
        .filter(
            Due.codigo_recinto_embarque == recinto,
            Due.codigo_recinto_despacho != recinto,
            Due.data_desembaraco.isnot(None),
            Due.data_desembaraco >= data_inicio,
            Due.data_desembaraco < data_fim,
        )
        .all()
    )


def due_to_df(dues):
    rows = []
    for d in dues:
        rows.append({
            'due': d.numero_due,
            'data_criacao_due': pd.to_datetime(d.data_criacao_due, errors='coerce'),
            'data_desembaraco': pd.to_datetime(d.data_desembaraco, errors='coerce'),
            'data_inicial_entrada_recinto': pd.to_datetime(d.data_inicial_entrada_recinto, errors='coerce'),
            'lista_id_conteiner': d.lista_id_conteiner,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df['diff_criacao_desembaraco'] = df['data_desembaraco'] - df['data_criacao_due']
        df['diff_criacao_entrada'] = df['data_inicial_entrada_recinto'] - df['data_criacao_due']
    return df


def expandir_conteiners(df_dues):
    rows = []
    for _, row in df_dues.iterrows():
        for cont in split_conteiners(row['lista_id_conteiner']):
            rows.append({
                'due': row['due'],
                'conteiner': cont,
                'data_desembaraco': row['data_desembaraco'],
                'data_criacao_due': row['data_criacao_due'],
                'data_inicial_entrada_recinto': row['data_inicial_entrada_recinto'],
            })
    return pd.DataFrame(rows)


def buscar_eventos_conteiner(session, conteiners, inicio_eventos, fim_eventos,
                             inicio_desembaraco, fim_desembaraco, recinto):
    if not conteiners:
        return pd.DataFrame(), pd.DataFrame()

    acessos = (
        session.query(AcessoVeiculo)
        .filter(
            AcessoVeiculo.numeroConteiner.in_(conteiners),
            AcessoVeiculo.codigoRecinto == recinto,
            AcessoVeiculo.direcao == 'E',
            AcessoVeiculo.dataHoraOcorrencia >= inicio_eventos,
            AcessoVeiculo.dataHoraOcorrencia < fim_desembaraco,
        )
        .order_by(AcessoVeiculo.numeroConteiner, AcessoVeiculo.dataHoraOcorrencia)
        .all()
    )

    embarques = (
        session.query(EmbarqueDesembarque)
        .filter(
            EmbarqueDesembarque.numeroConteiner.in_(conteiners),
            EmbarqueDesembarque.codigoRecinto == recinto,
            EmbarqueDesembarque.embarqueDesembarque == 'E',
            EmbarqueDesembarque.dataHoraOcorrencia >= inicio_desembaraco,
            EmbarqueDesembarque.dataHoraOcorrencia < fim_eventos,
        )
        .order_by(EmbarqueDesembarque.numeroConteiner, EmbarqueDesembarque.dataHoraOcorrencia)
        .all()
    )

    df_acc = pd.DataFrame([
        {
            'conteiner': a.numeroConteiner,
            'data entrada': pd.to_datetime(a.dataHoraOcorrencia, errors='coerce'),
            'id_entrada': a.id,
        }
        for a in acessos
    ])

    df_emb = pd.DataFrame([
        {
            'conteiner': e.numeroConteiner,
            'data embarque': pd.to_datetime(e.dataHoraOcorrencia, errors='coerce'),
            'id_embarque': e.id,
        }
        for e in embarques
    ])

    return df_acc, df_emb


def juntar_eventos(df_containers, df_acc, df_emb):
    df = df_containers.copy()

    if not df.empty and not df_acc.empty:
        df_acc = df_acc.sort_values(['conteiner', 'data entrada']).drop_duplicates('conteiner', keep='first')
        df = df.merge(df_acc[['conteiner', 'data entrada', 'id_entrada']], on='conteiner', how='left')

    if not df.empty and not df_emb.empty:
        df_emb = df_emb.sort_values(['conteiner', 'data embarque']).drop_duplicates('conteiner', keep='first')
        df = df.merge(df_emb[['conteiner', 'data embarque', 'id_embarque']], on='conteiner', how='left')

    if not df.empty:
        df['diff_entrada_desembaraco'] = df['data_desembaraco'] - df['data entrada']
        df['diff_embarque_desembaraco'] = df['data embarque'] - df['data_desembaraco']

    return df


def salvar_base(df_final, prefixo='output/analise_exportacao_8931359'):
    os.makedirs('output', exist_ok=True)
    cols = ['due', 'conteiner', 'data entrada', 'data_criacao_due', 'data_desembaraco', 'data embarque']
    df_saida = df_final.reindex(columns=cols)
    df_saida.to_csv(f'{prefixo}.csv', index=False, encoding='utf-8-sig')
    df_saida.to_excel(f'{prefixo}.xlsx', index=False)


def analisar(session, data_inicio=None, data_fim=None, recinto=RECINTO_PADRAO):
    if data_fim is None:
        data_fim = datetime.now() - timedelta(days=7)
    if data_inicio is None:
        data_inicio = datetime.now() - timedelta(days=14)

    data_inicio = parse_dt(data_inicio)
    data_fim = parse_dt(data_fim)

    if data_inicio >= data_fim:
        raise ValueError('data_inicio precisa ser menor que data_fim')

    print(f'Parâmetros: {data_inicio}, {data_fim}, {recinto}')
    dues = buscar_dues(session, recinto, data_inicio, data_fim)
    df_dues = due_to_df(dues)

    dues_externas = buscar_dues_externas(session, recinto, data_inicio, data_fim)
    df_dues_externas = due_to_df(dues_externas)
    print(f'Total de Dues registradas fora para embarque no recinto no período: {len(df_dues_externas)}')


    if df_dues.empty:
        print(f'Nenhuma DUE encontrada para o recinto {recinto} no intervalo informado.')
        return None, None, None, None

    s1 = stats_timedelta(df_dues['diff_criacao_desembaraco'])
    s2 = stats_timedelta(df_dues['diff_criacao_entrada'])

    print_stats('DUE: data_criacao_due vs data_desembaraco', s1)
    print_stats('DUE: data_criacao_due vs data_inicial_entrada_recinto', s2)

    df_containers = expandir_conteiners(df_dues)
    if df_containers.empty:
        print('Nenhum contêiner encontrado nas DUEs.')
        return df_dues, df_containers, None, None

    conteiners = df_containers['conteiner'].dropna().unique().tolist()

    inicio_eventos = data_inicio - timedelta(days=14)
    fim_eventos = data_fim + timedelta(days=10)

    df_acc, df_emb = buscar_eventos_conteiner(session, conteiners, inicio_eventos, fim_eventos,
                                              data_inicio, data_fim, recinto)
    df_final = juntar_eventos(df_containers, df_acc, df_emb)

    s3 = stats_timedelta(df_final['diff_entrada_desembaraco'])
    s4 = stats_timedelta(df_final['diff_embarque_desembaraco'])

    print_stats('Contêiner: entrada AcessoVeiculo vs desembaraço da DUE', s3)
    print_stats('Contêiner: embarque EmbarqueDesembarque vs desembaraço da DUE', s4)

    salvar_base(df_final)

    return df_dues, df_containers, df_final, {
        'due_criacao_desembaraco': s1,
        'due_criacao_entrada': s2,
        'entrada_desembaraco': s3,
        'embarque_desembaraco': s4,
    }


if __name__ == '__main__':
    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    data_fim = datetime.now() - timedelta(days=21)
    data_inicio = datetime.now() - timedelta(days=40)

    # padrão: de 14 dias atrás até 7 dias atrás
    analisar(session, data_inicio, data_fim, RECINTO_PADRAO)
