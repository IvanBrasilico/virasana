"""Funções para leitura e tratamento dos dados de pesagem e gate dos recintos.
"""
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from ajna_commons.flask.conf import SQL_URI
from ajna_commons.flask.log import logger

# SQL para corrigir bug que não consegue capturar Evento automático de
# Verificação física

SQLS_A_EXECUTAR = [
    '''
    insert into ovr_eventos (ovr_id, tipoevento_id, fase, user_name, motivo)
    select ovr_id, tipoevento_id, fase, user_name, motivo from
    (SELECT o.id as ovr_id, 22 as tipoevento_id, 1 as fase, v.user_name, concat('RVF ', v.id) as motivo,
    v.id as rvf_id, count(i.id) as qtde_fotos
    FROM ovr_ovrs o inner join ovr_verificacoesfisicas v on o.id = v.ovr_id
    inner join ovr_imagensrvf i on i.rvf_id = v.id
    left join ovr_eventos e on e.ovr_id = o.id and e.tipoevento_id = 22
    where o.datahora > "2022-01-01" and e.id is null
    group by v.id having qtde_fotos > 4) as sem_eventos;
    ''',
]


def executa_sql_raw(session, sql):
    try:
        result = session.execute(sql)
        session.commit()
        # Dependendo do driver, o atributo `rowcount` pode indicar quantas linhas foram afetadas
        logger.info(f'acertos_sql: SQL "{sql[:30]}" executado')
        logger.info(f"acertos_sql: Linhas inseridas: {result.rowcount}")

        # Alguns drivers podem suportar mensagens do servidor via `cursor.messages`
        if hasattr(result.cursor, "messages"):
            logger.info(f"acertos_sql: Mensagens do Servidor: {result.cursor.messages}")

    except Exception as e:
        print(f"acertos_sql: Erro: {e}")
        session.rollback()
    finally:
        session.close()


def update(connection):
    session = scoped_session(sessionmaker(autocommit=False,
                                          autoflush=False,
                                          bind=connection))
    try:
        for sql in SQLS_A_EXECUTAR:
            # logger.info(sql)
            executa_sql_raw(session, sql)
    finally:
        session.close()


if __name__ == '__main__':  # pragma: no cover
    connection = create_engine(SQL_URI)
    update(connection)
