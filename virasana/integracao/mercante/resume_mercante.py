"""
Após a integração dos XMLs, resumir em tabelas de Estado

Os arquivos XML possuem movimentos: 'I'nclusao, 'A'tualizacao e 'E'xclusao
Este script consolida em tabelas.


Este arquivo pode ser chamado em um prompt de comando no Servidor ou
programado para rodar via crontab, conforme exempo em /periodic_updates.sh
"""
import logging
import os
import time
import sqlalchemy
from datetime import datetime
from sqlalchemy import func, select, and_
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ajna_commons.flask.conf import SQL_URI

from ajna_commons.flask.log import logger
from sqlalchemy.orm.exc import MultipleResultsFound

from virasana.integracao.mercante.mercantealchemy import Conhecimento, \
    ConteinerVazio, ControleResumo, Item, Manifesto, NCMItem, \
    t_conhecimentosEmbarque, t_ConteinerVazio, t_itensCarga, \
    t_manifestosCarga, t_NCMItemCarga, t_exclusoesEscala, Escala, \
    t_ManifestoEscala, EscalaManifesto


def get_pendentes(session, origem, tipomovimento, limit=100000):
    controle = ControleResumo.get_(session, str(origem), tipomovimento)
    maxid = controle.maxid
    logger.info('%s - inicio em ID %s - tipo %s' % (origem, maxid, tipomovimento))
    s = select([origem]).where(
        and_(origem.c['id'] > maxid, origem.c['tipoMovimento'] == tipomovimento)
    ).order_by(origem.c['id']).limit(limit)
    resultproxy = session.execute(s)
    return controle, resultproxy


def monta_campos(destino):
    keys = destino.__table__.columns.keys()
    keys = [key for key in keys if key not in
            ('ID', 'last_modified', 'dataInicioOperacaoDate')]
    return keys


def processa_resumo(engine, origem, destino, chaves):
    Session = sessionmaker(bind=engine)
    session = Session()
    # Fazer INSERTS PRIMEIRO
    movimentos = ['I', 'A', 'E']
    for tipomovimento in movimentos:
        controle, resultproxy = get_pendentes(session, origem, tipomovimento)
        campos_destino = monta_campos(destino)
        # Tratar atualização de EscalaManifesto como inclusão
        if destino.__name__ == 'EscalaManifesto':
            if tipomovimento == 'A':
                tipomovimento = 'I'
        cont = 0
        for row in resultproxy:
            if row['id'] > controle.maxid:
                controle.maxid = row['id']
            dict_campos = {key: row[key]
                           for key in campos_destino}
            if tipomovimento == 'I':
                objeto = destino(**dict_campos)
                session.add(objeto)
                # destino.__table__.insert().prefix_with('IGNORE').values(**dict_campos)
                cont += 1
            else:  # A = Update / E = Delete
                chaves_valores = [getattr(destino, chave) == row[chave] for chave in chaves]
                # print(chaves_valores)
                try:
                    objeto = session.query(destino).filter(*chaves_valores).one_or_none()
                    if objeto:
                        cont += 1
                        if tipomovimento == 'E':
                            session.delete(objeto)
                        else:
                            for k, v in dict_campos.items():
                                setattr(objeto, k, v)
                except MultipleResultsFound:
                    filtro = {chave: row[chave] for chave in chaves}
                    logger.error(
                        'Erro! Multiplos registros encontrados para %s com filtro %s'
                        'Registro %s não atualizado!' %
                        (destino.__tablename__, filtro, row['id'])
                    )
        session.add(controle)
        session.commit()
        logger.info('%s Resumos tipo %s processados' %
                    (cont, tipomovimento)
                    )


def exclui_orfaos(engine):
    """Exclui todos os registros que não contenham """
    Session = sessionmaker(bind=engine)
    session = Session()
    sql_delete_itens = 'DELETE FROM itensresumo WHERE numeroCEMercante not in (SELECT numeroCEMercante from conhecimentosresumo);'
    sql_delete_ncmitem = 'DELETE FROM ncmitemresumo WHERE numeroCEMercante not in (SELECT numeroCEMercante from conhecimentosresumo);'
    # sql_delete_conhecimento = 'DELETE FROM conhecimentosresumo WHERE manifestoCE not in (SELECT numero from manifestosresumo);'
    # sql_delete_conteinervazio = 'DELETE FROM conteinervazioresumo WHERE manifesto not in (SELECT numero from manifestosresumo);'
    # sql_delete_escalamanifesto = 'DELETE FROM escalamanifestoresumo WHERE escala not in (SELECT escala from escalaeresumo);'
    # sql_delete_manifesto = 'DELETE FROM manifestosresumo WHERE numero not in (SELECT manifesto from escalamanifestoresumo);'
    # session.execute(sql_delete_escalamanifesto)
    # session.execute(sql_delete_manifesto)
    # session.execute(sql_delete_conteinervazio)
    # session.execute(sql_delete_conhecimento)
    session.execute(sql_delete_itens)
    session.execute(sql_delete_ncmitem)


def mercante_resumo(engine):
    logger.info('Iniciando resumo da base Mercante...')
    migracoes = {t_ManifestoEscala: EscalaManifesto,
                 t_manifestosCarga: Manifesto,
                 t_conhecimentosEmbarque: Conhecimento,
                 t_itensCarga: Item,
                 t_NCMItemCarga: NCMItem,
                 t_ConteinerVazio: ConteinerVazio,
                 }

    chaves = {EscalaManifesto: ['manifesto', 'escala'],
              Manifesto: ['numero'],
              Conhecimento: ['numeroCEmercante'],
              Item: ['numeroCEmercante', 'numeroSequencialItemCarga'],
              NCMItem: ['numeroCEMercante', 'codigoConteiner',
                        'numeroSequencialItemCarga', 'identificacaoNCM'],
              ConteinerVazio: ['manifesto', 'idConteinerVazio'],
              }

    t0 = time.time()
    for origem, destino in migracoes.items():
        t1 = time.time()
        processa_resumo(engine, origem, destino, chaves[destino])
        t2 = time.time()
        logger.info('Resumos processados em %0.2f s' %
                    (t2 - t1)
                    )
    logger.info('FINAL: Resumos processados em %0.2f s' %
                (t2 - t0)
                )
    # exclui_orfaos(engine)


if __name__ == '__main__':
    os.environ['DEBUG'] = '1'
    logger.setLevel(logging.DEBUG)
    engine = sqlalchemy.create_engine(SQL_URI)
    # engine = sqlalchemy.create_engine('sqlite:///teste.db')
    mercante_resumo(engine)
