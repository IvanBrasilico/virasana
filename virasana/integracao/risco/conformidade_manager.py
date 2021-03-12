from decimal import Decimal

from sqlalchemy import and_, func, text, desc
from virasana.integracao.risco.conformidade_alchemy import Conformidade


def get_conformidade(session, datainicio=None, datafim=None, cod_recinto='',
                     isocode_group=None, isocode_size=None):
    sql_conformidade = '''
    SELECT cod_recinto as Recinto, count(*) as "Qtde de imagens",
     CAST(avg(height) AS INT) as "Linhas (média)",
     CAST(avg(width) AS INT) as "Colunas (média)",
     avg(ratio) as "Relação largura/altura (média)",
     SUM(height < 700) / count(*) * 100 as "% Tamanho pequeno",
     SUM(ratio < 1.5) / count(ratio) * 100 as "% relação abaixo 1.5",
     avg(laplacian) as "Índice de nitidez ou ruído",
     avg(bbox_score) *100 as "% Confiança da VC",
     (sum(bbox_classe>=3) / count(*)) * 100 as "% VC não encontra CC"
     from ajna_conformidade 
     where dataescaneamento between :datainicio and :datafim
     '''
    if cod_recinto:
        sql_conformidade = sql_conformidade + ' and cod_recinto=:cod_recinto'
    if isocode_size:
        sql_conformidade = sql_conformidade + ' and isocode_size=:isocode_size'
    if isocode_group:
        sql_conformidade = sql_conformidade + ' and isocode_group=:isocode_group'
    sql_conformidade = sql_conformidade + ' group by cod_recinto'
    rs = session.execute(sql_conformidade, {'datainicio': datainicio,
                                            'datafim': datafim,
                                            'cod_recinto': cod_recinto,
                                            'isocode_size': isocode_size,
                                            'isocode_group': isocode_group})
    lista_conformidade = []
    for row in rs:
        linha = []
        for col in row:
            if isinstance(col, int):
                linha.append(col)
            elif isinstance(col, Decimal):
                linha.append('{:0.1f}'.format(col))
            else:
                linha.append(col)
        lista_conformidade.append(linha)
    return rs.keys(), lista_conformidade


def get_conformidade_recinto(session, recinto=None, datainicio=None,
                             datafim=None, isocode_group=None, isocode_size=None,
                             order=None, reverse=False,
                             paginaatual=0):
    ROWS_PER_PAGE = 10
    filtro = and_(Conformidade.dataescaneamento.between(datainicio, datafim),
                  Conformidade.cod_recinto == recinto)
    if isocode_group:
        filtro = and_(filtro, Conformidade.isocode_group == isocode_group)
    if isocode_size:
        filtro = and_(filtro, Conformidade.isocode_size == isocode_size)
    npaginas = int(session.query(func.count(Conformidade.ID)).filter(filtro).scalar()
                   / ROWS_PER_PAGE)
    q = session.query(Conformidade).filter(filtro)
    if order:
        print(reverse)
        if reverse:
            q = q.order_by(desc(text(order)))
        else:
            q = q.order_by(text(order))
    lista_conformidade = q.limit(ROWS_PER_PAGE).offset(ROWS_PER_PAGE * paginaatual).all()
    print(datainicio, datafim)
    return lista_conformidade, npaginas
