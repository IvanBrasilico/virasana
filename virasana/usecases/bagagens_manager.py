from datetime import datetime, timedelta
from typing import List

from bhadrasana.models.ovr import Recinto
from pymongo.database import Database
from virasana.integracao.carga import get_peso_balanca
from virasana.integracao.gmci_alchemy import GMCI
from virasana.integracao.mercante.mercantealchemy import Item, Conhecimento, Manifesto


def testa_filtros(item, portoorigem: str, cpf_cnpj: str, numero_conteiner: str):
    if numero_conteiner and numero_conteiner not in item.codigoConteiner:
        return False
    for conhecimento in item.conhecimentos:
        if portoorigem and portoorigem not in conhecimento.portoOrigemCarga:
            return False
        if cpf_cnpj and cpf_cnpj not in conhecimento.consignatario:
            return False
    return True


def get_bagagens(mongodb: Database,
                 session, datainicio: datetime, datafim: datetime,
                 portoorigem: str, cpf_cnpj: str, numero_conteiner: str,
                 somente_sem_imagem=False
                 ) -> List[Item]:
    query = '''SELECT codigoConteiner, c.numeroCEmercante FROM itensresumo i
    INNER JOIN conhecimentosresumo c ON i.numeroCEmercante = c.numeroCEmercante
    WHERE NCM LIKE '9797%' AND c.tipoBLConhecimento IN(10, 11) AND c.tipoTrafego = 5
    AND c.portoDestFinal = 'BRSSZ' 
    AND c.dataEmissao BETWEEN :datainicio AND :datafim
    LIMIT 100'''
    conteineres = session.execute(query, {'datainicio': datainicio, 'datafim': datafim})
    # lista_itens = q.limit(ROWS_PER_PAGE).offset(ROWS_PER_PAGE * paginaatual).all()
    # lista_itens = q.fetchall()
    # print(datainicio, datafim)
    lista_itens = []
    for row in conteineres:
        item = session.query(Item).filter(Item.codigoConteiner == row[0]). \
            filter(Item.numeroCEmercante == row[1]). \
            one_or_none()
        conhecimento = session.query(Conhecimento).filter(
            Conhecimento.numeroCEmercante == item.numeroCEmercante).one_or_none()
        manifesto = session.query(Manifesto).filter(
            Manifesto.numero == conhecimento.manifestoCE).one_or_none()
        item.manifesto = manifesto
        filhotes = session.query(Conhecimento).filter(
            Conhecimento.numeroCEMaster == item.numeroCEmercante).all()
        item.conhecimentos = [conhecimento, *filhotes]
        # Procura escaneamentos do contêiner até 32 dias após emissão do conhecimento
        # Se não encontrar, procura pelo conhecimento
        dataminima = datetime.strptime(conhecimento.dataEmissao, '%Y-%m-%d') + timedelta(days=2)
        datamaxima = dataminima + timedelta(days=30)
        gmci = session.query(GMCI).filter(GMCI.num_conteiner == item.codigoConteiner). \
            filter(GMCI.datahora.between(dataminima, datamaxima)).one_or_none()
        if gmci:
            recinto = session.query(Recinto).filter(Recinto.id == gmci.cod_recinto).one_or_none()
            if recinto:
                gmci.nome_recinto = recinto.nome
        item.gmci = gmci
        grid_data = mongodb['fs.files'].find_one(
            {'metadata.numeroinformado': item.codigoConteiner,
             'metadata.dataescaneamento': {'$gte': dataminima, '$lte': datamaxima}})
        if not grid_data:
            grid_data = mongodb['fs.files'].find_one(
                {'metadata.carga.conhecimento.conhecimento': item.numeroCEmercante})
        item.pesoBalanca = 0.
        if grid_data:
            item.id_imagem = grid_data.get('_id')
            item.pesoBalanca = get_peso_balanca(grid_data.get('metadata').get('pesagens'))
        if testa_filtros(item, portoorigem, cpf_cnpj, numero_conteiner):
            if somente_sem_imagem:
                if not grid_data:
                    lista_itens.append(item)
            else:
                lista_itens.append(item)
    return lista_itens
