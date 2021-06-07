from datetime import datetime, timedelta
from typing import List

from bhadrasana.models.ovr import Recinto
from pymongo.database import Database
from sqlalchemy import desc
from virasana.integracao.bagagens.viajantesalchemy import Viagem
from virasana.integracao.carga import get_peso_balanca
from virasana.integracao.gmci_alchemy import GMCI
from virasana.integracao.mercante.mercantealchemy import Item, Conhecimento, Manifesto


def get_bagagens(mongodb: Database,
                 session, datainicio: datetime, datafim: datetime,
                 portoorigem: str, cpf_cnpj: str, numero_conteiner: str,
                 somente_sem_imagem=False
                 ) -> List[Item]:
    # tipoTrafego 5 = lci
    # tipoBLConhecimento 10 = MBL 11 = BL 12 = HBL 15 = MHBL
    query = '''SELECT codigoConteiner, c.numeroCEmercante, c.tipoBLConhecimento FROM itensresumo i
    INNER JOIN conhecimentosresumo c ON i.numeroCEmercante = c.numeroCEmercante
    WHERE NCM LIKE '9797%' AND c.tipoTrafego = 5
    AND c.portoDestFinal = 'BRSSZ' AND codigoConteiner like :codigoConteiner
    AND c.portoOrigemCarga like :portoOrigemCarga
    AND c.dataEmissao BETWEEN :datainicio AND :datafim
    LIMIT 100'''
    if numero_conteiner:
        numero_conteiner = numero_conteiner.upper()
    numero_conteiner = ''.join([s for s in numero_conteiner if s.isdigit() or s.isalpha()])
    if portoorigem:
        portoorigem = portoorigem.upper()
    if cpf_cnpj:
        cpf_cnpj = ''.join([s for s in cpf_cnpj if s.isdigit()])
    q = session.query(Conhecimento.numeroCEmercante, Conhecimento.tipoBLConhecimento,
                      Item.codigoConteiner). \
        join(Item, Item.numeroCEmercante == Conhecimento.numeroCEmercante). \
        filter(Item.NCM.like('9797%')). \
        filter(Conhecimento.tipoTrafego == 5). \
        filter(Conhecimento.portoDestFinal == 'BRSSZ'). \
        filter(Item.codigoConteiner.like(numero_conteiner + '%')). \
        filter(Conhecimento.portoOrigemCarga.like(portoorigem + '%')). \
        filter(Conhecimento.consignatario.like(cpf_cnpj + '%')). \
        filter(Conhecimento.dataEmissao.between(datainicio, datafim))
    print(str(q))
    print(f'numero_conteiner:{numero_conteiner}, portoorigem:{portoorigem}, '
          f'datainicio: {datainicio}, datafim:{datafim}')
    conteineres = q.all()
    print(f'{len(conteineres)} itens totais encontrados...')
    lista_itens = []
    for row in conteineres:
        print(row)
    numeros_ces = set([row[0] for row in conteineres])
    print(numeros_ces)
    itens = session.query(Item). \
        filter(Item.numeroCEmercante.in_(numeros_ces)).all()
    print(f'{len(itens)} itens encontrados...')
    for item in itens:
        conhecimento = session.query(Conhecimento).filter(
            Conhecimento.numeroCEmercante == item.numeroCEmercante).one_or_none()
        if not int(conhecimento.tipoBLConhecimento) in (10, 11):
            conhecimento = session.query(Conhecimento).filter(
                Conhecimento.numeroCEmercante == conhecimento.numeroCEMaster).one_or_none()
        print(item.codigoConteiner)
        manifesto = session.query(Manifesto).filter(
            Manifesto.numero == conhecimento.manifestoCE).one_or_none()
        item.manifesto = manifesto
        filhotes = session.query(Conhecimento).filter(
            Conhecimento.numeroCEMaster == conhecimento.numeroCEmercante).all()
        print(conhecimento.tipoBLConhecimento, conhecimento.numeroCEmercante, filhotes)
        item.conhecimentos = [conhecimento, *filhotes]
        data_inicial_viagens = datetime.now() - timedelta(days=365 * 2)
        for ce in item.conhecimentos:
            print(ce)
            if int(ce.tipoBLConhecimento) in (11, 12):
                viagens = session.query(Viagem).filter(Viagem.cpf == ce.consignatario). \
                    filter(Viagem.data_chegada > data_inicial_viagens). \
                    order_by(desc(Viagem.data_chegada))
                ce.viagens = viagens
        # Procura escaneamentos do contêiner até 32 dias após emissão do conhecimento
        # Se não encontrar, procura pelo conhecimento
        dataminima = datetime.strptime(conhecimento.dataEmissao, '%Y-%m-%d') + timedelta(days=2)
        datamaxima = dataminima + timedelta(days=30)
        gmci = session.query(GMCI).filter(GMCI.num_conteiner == item.codigoConteiner). \
            filter(GMCI.datahora.between(dataminima, datamaxima)).first()
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
        # if (not somente_sem_imagem) or (grid_data is None):
        lista_itens.append(item)
    return lista_itens
