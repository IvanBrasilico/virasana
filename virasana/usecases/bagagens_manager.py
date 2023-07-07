from datetime import datetime, timedelta
from typing import List, Tuple

from ajna_commons.flask.log import logger
from bhadrasana.models.laudo import Empresa
from bhadrasana.models.ovr import Recinto, OVR
from bhadrasana.models.rvf import RVF, ImagemRVF
from pymongo.database import Database
from sqlalchemy import desc, func, or_
from virasana.forms.filtros import FormClassificacaoRisco
from virasana.integracao.bagagens.viajantesalchemy import Viagem, Pessoa, DSI, ClassificacaoRisco, ClasseRisco
from virasana.integracao.carga import get_peso_balanca
from virasana.integracao.gmci_alchemy import GMCI
from virasana.integracao.mercante.mercantealchemy import Item, Conhecimento, Manifesto


def get_classificacaorisco(session, conhecimento, filhotes):
    ces_risco_pesquisa = [*[ce.numeroCEmercante for ce in filhotes], conhecimento.numeroCEmercante]
    classificacoes = session.query(ClassificacaoRisco). \
        filter(ClassificacaoRisco.numeroCEmercante.in_(ces_risco_pesquisa)).all()
    classificacaorisco = None
    classeriscoatual = ClasseRisco.NAO_CLASSIFICADO.value
    for classificacao in classificacoes:
        if classificacao.classerisco > classeriscoatual:
            classificacaorisco = classificacao
            classeriscoatual = classificacao.classerisco
    if classificacaorisco is None:
        form_classificacao = FormClassificacaoRisco(numeroCEmercante=conhecimento.numeroCEmercante)
        canal = None
    else:
        form_classificacao = FormClassificacaoRisco(numeroCEmercante=conhecimento.numeroCEmercante,
                                                    classerisco=classificacaorisco.classerisco,
                                                    descricao=classificacaorisco.descricao)
        canal = ClasseRisco(classificacaorisco.classerisco).name
    return classificacaorisco, form_classificacao, canal


def risco_despachante(session, despachante):
    total_dsis = session.query(DSI.numero).filter(DSI.despachante == despachante).count()
    total_eua = session.query(DSI).join(Conhecimento, DSI.numeroCEmercante == Conhecimento.numeroCEmercante). \
        filter(DSI.despachante == despachante). \
        filter(Conhecimento.portoOrigemCarga.like('US%')).count()
    total_autuacoes = session.query(DSI).join(Conhecimento, DSI.numeroCEmercante == Conhecimento.numeroCEmercante). \
        join(OVR, OVR.numeroCEmercante == Conhecimento.numeroCEmercante). \
        filter(DSI.despachante == despachante). \
        filter(OVR.fase == 4).count()
    return f'Despachante {despachante} DSIs {total_dsis}({total_eua} EUA) Autos: {total_autuacoes}'


def get_bagagens(mongodb: Database,
                 session, datainicio: datetime, datafim: datetime,
                 portoorigem: str, cpf_cnpj: str, numero_conteiner: str,
                 ncm='9797', portodestino='BRSSZ',
                 selecionados=False,
                 concluidos=False,
                 classificados=False,
                 somente_sem_imagem=False,
                 filtrar_dsi=False
                 ) -> Tuple[List[Item], list]:
    # tipoTrafego 5 = lci
    # tipoBLConhecimento 10 = MBL 11 = BL 12 = HBL 15 = MHBL
    cpf_cnpj_lista = None
    if numero_conteiner:
        numero_conteiner = numero_conteiner.upper()
    numero_conteiner = ''.join([s for s in numero_conteiner if s.isdigit() or s.isalpha()])
    if portoorigem:
        portoorigem = portoorigem.upper()
    if cpf_cnpj:
        cpf_cnpj = ''.join([s for s in cpf_cnpj if s.isdigit() or s == ';'])
        cpf_cnpj_lista = [item.strip() for item in cpf_cnpj.split(';')]
    q = session.query(Conhecimento.numeroCEmercante, Conhecimento.tipoBLConhecimento,
                      Item.codigoConteiner, Conhecimento.numeroCEMaster)
    q = q.join(Item, or_(Item.numeroCEmercante == Conhecimento.numeroCEmercante,
                         Item.numeroCEmercante == Conhecimento.numeroCEMaster)
               )
    if selecionados or concluidos:
        q = q.join(OVR, OVR.numeroCEmercante == Conhecimento.numeroCEmercante)
        # ,  OVR.numeroCEmercante == Conhecimento.numeroCEMaster))
    if filtrar_dsi:
        q = q.join(DSI, DSI.numeroCEmercante == Conhecimento.numeroCEmercante)
    if classificados:
        q = q.join(ClassificacaoRisco, ClassificacaoRisco.numeroCEmercante == Conhecimento.numeroCEmercante)
    q = q.filter(Item.NCM.like(ncm + '%')). \
        filter(Conhecimento.tipoTrafego == 5). \
        filter(Conhecimento.portoDestFinal.like(portodestino + '%')). \
        filter(Item.codigoConteiner.like(numero_conteiner + '%')). \
        filter(Conhecimento.portoOrigemCarga.like(portoorigem + '%'))
    if cpf_cnpj_lista:
        q = q.filter(Conhecimento.consignatario.in_(cpf_cnpj_lista))
    if selecionados:
        q = q.filter(OVR.fase < 3)
    if concluidos:
        q = q.filter(OVR.fase >= 3)
    if classificados:
        q = q.filter(ClassificacaoRisco.classerisco > ClasseRisco.VERDE.value)
    if filtrar_dsi:
        q = q.filter(DSI.data_registro.between(datainicio, datafim))
    else:
        q = q.filter(Conhecimento.dataEmissao.between(datainicio, datafim))
    print(str(q))
    print(f'numero_conteiner:{numero_conteiner}, portoorigem:{portoorigem}, '
          f'datainicio: {datainicio}, datafim:{datafim}')
    if filtrar_dsi:
        conteineres = q.order_by(DSI.numero).limit(200).all()
    else:
        conteineres = q.order_by(Conhecimento.dataEmissao).limit(200).all()
    print(f'{len(conteineres)} itens totais encontrados...')
    lista_itens = []
    for row in conteineres:
        print(row)
    numeros_ces = set([row[0] for row in conteineres])
    print(numeros_ces)
    numeros_ces_master = set([row[3] for row in conteineres if row[3] and len(row[3]) == 15])
    print(numeros_ces_master)
    numeros_ces = numeros_ces.union(numeros_ces_master)
    print(numeros_ces)
    itens = session.query(Item). \
        filter(Item.numeroCEmercante.in_(numeros_ces)).all()
    print(f'{len(itens)} itens encontrados...')
    conteineres_incluidos = set()
    for item in itens:
        if not item.codigoConteiner:
            continue
        if item.codigoConteiner in conteineres_incluidos:
            continue
        conteineres_incluidos.add(item.codigoConteiner)
        item.dsis = []
        conhecimento = session.query(Conhecimento).filter(
            Conhecimento.numeroCEmercante == item.numeroCEmercante).one_or_none()
        if conhecimento is None:
            logger.error(f'Contêiner {item.codigoConteiner} CE {item.numeroCEmercante} não encontrado...')
            continue
        if not int(conhecimento.tipoBLConhecimento) in (10, 11):
            conhecimento_master = session.query(Conhecimento).filter(
                Conhecimento.numeroCEmercante == conhecimento.numeroCEMaster).one_or_none()
            if conhecimento_master:
                conhecimento = conhecimento_master
        filhotes = session.query(Conhecimento).filter(
            Conhecimento.numeroCEMaster == conhecimento.numeroCEmercante).all()
        print('****', item.codigoConteiner)
        print(f'****Pesquisando classificacaorisco {conhecimento.numeroCEmercante} e filhos...')
        ### No caso de Master, pesquisará todas as classificações dos filhotes e exibirá a maior
        conhecimento.classificacaorisco, conhecimento.form_classificacao, \
        conhecimento.canal = get_classificacaorisco(session, conhecimento, filhotes)
        manifesto = session.query(Manifesto).filter(
            Manifesto.numero == conhecimento.manifestoCE).one_or_none()
        item.manifesto = manifesto
        for ce in filhotes:
            ce.classificacaorisco, _, ce.canal = get_classificacaorisco(session, ce, [])
        # print(conhecimento.tipoBLConhecimento, conhecimento.numeroCEmercante, filhotes)
        item.conhecimentos = [conhecimento, *filhotes]
        data_inicial_viagens = datetime.now() - timedelta(days=365 * 2)
        # Pegar info Fichas e RVFs
        rvfs = session.query(RVF).filter(RVF.numerolote == item.codigoConteiner).all()
        item.rvfs = rvfs
        conhecimentos_ids = [ce.numeroCEmercante for ce in item.conhecimentos]
        rvfs = session.query(RVF).filter(RVF.numeroCEmercante.in_(conhecimentos_ids)).all()
        item.rvfs.extend([rvf for rvf in rvfs if rvf not in item.rvfs])
        if rvfs and len(rvfs) > 0:
            item.max_data_rvf = max([rvf.datahora for rvf in rvfs])
        else:
            item.max_data_rvf = datetime.strptime(conhecimento.dataEmissao, '%Y-%m-%d')
        item.qtdefotos = session.query(func.count(ImagemRVF.id)). \
            filter(ImagemRVF.rvf_id.in_([rvf.id for rvf in item.rvfs])).scalar()
        ovrs = session.query(OVR).filter(OVR.numeroCEmercante.in_(conhecimentos_ids)).all()
        item.fichas = [ovr.id for ovr in ovrs]
        # Pegar viagens do CPF
        item.max_numero_dsi = ''
        for ce in item.conhecimentos:
            # print(ce)
            ce.nome_consignatario = ''
            ce.dsis = []
            cpf_cnpj = ce.consignatario
            # print('*************', cpf_cnpj)
            if len(cpf_cnpj) == 11:
                logger.debug(cpf_cnpj)
                pessoa = session.query(Pessoa).filter(Pessoa.cpf == cpf_cnpj).one_or_none()
                if pessoa:
                    ce.nome_consignatario = pessoa.nome
                dsis = session.query(DSI).filter(DSI.consignatario == cpf_cnpj).all()
                ce.dsis = dsis
                despachantes_set = set()
                for dsi in dsis:
                    item.dsis.append(dsi)
                    item.max_numero_dsi = max(item.max_numero_dsi, dsi.numero)
                    despachantes_set.add(dsi.despachante)
                item.despachantes_risco = [risco_despachante(session, despachante) for despachante in despachantes_set]
            else:
                try:
                    empresa = session.query(Empresa). \
                        filter(Empresa.cnpj.like(cpf_cnpj[:8] + '%')).limit(1).first()
                    ce.nome_consignatario = empresa.nome
                except Exception as err:
                    logger.error(f'Empresa {cpf_cnpj} não encontrada')
                    ce.nome_consignatario = ''
            if int(ce.tipoBLConhecimento) in (11, 12):
                viagens = session.query(Viagem).filter(Viagem.cpf == ce.consignatario). \
                    filter(Viagem.data_chegada > data_inicial_viagens). \
                    order_by(desc(Viagem.data_chegada)).limit(7)
                ce.viagens = viagens
        # Procura escaneamentos do contêiner até 45 dias após emissão do conhecimento
        # Se não encontrar, procura pelo conhecimento
        dataminima = datetime.strptime(conhecimento.dataEmissao, '%Y-%m-%d') + timedelta(days=5)
        datamaxima = dataminima + timedelta(days=40)
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
        if (not somente_sem_imagem) or (grid_data is None):
            lista_itens.append(item)
    return lista_itens, conteineres
