# coding: utf-8
import datetime
from sqlalchemy import create_engine, and_
from sqlalchemy import BigInteger, Column, CHAR, \
    DateTime, func, Integer, Index, MetaData, select, \
    Table, Text, VARCHAR
from sqlalchemy.dialects.mysql import BIGINT, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

from ajna_commons.flask.conf import SQL_URI

# from sqlalchemy.orm import relationship
# from sqlachemy import ForeignKey

Base = declarative_base()
metadata = Base.metadata


# Tabelas auxiliares / log

class Enumerado(Base):
    __tablename__ = 'Enumerado'
    id = Column(CHAR(2), primary_key=True)
    tipoTrafegoManifesto = Column(VARCHAR(40))
    tipoTrafegoConhecimento = Column(VARCHAR(40))
    tipoBLConhecimentoMercante = Column(VARCHAR(40))
    tipoItemCarga = Column(VARCHAR(40))

    @classmethod
    def getEnumerado(cls, session, id: str):
        return session.query(Enumerado).filter(Enumerado.id == id).one_or_none()

    @classmethod
    def getTipo(cls, session, tipo: str, id: str):
        enumerado = session.query(Enumerado).filter(Enumerado.id == id).one_or_none()
        if enumerado:
            return getattr(cls, tipo)
        return None

    @classmethod
    def getTipoTrafegoManifesto(cls, session, id: str):
        enumerado = cls.getEnumerado(session, id)
        if enumerado:
            return enumerado.tipoTrafegoManifesto
        return None

    @classmethod
    def getTipoTrafegoConhecimento(cls, session, id: str):
        enumerado = cls.getEnumerado(session, id)
        if enumerado:
            return enumerado.tipoTrafegoConhecimento
        return None

    @classmethod
    def getTipoBLConhecimentoMercante(cls, session, id: str):
        enumerado = cls.getEnumerado(session, id)
        if enumerado:
            return enumerado.tipoBLConhecimentoMercante
        return None


ArquivoBaixado = Table(
    'arquivosbaixados', metadata,
    Column('ID', Integer, primary_key=True, autoincrement=True),
    Column('nome', VARCHAR(50), index=True),
    Column('filename_date', TIMESTAMP, index=True),
    Column('create_date', TIMESTAMP, index=True,
           server_default=func.current_timestamp())
)


def data_ultimo_arquivo_baixado(engine):
    with engine.begin() as conn:
        s = select([func.Max(ArquivoBaixado.c.filename_date)])
        c = conn.execute(s).fetchone()
    return c[0]


def grava_arquivo_baixado(engine, nome, data):
    timestamp = data.strftime('%Y-%m-%d %H:%M:%S')
    with engine.begin() as conn:
        sql = ArquivoBaixado.insert()
        return conn.execute(sql,
                            nome=nome,
                            filename_date=timestamp)


# Tabelas de lista do XML

t_ManifestoEscala = Table(
    'EscalaManifesto', metadata,
    Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('manifesto', Text),
    Column('escala', Text),
    Column('tipoMovimento', Text),
    Column('create_date', TIMESTAMP, index=True,
           server_default=func.current_timestamp())
)

t_ConteinerVazio = Table(
    'ConteinerVazio', metadata,
    Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('idConteinerVazio', Text),
    Column('isoConteinerVazio', Text),
    Column('manifesto', Text),
    Column('taraConteinerVazio', Text),
    Column('tipoMovimento', Text),
    Column('create_date', TIMESTAMP, index=True,
           server_default=func.current_timestamp())
)

t_NCMItemCarga = Table(
    'NCMItemCarga', metadata,
    Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('codigoConteiner', Text),
    Column('codigoTipoEmbalagem', Text),
    Column('descritivo', Text),
    Column('identificacaoNCM', Text),
    Column('itemEmbaladoMadeira', Text),
    Column('marcaMercadoria', Text),
    Column('numeroCEMercante', Text),
    Column('numeroIdentificacao', Text),
    Column('numeroSequencialItemCarga', Text),
    Column('qtdeVolumes', Text),
    Column('tipoMovimento', Text),
    Column('create_date', TIMESTAMP, index=True,
           server_default=func.current_timestamp())
)

# Tabelas base do XML
t_conhecimentosEmbarque = Table(
    'conhecimentosEmbarque', metadata,
    Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('codigoEmpresaNavegacao', Text),
    Column('codigoTerminalCarregamento', Text),
    Column('consignatario', Text),
    Column('cubagem', Text),
    Column('dataAtualizacao', Text),
    Column('dataEmissao', Text),
    Column('descricao', Text),
    Column('embarcador', Text),
    Column('horaAtualizacao', Text),
    Column('indicadorShipsConvenience', Text),
    Column('manifestoCE', Text),
    Column('numeroCEMaster', Text),
    Column('numeroCEmercante', Text),
    Column('paisDestinoFinalMercante', Text),
    Column('portoDestFinal', Text),
    Column('portoOrigemCarga', Text),
    Column('tipoBLConhecimento', Text),
    Column('tipoMovimento', Text),
    Column('tipoTrafego', Text),
    Column('create_date', TIMESTAMP, index=True,
           server_default=func.current_timestamp())
)

t_exclusoesEscala = Table(
    'exclusoesEscala', metadata,
    Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('dataExclusao', Text),
    Column('horaExclusao', Text),
    Column('numeroEscalaMercante', Text),
    Column('tipoMovimento', Text),
    Column('create_date', TIMESTAMP, index=True,
           server_default=func.current_timestamp())
)

t_itensCarga = Table(
    'itensCarga', metadata,
    Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('NCM', Text),
    Column('codigoConteiner', Text),
    Column('codigoTipoEmbalagem', Text),
    Column('contraMarca', Text),
    Column('cubagemM3', Text),
    Column('dataAtualizacao', Text),
    Column('horaAtualizacao', Text),
    Column('indicadorUsoParcial', Text),
    Column('isoCode', Text),
    Column('lacre', Text),
    Column('marca', Text),
    Column('numeroCEmercante', Text),
    Column('numeroSequencialItemCarga', Text),
    Column('pesoBruto', Text),
    Column('qtdeItens', Text),
    Column('tara', Text),
    Column('tipoItemCarga', Text),
    Column('tipoMovimento', Text),
    Column('create_date', TIMESTAMP, index=True,
           server_default=func.current_timestamp())
)

t_manifestosCarga = Table(
    'manifestosCarga', metadata,
    Column('id', BIGINT, primary_key=True, autoincrement=True),
    Column('codAgenciaInformante', Text),
    Column('codigoEmpresaNavegacao', Text),
    Column('codigoTerminalCarregamento', Text),
    Column('codigoTerminalDescarregamento', Text),
    Column('dataAtualizacao', Text),
    Column('dataEncerramento', Text),
    Column('dataInicioOperacao', Text),
    Column('horaAtualizacao', Text),
    Column('numero', Text),
    Column('numeroImoDPC', Text),
    Column('numeroViagem', Text),
    Column('portoCarregamento', Text),
    Column('portoDescarregamento', Text),
    Column('quantidadeConhecimento', Text),
    Column('tipoMovimento', Text),
    Column('tipoTrafego', Text),
    Column('create_date', TIMESTAMP, index=True,
           server_default=func.current_timestamp())
)


### Tabelas resumo


class Escala(Base):
    __tablename__ = 'escalasresumo'
    ID = Column(BIGINT,
                primary_key=True, autoincrement=True)
    numero = Column(VARCHAR(20), unique=True)
    # manifestos = relationship("ManifestoEscala", back_populates='escala',  cascade="delete, delete-orphan")


class Manifesto(Base):
    __tablename__ = 'manifestosresumo'
    ID = Column(BIGINT,
                primary_key=True, autoincrement=True)
    numero = Column(VARCHAR(15), unique=True)
    codAgenciaInformante = Column(VARCHAR(20))
    codigoEmpresaNavegacao = Column(VARCHAR(20), index=True)
    codigoTerminalCarregamento = Column(VARCHAR(20), index=True)
    codigoTerminalDescarregamento = Column(VARCHAR(20), index=True)
    dataAtualizacao = Column(VARCHAR(10))
    dataEncerramento = Column(VARCHAR(10))
    dataInicioOperacao = Column(VARCHAR(10))
    horaAtualizacao = Column(VARCHAR(10))
    numeroImoDPC = Column(VARCHAR(20))
    numeroViagem = Column(VARCHAR(20))
    portoCarregamento = Column(VARCHAR(20), index=True)
    portoDescarregamento = Column(VARCHAR(20), index=True)
    quantidadeConhecimento = Column(VARCHAR(5))
    tipoTrafego = Column(VARCHAR(20), index=True)
    create_date = Column(TIMESTAMP, index=True,
                         server_default=func.current_timestamp())
    last_modified = Column(DateTime, index=True,
                           onupdate=func.current_timestamp())
    dataInicioOperacaoDate = Column(DateTime, index=True)
    # listavazios = relationship("ConteinerVazio", cascade="delete, delete-orphan")
    # listaconhecimentos = relationship("Conhecimento", cascade="delete, delete-orphan")


"""
    escalas = relationship("ManifestoEscala",
                           back_populates='manifesto',
                           cascade="delete, delete-orphan")


class ManifestoEscala(Base):
    __tablename__ = 'manifestosescalasresumo'
    escala_id = Column(BIGINT, ForeignKey('escalasresumo.ID'), primary_key=True)
    manifesto_id = Column(BIGINT, ForeignKey('manifestosresumo.ID'), primary_key=True)
    escala = relationship("Escala", back_populates='escalas',
                           cascade="delete, delete-orphan")
    manifesto = relationship("Manifesto", back_populates='manifestos',
                           cascade="delete, delete-orphan")

"""


class Conhecimento(Base):
    __tablename__ = 'conhecimentosresumo'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    numeroCEMaster = Column(VARCHAR(15), index=True)
    numeroCEmercante = Column(VARCHAR(15), unique=True)
    codigoEmpresaNavegacao = Column(VARCHAR(20), index=True)
    codigoTerminalCarregamento = Column(VARCHAR(20), index=True)
    consignatario = Column(VARCHAR(200))
    cubagem = Column(VARCHAR(20))
    dataAtualizacao = Column(VARCHAR(10))
    dataEmissao = Column(VARCHAR(20))
    descricao = Column(VARCHAR(800))
    embarcador = Column(VARCHAR(800))
    horaAtualizacao = Column(VARCHAR(8))
    indicadorShipsConvenience = Column(VARCHAR(10))
    manifestoCE = Column(VARCHAR(15), index=True)
    paisDestinoFinalMercante = Column(VARCHAR(10), index=True)
    portoDestFinal = Column(VARCHAR(10), index=True)
    portoOrigemCarga = Column(VARCHAR(10), index=True)
    tipoBLConhecimento = Column(VARCHAR(10), index=True)
    tipoTrafego = Column(VARCHAR(10), index=True)
    create_date = Column(TIMESTAMP, index=True,
                         server_default=func.current_timestamp())
    last_modified = Column(DateTime, onupdate=func.current_timestamp())
    # listaconteineres = relationship("Item", cascade="delete, delete-orphan")
    # listancm = relationship("NCMItem", cascade="delete, delete-orphan")


class Item(Base):  # Conteiner Cheio
    __tablename__ = 'itensresumo'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    # TODO: Confirmar que chave é esta
    numeroCEmercante = Column(VARCHAR(15))
    # ForeignKey('conhecimentosresumo.numeroCEmercante'))
    numeroSequencialItemCarga = Column(VARCHAR(10), index=True)
    codigoConteiner = Column(VARCHAR(11), index=True)
    NCM = Column(VARCHAR(10), index=True)
    codigoTipoEmbalagem = Column(VARCHAR(20))
    contraMarca = Column(VARCHAR(100))
    cubagemM3 = Column(VARCHAR(20))
    dataAtualizacao = Column(VARCHAR(10))
    horaAtualizacao = Column(VARCHAR(10))
    indicadorUsoParcial = Column(VARCHAR(10))
    isoCode = Column(VARCHAR(20))
    lacre = Column(VARCHAR(50))
    marca = Column(VARCHAR(100))
    pesoBruto = Column(VARCHAR(20))
    qtdeItens = Column(VARCHAR(10))
    tara = Column(VARCHAR(20))
    tipoItemCarga = Column(VARCHAR(20), index=True)
    create_date = Column(TIMESTAMP, index=True,
                         server_default=func.current_timestamp())
    last_modified = Column(DateTime, index=True,
                           onupdate=func.current_timestamp())


Index('ix_itens_chave', Item.numeroCEmercante, Item.numeroSequencialItemCarga)


class NCMItem(Base):
    __tablename__ = 'ncmitemresumo'
    ID = Column(BIGINT,
                primary_key=True, autoincrement=True)
    numeroCEMercante = Column(CHAR(15))
    #                 ForeignKey('conhecimentosresumo.numeroCEmercante'))
    numeroSequencialItemCarga = Column(CHAR(5), index=True)
    codigoConteiner = Column(CHAR(11), index=True)
    identificacaoNCM = Column(VARCHAR(10), index=True)
    codigoTipoEmbalagem = Column(VARCHAR(10))
    descritivo = Column(VARCHAR(100))
    itemEmbaladoMadeira = Column(VARCHAR(10))
    marcaMercadoria = Column(VARCHAR(100))
    numeroIdentificacao = Column(VARCHAR(10))
    qtdeVolumes = Column(VARCHAR(10))
    create_date = Column(TIMESTAMP, index=True,
                         server_default=func.current_timestamp())
    last_modified = Column(DateTime, index=True,
                           onupdate=func.current_timestamp())


Index('ix_ncmitem_chave', NCMItem.numeroCEMercante,
      NCMItem.codigoConteiner, NCMItem.numeroSequencialItemCarga,
      NCMItem.identificacaoNCM)


class ConteinerVazio(Base):
    __tablename__ = 'conteinervazioresumo'
    ID = Column(BIGINT,
                primary_key=True, autoincrement=True)
    manifesto = Column(CHAR(15))
    # ForeignKey('manifestosresumo.numero'))
    idConteinerVazio = Column(CHAR(11), index=True)
    isoConteinerVazio = Column(VARCHAR(10))
    taraConteinerVazio = Column(VARCHAR(10))
    create_date = Column(TIMESTAMP, index=True,
                         server_default=func.current_timestamp())
    last_modified = Column(DateTime, index=True,
                           onupdate=func.current_timestamp())


class EscalaManifesto(Base):
    __tablename__ = 'escalamanifestoresumo'
    ID = Column(BIGINT,
                primary_key=True, autoincrement=True)
    manifesto = Column(CHAR(15))
    escala = Column(CHAR(15))
    create_date = Column(TIMESTAMP, index=True,
                         server_default=func.current_timestamp())
    last_modified = Column(DateTime, index=True,
                           onupdate=func.current_timestamp())


Index('ix_conteineresVazios', ConteinerVazio.manifesto,
      ConteinerVazio.idConteinerVazio)


class ControleResumo(Base):
    """Guarda, para classe e operação, último ID processado."""
    __tablename__ = 'controleresumo'
    ID = Column(Integer,
                primary_key=True, autoincrement=True)
    nomeclasse = Column(CHAR(50))
    tipomovimento = Column(CHAR(1))
    maxid = Column(BIGINT)

    @classmethod
    def get_(cls, session, nomeclasse, tipomovimento):
        query = session.query(ControleResumo).filter(
            and_(ControleResumo.nomeclasse == nomeclasse,
                 ControleResumo.tipomovimento == tipomovimento)
        )
        controle = query.one_or_none()
        if controle is None:
            controle = ControleResumo(nomeclasse=nomeclasse,
                                      tipomovimento=tipomovimento,
                                      maxid=0)
        return controle


class RiscoAtivo(Base):
    __tablename__ = 'riscosativos'
    ID = Column(BIGINT,
                primary_key=True, autoincrement=True)
    user_name = Column(CHAR(15), index=True)
    campo = Column(CHAR(40), index=True)
    valor = Column(CHAR(40), index=True)
    motivo = Column(CHAR(40), index=True)


if __name__ == '__main__':
    confirma = input('Recriar todas as tabelas ** APAGA TODOS OS DADOS ** (S/N)')
    if confirma != 'S':
        exit('Saindo... (só recrio se digitar "S", digitou %s)' % confirma)
    print('Recriando tabelas, aguarde...')
    # engine = create_engine('mysql+pymysql://ivan@localhost:3306/mercante')
    banco = input('Escolha a opção de Banco (1 - MySQL/ 2 - Sqlite)')
    if banco == '1':
        engine = create_engine(SQL_URI)
        metadata.drop_all(engine)
        metadata.create_all(engine)
    if banco == '2':
        engine = create_engine('sqlite:///teste.db')
        metadata.drop_all(engine)
        metadata.create_all(engine)
