import sys

from sqlalchemy import Column, String, Integer, Numeric, Date, create_engine, CHAR, ForeignKey, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

sys.path.append('../ajna_docs/commons')
from ajna_commons.flask.conf import SQL_URI

Base = declarative_base()
metadata = Base.metadata


class Due(Base):
    __tablename__ = 'pucomex_due'

    numero_due = Column(CHAR(14), primary_key=True)
    data_criacao_due = Column(Date)
    data_registro_due = Column(Date)
    ni_declarante = Column(CHAR(14))
    cnpj_estabelecimento_exportador = Column(CHAR(14))
    telefone_contato = Column(String(100))
    email_contato = Column(String(100))
    nome_contato = Column(String(100))
    codigo_iso3_pais_importador = Column(String(100))
    nome_pais_importador = Column(String(100))
    duev_nm_tp_doc_fiscal_c = Column(String(100))
    duev_cetp_nm_c = Column(String(100))
    codigo_recinto_despacho = Column(CHAR(7))
    codigo_recinto_embarque = Column(CHAR(7))
    codigo_unidade_embarque = Column(CHAR(7))
    lista_id_conteiner = Column(String(650))  # Até 50 contêineres aproximadamente
    itens = relationship("DueItem", backref="due")

    def __repr__(self):
        return (f"<Due(numero_due={self.numero_due}, data_criacao_due={self.data_criacao_due}, "
                f"ni_declarante={self.ni_declarante})>")

    def get_lista_conteiners(self):
        return self.lista_id_conteiner.split(', ')


class DueItem(Base):
    __tablename__ = 'pucomex_due_itens'

    nr_due = Column(CHAR(14), ForeignKey('pucomex_due.numero_due'), primary_key=True)
    due_nr_item = Column(Integer, primary_key=True)
    descricao_item = Column(String(100))
    descricao_complementar_item = Column(String(100))
    nfe_nr_item = Column(Integer)
    nfe_ncm = Column(String(100))
    unidade_comercial = Column(String(100))
    qt_unidade_comercial = Column(Numeric)
    valor_total_due_itens = Column(Numeric)
    nfe_nm_importador = Column(String(100))
    pais_destino_item = Column(String(100))

    def __repr__(self):
        return (f"<DueItem(nr_due={self.nr_due}, due_nr_item={self.due_nr_item}, "
                f"descricao_item={self.descricao_item})>")


class DueConteiner(Base):
    __tablename__ = 'pucomex_due_conteiner'

    id = Column(BigInteger(), primary_key=True)
    numero_due = Column(CHAR(14), ForeignKey('pucomex_due.numero_due'), nullable=False)
    numero_conteiner = Column(String(11), index=True)

    def __repr__(self):
        return f"<DueConteiner(numero_due={self.numero_due}, numero_conteiner={self.numero_conteiner})>"

if __name__ == '__main__':
    confirma = input('Recriar todas as tabelas ** APAGA TODOS OS DADOS ** (S/N)')
    if confirma != 'S':
        exit('Saindo... (só recrio se digitar "S", digitou %s)' % confirma)
    print('Recriando tabelas, aguarde...')
    engine = create_engine(SQL_URI)
    # metadata.drop_all(engine, [metadata.tables['pucomex_due'],
    #                           metadata.tables['pucomex_due_itens'],
    metadata.create_all(engine, [metadata.tables['pucomex_due'],
                                 metadata.tables['pucomex_due_itens'],
                                 metadata.tables['pucomex_due_conteiner'],])
