from marshmallow_sqlalchemy import ModelSchema

from ajna_commons.utils.sanitiza import sanitizar
from virasana.integracao.mercante.mercantealchemy import Manifesto, ConteinerVazio, \
    Conhecimento, Item, NCMItem, Enumerado


class BaseSchema(ModelSchema):
    excluded_keys = ['create_date', 'last_modified',
                     'dataInicioOperacaoDate']

    def dump(self, objeto):
        original = super().dump(objeto)
        result = {}
        for k, v in self.carga_fields.items():
            result[k] = sanitizar(original.get(v))
        return result


class ManifestoSchema(BaseSchema):
    carga_fields = \
        {'manifesto': 'numero',
         'codigoportocarregamento': 'portoCarregamento',
         'codigoportodescarregamento': 'portoDescarregamento',
         'codigoterminalcarregamento': 'codigoTerminalCarregamento',
         'codigoempresanavegacao': 'codigoEmpresaNavegacao',
         'codigoterminaldescarregamento': 'codigoTerminalDescarregamento',
         'codigoagencianavegacao': 'codAgenciaInformante'}

    class Meta:
        model = Manifesto

    def dump(self, manifesto, session):
        result = super().dump(manifesto)
        result['tipomanifesto'] = \
            Enumerado.getTipoTrafegoManifesto(session, manifesto.tipoTrafego)
        return result


class ConteinerVazioSchema(BaseSchema):
    carga_fields = \
        {'container': 'idConteinerVazio',
         'tara(kg)': 'taraConteinerVazio'}

    class Meta:
        model = ConteinerVazio


class ItemSchema(BaseSchema):
    carga_fields = \
        {'container': 'codigoConteiner',
         'item': 'numeroSequencialItemCarga',
         'lacre': 'lacre',
         'taracontainer': 'tara',
         'volumeitem': 'cubagemM3',
         'pesobrutoitem': 'pesoBruto',
         'indicadorusoparcial': 'indicadorUsoParcial',
         # 'tipoItemCarga':
         }

    class Meta:
        model = Item


class NCMItemSchema(BaseSchema):
    carga_fields = \
        {'ncm': 'identificacaoNCM',
         'item': 'numeroSequencialItemCarga',
         'descricao': 'descritivo',
         'embalagem': 'codigoTipoEmbalagem',
         'madeira': 'itemEmbaladoMadeira',
         'marca': 'marcaMercadoria',
         'numeroidentificacao': 'numeroIdentificacao',
         'qtdevolumes': 'qtdeVolumes'}

    class Meta:
        model = NCMItem


class ConhecimentoSchema(BaseSchema):
    carga_fields = \
        {'conhecimento': 'numeroCEmercante',
         'codigoagentenavegacao': 'codigoEmpresaNavegacao',
         'cpfcnpjconsignatario': 'consignatario',
         'cubagem': 'cubagem',
         'dataemissao': 'dataEmissao',
         'descricaomercadoria': 'descricao',
         'identificacaoembarcador': 'embarcador',
         'manifesto': 'manifestoCE',
         'paisdestino': 'paisDestinoFinalMercante',
         'codigoportodestino': 'portoDestFinal',
         'codigoportoorigem': 'portoOrigemCarga'
         }

    class Meta:
        model = Conhecimento

    def dump(self, conhecimento, session):
        result = super().dump(conhecimento)
        result['tipo'] = \
            Enumerado.getTipoBLConhecimentoMercante(session,
                                                    conhecimento.tipoBLConhecimento)
        result['trafego'] = \
            Enumerado.getTipoTrafegoConhecimento(session, conhecimento.tipoTrafego)
        return result


manifesto_schema = ManifestoSchema()
conteinervazio_schema = ConteinerVazioSchema()
item_schema = ItemSchema()
ncmitem_schema = NCMItemSchema()
conhecimento_schema = ConhecimentoSchema()


def manifesto_carga(session, manifestos: list, numeroconteiner: str = None):
    dict_carga = {'vazio': True}
    dict_carga['manifesto'] = []
    dict_carga['container'] = []
    for numeromanifesto in manifestos:
        manifesto = session.query(Manifesto).filter(Manifesto.numero == numeromanifesto).one()
        dict_carga['manifesto'].append(manifesto_schema.dump(manifesto, session))
        conteineres = session.query(ConteinerVazio).filter(ConteinerVazio.manifesto == numeromanifesto).all()
        for conteiner in conteineres:
            if numeroconteiner is None:
                dict_carga['container'].append(conteinervazio_schema.dump(conteiner))
            elif conteiner.idConteinerVazio == numeroconteiner:
                dict_carga['container'].append(conteinervazio_schema.dump(conteiner))
    return dict_carga


def conhecimento_carga(session, conhecimentos: list, numeroconteiner: str = None):
    dict_carga = {'vazio': False}
    dict_carga['conhecimento'] = []
    dict_carga['ncm'] = []
    dict_carga['container'] = []
    for numeroconhecimento in conhecimentos:
        conhecimento = session.query(Conhecimento).filter(Conhecimento.numeroCEmercante == numeroconhecimento).one()
        dict_carga['conhecimento'].append(conhecimento_schema.dump(conhecimento, session))
        itens = session.query(Item).filter(Item.numeroCEmercante == numeroconhecimento).all()
        for item in itens:
            if item.codigoConteiner == numeroconteiner:
                dict_carga['container'].append(item_schema.dump(item))
        ncms = session.query(NCMItem).filter(NCMItem.numeroCEMercante == numeroconhecimento).all()
        for ncmitem in ncms:
            if ncmitem.codigoConteiner == numeroconteiner:
                dict_carga['ncm'].append(ncmitem_schema.dump(ncmitem))
    return dict_carga
