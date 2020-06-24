"""Classes e funções para integrar XML do Mercante em tabelas."""
from xml.etree.ElementTree import Element
from ajna_commons.flask.log import logger

# TODO: Tratar lista de Escalas (manifestosCarga)
# TODO: Tratar lista de lacres  (itensCarga)
# TODO: Ver se precisa e como guardar informações adicionais
#  de Embarcador e Consignatario

MERCANTE_DIR = 'mercante'


class ParseXML:

    def _campos(self):
        return [campo for campo in dir(self) if campo[0] != '_']

    def _parse_node(self, node):
        for campo in self._campos():
            # print(campo)
            alvo = node.find(campo)
            if alvo is not None:
                # print(alvo.text, alvo.tag)
                try:
                    destino = getattr(self, campo)
                except AttributeError as err:
                    logger.error(err)
                    continue
                if isinstance(destino, str):
                    setattr(self, campo, alvo.text)
                else:
                    setattr(self, campo, alvo)
            else:
                pass
                # logger.info('Não encontrou %s' % campo)

    def _to_dict(self):
        result = {}
        for campo in self._campos():
            valor = getattr(self, campo)
            if isinstance(valor, ParseXML):
                valor = str(valor)
            result[campo] = valor
        return result


class Embarcador(ParseXML):
    def __init__(self):
        self.cnpjShipper: str = ''
        self.idEmbarcador: str = ''

    def __repr__(self):
        if self.cnpjShipper:
            return self.cnpjShipper
        return self.idEmbarcador


class Consignatario(ParseXML):
    def __init__(self):
        self.tipoConsignatario: str = ''
        self.cnpjConsignatario: str = ''
        self.nomeConsignatarioEstrangeiro: str = ''
        self.dadosComplementaresConsignatario: str = ''

    def __repr__(self):
        if (self.cnpjConsignatario is not None and
                self.cnpjConsignatario and
                self.cnpjConsignatario != ''):
            return self.cnpjConsignatario
        return self.nomeConsignatarioEstrangeiro


class ManifestoCE(ParseXML):
    def __init__(self):
        self.numeroManifesto: str = ''

    def __repr__(self):
        return self.numeroManifesto


class Conhecimento(ParseXML):
    def __init__(self):
        self.tipoMovimento: str = ''
        self.dataAtualizacao: str = ''
        self.horaAtualizacao: str = ''
        self.tipoTrafego: str = ''
        self.tipoBLConhecimento: str = ''
        self.numeroCEMaster: str = ''
        self.dataEmissao: str = ''
        self.cubagem: str = ''
        self.portoDestFinal: str = ''
        self.portoOrigemCarga: str = ''
        self.descricao: str = ''
        self.numeroCEmercante: str = ''
        self._embarcador: Embarcador = None
        self._consignatario: Consignatario = None
        self.codigoTerminalCarregamento: str = ''
        self.paisDestinoFinalMercante: str = ''
        self.indicadorShipsConvenience: str = ''
        self._manifestoCE: ManifestoCE = None
        self.codigoEmpresaNavegacao: str = ''

    @property
    def embarcador(self) -> str:
        return self._embarcador

    @embarcador.setter
    def embarcador(self, node: Element):
        self._embarcador = Embarcador()
        self._embarcador._parse_node(node)

    @property
    def consignatario(self) -> str:
        return self._consignatario

    @property
    def dadoscomplementaresconsignatario(self) -> str:
        return self._consignatario.dadosComplementaresConsignatario

    @consignatario.setter
    def consignatario(self, node: Element):
        self._consignatario = Consignatario()
        self._consignatario._parse_node(node)

    @property
    def manifestoCE(self) -> str:
        return self._manifestoCE

    @manifestoCE.setter
    def manifestoCE(self, node: Element):
        self._manifestoCE = ManifestoCE()
        self._manifestoCE._parse_node(node)


class Manifesto(ParseXML):
    def __init__(self):
        self.tipoMovimento: str = ''
        self.numero: str = ''
        self.codAgenciaInformante: str = ''
        self.codigoEmpresaNavegacao: str = ''
        self.numeroImoDPC: str = ''
        self.dataEncerramento: str = ''
        self.dataInicioOperacao: str = ''
        self.portoCarregamento: str = ''
        self.portoDescarregamento: str = ''
        self.tipoTrafego: str = ''
        self.numeroViagem: str = ''
        self.quantidadeConhecimento: str = ''
        self.dataAtualizacao: str = ''
        self.horaAtualizacao: str = ''
        self.codigoTerminalCarregamento: str = ''
        self.codigoTerminalDescarregamento: str = ''


class Lacre(ParseXML):
    def __init__(self):
        self.identificacaoLacre: str = ''

    def __repr__(self):
        return self.identificacaoLacre


class NCM(ParseXML):
    def __init__(self):
        self.identificacaoNCM: str = ''

    def __repr__(self):
        return self.identificacaoNCM


class ItemCarga(ParseXML):
    def __init__(self):
        self.tipoMovimento: str = ''
        self.dataAtualizacao: str = ''
        self.horaAtualizacao: str = ''
        self.numeroCEmercante: str = ''
        self.tipoItemCarga: str = ''
        self.numeroSequencialItemCarga: str = ''
        self.cubagemM3: str = ''
        self._NCM: Element = None
        # Campos do conteiner
        self.pesoBruto: str = ''
        self.codigoConteiner: str = ''
        self.isoCode: str = ''
        self.tara: str = ''
        self.indicadorUsoParcial: str = ''
        self._lacre: Element = None
        # Campos da carga solta
        self.codigoTipoEmbalagem: str = ''
        self.qtdeItens: str = ''
        self.marca: str = ''
        self.contraMarca: str = ''

    @property
    def lacre(self) -> str:
        return self._lacre

    @lacre.setter
    def lacre(self, node: Element):
        self._lacre = Lacre()
        self._lacre._parse_node(node)

    @property
    def NCM(self) -> str:
        return self._NCM

    @NCM.setter
    def NCM(self, node: Element):
        self._NCM = NCM()
        self._NCM._parse_node(node)


class ExclusaoEscala(ParseXML):
    def __init__(self):
        self.tipoMovimento: str = ''
        self.numeroEscalaMercante: str = ''
        self.dataExclusao: str = ''
        self.horaExclusao: str = ''

class EscalaXML(ParseXML):
    def __init__(self):
        self.tipoMovimento: str = ''
        self.numeroDaEscala: str = ''
        self.dataAtualizacao: str = ''
        self.horaAtualizacao: str = ''
        self.agenciaDeNavegacao: str = ''
        self.portoDaEscala: str = ''
        self.embarcacao: str = ''
        self.numeroViagemDoArmador: str = ''
        self.tipoOperacaoPrevista: str = ''
        self.bandeiraDaEmbarcacao: str = ''
        self.empresaDeNavegacao: str = ''
        self.dataPrevistaAtracacao: str = ''
        self.dataPrevistaDesatracacao: str = ''
        self.dataEfetivaPrimeiraAtracacao: str = ''
        self.dataEfetivaPrimeiraDesatracacao: str = ''



# Entidades que ficam em listas dentro de outra Entidade no XML
# Tratamento tem que se diferente na varredura de nodes do XML

class ConteinerVazio(ParseXML):
    _tag = 'conteinersVazio'

    def __init__(self, manifesto: Manifesto):
        self.tipoMovimento = manifesto.tipoMovimento
        self.manifesto = manifesto.numero
        self.idConteinerVazio: str = ''
        self.isoConteinerVazio: str = ''
        self.taraConteinerVazio: str = ''


class NCMItemCarga(ParseXML):
    _tag = 'NCM'

    def __init__(self, itemcarga: ItemCarga):
        self.tipoMovimento = itemcarga.tipoMovimento
        self.numeroCEMercante = itemcarga.numeroCEmercante
        self.numeroSequencialItemCarga = itemcarga.numeroSequencialItemCarga
        self.codigoConteiner = itemcarga.codigoConteiner
        self.identificacaoNCM: str = ''
        self.numeroIdentificacao: str = ''
        self.marcaMercadoria: str = ''
        self.qtdeVolumes: str = ''
        self.codigoTipoEmbalagem: str = ''
        self.itemEmbaladoMadeira: str = ''
        self.descritivo: str = ''

    def __repr__(self):
        return self.identificacaoNCM

class EscalaManifesto(ParseXML):
    _tag = 'numeroEscalaMercante'

    def __init__(self, manifesto: Manifesto):
        self.tipoMovimento = manifesto.tipoMovimento
        self.manifesto = manifesto.numero
        self.escala: str = ''


    def _parse_node(self, node):
        """Esta classe não está em subnode. A lista é o próprio node pai."""
        self.escala = node.text


classes = {'conhecimentosEmbarque': Conhecimento,
           'manifestosCarga': Manifesto,
           'itensCarga': ItemCarga,
           'exclusoesEscala': ExclusaoEscala,
           'escala': EscalaXML}


classes_em_lista = {'manifestosCarga': [ConteinerVazio, EscalaManifesto],
                    'itensCarga': [NCMItemCarga]}
