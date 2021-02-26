"""Funções para leitura e tratamento arquivos XML criados pelos escâneres."""
import sys
from datetime import datetime, timedelta

import chardet
# from ajna_commons.conf import ENCODE
from ajna_commons.flask.log import logger
from ajna_commons.utils.sanitiza import sanitizar, unicode_sanitizar
from gridfs import GridFS

if sys.platform == 'win32':  # pragma: no cover
    import lxml.etree as ET
else:
    # defusedxml under 5.0 pode ser incompatível com python3.6
    # if int(sys.version_info[1]) > 5:
    #    import lxml.etree as ET
    # else:
    import defusedxml.ElementTree as ET

FALTANTES = {'metadata.xml.date': None,
             'metadata.contentType': 'image/jpeg'
             }

# TODO: inserir campos dos novos XML, começando por campos de container e
# data adicionados ontem ao AVATAR.
FIELDS = ('TruckId', 'Site', 'Date', 'PlateNumber', 'IsContainerEmpty',
          'Login', 'Workstation', 'UpdateDateTime', 'ClearImgCount',
          'UpdateCount', 'LastStateDateTime', 'Custom1', 'Custom2')

TAGS_NUMERO = ['ContainerId', 'container_no', 'ContainerID1', 'Container1']
TAGS_DATA = ['Date', 'SCANTIME', 'ScanTime', 'createdate']

# Abaixo um dicionário para traduzir tags de XMLs em outro padrão
# para um nome comum - NOME DIFERENTE: NOME COMUM
XML_DEPARA = {
    'container_no': 'ContainerId',
    'ContainerID1': 'ContainerId',
    'Container1': 'ContainerId',
    'SCANTIME': 'Date',
    'ScanTime': 'Date',
    'createdate': 'Date',
    'OPERATORID': 'Login',
    'TYPE': 'Custom2'
}

# Fields to be converted to ISODate
DATE_FIELDS = ('Date', 'UpdateDateTime', 'LastStateDateTime',
               'SCANTIME', 'ScanTime')

DATA = 'metadata.xml.date'

CHAVES_XML = [
    'metadata.xml.container',
    'metadata.xml.alerta',
]
for field in FIELDS:
    CHAVES_XML.append('metadata.xml.' + field.lower())


def create_indexes(db):
    """Utilitário. Cria índices relacionados à integração."""
    db['fs.files'].create_index('metadata.xml.container')
    db['fs.files'].create_index('metadata.xml.alerta')
    for field in FIELDS:
        if field == 'Login':
            # TODO: Delete and see why:
            # pymongo.errors.OperationFailure: Btree::insert: key too large
            #  to index, failing test.fs.files.$metadata.xml.login_1 31720
            # { : "rafael gonꟃ愀氀瘀攀猀㰀⼀䰀漀最椀渀㸀㰀圀漀爀欀猀琀愀琀椀漀渀㸀䐀䄀圀
            #  ㄀㰀⼀圀漀爀欀猀琀愀琀椀漀渀㸀㰀�..." }
            continue
        db['fs.files'].create_index('metadata.xml.' + field.lower())


def xml_todict(xml) -> dict:
    """Recebe um XML string stream, retorna um dict com campos do XML.

    Lê o XML que acompanha as imagens no formato do Escâner da ALFSTS,
    retorna dict(s) com os dados considerados mais importantes e resumos
    (ex.: número do(s) cc(s), datas, etc) - configurar na var. fields

    Args:
        xml: string de texto na memória com o conteúdo do arquivo XML

    Returns:
        Dicionário com as tags selecionadas. A tag container é uma lista
        (imagens de cc de 20' podem conter dois contêineres escaneados)

    """
    result = {}
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as err:  # pragma: no cover
        print(err)
        print('*****************XML**********************')
        print(xml)
        print('*****************XML END')
        xml = unicode_sanitizar(xml)
        # logger.error(err, exc_info=True)
        try:
            root = ET.fromstring(xml)
        except ET.ParseError as err:
            print('*****************XML**********************')
            print(xml)
            print('*****************XML END')
            print(str(err))
            return result
    alerta = \
        (xml.find('>AL<') != -1) or \
        (xml.find('>al<') != -1) or \
        (xml.find('>ALER') != -1) or \
        (xml.find('>aler') != -1) or \
        (xml.find('>Aler') != -1)
    result['alerta'] = alerta

    allfields = [*FIELDS, *XML_DEPARA.keys()]
    for field in allfields:
        for tag in root.iter(field):
            text = ''
            if tag.text:
                text = sanitizar(tag.text)
            if field in DATE_FIELDS:
                for char in 'tT_':
                    text = text.replace(char, ' ')
                try:
                    text = text.replace(',', '.')
                    parse_str = '%Y-%m-%d %H:%M:%S.%Z'
                    text = datetime.strptime(text, parse_str)
                except ValueError as err:
                    logger.info('text: %s parser: %s err: %s' %
                                (text, parse_str, err))
                    try:
                        text = text.split('.')[0]
                        text = text.replace('Z', '')
                        text = text.replace('z', '')
                        parse_str = '%Y-%m-%d %H:%M:%S'
                        text = datetime.strptime(text, parse_str)
                    except ValueError as err:
                        logger.info('text: %s parser: %s err: %s' %
                                    (text, parse_str, err))
            akey = XML_DEPARA.get(field)
            if akey is None:
                akey = field
            result[akey.lower()] = text
    lista_conteineres = []
    for tag in root.iter('attribute'):
        if tag.attrib.get('name') in TAGS_NUMERO:
            numero = tag.text
            lista_conteineres.append(numero.casefold())
    for atag in TAGS_NUMERO:
        for tag in root.iter(atag):
            numero = tag.text
            if numero is not None:
                numero = numero.replace('?', 'X')
                lista_conteineres.append(numero.casefold())
    result['container'] = lista_conteineres
    return result


def dados_xml_grava_fsfiles(db, batch_size=5000,
                            data_inicio=datetime(1900, 1, 1),
                            update=True,
                            recinto=None):
    """Busca por registros no GridFS sem info do XML.

    Busca por registros no fs.files (GridFS - imagens) que não tenham metadata
    importada do arquivo XML. Itera estes registros, consultando a
    xml_todict para ver se retorna informações do XML. Encontrando
    estas informações, grava no campo metadata.xml do fs.files

    Args:
        db: connection to mongo with database setted

        batch_size: número de registros a consultar/atualizar por chamada

        data_inicio: filtra por data de escaneamento maior que a informada

        update: Caso seja setado como False, apenas faz consulta, sem
            atualizar metadata da collection fs.files

        recinto: Código do recinto (metadata.recinto)

    Returns:
        Número de registros encontrados

    """
    filtro = {'metadata.xml': None,
              'metadata.dataescaneamento': {'$gt': data_inicio},
              'metadata.contentType': 'image/jpeg'
              }
    if recinto:
        filtro.update({'metadata.recinto': recinto})
    file_cursor = db['fs.files'].find(filtro).limit(batch_size)
    fs = GridFS(db)
    total = db['fs.files'].count_documents(filtro)
    acum = 0
    for linha in file_cursor:
        numero = linha.get('metadata').get('numeroinformado')
        data = linha.get('uploadDate')
        filename = linha.get('filename')
        print(numero, data, filename)
        # Primeiro procura XML com o mesmo número de container e Upload próximo
        if numero and data:
            data_upload_antes = data - timedelta(hours=1)
            data_upload_depois = data + timedelta(hours=1)
            xml_document = db['fs.files'].find_one(
                {'metadata.numeroinformado': numero,
                 'uploadDate': {'$gt': data_upload_antes,
                                '$lt': data_upload_depois},
                 'metadata.contentType': 'text/xml'
                 })
        else:
            xml_document = None
        # Depois, caso não encontre, tenta por filename
        if not xml_document:
            if not filename:
                continue
            if filename[:5] == 'XRAY-':
                posi = filename.find('--Array')
                xml_filename = filename[5:posi]
            else:
                posi = filename.find('_icon')
                if posi != -1:
                    xml_filename = filename[:posi]
                else:
                    posi = filename.find('stamp.jpg')
                    if posi != -1:
                        xml_filename = filename[:-11]
                    else:
                        xml_filename = filename[:-4]
            final_filename = xml_filename + '.xml'
            xml_document = db['fs.files'].find_one(
                {'filename': final_filename})
            if not xml_document:
                final_filename = xml_filename + '.XML'
                xml_document = db['fs.files'].find_one(
                    {'filename': final_filename})
            print('xml alternativo:', final_filename)
        if not xml_document:
            logger.info('Numero %s filename %s não encontrado!!!' %
                        (numero, filename))
            continue
        file_id = xml_document.get('_id')
        if not fs.exists(file_id):
            continue
        raw = fs.get(file_id).read()
        encode = chardet.detect(raw)
        # print(encode)
        # print(raw)
        encoding = [encode['encoding'],
                    'latin1',
                    'utf8',
                    'ascii',
                    'windows-1250',
                    'windows-1252']
        dados_xml = {}
        for e in encoding:
            try:
                xmlo = raw.decode(e)
                try:
                    ET.fromstring(xml)
                    dados_xml = xml_todict(xmlo)
                    break
                except Exception:
                    # TODO: see why sometimes a weird character
                    # appears in front of content
                    posi = xmlo.find('<DataForm>')
                    # print('POSI', posi)
                    if posi == -1:
                        xml = xmlo[2:]
                    else:
                        xml = xmlo[posi:]
                    ET.fromstring(xml)
                    dados_xml = xml_todict(xml)
                    break
            except Exception as err:
                print('Erro de encoding', e, err)
        if dados_xml != {}:
            print(dados_xml)
            if update:
                db['fs.files'].update_one(
                    {'_id': linha['_id']},
                    {'$set': {'metadata.xml': dados_xml}}
                )
            acum += 1
    logger.info(' '.join([
        'Resultado dados_xml_grava_fsfiles',
        'Pesquisados', str(total),
        'Encontrados', str(acum)
    ]))
    return acum


if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    print('Criando índices para XML')
    create_indexes(db)
