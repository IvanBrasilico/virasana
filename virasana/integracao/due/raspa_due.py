import json
from json import JSONDecodeError

import requests
import time
from datetime import datetime, time
from selenium import webdriver

GECKO_PATH = "C:\\Users\\25052288840\\Downloads\\chromedriver.exe"

SUITE_URL = "https://www.suiterfb.receita.fazenda"
POS_ACD_URL = "https://portalunico.suiterfb.receita.fazenda/cct/api/deposito-carga/consultar-estoque-pos-acd?numeroConteiner="
DUE_ITEMS_URL = "https://portalunico.suiterfb.receita.fazenda/due/api/due/obterDueComItensResumidos?due="

VIRASANA_URL = "https://ajna.labin.rf08.srf/virasana/"


def raspa_containers_vazios_sem_due(
    datainicial: str, datafinal: str,
    virasana_url: str = VIRASANA_URL) -> list:
    print('Conectando virasana')
    params = {'query':
                  {'metadata.dataescaneamento': {'$gte': datainicial, '$lte': datafinal},
                   'metadata.contentType': 'image/jpeg',
                   'metadata.due': {'$exists': False},
                   'metadata.carga.vazio': True
                   },
              'projection':
                  {'metadata.numeroinformado': 1,
                   'metadata.dataescaneamento': 1}
              }
    r = requests.post(virasana_url + "grid_data", json=params, verify=False)
    try:
        lista_containeres = list(r.json())
        conteineres_ids = {linha['metadata']['numeroinformado']: linha['_id']
                       for linha in lista_containeres}
        print('%s Contêineres recuperados.' % len(lista_containeres))
    except JSONDecodeError as err:
        print(r.text)
        # logger.error(err, exc_info=True)
        # logger.error(r.text)
        return [err, r.text]
    return conteineres_ids

def raspa_containers_sem_due(
        datainicial: str, datafinal: str,
        tipomanifesto: str = None,
        virasana_url: str = VIRASANA_URL) -> list:
    print('Conectando virasana')
    params = {'query':
                  {'metadata.dataescaneamento': {'$gte': datainicial, '$lte': datafinal},
                   'metadata.contentType': 'image/jpeg',
                   'metadata.due': {'$exists': False},
                   'metadata.carga.manifesto.tipomanifesto': tipomanifesto
                   },
              'projection':
                  {'metadata.numeroinformado': 1,
                   'metadata.dataescaneamento': 1}
              }
    r = requests.post(virasana_url + "grid_data", json=params, verify=False)
    lista_containeres = list(r.json())
    conteineres_ids = {linha['metadata']['numeroinformado']: linha['_id']
                       for linha in lista_containeres}
    print('%s Contêineres recuperados.' % len(lista_containeres))
    return conteineres_ids


def auth_suite_rfb(driver, portal_url=SUITE_URL):
    driver.get(portal_url)
    # time.sleep(1)
    LOGON_REDIRECT = "/camweb/grupo?sis=SUITERFB&url=www.suiterfb.receita.fazenda/api/private/redirect"
    driver.get(portal_url + LOGON_REDIRECT)
    # time.sleep(1)
    form = driver.find_element_by_name("loginCertForm")
    form.submit()
    # time.sleep(2)


def limpa_pre(page_source: str):
    inicio = page_source.find('">{"')
    fim = page_source.find('</pre>')
    return page_source[inicio + 2:fim]


def get_dues_json_pos_acd(page_source: str):
    result = []
    try:
        json_pos_acd = json.loads(limpa_pre(page_source))
        lista_due = json_pos_acd['lista']
        for item in lista_due:
            result.append(item['numeroDUE'])
    except JSONDecodeError:
        pass
    return result


def get_dues_pos_acd(driver, conteineres, pos_acd_url=POS_ACD_URL):
    conteineres_listadue = {}
    for conteiner in conteineres:
        driver.get(pos_acd_url + conteiner)
        pos_acd = driver.page_source
        conteineres_listadue[conteiner] = get_dues_json_pos_acd(pos_acd)
    return conteineres_listadue


def get_dues_json_due(page_source: str):
    try:
        json_due = json.loads(limpa_pre(page_source))
        return json_due
    except JSONDecodeError:
        return None


def detalha_dues(driver, conteineres_listadue, due_items_url=DUE_ITEMS_URL):
    due_detalhe = {}
    for conteiner, dues in conteineres_listadue.items():
        for due in dues:
            if due and isinstance(due, str) and due_detalhe.get(due) is None:
                driver.get(due_items_url + due)
                due_page = driver.page_source
                detalhe = get_dues_json_due(due_page)
                if detalhe:
                    due_detalhe[due] = detalhe
    return due_detalhe


def monta_due_ajna(due):
    def get_dados_recinto(recinto_dict):
        try:
            result = {}
            result['codigo'] = recinto_dict.get('codigo')
            depositario = recinto_dict.get('depositario')
            if depositario:
                result['depositario'] = depositario.get('depositario')
                result['nome'] = depositario.get('nome')
                result['descricao'] = depositario.get('descricao')
                unidade = depositario.get('unidadeLocalRFB')
                if unidade:
                    unidadeLocalRFB = depositario.get('codigo')
        except AttributeError:
            return None

        return result

    keys = ['canal', 'chaveAcesso', 'dataProcessamentoTa', 'descricaoTipoItemDue',
            'exportadorEstrangeiro', 'formaExportacao', 'indicadorOEA', 'informacoesComplementares']
    pacote = {}
    for key in keys:
        item = due.get(key)
        if item is not None:
            pacote[key] = item

    declarante = due.get('niDeclarante')
    if declarante:
        pacote['Declarante'] = declarante.get('numero')
        pacote['Nome Declarante'] = declarante.get('nome')

    destino = due.get('paisImportador')
    if destino:
        pacote['PaisImportador'] = destino.get('nome')

    lista_items = due.get('listaInfoItemDue')
    itensDue = []
    for item in lista_items:
        itemDue = {}
        itemDue['descricaoMercadoria'] = item.get('descricaoMercadoria')
        itemDue['exportadorEstrangeiro'] = item.get('exportadorEstrangeiro')
        ncm = item.get('ncm')
        if ncm:
            itemDue['ncm'] = ncm.get('codigo')
        exportador = item.get('niExportador')
        if exportador:
            itemDue['Exportador'] = exportador.get('numero')
            itemDue['NomeExportador'] = exportador.get('nome')
        itensDue.append(itemDue)
    pacote['itens'] = itensDue

    for recinto_tipo in ['recintoAduaneiroDespacho', 'recintoAduaneiroEmbarque']:
        pacote[recinto_tipo] = get_dados_recinto(due.get(recinto_tipo))
    ruc = due.get('ruc')
    if ruc:
        pacote['ruc'] = ruc.get('numero')
    pacote['listaHistorico'] = due.get('listaHistorico')

    return pacote


if __name__ == '__main__':
    driver = webdriver.Chrome(GECKO_PATH)
    diaapesquisar = datetime(2019, 9, 2)
    datainicial = datetime.strftime(datetime.combine(diaapesquisar, time.min), '%Y-%m-%d  %H:%M:%S')
    datafinal = datetime.strftime(datetime.combine(diaapesquisar, time.max), '%Y-%m-%d %H:%M:%S')
    print(datainicial, datafinal)
    for tipo_manifesto in (None, 'lce'):
        conteineres_ids = raspa_containers_sem_due(
            datainicial, datafinal, tipo_manifesto)
        try:
            auth_suite_rfb(driver)
            conteineres_listadue = get_dues_pos_acd(driver,
                                                    list(conteineres_ids.keys()))
            due_detalhe = detalha_dues(driver, conteineres_listadue)
        finally:
            driver.close()
        pacote_carregamento = {}
        for conteiner, numeros_dues in conteineres_listadue.items():
            _id = conteineres_ids[conteiner]
            lista_dues = []
            for due in numeros_dues:
                if due is not None:
                    pacote = monta_due_ajna(due_detalhe[due])
                    lista_dues.append({'numero': due, **pacote})
            if numeros_dues and len(numeros_dues) > 0:
                pacote_carregamento[_id] = lista_dues
        r = requests.post(VIRASANA_URL + "dues/update",
                          json=pacote_carregamento, verify=False)
        print(r.status_code)
        print(r.text)
