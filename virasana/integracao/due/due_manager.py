'''
Este módulo implementa a lógica necessária para puxar os dados de exportação para o Ajna
'''
import sys
import warnings
from datetime import datetime, timedelta
from typing import List, Optional

import requests
import urllib3
from bson import ObjectId
from sqlalchemy import not_
from sqlalchemy.exc import SQLAlchemyError

from bhadrasana.models.laudo import Empresa
from bhadrasana.models.laudo import get_empresa
from bhadrasana.models.ovr import Recinto
from virasana.integracao.due.due_alchemy import Due, DueItem, DueConteiner

# Suppress only the InsecureRequestWarning
warnings.simplefilter('ignore', urllib3.exceptions.InsecureRequestWarning)

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')
from ajna_commons.flask.log import logger

from bhadrasana.models.apirecintos import AcessoVeiculo, InspecaoNaoInvasiva

VIRASANA_URL = "https://ajna1.rfoc.srf/virasana/"


def raspa_container(containeres: List[str], datainicio: datetime, datafim: datetime,
                    virasana_url: str = VIRASANA_URL,
                    somente_sem_due: bool = True) -> List:
    """Recebe uma lista de siglas de contêineres e pesquisa as imagens no MongoDB.

        Args:
            containeres: lista de strings de siglas de contêineres, filtra numeroinformado
            datainicio, datafim: período da pesquisa, filtra dataescaneamento
            virasana_url: URL base do Virasana
            somente_sem_due: quando True, retorna apenas imagens sem metadata.due

        Returns:
            [{_id, metadata: [numeroinformado, dataescaneamento, due]}]
        """
    datainicial = datetime.strftime(datainicio, '%Y-%m-%d %H:%M:%S')
    datafinal = datetime.strftime(datafim, '%Y-%m-%d %H:%M:%S')

    logger.debug('Conectando virasana, função raspa_container.')

    query = {
        'metadata.dataescaneamento': {'$gte': datainicial, '$lte': datafinal},
        'metadata.contentType': 'image/jpeg',
        'metadata.numeroinformado': {'$in': containeres},
    }

    if somente_sem_due:
        query["$or"] = [
            {"metadata.due": {"$exists": False}},
            {"metadata.due": None},
            {"metadata.due": ""}
        ]

    params = {
        'query': query,
        'projection': {
            'metadata.numeroinformado': 1,
            'metadata.dataescaneamento': 1,
            'metadata.due': 1,
        }
    }

    r = requests.post(virasana_url + "grid_data", json=params, verify=False)
    logger.debug(f'{r}: {r.text}')

    lista_containeres = list(r.json())

    if lista_containeres and len(lista_containeres) > 0:
        logger.debug(f'GridData: {lista_containeres[0]}')

    return lista_containeres


def get_conteineres_escaneados_sem_due(session, datainicio: datetime, datafim: datetime,
                                       codigos_recintos: list = None) -> list:
    """1 - Pega acessos veículo do período e recinto, se passado. 2 - Em seguida pega escaneamentos
    correspondentes (efetuados logo após no fluxo). 3 - Recupera os _ids das imagens de escaneamento
    adquiridas do MongoDB. Obs.: o centro da pesquisa é sempre em torno do número do contêiner.

    Args:
        session: conexão SQLAlchemy
        datainicio, datafim: periodo between
        codigoRecinto: opcional

    Returns: lista com escaneamentos sem due. Ver campos abaixo em lista_final[0]

    """
    # Passo 1 - Pega os acessos para os parâmetros de data e recinto passados.
    # Filtros: A"C"esso, direção "E"ntrada, numeroNfe não vazio, numeroConteiner não vazio,
    q = session.query(AcessoVeiculo).filter(AcessoVeiculo.operacao == 'C')
    q = q.filter(AcessoVeiculo.direcao == 'E')
    q = q.filter(AcessoVeiculo.dataHoraOcorrencia.between(datainicio, datafim))
    if codigos_recintos:
        q = q.filter(AcessoVeiculo.codigoRecinto.in_(codigos_recintos))
    q = q.filter(AcessoVeiculo.numeroConteiner.isnot(None))
    q = q.filter(AcessoVeiculo.numeroConteiner != '')
    q = q.filter(AcessoVeiculo.listaNfe != '')
    q = q.filter(
        (AcessoVeiculo.numeroDeclaracao.is_(None)) |
        (AcessoVeiculo.numeroDeclaracao == '')
    )
    lista_entradas = q.order_by(AcessoVeiculo.dataHoraOcorrencia).all()
    conteineres_exportacao_detalhes = {}
    conteineres_exportacao = set()
    for entrada in lista_entradas:
        conteineres_exportacao_detalhes[entrada.numeroConteiner] = {
            'id': entrada.id,
            'tipoDeclaracao': entrada.tipoDeclaracao,
            'numeroDeclaracao': entrada.numeroDeclaracao
        }
        conteineres_exportacao.add(entrada.numeroConteiner)
    logger.info(f'Acessos: {list(conteineres_exportacao)[:10]} {len(conteineres_exportacao)}')
    # Passo 2 - Pega Inspeções não Invasivas para a lista de contêineres extraídos dos Acessos
    # Filtra o mesmo recinto e adiciona três horas na data fim (escaneamento ocorre depois do acesso)
    datafim_escaneamento = datafim + timedelta(hours=3)
    q2 = session.query(InspecaoNaoInvasiva)
    q2 = q2.filter(InspecaoNaoInvasiva.dataHoraOcorrencia.between(datainicio, datafim_escaneamento))
    if codigos_recintos:
        q2 = q2.filter(InspecaoNaoInvasiva.codigoRecinto.in_(codigos_recintos))
    q2 = q2.filter(InspecaoNaoInvasiva.numeroConteiner.in_(conteineres_exportacao))
    lista_escaneamentos = q2.order_by(InspecaoNaoInvasiva.dataHoraOcorrencia).all()
    conteineres_escaneados_detalhes = {}
    conteineres_exportacao_escaneados = set()
    for escaneamento in lista_escaneamentos:
        conteineres_escaneados_detalhes[escaneamento.numeroConteiner] = {
            'id': escaneamento.id,
            'dataescaneamento': escaneamento.dataHoraOcorrencia
        }
        conteineres_exportacao_escaneados.add(escaneamento.numeroConteiner)
    logger.info(
        f'Escaneamentos: {list(conteineres_exportacao_escaneados)[:10]} {len(conteineres_exportacao_escaneados)}')
    # Passo 3 - Recupera o _id das imagens capturadas pelo Avatar e outros dados, para completar os dados
    griddata = raspa_container(list(conteineres_exportacao_escaneados), datainicio, datafim_escaneamento)
    lista_final = [('AcessoVeiculo', 'InspecaoNaoInvasiva',
                    'numero_conteiner', 'id_imagem', 'dataescaneamento', 'due_acesso')]
    for dados_imagem in griddata:
        _id = dados_imagem['_id']
        metadata = dados_imagem['metadata']
        numero_conteiner = metadata['numeroinformado']
        dataescaneamento = metadata['dataescaneamento']
        dados_acesso = conteineres_exportacao_detalhes[numero_conteiner]
        dados_inspecao = conteineres_escaneados_detalhes[numero_conteiner]
        due_acesso = None
        if dados_acesso.get('tipoDeclaracao') == 'DUE':
            due_acesso = dados_acesso['numeroDeclaracao']
        lista_final.append((dados_acesso['id'], dados_inspecao['id'],
                            numero_conteiner, _id, dataescaneamento, due_acesso))
    logger.info(f'Lista final: {lista_final[:2]}, {len(lista_final)}')
    return lista_final


def update_due_mongo_db(db, dues):
    for _id, due in dues.items():
        result = db.fs.files.update_one(
            {'_id': ObjectId(_id)},
            {'$set': {'metadata.due': due, 'metadata.carga.vazio': False}}
        )
        logger.debug(result)


def set_conteineres_escaneados_sem_due(db, session, df_escaneamentos_sem_due, df_dues):
    # Passo 5: Atualizar metadata do _id e AcessoVeiculo com número da DUE
    conteineres_due = dict()
    logger.info(f'set_conteineres_escaneados_sem_due: {len(df_dues)} DUEs a atualizar.')
    for index, row in df_dues.iterrows():
        conteineres = row['lista_id_conteiner']
        numero_due = row['numero_due']
        for conteiner in conteineres.split(','):
            conteineres_due[conteiner.strip()] = numero_due
    logger.info(f'set_conteineres_escaneados_sem_due: {len(conteineres_due)} contêineres a atualizar.')
    acessos_veiculos_ids = df_escaneamentos_sem_due[
        df_escaneamentos_sem_due['numero_conteiner'].isin(conteineres_due.keys())]['AcessoVeiculo'].unique()
    logger.info(f'set_conteineres_escaneados_sem_due: {len(acessos_veiculos_ids)} AcessosVeiculo para atualizar.')
    acessosveiculos = session.query(AcessoVeiculo).filter(AcessoVeiculo.id.in_(acessos_veiculos_ids)).all()
    try:
        for acessoveiculo in acessosveiculos:
            due = conteineres_due.get(acessoveiculo.numeroConteiner)
            if due is None:  # DUE-conteiner não foi encontrada pelo passo do RD
                continue
            acessoveiculo.tipoDeclaracao = 'DUE'
            acessoveiculo.numeroDeclaracao = due
            session.add(acessoveiculo)
            logger.debug(f'{acessoveiculo.numeroConteiner},{acessoveiculo.id},{due}')
        session.commit()
        logger.info(f'set_conteineres_escaneados_sem_due: {len(acessosveiculos)} AcessosVeiculo atualizados.')
    except Exception as err:
        session.rollback()
        raise err
    cont_ids_atualizados = 0
    for index, row in df_escaneamentos_sem_due.iterrows():
        _id = row['id_imagem']
        conteiner = row['numero_conteiner']
        due = conteineres_due.get(conteiner)
        if due is None:  # DUE-conteiner não foi encontrada pelo passo do RD
            continue
        # Monta dict de _id: due
        result = db.fs.files.update_one(
            {'_id': ObjectId(_id)},
            {'$set': {'metadata.due': due, 'metadata.carga.vazio': False}}
        )
        # _ids_dues[_id] = due
        logger.debug(f'{conteiner},{_id},{due}, {result}')
        cont_ids_atualizados += 1
        logger.info(f'set_conteineres_escaneados_sem_due: {cont_ids_atualizados} imagens MongoDB atualizadas.')


def get_conteineres_escaneados_com_due(session, datainicio: datetime, datafim: datetime,
                                       codigos_recintos: list = None) -> list:
    """Retorna lista com escaneamentos de contêineres que já possuem DUE informada no AcessoVeiculo.

    O centro da pesquisa é o número do contêiner, seguindo a mesma lógica de
    get_conteineres_escaneados_sem_due.

    Diferença principal:
    - filtra AcessoVeiculo.tipoDeclaracao == 'DUE'
    - exige AcessoVeiculo.numeroDeclaracao preenchido
    - retorna AcessoVeiculo.numeroDeclaracao como numero_due e due_acesso

    Returns:
        lista com cabeçalho na primeira linha.
    """

    # Passo 1 - Pega acessos veículo com DUE informada.
    q = session.query(AcessoVeiculo).filter(AcessoVeiculo.operacao == 'C')
    q = q.filter(AcessoVeiculo.direcao == 'E')
    q = q.filter(AcessoVeiculo.dataHoraOcorrencia.between(datainicio, datafim))

    if codigos_recintos:
        q = q.filter(AcessoVeiculo.codigoRecinto.in_(codigos_recintos))

    q = q.filter(AcessoVeiculo.numeroConteiner.isnot(None))
    q = q.filter(AcessoVeiculo.numeroConteiner != '')
    q = q.filter(AcessoVeiculo.listaNfe != '')

    q = q.filter(AcessoVeiculo.tipoDeclaracao == 'DUE')
    q = q.filter(AcessoVeiculo.numeroDeclaracao.isnot(None))
    q = q.filter(AcessoVeiculo.numeroDeclaracao != '')

    lista_entradas = q.order_by(AcessoVeiculo.dataHoraOcorrencia).all()

    lista_final = [(
        'AcessoVeiculo',
        'InspecaoNaoInvasiva',
        'numero_conteiner',
        'id_imagem',
        'dataescaneamento',
        'tipo_declaracao',
        'numero_due',
        'due_acesso',
        'due_mongo'
    )]

    conteineres_exportacao_detalhes = {}
    conteineres_exportacao = set()

    for entrada in lista_entradas:
        conteineres_exportacao_detalhes[entrada.numeroConteiner] = {
            'id': entrada.id,
            'tipoDeclaracao': entrada.tipoDeclaracao,
            'numeroDeclaracao': entrada.numeroDeclaracao
        }
        conteineres_exportacao.add(entrada.numeroConteiner)

    logger.info(
        f'Acessos com DUE: {list(conteineres_exportacao)[:10]} {len(conteineres_exportacao)}'
    )

    if not conteineres_exportacao:
        logger.info('Nenhum AcessoVeiculo com DUE encontrado.')
        return lista_final

    # Passo 2 - Pega inspeções não invasivas para os contêineres dos acessos.
    datafim_escaneamento = datafim + timedelta(hours=3)

    q2 = session.query(InspecaoNaoInvasiva)
    q2 = q2.filter(InspecaoNaoInvasiva.dataHoraOcorrencia.between(datainicio, datafim_escaneamento))

    if codigos_recintos:
        q2 = q2.filter(InspecaoNaoInvasiva.codigoRecinto.in_(codigos_recintos))

    q2 = q2.filter(InspecaoNaoInvasiva.numeroConteiner.in_(conteineres_exportacao))

    lista_escaneamentos = q2.order_by(InspecaoNaoInvasiva.dataHoraOcorrencia).all()

    conteineres_escaneados_detalhes = {}
    conteineres_exportacao_escaneados = set()

    for escaneamento in lista_escaneamentos:
        conteineres_escaneados_detalhes[escaneamento.numeroConteiner] = {
            'id': escaneamento.id,
            'dataescaneamento': escaneamento.dataHoraOcorrencia
        }
        conteineres_exportacao_escaneados.add(escaneamento.numeroConteiner)

    logger.info(
        f'Escaneamentos com DUE: {list(conteineres_exportacao_escaneados)[:10]} '
        f'{len(conteineres_exportacao_escaneados)}'
    )

    if not conteineres_exportacao_escaneados:
        logger.info('Nenhum escaneamento encontrado para acessos com DUE.')
        return lista_final

    # Passo 3 - Recupera imagens no Mongo.
    griddata = raspa_container(
        list(conteineres_exportacao_escaneados),
        datainicio,
        datafim_escaneamento,
        somente_sem_due=False
    )

    for dados_imagem in griddata:
        _id = dados_imagem['_id']
        metadata = dados_imagem['metadata']

        numero_conteiner = metadata.get('numeroinformado')
        dataescaneamento = metadata.get('dataescaneamento')
        due_mongo = metadata.get('due')

        if not numero_conteiner:
            continue

        dados_acesso = conteineres_exportacao_detalhes.get(numero_conteiner)
        dados_inspecao = conteineres_escaneados_detalhes.get(numero_conteiner)

        if not dados_acesso or not dados_inspecao:
            continue

        tipo_declaracao = dados_acesso.get('tipoDeclaracao')
        numero_due = dados_acesso.get('numeroDeclaracao')

        lista_final.append((
            dados_acesso['id'],
            dados_inspecao['id'],
            numero_conteiner,
            _id,
            dataescaneamento,
            tipo_declaracao,
            numero_due,
            numero_due,
            due_mongo
        ))

    logger.info(f'Lista final com DUE: {lista_final[:2]}, {len(lista_final)}')

    return lista_final



def update_instance(model_instance, update_dict):
    """Atualiza somente atributos existentes no modelo SQLAlchemy."""
    for key, value in update_dict.items():
        if hasattr(model_instance, key):
            setattr(model_instance, key, value)


CAMPOS_DATA_DUE = {
    'data_criacao_due',
    'data_registro_due',
    'data_carga_apresentada_despacho',
    'data_desembaraco',
    'data_inicial_entrada_recinto',
    'data_primeira_recepcao_nota_recinto',
    'data_ultima_recepcao_nota_recinto',
    'data_final_manifestacao_carga',
}

CAMPOS_DATA_ITEM_DUE = {'nfe_dt_emissao'}
VALORES_TEXTUAIS_NULOS = {'', 'none', 'null', 'nan', 'nat'}
LIMITE_MENSAGEM_ERRO = 300


def valor_nulo(valor):
    """Reconhece None, NaN, NaT e representacoes textuais vazias."""
    if valor is None:
        return True
    try:
        if valor != valor:
            return True
    except (TypeError, ValueError):
        pass
    return isinstance(valor, str) and valor.strip().lower() in VALORES_TEXTUAIS_NULOS


def normaliza_datetime(valor, nome_campo):
    """Converte datas usuais do payload; valor vazio vira NULL"""
    if valor_nulo(valor):
        return None
    if isinstance(valor, datetime):
        return valor

    texto = str(valor).strip()
    formatos = (
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S',
    )
    for formato in formatos:
        try:
            return datetime.strptime(texto, formato)
        except ValueError:
            continue
    raise ValueError('Data invalida em {}: {!r}'.format(nome_campo, valor))


def sanitiza_texto_banco(valor):
    """Prepara texto para colunas legadas compativeis com latin1/MySQL"""
    texto = str(valor).strip().replace('\x00', '')
    texto = texto.replace('\ufffd', '?')

    caracteres = []
    for caractere in texto:
        codigo = ord(caractere)
        if caractere in ('\t', '\n', '\r'):
            caracteres.append(' ')
        elif codigo < 32 or 127 <= codigo <= 159:
            caracteres.append('?')
        else:
            caracteres.append(caractere)

    texto = ''.join(caracteres)
    return texto.encode('latin-1', 'replace').decode('latin-1')


def normaliza_registro(registro, campos_data=None):
    """Normaliza nulos, datas e textos antes da persistencia"""
    campos_data = campos_data or set()
    resultado = {}
    campos_sanitizados = []

    for chave, valor in registro.items():
        if chave in campos_data:
            resultado[chave] = normaliza_datetime(valor, chave)
        elif valor_nulo(valor):
            resultado[chave] = None
        elif isinstance(valor, str):
            texto_original = valor.strip().replace('\x00', '')
            texto_normalizado = sanitiza_texto_banco(valor)
            resultado[chave] = texto_normalizado
            if texto_original != texto_normalizado:
                campos_sanitizados.append(chave)
        else:
            resultado[chave] = valor

    return resultado, campos_sanitizados


def normaliza_identificador(valor, tamanho=None):
    if valor_nulo(valor):
        return None
    texto = str(valor).strip()
    if texto.endswith('.0') and texto[:-2].isdigit():
        texto = texto[:-2]
    if tamanho:
        texto = texto[:tamanho].zfill(tamanho)
    return texto


def normaliza_inteiro(valor, nome_campo, obrigatorio=False):
    if valor_nulo(valor):
        if obrigatorio:
            raise ValueError('{} nao informado'.format(nome_campo))
        return None
    try:
        numero = float(valor)
        if not numero.is_integer():
            raise ValueError
        return int(numero)
    except (TypeError, ValueError):
        raise ValueError('{} invalido: {!r}'.format(nome_campo, valor))


def resumo_erro(erro, limite=LIMITE_MENSAGEM_ERRO):
    """Retorna somente a causa curta, sem SQL e parametros completos"""
    mensagem = str(getattr(erro, 'orig', erro)).replace('\r', ' ').replace('\n', ' ').strip()
    while '  ' in mensagem:
        mensagem = mensagem.replace('  ', ' ')
    if len(mensagem) > limite:
        return mensagem[:limite] + '...'
    return mensagem


def extrair_conteineres(valor):
    if valor_nulo(valor):
        return []
    resultado = []
    vistos = set()
    for parte in str(valor).replace(';', ',').split(','):
        conteiner = ''.join(parte.strip().upper().split())
        if conteiner and len(conteiner) <= 11 and conteiner not in vistos:
            resultado.append(conteiner)
            vistos.add(conteiner)
    return resultado


def limitar_campo_ao_modelo(dados, modelo, nome_campo):
    """
    Limita um campo textual ao tamanho declarado no modelo SQLAlchemy.

    Retorna (truncado, tamanho_original, limite). Se a coluna nao possuir
    limite textual, nenhum corte e realizado.
    """
    valor = dados.get(nome_campo)
    if valor_nulo(valor):
        dados[nome_campo] = None
        return False, 0, None

    coluna = modelo.__table__.columns.get(nome_campo)
    limite = getattr(getattr(coluna, 'type', None), 'length', None)
    texto = str(valor).strip()
    tamanho_original = len(texto)

    if limite and tamanho_original > limite:
        dados[nome_campo] = texto[:limite]
        return True, tamanho_original, limite

    dados[nome_campo] = texto
    return False, tamanho_original, limite


def integra_dues(session, df_dues):
    """Faz UPSERT em lote de DUEs e retorna os numeros persistidos."""
    sucessos = []
    erros = []
    total_sanitizados = 0
    total_listas_conteiner_truncadas = 0
    maior_lista_conteiner = 0
    limite_lista_conteiner = None
    logger.info('Iniciando UPSERT em lote de %s DUEs', len(df_dues))

    colunas_due = set(c.name for c in Due.__table__.columns)
    colunas_conteiner = set(c.name for c in DueConteiner.__table__.columns)
    registros = []
    conteineres_por_due = {}

    # 1. Mantem toda a normalizacao/validacao atual, agora em memoria.
    for index, row in df_dues.iterrows():
        dados_brutos = row.to_dict()
        numero_due = normaliza_identificador(dados_brutos.get('numero_due'))
        if not numero_due:
            erros.append({'indice': int(index), 'numero_due': None,
                          'erro': 'numero_due nao informado'})
            logger.warning('DUE ignorada no indice %s: numero_due ausente', index)
            continue
        try:
            dados, campos_sanitizados = normaliza_registro(
                dados_brutos, CAMPOS_DATA_DUE)
            dados['numero_due'] = numero_due
            dados['ni_declarante'] = normaliza_identificador(
                dados.get('ni_declarante'), 14)
            dados['cnpj_estabelecimento_exportador'] = normaliza_identificador(
                dados.get('cnpj_estabelecimento_exportador'), 14)

            if campos_sanitizados:
                total_sanitizados += 1
                logger.warning(
                    'DUE %s teve %s campo(s) textual(is) sanitizado(s): %s',
                    numero_due, len(campos_sanitizados),
                    ', '.join(campos_sanitizados))

            # Os vinculos usam a lista completa antes do corte do campo legado.
            # Assim, nenhum vinculo DUE-conteiner e perdido mesmo quando a coluna
            # lista_id_conteiner precisa ser limitada para caber no banco.
            conteineres_por_due[numero_due] = extrair_conteineres(
                dados.get('lista_id_conteiner'))

            truncado, tamanho_original, limite = limitar_campo_ao_modelo(
                dados, Due, 'lista_id_conteiner')
            if limite is not None:
                limite_lista_conteiner = limite
            if truncado:
                total_listas_conteiner_truncadas += 1
                maior_lista_conteiner = max(
                    maior_lista_conteiner, tamanho_original)

            dados = {k: v for k, v in dados.items() if k in colunas_due}
            registros.append((int(index), dados))
        except (ValueError, TypeError) as erro:
            mensagem = resumo_erro(erro)
            erros.append({'indice': int(index), 'numero_due': numero_due,
                          'erro': mensagem})
            logger.error('Falha na DUE %s, indice %s: %s',
                         numero_due, index, mensagem)

    if not registros:
        logger.info(
            'UPSERT de DUEs concluido: recebidas=%s, sucesso=0, erros=%s, sanitizadas=%s',
            len(df_dues), len(erros), total_sanitizados)
        return {
            'sucessos': [], 'erros': erros,
            'total_recebidos': len(df_dues), 'total_sucessos': 0,
            'total_erros': len(erros), 'total_sanitizados': total_sanitizados,
        }

    # Ultima ocorrencia da mesma DUE no payload prevalece.
    por_numero = {}
    ordem = []
    for index, dados in registros:
        numero_due = dados['numero_due']
        if numero_due not in por_numero:
            ordem.append(numero_due)
        por_numero[numero_due] = (index, dados)

    numeros = set(ordem)
    try:
        existentes = set(
            linha[0] for linha in
            session.query(Due.numero_due).filter(Due.numero_due.in_(numeros)).all()
        )
    except SQLAlchemyError as erro:
        session.rollback()
        logger.error('Falha ao consultar DUEs existentes: %s', resumo_erro(erro))
        raise

    inserts = []
    updates = []
    for numero_due in ordem:
        _, dados = por_numero[numero_due]
        if numero_due in existentes:
            updates.append(dados)
        else:
            inserts.append(dados)

    # 2. Prepara novos vinculos DUE-conteiner em memoria e busca os existentes
    # em uma unica consulta para todo o lote.
    pares_desejados = set()
    for numero_due in ordem:
        for conteiner in conteineres_por_due.get(numero_due, []):
            pares_desejados.add((numero_due, conteiner))

    pares_existentes = set()
    if pares_desejados:
        try:
            linhas = session.query(
                DueConteiner.numero_due, DueConteiner.numero_conteiner
            ).filter(DueConteiner.numero_due.in_(numeros)).all()
            pares_existentes = set((linha[0], linha[1]) for linha in linhas)
        except SQLAlchemyError as erro:
            session.rollback()
            logger.error('Falha ao consultar vinculos DUE-conteiner: %s',
                         resumo_erro(erro))
            raise

    novos_conteineres = []
    for numero_due, conteiner in sorted(pares_desejados - pares_existentes):
        dados_conteiner = {
            'numero_due': numero_due,
            'numero_conteiner': conteiner,
        }
        novos_conteineres.append({
            k: v for k, v in dados_conteiner.items() if k in colunas_conteiner
        })

    if total_listas_conteiner_truncadas:
        logger.warning(
            'Campo lista_id_conteiner truncado em %s DUE(s): limite=%s, '
            'maior_tamanho_recebido=%s. Os vinculos individuais foram preservados.',
            total_listas_conteiner_truncadas, limite_lista_conteiner,
            maior_lista_conteiner)

    logger.info(
        'DUEs preparadas para gravacao em lote: novas=%s, atualizacoes=%s, '
        'duplicadas_no_payload=%s, novos_vinculos_conteiner=%s',
        len(inserts), len(updates), len(registros) - len(ordem),
        len(novos_conteineres))

    # 3. Grava em blocos. A ordem e proposital: DUEs antes dos vinculos,
    # preservando a integridade referencial existente.
    tamanho_bloco = 500
    try:
        for inicio in range(0, len(inserts), tamanho_bloco):
            bloco = inserts[inicio:inicio + tamanho_bloco]
            if bloco:
                session.bulk_insert_mappings(Due, bloco)

        for inicio in range(0, len(updates), tamanho_bloco):
            bloco = updates[inicio:inicio + tamanho_bloco]
            if bloco:
                session.bulk_update_mappings(Due, bloco)

        for inicio in range(0, len(novos_conteineres), tamanho_bloco):
            bloco = novos_conteineres[inicio:inicio + tamanho_bloco]
            if bloco:
                session.bulk_insert_mappings(DueConteiner, bloco)

        session.commit()
        sucessos = list(ordem)
    except Exception as erro:
        session.rollback()
        logger.error('Falha no UPSERT em lote das DUEs: %s', resumo_erro(erro))
        raise

    logger.info(
        'UPSERT de DUEs concluido: recebidas=%s, sucesso=%s, erros=%s, sanitizadas=%s',
        len(df_dues), len(sucessos), len(erros), total_sanitizados)
    return {
        'sucessos': sucessos,
        'erros': erros,
        'total_recebidos': len(df_dues),
        'total_sucessos': len(sucessos),
        'total_erros': len(erros),
        'total_sanitizados': total_sanitizados,
    }

def trunca_descricao(dados, nome_campo, tamanho=1000):
    descricao = dados.get(nome_campo)
    if valor_nulo(descricao):
        dados[nome_campo] = None
        return
    descricao = str(descricao).strip()
    if len(descricao) > tamanho:
        logger.warning('Campo %s truncado de %s para %s caracteres',
                       nome_campo, len(descricao), tamanho)
        descricao = descricao[:tamanho]
    dados[nome_campo] = descricao


def integra_dues_itens(session, df_itens_dues, dues_permitidas=None):
    """Faz UPSERT em lote apenas dos itens cuja DUE-pai foi persistida.

    Mantem as mesmas regras de normalizacao, sanitizacao, truncamento e
    validacao do fluxo anterior, mas elimina SELECT, SAVEPOINT e FLUSH por
    item. A existencia das DUEs-pai e dos itens ja gravados e consultada em
    lote; inserts e updates tambem sao enviados ao SQLAlchemy em lote.
    """
    sucessos = 0
    erros = []
    ignorados = []
    total_sanitizados = 0
    permitidas = set(dues_permitidas) if dues_permitidas is not None else None
    logger.info('Iniciando UPSERT em lote de %s itens de DUE', len(df_itens_dues))

    # 1. Normaliza/valida em memoria. Erros de dados continuam isolados por item.
    registros = []
    for index, row in df_itens_dues.iterrows():
        dados_brutos = row.to_dict()
        nr_due = normaliza_identificador(dados_brutos.get('nr_due'))
        if not nr_due:
            erros.append({'indice': int(index), 'nr_due': None,
                          'erro': 'nr_due nao informado'})
            continue
        if permitidas is not None and nr_due not in permitidas:
            ignorados.append({'indice': int(index), 'nr_due': nr_due,
                              'motivo': 'DUE-pai nao persistida neste lote'})
            continue
        try:
            dados, campos_sanitizados = normaliza_registro(
                dados_brutos, CAMPOS_DATA_ITEM_DUE)
            dados['nr_due'] = nr_due
            dados['due_nr_item'] = normaliza_inteiro(
                dados.get('due_nr_item'), 'due_nr_item', obrigatorio=True)
            dados['nfe_nr_item'] = normaliza_inteiro(
                dados.get('nfe_nr_item'), 'nfe_nr_item')
            trunca_descricao(dados, 'descricao_complementar_item')
            trunca_descricao(dados, 'descricao_item')

            # Descarta chaves que nao pertencem ao modelo, como fazia
            # update_instance no fluxo anterior.
            colunas_modelo = set(c.name for c in DueItem.__table__.columns)
            dados = {k: v for k, v in dados.items() if k in colunas_modelo}

            if campos_sanitizados:
                total_sanitizados += 1
                logger.warning(
                    'Item %s da DUE %s teve %s campo(s) sanitizado(s): %s',
                    dados['due_nr_item'], nr_due, len(campos_sanitizados),
                    ', '.join(campos_sanitizados))
            registros.append((int(index), dados))
        except (ValueError, TypeError) as erro:
            mensagem = resumo_erro(erro)
            erros.append({'indice': int(index), 'nr_due': nr_due,
                          'erro': mensagem})
            logger.error('Falha no item indice %s, DUE %s: %s',
                         index, nr_due, mensagem)

    if not registros:
        logger.info(
            'UPSERT de itens concluido: recebidos=%s, sucesso=0, erros=%s, '
            'ignorados=%s, sanitizados=%s',
            len(df_itens_dues), len(erros), len(ignorados), total_sanitizados)
        return {
            'sucessos': 0,
            'erros': erros,
            'ignorados': ignorados,
            'total_recebidos': len(df_itens_dues),
            'total_sucessos': 0,
            'total_erros': len(erros),
            'total_ignorados': len(ignorados),
            'total_sanitizados': total_sanitizados,
        }

    # 2. Confere todas as DUEs-pai com UMA consulta, em vez de uma por item.
    numeros_due = set(dados['nr_due'] for _, dados in registros)
    try:
        dues_existentes = set(
            valor[0] for valor in
            session.query(Due.numero_due).filter(Due.numero_due.in_(numeros_due)).all()
        )
    except SQLAlchemyError as erro:
        session.rollback()
        logger.error('Falha ao consultar DUEs-pai dos itens: %s', resumo_erro(erro))
        raise

    registros_validos = []
    for index, dados in registros:
        if dados['nr_due'] not in dues_existentes:
            mensagem = 'DUE-pai {} nao existe'.format(dados['nr_due'])
            erros.append({'indice': index, 'nr_due': dados['nr_due'],
                          'erro': mensagem})
            logger.error('Falha no item indice %s, DUE %s: %s',
                         index, dados['nr_due'], mensagem)
        else:
            registros_validos.append((index, dados))

    if not registros_validos:
        logger.info(
            'UPSERT de itens concluido: recebidos=%s, sucesso=0, erros=%s, '
            'ignorados=%s, sanitizados=%s',
            len(df_itens_dues), len(erros), len(ignorados), total_sanitizados)
        return {
            'sucessos': 0,
            'erros': erros,
            'ignorados': ignorados,
            'total_recebidos': len(df_itens_dues),
            'total_sucessos': 0,
            'total_erros': len(erros),
            'total_ignorados': len(ignorados),
            'total_sanitizados': total_sanitizados,
        }

    # 3. Busca as chaves existentes em UMA consulta por conjunto de DUEs.
    # Como a chave logica e (nr_due, due_nr_item), nao precisamos carregar
    # todas as colunas dos objetos existentes.
    numeros_validos = set(dados['nr_due'] for _, dados in registros_validos)
    try:
        existentes = set(
            (linha[0], linha[1]) for linha in
            session.query(DueItem.nr_due, DueItem.due_nr_item).filter(
                DueItem.nr_due.in_(numeros_validos)
            ).all()
        )
    except SQLAlchemyError as erro:
        session.rollback()
        logger.error('Falha ao consultar itens de DUE existentes: %s', resumo_erro(erro))
        raise

    # Se o mesmo item vier repetido no proprio payload, a ultima ocorrencia
    # prevalece, equivalente ao efeito final dos UPSERTs sequenciais.
    por_chave = {}
    ordem = []
    for index, dados in registros_validos:
        chave = (dados['nr_due'], dados['due_nr_item'])
        if chave not in por_chave:
            ordem.append(chave)
        por_chave[chave] = (index, dados)

    inserts = []
    updates = []
    for chave in ordem:
        _, dados = por_chave[chave]
        if chave in existentes:
            updates.append(dados)
        else:
            inserts.append(dados)

    logger.info(
        'Itens preparados para gravacao em lote: novos=%s, atualizacoes=%s, '
        'duplicados_no_payload=%s',
        len(inserts), len(updates), len(registros_validos) - len(ordem))

    # 4. Grava em blocos. bulk_* evita ORM, SELECT e FLUSH por registro.
    # 500 limita o tamanho de cada comando/transacao interna sem voltar ao
    # custo de centenas de round-trips individuais.
    tamanho_bloco = 500
    try:
        for inicio in range(0, len(inserts), tamanho_bloco):
            bloco = inserts[inicio:inicio + tamanho_bloco]
            if bloco:
                session.bulk_insert_mappings(DueItem, bloco)

        for inicio in range(0, len(updates), tamanho_bloco):
            bloco = updates[inicio:inicio + tamanho_bloco]
            if bloco:
                session.bulk_update_mappings(DueItem, bloco)

        session.commit()
        sucessos = len(ordem)
    except Exception as erro:
        session.rollback()
        logger.error('Falha no UPSERT em lote dos itens de DUE: %s', resumo_erro(erro))
        raise

    logger.info(
        'UPSERT de itens concluido: recebidos=%s, sucesso=%s, erros=%s, '
        'ignorados=%s, sanitizados=%s',
        len(df_itens_dues), sucessos, len(erros), len(ignorados),
        total_sanitizados)
    return {
        'sucessos': sucessos,
        'erros': erros,
        'ignorados': ignorados,
        'total_recebidos': len(df_itens_dues),
        'total_sucessos': sucessos,
        'total_erros': len(erros),
        'total_ignorados': len(ignorados),
        'total_sanitizados': total_sanitizados,
    }

def get_dados(session, grid_data):
    try:
        logger.info(f'get_dados: {grid_data["metadata"]["due"]}')
        numero_due = grid_data['metadata']['due']
        due = session.query(Due).filter(Due.numero_due == numero_due).one()
        declarante = due.ni_declarante
        empresa = session.query(Empresa).filter(Empresa.cnpj == declarante).one_or_none()
        if empresa:
            nome_declarante = declarante.get('Nome Declarante')
        else:
            nome_declarante = 'Não encontrado.'
        destino = due.codigo_iso3_pais_importador
        numero = due.numero_due
        return '%s - EXPORTADOR %s / %s - PAÍS DESTINO %s' % \
            (numero, declarante, nome_declarante, destino)
    except Exception as err:
        logger.error(f'get_dados: {err}')
        return ''


def due_summary(session, grid_out) -> dict:
    result = {}
    try:
        grid_data = grid_out.metadata
        logger.info(f'due_summary: {grid_data["due"]}')
        numero_due = grid_data['due']
        due = session.query(Due).filter(Due.numero_due == numero_due).one()
        declarante = due.ni_declarante
        empresa = session.query(Empresa).filter(Empresa.cnpj == declarante).one_or_none()
        if empresa:
            nome_declarante = declarante.get('Nome Declarante')
        else:
            nome_declarante = 'Não encontrado.'
        numero = due.numero_due
        destino = due.codigo_iso3_pais_importador
        result = {'RESUMO PUCOMEX': '',
                  'DUE Nº': numero,
                  'EXPORTADOR': '%s / %s' % (declarante, nome_declarante),
                  'PAÍS DESTINO': destino}
    except Exception as err:
        result['ERRO AO BUSCAR DADOS DUE'] = f'due_summary: {err}'
        logger.error(f'due_summary: {err}', exc_info=True)
    return result


def get_due(session, numerodeclaracao: str) -> Optional[Due]:
    """
    Retorna due se encontrada, ou None.
    Args:
        session: sessão SQL Alchemy
        numerodeclaracao: número da DUE
    """
    return session.query(Due).filter(Due.numero_due == numerodeclaracao).one_or_none()


def get_recinto_siscomex(session, codigo_siscomex) -> Optional[Recinto]:
    return session.query(Recinto).filter(
        Recinto.cod_siscomex == codigo_siscomex).one_or_none()


class DueView():
    """DueView é uma Due já com os campos adicionais para visualização."""

    def __init__(self, session, due: Due):
        # Reflect all columns - copia campos da due
        for key in due.__mapper__.attrs.keys():
            setattr(self, key, getattr(due, key))
        # Criação manual de campos, para documentação e reflection/autocomplete
        self.numero_due = due.numero_due
        self.data_criacao_due = due.data_criacao_due
        self.data_registro_due = due.data_registro_due
        self.ni_declarante = due.ni_declarante
        self.cnpj_estabelecimento_exportador = due.cnpj_estabelecimento_exportador
        self.codigo_iso3_pais_importador = due.codigo_iso3_pais_importador
        self.nome_pais_importador = due.nome_pais_importador
        self.codigo_recinto_despacho = due.codigo_recinto_despacho
        self.codigo_recinto_embarque = due.codigo_recinto_embarque
        try:
            empresa = get_empresa(session, due.ni_declarante)
        except ValueError:
            empresa = None
        self.nome_declarante = 'Não encontrado na base' if empresa is None else '*' + empresa.nome
        recinto_embarque = get_recinto_siscomex(session, due.codigo_recinto_embarque)
        self.nome_recinto_embarque = '' if recinto_embarque is None else recinto_embarque.nome
        recinto_despacho = get_recinto_siscomex(session, due.codigo_recinto_despacho)
        self.nome_recinto_despacho = '' if recinto_despacho is None else recinto_despacho.nome
        self.itens = list(session.query(DueItem).filter(DueItem.nr_due == due.numero_due).all())
        self.conteineres = list(session.query(DueConteiner).filter(
            DueConteiner.numero_due == due.numero_due).all())


def get_due_view(session, numerodeclaracao: str) -> Optional[DueView]:
    """
    Retorna due_view se due encontrada, ou None.

    Args:
        session: sessão SQL Alchemy
        numerodeclaracao: número da DUE
    """
    due = session.query(Due).filter(Due.numero_due == numerodeclaracao).one_or_none()
    if due is not None:
        return DueView(session, due)
    return None


def get_itens_due(session, numerodeclaracao: str) -> List[DueItem]:
    return session.query(DueItem).filter(DueItem.nr_due == numerodeclaracao).all()


def get_dues_container(session, numero: str,
                       datainicio: datetime,
                       datafim: datetime,
                       limit=40
                       ) -> List[DueView]:
    if numero is None or numero == '':
        raise ValueError('get_dues_container: Informe o número do contêiner!')
    q = session.query(Due).join(DueConteiner, Due.numero_due == DueConteiner.numero_due). \
        filter(DueConteiner.numero_conteiner == numero,
               Due.data_criacao_due.between(datainicio, datafim)).limit(limit)
    logger.info(q.statement)
    dues = q.all()
    return [get_due_view(session, due.numero_due) for due in dues]


def get_dues_empresa(session, cnpj: str, limit=40) -> List[Due]:
    if cnpj is None or len(cnpj) < 8:
        raise ValueError('get_dues: Informe o CNPJ da empresa com no mínimo 8 posições!')
    return session.query(Due).filter(Due.cnpj_estabelecimento_exportador.like(cnpj + '%')). \
        limit(limit).all()


def get_dues_sem_detalhes(session, datainicio: datetime, datafim: datetime,
                          codigos_recintos: list = None) -> list:
    """1 - Pega acessos veículo do período e recintos que não possuem DUE correspondente.

    Args:
        session: conexão SQLAlchemy
        datainicio, datafim: periodo between
        codigoRecinto: opcional

    Returns: lista de números de due.

    """
    # Filtros: A"C"esso, direção "E"ntrada, numeroNfe não vazio, numeroConteiner não vazio,
    q = (
        session.query(AcessoVeiculo.numeroDeclaracao)
        .filter(AcessoVeiculo.operacao == 'C')
        .filter(AcessoVeiculo.direcao == 'E')
        .filter(AcessoVeiculo.dataHoraOcorrencia.between(datainicio, datafim))
        .filter(AcessoVeiculo.numeroConteiner.isnot(None))
        .filter(AcessoVeiculo.numeroConteiner != '')
        .filter(AcessoVeiculo.tipoDeclaracao == 'DUE')
        .filter(AcessoVeiculo.numeroDeclaracao.isnot(None))
        .filter(AcessoVeiculo.numeroDeclaracao != '')
        .filter(
            not_(
                session.query(Due)
                .filter(Due.numero_due == AcessoVeiculo.numeroDeclaracao)
                .exists()
            )
        )
    )
    if codigos_recintos:
        q = q.filter(AcessoVeiculo.codigoRecinto.in_(codigos_recintos))
    q = q.distinct()
    return [acesso.numeroDeclaracao for acesso in q.all()]