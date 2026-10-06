"""Persistencia em lote dos dados de DTA recebidos do Contagil."""

from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation

from sqlalchemy import Date, DateTime, MetaData, Numeric, Table
from sqlalchemy.dialects.mysql import insert as mysql_insert


TABELA_DTA = "dados_dta"
SCHEMA_DTA = None  # Informe o schema caso a tabela nao esteja no banco padrao.
CHUNK_SIZE_BANCO = 200


class ErroIntegracaoDTA(ValueError):
    """Erro de validacao ou persistencia de dados de DTA."""


def _normalizar_vazio(valor):
    """Converte textos vazios recebidos do Contagil em NULL."""
    if valor is None:
        return None

    if isinstance(valor, str):
        valor = valor.strip()
        return valor if valor else None

    return valor


def _normalizar_data(valor, nome_coluna, indice_registro):
    """Converte formatos usuais de data para datetime.date."""
    valor = _normalizar_vazio(valor)

    if valor is None:
        return None

    if isinstance(valor, datetime):
        return valor.date()

    if isinstance(valor, date):
        return valor

    texto = str(valor).strip()

    formatos = (
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    )

    for formato in formatos:
        try:
            return datetime.strptime(texto, formato).date()
        except ValueError:
            continue

    raise ErroIntegracaoDTA(
        "Registro {}: data invalida para a coluna '{}': '{}'. "
        "Formatos aceitos: DD/MM/AAAA, AAAA-MM-DD e formatos com horario.".format(
            indice_registro,
            nome_coluna,
            texto,
        )
    )


def _normalizar_data_hora(valor, nome_coluna, indice_registro):
    """Converte formatos usuais de data e hora para datetime.datetime."""
    valor = _normalizar_vazio(valor)

    if valor is None:
        return None

    if isinstance(valor, datetime):
        return valor

    if isinstance(valor, date):
        return datetime.combine(valor, time.min)

    texto = str(valor).strip()

    formatos = (
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%d/%m/%Y",
        "%Y-%m-%d",
    )

    # Remove o sufixo UTC Z, quando presente.
    texto_sem_z = texto[:-1] if texto.endswith("Z") else texto

    for formato in formatos:
        try:
            return datetime.strptime(texto_sem_z, formato)
        except ValueError:
            continue

    raise ErroIntegracaoDTA(
        "Registro {}: data/hora invalida para a coluna '{}': '{}'.".format(
            indice_registro,
            nome_coluna,
            texto,
        )
    )


def _normalizar_decimal(valor, nome_coluna, indice_registro):
    """Converte numeros recebidos como texto para Decimal."""
    valor = _normalizar_vazio(valor)

    if valor is None or isinstance(valor, Decimal):
        return valor

    if isinstance(valor, (int, float)):
        return Decimal(str(valor))

    texto = str(valor).strip()

    # Aceita decimal brasileiro apenas quando nao existe ponto decimal.
    if "," in texto and "." not in texto:
        texto = texto.replace(",", ".")

    try:
        return Decimal(texto)
    except (InvalidOperation, ValueError):
        raise ErroIntegracaoDTA(
            "Registro {}: numero invalido para a coluna '{}': '{}'.".format(
                indice_registro,
                nome_coluna,
                valor,
            )
        )


def _normalizar_valor_coluna(valor, coluna, indice_registro):
    """Normaliza um valor de acordo com o tipo real da coluna refletida."""
    valor = _normalizar_vazio(valor)

    if valor is None:
        return None

    # DateTime deve ser verificado antes de Date.
    if isinstance(coluna.type, DateTime):
        return _normalizar_data_hora(
            valor,
            coluna.name,
            indice_registro,
        )

    if isinstance(coluna.type, Date):
        return _normalizar_data(
            valor,
            coluna.name,
            indice_registro,
        )

    if isinstance(coluna.type, Numeric):
        return _normalizar_decimal(
            valor,
            coluna.name,
            indice_registro,
        )

    return valor


def _carregar_tabela(db_session):
    bind = db_session.get_bind()
    metadata = MetaData()

    return Table(
        TABELA_DTA,
        metadata,
        schema=SCHEMA_DTA,
        autoload_with=bind,
    )


def _colunas_chave(tabela):
    """Define a chave natural usada para validar cada registro de DTA."""
    chave_natural = ["codigo_declaracao_transito", "nr_ce"]
    existentes = {coluna.name for coluna in tabela.columns}
    ausentes = [
        nome for nome in chave_natural
        if nome not in existentes
    ]

    if ausentes:
        raise ErroIntegracaoDTA(
            "A tabela nao possui as colunas da chave natural: {}.".format(
                ", ".join(ausentes)
            )
        )

    return chave_natural


def _preparar_registros(tabela, registros):
    if not isinstance(registros, list) or not registros:
        raise ErroIntegracaoDTA(
            "'registros' deve ser uma lista nao vazia."
        )

    colunas_por_nome = {
        coluna.name: coluna
        for coluna in tabela.columns
    }
    colunas_tabela = set(colunas_por_nome)
    chaves = _colunas_chave(tabela)
    preparados = []
    ignoradas = set()

    for indice, registro in enumerate(registros, 1):
        if not isinstance(registro, dict):
            raise ErroIntegracaoDTA(
                "Registro {} deve ser um objeto JSON.".format(indice)
            )

        desconhecidas = set(registro) - colunas_tabela
        ignoradas.update(desconhecidas)

        preparado = {}

        for nome, valor in registro.items():
            if nome not in colunas_por_nome:
                continue

            preparado[nome] = _normalizar_valor_coluna(
                valor,
                colunas_por_nome[nome],
                indice,
            )

        ausentes = [
            nome for nome in chaves
            if preparado.get(nome) is None
        ]

        if ausentes:
            raise ErroIntegracaoDTA(
                "Registro {} sem chave obrigatoria: {}.".format(
                    indice,
                    ", ".join(ausentes),
                )
            )

        preparados.append(preparado)

    return preparados, sorted(ignoradas), chaves


def _chunks(lista, tamanho):
    if tamanho <= 0:
        raise ErroIntegracaoDTA(
            "CHUNK_SIZE_BANCO deve ser maior que zero."
        )

    for inicio in range(0, len(lista), tamanho):
        yield lista[inicio:inicio + tamanho]


def integra_dtas(db_session, registros):
    """
    Executa UPSERT atomico em MySQL/MariaDB.

    A transacao inteira e confirmada somente depois que todos os blocos forem
    processados. Em qualquer falha, todos os blocos da requisicao sao revertidos.
    """
    tabela = _carregar_tabela(db_session)
    preparados, ignoradas, chaves = _preparar_registros(
        tabela,
        registros,
    )
    total = 0

    try:
        for bloco in _chunks(preparados, CHUNK_SIZE_BANCO):
            comando = mysql_insert(tabela).values(bloco)

            nomes_presentes = set()
            for registro in bloco:
                nomes_presentes.update(registro)

            atualizacoes = {
                coluna.name: comando.inserted[coluna.name]
                for coluna in tabela.columns
                if coluna.name not in chaves
                and coluna.name in nomes_presentes
            }

            if atualizacoes:
                comando = comando.on_duplicate_key_update(**atualizacoes)

            db_session.execute(comando)
            total += len(bloco)

        db_session.commit()

        return {
            "recebidos": len(registros),
            "processados": total,
            "colunas_ignoradas": ignoradas,
            "chaves": chaves,
        }

    except ErroIntegracaoDTA:
        db_session.rollback()
        raise

    except Exception as erro:
        db_session.rollback()
        raise ErroIntegracaoDTA(
            "Falha ao persistir os dados de DTA: {}".format(erro)
        )
