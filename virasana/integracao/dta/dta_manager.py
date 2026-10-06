"""Persistencia em lote dos dados de DTA recebidos do Contagil."""

from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.mysql import insert as mysql_insert


TABELA_DTA = "navii_dtas"
SCHEMA_DTA = None  # Informe o schema caso a tabela nao esteja no banco padrao.
CHUNK_SIZE_BANCO = 200


class ErroIntegracaoDTA(ValueError):
    """Erro de validacao ou persistencia de dados de DTA."""


def _normalizar_valor(valor):
    """Converte valores vazios do Contagil em NULL sem alterar outros tipos."""
    if valor is None:
        return None
    if isinstance(valor, str):
        valor = valor.strip()
        return valor if valor else None
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
    ausentes = [nome for nome in chave_natural if nome not in existentes]
    if ausentes:
        raise ErroIntegracaoDTA(
            "A tabela nao possui as colunas da chave natural: {}.".format(
                ", ".join(ausentes)
            )
        )
    return chave_natural


def _preparar_registros(tabela, registros):
    if not isinstance(registros, list) or not registros:
        raise ErroIntegracaoDTA("'registros' deve ser uma lista nao vazia.")

    colunas_tabela = {coluna.name for coluna in tabela.columns}
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

        preparado = {
            nome: _normalizar_valor(valor)
            for nome, valor in registro.items()
            if nome in colunas_tabela
        }

        ausentes = [nome for nome in chaves if preparado.get(nome) is None]
        if ausentes:
            raise ErroIntegracaoDTA(
                "Registro {} sem chave obrigatoria: {}.".format(
                    indice, ", ".join(ausentes)
                )
            )

        preparados.append(preparado)

    return preparados, sorted(ignoradas), chaves


def _chunks(lista, tamanho):
    for inicio in range(0, len(lista), tamanho):
        yield lista[inicio:inicio + tamanho]


def integra_dtas(db_session, registros):
    """
    Executa UPSERT atomico em MySQL/MariaDB.

    A transacao inteira e confirmada somente depois que todos os blocos forem
    processados. Em qualquer falha, tudo e revertido.
    """
    tabela = _carregar_tabela(db_session)
    preparados, ignoradas, chaves = _preparar_registros(tabela, registros)

    total = 0
    try:
        for bloco in _chunks(preparados, CHUNK_SIZE_BANCO):
            comando = mysql_insert(tabela).values(bloco)
            atualizacoes = {
                coluna.name: comando.inserted[coluna.name]
                for coluna in tabela.columns
                if coluna.name not in chaves
                and coluna.name in bloco[0]
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
    except Exception:
        db_session.rollback()
        raise
