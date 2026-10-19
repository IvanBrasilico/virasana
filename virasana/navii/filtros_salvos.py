# coding: utf-8
"""
Filtros salvos do NAVI-I: cada usuário guarda o conjunto de filtros atual com
um nome e o recupera depois.

Persistência no MongoDB (coleção `navii_filtros_salvos`), um documento por
(usuário, nome):

    {'usuario': '12345678900', 'nome': 'Eletrônicos da China',
     'tipo_janela': 'efetiva', 'filtros': {...}, 'criado': datetime, 'atualizado': datetime}

Os filtros ficam como documento livre (JSON), porque os campos da tela mudam
com frequência; ao salvar, só as chaves de CAMPOS_SALVAVEIS são mantidas.
O período (datas) NÃO é salvo, por decisão do usuário: ao recuperar, vale o
período sugerido pela tela, e o filtro não "envelhece". Cada usuário só vê e
apaga os seus filtros.

Usa só o básico da API do pymongo (find/update_one/delete_one/create_index),
para os testes rodarem com uma coleção falsa em memória.
"""
from datetime import datetime
from typing import List, Optional

from virasana.navii.busca_navii import (
    CAMPOS_ESCOLHA, CAMPOS_LISTA, CAMPOS_MARCAVEIS, CAMPOS_PESO, TIPOS_JANELA_VALIDOS, normaliza_filtros,
)

COLECAO = 'navii_filtros_salvos'
TAMANHO_MAXIMO_NOME = 60
MAXIMO_POR_USUARIO = 100
CAMPOS_SALVAVEIS = (CAMPOS_LISTA + CAMPOS_MARCAVEIS + CAMPOS_ESCOLHA + CAMPOS_PESO
                    + ('produto', 'empresa', 'ncms_max'))


class FiltroInvalido(ValueError):
    pass


def colecao(db):
    """Coleção dos filtros salvos, com o índice único (usuário, nome)."""
    col = db[COLECAO]
    col.create_index([('usuario', 1), ('nome', 1)], unique=True)
    return col


def valida_nome(nome: Optional[str]) -> str:
    nome = ' '.join((nome or '').split())
    if not nome:
        raise FiltroInvalido('Informe um nome para o filtro.')
    if len(nome) > TAMANHO_MAXIMO_NOME:
        raise FiltroInvalido(f'Nome muito longo (máximo {TAMANHO_MAXIMO_NOME} caracteres).')
    return nome


def limpa_filtros(filtros: Optional[dict]) -> dict:
    """Só os campos conhecidos, normalizados como na busca; sem datas."""
    filtros = normaliza_filtros({k: v for k, v in (filtros or {}).items() if k in CAMPOS_SALVAVEIS})
    limpos = {}
    for campo, valor in filtros.items():
        if campo in CAMPOS_MARCAVEIS:
            if valor in (True, 1, '1', 'true', 'True'):
                limpos[campo] = True
        elif campo == 'ncms_max':
            try:
                if int(valor) > 0:
                    limpos[campo] = int(valor)
            except (TypeError, ValueError):
                raise FiltroInvalido('Quantidade máxima de NCMs inválida.')
        elif campo in CAMPOS_LISTA or campo in CAMPOS_PESO or campo == 'corad':   # corad 0..4; peso: número >= 0
            limpos[campo] = valor
        elif str(valor).strip():
            limpos[campo] = str(valor).strip()
    return limpos


def lista(col, usuario: str) -> List[dict]:
    """Filtros do usuário, em ordem de nome."""
    documentos = col.find({'usuario': usuario}, {'_id': 0, 'usuario': 0})
    return sorted(documentos, key=lambda d: d['nome'].lower())


def salva(col, usuario: str, nome: str, tipo_janela: str, filtros: Optional[dict]) -> dict:
    """Cria ou substitui (mesmo nome) um filtro do usuário."""
    nome = valida_nome(nome)
    if tipo_janela not in TIPOS_JANELA_VALIDOS:
        raise FiltroInvalido('Janela inválida.')
    existentes = {d['nome'] for d in lista(col, usuario)}
    if nome not in existentes and len(existentes) >= MAXIMO_POR_USUARIO:
        raise FiltroInvalido(f'Limite de {MAXIMO_POR_USUARIO} filtros salvos. Apague algum antes.')
    agora = datetime.now()
    documento = {'tipo_janela': tipo_janela, 'filtros': limpa_filtros(filtros), 'atualizado': agora}
    col.update_one({'usuario': usuario, 'nome': nome},
                   {'$set': documento, '$setOnInsert': {'criado': agora}}, upsert=True)
    return {'nome': nome, **documento}


def apaga(col, usuario: str, nome: str) -> bool:
    return col.delete_one({'usuario': usuario, 'nome': valida_nome(nome)}).deleted_count > 0
