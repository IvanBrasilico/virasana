# coding: utf-8
"""
COLETA DO USO do NAVI-I para as estatísticas de uso (aba gerencial; plano aprovado em 17/10/2026 — `CONTEXTO.md`,
"Estatísticas de uso"). F1: só a coleta, sem tela.

Mongo do virasana (como os filtros salvos), coleção `navii_uso_pesquisas`, um documento por PESQUISA (a 1ª página de
uma busca; as seguintes pertencem a ela):

    {'usuario': CPF, 'setor': setor no Fichas no momento (ou None), 'aba': id da aba do navegador,
     'anterior': id da pesquisa anterior da mesma aba (só se for do mesmo usuário), 'origem': tela | link | salvo |
     card | desfazer | outra, 'inicio', 'atualizado', 'janela', 'periodo': {inicio, fim} (o usado na busca; None =
     sem limite de data), 'filtros' (normalizados, sem as datas), 'total', 'tempos', 'paginas', 'n_cards' (todos os
     enviados), 'cards': [{i: item, c: contêiner, ce, p: posição, d: dia do desembarque, pr: dia da previsão}] (até
     MAX_CARDS), 'vistos': {item: {s: segundos visíveis, x: [interações]}}, 'acoes': [{t: 'ficha', ...}]}

O SERVIDOR registra a pesquisa e os cards que enviou — nada disso vem do navegador. O navegador só informa, por card,
os segundos visíveis (acumulados; gravados com `$max`, então reenviar não soma) e as interações; o servidor só aceita
cards que ELE enviou àquele usuário naquela pesquisa. "Visto" = algum segundo registrado (o navegador só conta
trechos de 1 s seguido ou mais, com >= 50% do card na tela) ou alguma interação (quem clicou viu); "examinado" = com
interação.

Usa só o básico do pymongo (find_one, insert_one, update_one, create_index), para os testes rodarem com mongomock.
"""
import math
import re
import time
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

from bson import ObjectId
from bson.errors import InvalidId
from sqlalchemy import select

from virasana.navii import tabelas_opcionais
from virasana.navii.acompanhamento_fichas import USUARIOS

COLECAO = 'navii_uso_pesquisas'
ORIGENS = ('tela', 'link', 'salvo', 'card', 'desfazer')
INTERACOES = ('imagem', 'descricao', 'extrato', 'conteiner', 'detalhes', 'menu', 'ficha', 'fichas')
MAX_CARDS = 1000                 # cards enviados guardados por pesquisa (50 páginas de 20)
MAX_LOTE = 200                   # cards por envio do navegador
MAX_SEGUNDOS = 3600.0            # por card: teto para a aba esquecida aberta
MAX_ACOES = 100
ABA_VALIDA = re.compile(r'^[A-Za-z0-9-]{8,64}$')
VALIDADE_SETOR = 600             # segundos do cache do setor do usuário

_setores: Dict[str, Tuple[Optional[str], float]] = {}


def colecao(db):
    """Coleção das pesquisas, com os índices das consultas da aba (por usuário e por período)."""
    col = db[COLECAO]
    col.create_index([('usuario', 1), ('inicio', -1)])
    col.create_index([('inicio', -1)])
    return col


def _id(valor) -> Optional[ObjectId]:
    try:
        return ObjectId(str(valor)) if valor else None
    except (InvalidId, TypeError):
        return None


def _inteiro(valor) -> Optional[int]:
    if isinstance(valor, bool):
        return None
    try:
        return int(valor) if isinstance(valor, int) or str(valor).isdigit() else None
    except (TypeError, ValueError):
        return None


def _segundos(valor) -> Optional[float]:
    """Segundos visíveis informados pelo navegador: número finito > 0, com teto; o resto vale None."""
    if isinstance(valor, bool) or not isinstance(valor, (int, float)) or not math.isfinite(valor) or valor <= 0:
        return None
    return round(min(float(valor), MAX_SEGUNDOS), 1)


def _dia(valor) -> Optional[str]:
    """'AAAA-MM-DD' de uma data (datetime, date ou texto ISO); None se não for data."""
    if isinstance(valor, (datetime, date)):
        return valor.strftime('%Y-%m-%d')
    texto = str(valor or '')[:10]
    return texto if re.match(r'^\d{4}-\d{2}-\d{2}$', texto) else None


def card_enviado(card: dict, posicao: int) -> dict:
    """O que se guarda de cada card enviado: item, contêiner, CE, posição e as datas de referência (desembarque e
    previsão de atracação) — a cobertura conta pela data do CONTÊINER, não pela da pesquisa."""
    desembarque = ((card.get('linhaDoTempo') or {}).get('desembarque') or {}).get('data')
    return {'i': card.get('item_id'), 'c': card.get('numeroConteiner'), 'ce': card.get('numeroCEmercante'),
            'p': posicao, 'd': _dia(desembarque), 'pr': _dia(card.get('dataPrevistaAtracacao'))}


def setor_do_usuario(session, usuario: str) -> Optional[str]:
    """Setor do usuário no Fichas (`ovr_usuarios`), guardado na pesquisa (o usuário pode mudar de setor depois).
    Sem a tabela ou sem cadastro: None. Em cache por alguns minutos."""
    setor, quando = _setores.get(usuario, (None, 0.0))
    if usuario in _setores and time.monotonic() - quando < VALIDADE_SETOR:
        return setor
    setor = None
    if tabelas_opcionais.existe(session, USUARIOS.name):
        valor = session.connection().execute(select(USUARIOS.c.setor_id).where(USUARIOS.c.cpf == usuario)).scalar()
        setor = (str(valor).strip() or None) if valor is not None else None
    _setores[usuario] = (setor, time.monotonic())
    return setor


def limpa_cache():
    _setores.clear()


def registra_pesquisa(col, usuario: str, setor: Optional[str], aba, anterior, origem, janela: str,
                      periodo: Tuple[Optional[str], Optional[str]], filtros: dict, total: Optional[int],
                      tempos: Optional[dict], cards: List[dict], agora: Optional[datetime] = None) -> str:
    """Grava a pesquisa (1ª página) com os cards enviados; devolve o id (o navegador o manda nas páginas seguintes e
    nos cards vistos). A pesquisa anterior só é ligada se existir e for do mesmo usuário."""
    agora = agora or datetime.now()
    anterior_id = _id(anterior)
    if anterior_id is not None and col.find_one({'_id': anterior_id, 'usuario': usuario}, {'_id': 1}) is None:
        anterior_id = None
    inicio, fim = periodo
    documento = {
        'usuario': usuario, 'setor': setor, 'aba': aba if isinstance(aba, str) and ABA_VALIDA.match(aba) else None,
        'anterior': anterior_id, 'origem': origem if origem in ORIGENS else 'outra', 'inicio': agora,
        'atualizado': agora, 'janela': janela, 'periodo': {'inicio': inicio, 'fim': fim} if inicio and fim else None,
        'filtros': {k: v for k, v in (filtros or {}).items() if k not in ('data_inicio', 'data_fim')},
        'total': total, 'tempos': tempos, 'paginas': 1, 'n_cards': len(cards),
        'cards': [card_enviado(card, posicao) for posicao, card in enumerate(cards[:MAX_CARDS])],
        'vistos': {}, 'acoes': [],
    }
    return str(col.insert_one(documento).inserted_id)


def registra_pagina(col, pesquisa, usuario: str, cards: List[dict], agora: Optional[datetime] = None) -> bool:
    """Acrescenta os cards de uma página seguinte à pesquisa (só se for do usuário). False: pesquisa não encontrada."""
    pesquisa_id = _id(pesquisa)
    documento = col.find_one({'_id': pesquisa_id, 'usuario': usuario}, {'n_cards': 1}) if pesquisa_id else None
    if documento is None:
        return False
    ja_enviados = documento.get('n_cards', 0)
    guardar = cards[:max(0, MAX_CARDS - ja_enviados)]
    col.update_one({'_id': pesquisa_id}, {
        '$push': {'cards': {'$each': [card_enviado(card, ja_enviados + n) for n, card in enumerate(guardar)]}},
        '$inc': {'paginas': 1, 'n_cards': len(cards)}, '$set': {'atualizado': agora or datetime.now()}})
    return True


def _lote(vistos, enviados: set) -> Tuple[dict, dict]:
    """({campo: segundos}, {campo: {'$each': interações}}) dos cards do lote que o servidor enviou."""
    maximos, interacoes = {}, {}
    for visto in (vistos if isinstance(vistos, list) else [])[:MAX_LOTE]:
        item = _inteiro(visto.get('item')) if isinstance(visto, dict) else None
        if item is None or item not in enviados:
            continue
        segundos = _segundos(visto.get('s'))
        tipos = sorted({x for x in visto.get('x') or [] if x in INTERACOES}) if isinstance(visto.get('x'), list) else []
        if segundos:
            maximos[f'vistos.{item}.s'] = segundos
        if tipos:
            interacoes[f'vistos.{item}.x'] = {'$each': tipos}
    return maximos, interacoes


def registra_vistos(col, pesquisa, usuario: str, vistos, agora: Optional[datetime] = None) -> Optional[int]:
    """Grava os segundos visíveis (`$max`: reenviar não soma) e as interações dos cards de um lote do navegador.
    Devolve quantos cards aceitou; None = pesquisa não encontrada (ou de outro usuário)."""
    pesquisa_id = _id(pesquisa)
    documento = col.find_one({'_id': pesquisa_id, 'usuario': usuario}, {'cards.i': 1}) if pesquisa_id else None
    if documento is None:
        return None
    maximos, interacoes = _lote(vistos, {card.get('i') for card in documento.get('cards', [])})
    aceitos = {campo.split('.')[1] for campo in list(maximos) + list(interacoes)}
    if aceitos:
        atualizacao = {'$set': {'atualizado': agora or datetime.now()}}
        if maximos:
            atualizacao['$max'] = maximos
        if interacoes:
            atualizacao['$addToSet'] = interacoes
        col.update_one({'_id': pesquisa_id}, atualizacao)
    return len(aceitos)


def registra_acao(col, pesquisa, usuario: str, acao: dict, agora: Optional[datetime] = None) -> bool:
    """Acrescenta uma ação (ex.: a Ficha aberta pelo card) à pesquisa de onde veio — o vínculo exato entre a Ficha e
    a pesquisa. False: pesquisa não encontrada (ou de outro usuário)."""
    pesquisa_id = _id(pesquisa)
    if pesquisa_id is None:
        return False
    agora = agora or datetime.now()
    resultado = col.update_one({'_id': pesquisa_id, 'usuario': usuario}, {
        '$push': {'acoes': {'$each': [{**acao, 'quando': agora}], '$slice': -MAX_ACOES}},
        '$set': {'atualizado': agora}})
    return resultado.matched_count == 1
