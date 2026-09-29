# coding: utf-8
"""
Blueprint Flask do NAVI-I — Análise de risco na importação.

Expõe:
  GET /navii/            -> página HTML (filtros + cards com rolagem infinita)
  GET /navii/api/ces      -> JSON paginado (consumido pelo JS da página)

Depende de `busca_ces_navii`, `busca_itens_navii` e `monta_card_navii`,
definidos em `busca_navii.py`.
"""
from datetime import datetime

from flask import Blueprint, current_app, jsonify, render_template, request
from flask_login import login_required

from virasana.navii.busca_navii import (
    busca_ces_navii, busca_itens_navii, monta_card_navii,
    _janela_efetiva, _janela_previsao,
)

bp_navii = Blueprint(
    'navii', __name__,
    url_prefix='/navii',
    template_folder='templates',
)

# Rota já existente em virasana/views.py (`/mini1/<_id>`) que serve a
# miniatura da imagem do GridFS a partir do `imagem_id`.
IMAGEM_URL_TEMPLATE = '/virasana/mini1/imagem_id'


def _serializa_valor(v):
    if isinstance(v, datetime):
        return v.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(v, dict):
        return {k: _serializa_valor(sub) for k, sub in v.items()}
    return v


def _serializa_card(card: dict) -> dict:
    """Converte datetimes (inclusive dentro da linha do tempo) para string, para virar JSON."""
    return {k: _serializa_valor(v) for k, v in card.items()}


def _defaults_periodo():
    """Sugestão de período inicial (pré-preenchida na tela) para cada janela."""
    inicio_prevista, fim_prevista = _janela_previsao()
    inicio_efetiva, fim_efetiva = _janela_efetiva()
    return {
        'prevista': {'inicio': inicio_prevista, 'fim': fim_prevista},
        'efetiva': {'inicio': inicio_efetiva.strftime('%Y-%m-%d'), 'fim': fim_efetiva.strftime('%Y-%m-%d')},
    }


@bp_navii.route('/')
@login_required
def index():
    return render_template(
        'navii.html',
        imagem_url_template=IMAGEM_URL_TEMPLATE,
        defaults_periodo=_defaults_periodo(),
    )


@bp_navii.route('/api/ces')
@login_required
def api_ces():
    session = current_app.config['db_session']

    tipo_janela = request.args.get('tipo_janela', 'efetiva')
    cursor_id = request.args.get('cursor_id', type=int)
    limit = request.args.get('limit', default=20, type=int)
    limit = max(1, min(limit, 50))

    filtros = {}
    for campo in ('ncm', 'produto', 'porto_origem', 'porto_destino', 'recinto', 'empresa'):
        valor = request.args.get(campo)
        if valor:
            filtros[campo] = valor.strip()
    if request.args.get('concluido') in ('1', 'true', 'True'):
        filtros['concluido'] = True
    ncms_max = request.args.get('ncms_max', type=int)
    if ncms_max:
        filtros['ncms_max'] = ncms_max
    data_inicio = request.args.get('data_inicio')
    data_fim = request.args.get('data_fim')
    if data_inicio and data_fim:
        filtros['data_inicio'] = data_inicio
        filtros['data_fim'] = data_fim

    try:
        resultados = busca_ces_navii(
            session,
            tipo_janela=tipo_janela,
            filtros=filtros,
            cursor_id=cursor_id,
            limit=limit,
        )
    except ValueError as err:
        return jsonify({'erro': str(err)}), 400

    numeros_ce = [ce.numeroCEmercante for _, ce, _ in resultados]
    itens_por_ce = busca_itens_navii(session, numeros_ce)

    cards = [
        _serializa_card(monta_card_navii(ciclo, ce, embarcacao, itens_por_ce.get(ce.numeroCEmercante)))
        for ciclo, ce, embarcacao in resultados
    ]
    next_cursor = cards[-1]['ciclo_de_vida_id'] if len(cards) == limit else None

    return jsonify({
        'cards': cards,
        'next_cursor': next_cursor,
        'tipo_janela': tipo_janela,
        'filtros': filtros,
    })
