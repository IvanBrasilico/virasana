# coding: utf-8
"""
Blueprint Flask do NAVI-I — Análise de risco na importação.

Expõe:
  GET /navii/            -> página HTML (filtros + cards com rolagem infinita)
  GET /navii/api/ces      -> JSON paginado (consumido pelo JS da página)

Depende de `busca_ces_navii` e `monta_card_navii`, definidos em
`busca_navii.py`.
"""
from datetime import datetime

from flask import Blueprint, current_app, jsonify, render_template, request
from flask_login import login_required

from virasana.navii.busca_navii import busca_ces_navii, monta_card_navii

bp_navii = Blueprint(
    'navii', __name__,
    url_prefix='/navii',
    template_folder='templates',
)

# Rota já existente em virasana/views.py (`/mini1/<_id>`) que serve a
# miniatura da imagem do GridFS a partir do `imagem_id`.
IMAGEM_URL_TEMPLATE = '/virasana/mini1/imagem_id'


def _serializa_card(card: dict) -> dict:
    """Converte datetimes para string, para virar JSON."""
    out = dict(card)
    for k, v in out.items():
        if isinstance(v, datetime):
            out[k] = v.strftime('%Y-%m-%d %H:%M:%S')
    return out


@bp_navii.route('/')
@login_required
def index():
    return render_template('navii.html', imagem_url_template=IMAGEM_URL_TEMPLATE)


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

    cards = [_serializa_card(monta_card_navii(ciclo, ce)) for ciclo, ce in resultados]
    next_cursor = cards[-1]['ciclo_de_vida_id'] if len(cards) == limit else None

    return jsonify({
        'cards': cards,
        'next_cursor': next_cursor,
        'tipo_janela': tipo_janela,
        'filtros': filtros,
    })
