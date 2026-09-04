# coding: utf-8
"""
Blueprint Flask para a tela de Análise de Risco (MVP v0.3).

Expõe:
  GET /risco/            -> página HTML (filtros + cards com rolagem infinita)
  GET /risco/api/ces      -> JSON paginado (consumido pelo JS da página)

Depende de `busca_ces_risco` e `monta_card`, definidos em
`ciclo_de_vida_busca_risco.py`. Ajuste o import abaixo conforme o caminho
real do arquivo no seu pacote.

Para plugar no app Flask existente:

    from bhadrasana.blueprints.risco.views_risco import bp_risco
    app.register_blueprint(bp_risco)

Há DOIS pontos marcados como TODO que dependem de como o seu app já está
montado — sem eles a tela sobe, mas a API retorna erro 500:

  1) get_session()  -> como pegar a sessão SQLAlchemy do seu app hoje.
  2) IMAGEM_URL_TEMPLATE -> rota que já existe (ou vai existir) para servir
     a imagem do GridFS/Mongo a partir do `imagem_id`.
"""
from datetime import datetime

from flask import Blueprint, g, jsonify, render_template, request, current_app

from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida_busca_risco import busca_ces_risco, monta_card

bp_risco = Blueprint(
    'risco', __name__,
    url_prefix='/risco',
    template_folder='templates',
)

# TODO(1): troque pelo jeito real de obter a sessão SQLAlchemy no seu app.
# Exemplos comuns no ecossistema ajna/bhadrasana/virasana:
#   - um scoped_session global importado de outro módulo
#   - algo já pendurado em flask.g por um before_request
def get_session():
    if not hasattr(g, 'db_session'):
        raise RuntimeError(
            'Configure get_session() em views_risco.py para retornar a sessão '
            'SQLAlchemy do seu app (bhadrasana/virasana).'
        )
    return g.db_session


# TODO(2): aponte para a rota real que serve a imagem do GridFS a partir do
# imagem_id salvo em CicloDeVidaImportacaoEscaneamentos.imagem_id.
# Se ainda não existir, o front mostra só o texto do ID por enquanto.
IMAGEM_URL_TEMPLATE ='/virasana/mini1/imagem_id" width="150px"'


def _serializa_card(card: dict) -> dict:
    """Converte datetimes para string, para virar JSON."""
    out = dict(card)
    for k, v in out.items():
        if isinstance(v, datetime):
            out[k] = v.strftime('%Y-%m-%d %H:%M:%S')
    return out


@bp_risco.route('/')
def index():
    return render_template('risco.html', imagem_url_template=IMAGEM_URL_TEMPLATE)


@bp_risco.route('/api/ces')
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
        resultados = busca_ces_risco(
            session,
            tipo_janela=tipo_janela,
            filtros=filtros,
            cursor_id=cursor_id,
            limit=limit,
        )
    except ValueError as err:
        return jsonify({'erro': str(err)}), 400

    cards = [_serializa_card(monta_card(ciclo, ce)) for ciclo, ce in resultados]
    next_cursor = cards[-1]['ciclo_de_vida_id'] if len(cards) == limit else None

    return jsonify({
        'cards': cards,
        'next_cursor': next_cursor,
        'tipo_janela': tipo_janela,
        'filtros': filtros,
    })
