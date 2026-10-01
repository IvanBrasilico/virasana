# coding: utf-8
"""
Blueprint Flask do NAVI-I — Análise de risco na importação.

Expõe:
  GET /navii/            -> página HTML (filtros + cards com rolagem infinita)
  GET /navii/api/ces      -> JSON paginado (consumido pelo JS da página); na
                             primeira página (sem cursor) traz também `total`
                             de contêineres que o filtro encontrou na base
  GET /navii/api/sugestoes -> autocompletar de porto, NCM e recinto
  GET|POST|DELETE /navii/api/filtros -> filtros salvos do usuário (MongoDB,
                             ver `filtros_salvos.py`)

Depende de `busca_ces_navii`, `conta_conteineres_navii` e `monta_cards_navii`,
definidos em `busca_navii.py`.
"""
from datetime import datetime

from flask import Blueprint, current_app, jsonify, render_template, request, url_for
from flask_login import current_user, login_required
from werkzeug.routing import BuildError

from virasana.navii import filtros_salvos, referencias
from virasana.navii.busca_navii import (
    CAMPOS_LISTA, busca_ces_navii, conta_conteineres_navii, monta_cards_navii,
    PORTO_ESCALA_PADRAO, _janela_efetiva, _janela_previsao, sugere_recintos,
)

bp_navii = Blueprint(
    'navii', __name__,
    url_prefix='/navii',
    template_folder='templates',
)


def _imagem_url_template() -> str:
    """URL da miniatura da imagem (rota `/mini1/<_id>` de virasana/views.py),
    com 'imagem_id' no lugar do id — o JS troca pelo id de cada imagem.

    Em produção a app roda sob o prefixo /virasana atrás de proxy: a URL
    precisa sair do `url_for`/`script_root`, nunca fixa. Sem a rota registrada
    (testes, app isolada), monta pelo `script_root`.
    """
    try:
        return url_for('mini', _id='imagem_id')
    except BuildError:
        return request.script_root + '/mini1/imagem_id'


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


@bp_navii.route('/', strict_slashes=False)  # sem redirect da barra final (proxy; ver CLAUDE.md)
@login_required
def index():
    return render_template(
        'navii.html',
        imagem_url_template=_imagem_url_template(),
        defaults_periodo=_defaults_periodo(),
        porto_escala_padrao=PORTO_ESCALA_PADRAO,
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
    for campo in CAMPOS_LISTA:  # repetidos (?ncm=8471&ncm=6109) ou separados por vírgula
        valores = [v for v in request.args.getlist(campo) if v.strip()]
        if valores:
            filtros[campo] = valores
    for campo in ('produto', 'empresa'):
        valor = request.args.get(campo)
        if valor and valor.strip():
            filtros[campo] = valor.strip()
    for campo in ('concluido', 'com_imagem'):
        if request.args.get(campo) in ('1', 'true', 'True'):
            filtros[campo] = True
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
        # Contagem só na primeira página: nas seguintes a tela já tem o total.
        total = None if cursor_id else conta_conteineres_navii(session, tipo_janela, filtros)
    except ValueError as err:
        return jsonify({'erro': str(err)}), 400

    cards = [_serializa_card(card) for card in monta_cards_navii(session, resultados)]
    next_cursor = cards[-1]['item_id'] if len(cards) == limit else None

    return jsonify({
        'cards': cards,
        'next_cursor': next_cursor,
        'total': total,
        'tipo_janela': tipo_janela,
        'filtros': filtros,
    })


def _usuario():
    """Identificação do usuário logado (dono dos filtros salvos)."""
    return getattr(current_user, 'id', None)


@bp_navii.route('/api/sugestoes')
@login_required
def api_sugestoes():
    """Autocompletar dos campos da tela: ?campo=porto|ncm|recinto&q=texto."""
    campo, termo = request.args.get('campo'), request.args.get('q', '')
    if campo == 'porto':
        sugestoes = referencias.sugere_portos(termo)
    elif campo == 'ncm':
        sugestoes = referencias.sugere_ncm(termo)
    elif campo == 'recinto':
        sugestoes = sugere_recintos(current_app.config['db_session'], termo)
    else:
        return jsonify({'erro': 'campo inválido'}), 400
    return jsonify({'sugestoes': sugestoes})


@bp_navii.route('/api/filtros', methods=['GET', 'POST', 'DELETE'])
@login_required
def api_filtros_salvos():
    """Filtros salvos do usuário logado.

    GET lista; POST {nome, tipo_janela, filtros} cria ou substitui; DELETE
    ?nome= apaga. Todas devolvem a lista atualizada em `filtros_salvos`.
    """
    usuario = _usuario()
    if not usuario:
        return jsonify({'erro': 'usuário não identificado'}), 401
    col = filtros_salvos.colecao(current_app.config['mongodb'])
    try:
        if request.method == 'POST':
            dados = request.get_json(silent=True) or {}
            filtros_salvos.salva(col, usuario, dados.get('nome'), dados.get('tipo_janela'), dados.get('filtros'))
        elif request.method == 'DELETE':
            if not filtros_salvos.apaga(col, usuario, request.args.get('nome')):
                return jsonify({'erro': 'filtro não encontrado'}), 404
    except filtros_salvos.FiltroInvalido as err:
        return jsonify({'erro': str(err)}), 400
    salvos = [{**d, 'atualizado': _serializa_valor(d.get('atualizado')), 'criado': _serializa_valor(d.get('criado'))}
              for d in filtros_salvos.lista(col, usuario)]
    return jsonify({'filtros_salvos': salvos})
