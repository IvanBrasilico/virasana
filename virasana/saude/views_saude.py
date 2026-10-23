"""Tela SAÚDE DO SISTEMA (`/saude/`, blueprint `bp_saude`) — painel do health check (23/10/2026).

Só o perfil Supervisor do Fichas (`ovr_perfis_usuarios`, a mesma regra das estatísticas de uso) vê a tela e pede
rodadas (decisão do usuário); os demais recebem 403. Os dados vêm do histórico no Mongo do virasana (`historico.py`),
gravado pelo serviço `servico_saude.py`; a tela só lê e, com "Rodar agora", grava um pedido que o serviço atende na
próxima volta (até 1 min).
"""
from datetime import datetime

from flask import Blueprint, current_app, jsonify, render_template, request, url_for
from flask_login import current_user, login_required
from werkzeug.routing import BuildError

from virasana.saude import historico
from virasana.saude.servico_saude import HORA_AGENDADA, INTERVALO_SEGUNDOS, PARADO_MINUTOS

bp_saude = Blueprint('saude', __name__, url_prefix='/saude')
LIMITE_RODADAS = 30


def _usuario():
    return getattr(current_user, 'id', None)


def eh_supervisor() -> bool:
    from virasana.navii.estatisticas_uso import acesso_do_usuario
    usuario = _usuario()
    return bool(usuario) and acesso_do_usuario(current_app.config['db_session'], usuario).supervisor


def _db():
    return current_app.config.get('mongodb')


def _negado():
    return jsonify({'erro': 'Acesso restrito ao perfil Supervisor do Fichas.'}), 403


def _sem_mongo():
    return jsonify({'erro': 'Banco do histórico (Mongo) não configurado no servidor.'}), 503


def _navii_url() -> str:
    try:
        return url_for('navii.index')
    except BuildError:
        return request.script_root + '/'


@bp_saude.route('/', strict_slashes=False)  # sem redirect da barra final (proxy; ver CLAUDE.md)
@login_required
def index():
    if not eh_supervisor():
        return render_template('saude.html', restrito=True, navii_url=_navii_url()), 403
    return render_template('saude.html', restrito=False, navii_url=_navii_url(),
                           painel_url=url_for('saude.api_painel'), rodadas_url=url_for('saude.api_rodadas'),
                           rodada_url=url_for('saude.api_rodada', rodada_id='ID_DA_RODADA'),
                           rodar_url=url_for('saude.api_rodar'), hora_agendada=HORA_AGENDADA,
                           retencao_dias=historico.RETENCAO_DIAS, intervalo=INTERVALO_SEGUNDOS)


@bp_saude.route('/api/painel')
@login_required
def api_painel():
    if not eh_supervisor():
        return _negado()
    if _db() is None:
        return _sem_mongo()
    return jsonify(historico.painel(_db(), datetime.now(), PARADO_MINUTOS))


@bp_saude.route('/api/rodadas')
@login_required
def api_rodadas():
    if not eh_supervisor():
        return _negado()
    if _db() is None:
        return _sem_mongo()
    try:
        antes = datetime.fromisoformat(request.args['antes']) if request.args.get('antes') else None
    except ValueError:
        return jsonify({'erro': 'data inválida em "antes"'}), 400
    return jsonify({'rodadas': historico.lista_rodadas(_db(), antes, LIMITE_RODADAS), 'limite': LIMITE_RODADAS})


@bp_saude.route('/api/rodada/<rodada_id>')
@login_required
def api_rodada(rodada_id):
    if not eh_supervisor():
        return _negado()
    if _db() is None:
        return _sem_mongo()
    rodada = historico.uma_rodada(_db(), rodada_id)
    return jsonify(rodada) if rodada else (jsonify({'erro': 'rodada não encontrada'}), 404)


@bp_saude.route('/api/rodar', methods=['POST'])
@login_required
def api_rodar():
    """Pede uma rodada agora: o serviço a faz na próxima volta (havendo um pedido em aberto, devolve esse)."""
    if not eh_supervisor():
        return _negado()
    if _db() is None:
        return _sem_mongo()
    pedido = historico.pede_rodada(_db(), _usuario(), datetime.now())
    return jsonify({'estado': pedido['estado'], 'usuario': pedido.get('usuario'),
                    'pedido_em': pedido['pedido_em'].isoformat(timespec='seconds')}), 202
