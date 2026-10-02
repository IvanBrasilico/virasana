# coding: utf-8
"""
Blueprint da tela "Completude de escaneamentos" (menu NAVII).

  GET /completude_escaneamentos/                 -> os dois fluxos (TRA e operador) por recinto
  GET /completude_escaneamentos/recinto/<codigo> -> contêineres de um recinto em um fluxo (?fluxo=tra|operador)

Lógica em `busca_completude.py`. Substitui, no padrão visual do NAVII, a tela
antiga `/analise/completude-escaneamentos-recintos-secos` (que continua no ar)
e acrescenta o fluxo de desembaraço no operador. Rotas sem redirect da barra
final e URLs só por `url_for`/`script_root` (a app roda sob /virasana atrás de
proxy — ver CLAUDE.md).
"""
from datetime import date, datetime, time, timedelta

from flask import Blueprint, abort, current_app, render_template, request
from flask_login import login_required

from virasana.completude.busca_completude import FLUXOS, detalhe, nome_recinto, panorama, totais

bp_completude = Blueprint('completude', __name__, url_prefix='/completude_escaneamentos')

DIAS_PADRAO = 7
MAXIMO_DIAS = 92


class ParametrosInvalidos(ValueError):
    pass


def _data(nome: str, padrao: date) -> date:
    valor = request.args.get(nome)
    if not valor:
        return padrao
    try:
        return datetime.strptime(valor, '%Y-%m-%d').date()
    except ValueError:
        raise ParametrosInvalidos(f'Data inválida: {valor}')


def periodo() -> tuple:
    """(início, fim) das datas da tela, validados; fim inclui o dia inteiro."""
    hoje = date.today()
    inicio, fim = _data('inicio', hoje - timedelta(days=DIAS_PADRAO)), _data('fim', hoje)
    if fim < inicio:
        raise ParametrosInvalidos('A data final é anterior à inicial.')
    if (fim - inicio).days > MAXIMO_DIAS:
        raise ParametrosInvalidos(f'Período máximo: {MAXIMO_DIAS} dias.')
    return inicio, fim


def _intervalo(inicio: date, fim: date) -> tuple:
    return datetime.combine(inicio, time.min), datetime.combine(fim, time.max)


def _sem_oog() -> bool:
    """Excluir os potenciais OOG (open top/plataforma) da cobrança de escaneamento?"""
    return request.args.get('sem_oog') in ('1', 'true', 'on')


def _urls_imagem() -> dict:
    """Miniatura e visualização da imagem (rotas de virasana/views.py), com o prefixo da app."""
    return {'mini': request.script_root + '/mini1/', 'arquivo': request.script_root + '/file?_id='}


@bp_completude.route('/', strict_slashes=False)  # sem redirect da barra final (proxy; ver CLAUDE.md)
@login_required
def index():
    contexto = {'erro': None, 'fluxos': None, 'sem_oog': _sem_oog()}
    try:
        inicio, fim = periodo()
        contexto['fluxos'] = panorama(current_app.config['db_session'], *_intervalo(inicio, fim),
                                      sem_oog=contexto['sem_oog'])
    except ParametrosInvalidos as err:
        contexto['erro'] = str(err)
        inicio, fim = date.today() - timedelta(days=DIAS_PADRAO), date.today()
    return render_template('navii_completude.html', inicio=inicio, fim=fim, **contexto)


@bp_completude.route('/recinto/<codigo>')
@login_required
def recinto(codigo):
    fluxo = request.args.get('fluxo', 'tra')
    if fluxo not in FLUXOS:
        abort(404)
    try:
        inicio, fim = periodo()
    except ParametrosInvalidos as err:
        return str(err), 400
    session = current_app.config['db_session']
    sem_oog = _sem_oog()
    linhas = detalhe(session, fluxo, codigo, *_intervalo(inicio, fim), sem_oog=sem_oog)
    resumo = totais([{
        'total': 1, 'sem_escaneamento': int(not lin['escaneamentos']), 'sem_imagem': int(not lin['imagens']),
        'total_saidas': int(bool(lin['saida_seco'] if fluxo == 'tra' else lin['saida'])),
        'saidas_sem_escaneamento': int(bool(lin['saida_seco'] if fluxo == 'tra' else lin['saida'])
                                       and not lin['escaneamentos']),
        'oog': int(lin['oog']), 'oog_sem_escaneamento': int(lin['oog'] and not lin['escaneamentos']),
    } for lin in linhas])
    return render_template('navii_completude_detalhe.html', fluxo=fluxo, dados_fluxo=FLUXOS[fluxo], codigo=codigo,
                           nome=nome_recinto(session, codigo), linhas=linhas, resumo=resumo,
                           inicio=inicio, fim=fim, urls_imagem=_urls_imagem(), sem_oog=sem_oog)
