# coding: utf-8
"""
Blueprint da tela "Risco histórico das Fichas".

  GET /risco_historico/          -> tela (filtros + resumo + uma tabela por dimensão)
  GET /risco_historico/planilha  -> mesma extração, em planilha Excel

Só extração e visualização (não altera outras telas). A lógica é a do script
`analytic_scripts/dados_risco_perdimentos.py`. Rotas sem redirect da barra
final e URLs só por `url_for` (a app roda sob /virasana atrás de proxy — ver
CLAUDE.md).
"""
import io
import threading
import time
from datetime import date, datetime, timedelta

from flask import Blueprint, current_app, render_template, request, send_file
from flask_login import login_required

from virasana.analytic_scripts.dados_risco_perdimentos import (
    DIMENSOES, TIPOS_PADRAO, TIPOS_RESULTADO, extrai, indicadores, planilha_xlsx, resumo,
)

bp_risco_historico = Blueprint('risco_historico', __name__, url_prefix='/risco_historico')

MINIMO_PADRAO = 2
LINHAS_POR_DIMENSAO = 100
FICHAS_POR_LINHA = 10
CACHE_SEGUNDOS = 30 * 60

# A extração lê todas as fichas do período (~1.500 em produção): guarda o
# resultado por parâmetros por alguns minutos, para a planilha e a reabertura
# da tela não repetirem as consultas. Cache por processo, só leitura.
_cache = {}
_trava = threading.Lock()


class ParametrosInvalidos(ValueError):
    pass


def _data(nome: str):
    valor = request.args.get(nome)
    if not valor:
        return None
    try:
        return datetime.strptime(valor, '%Y-%m-%d').date()
    except ValueError:
        raise ParametrosInvalidos(f'Data inválida: {valor}')


def parametros() -> dict:
    tipos = tuple(sorted({int(t) for t in request.args.getlist('tipos') if t.isdigit()} & set(TIPOS_RESULTADO)))
    try:
        minimo = int(request.args.get('minimo') or MINIMO_PADRAO)
    except ValueError:
        raise ParametrosInvalidos('Mínimo de fichas inválido.')
    p = {
        'inicio': _data('inicio'), 'fim': _data('fim'),
        'tipos': tipos or TIPOS_PADRAO,
        'minimo': max(1, min(minimo, 100)),
        'exportacao': request.args.get('exportacao') in ('1', 'on', 'true'),
    }
    if p['inicio'] and p['fim'] and p['fim'] < p['inicio']:
        raise ParametrosInvalidos('A data final é anterior à inicial.')
    return p


def extracao(p: dict) -> dict:
    chave = (p['inicio'], p['fim'], p['tipos'], p['exportacao'])
    agora = time.monotonic()
    with _trava:
        for velha in [k for k, (quando, _) in _cache.items() if agora - quando > CACHE_SEGUNDOS]:
            del _cache[velha]
        if chave in _cache:
            return _cache[chave][1]
    inicio = datetime.combine(p['inicio'], datetime.min.time()) if p['inicio'] else None
    fim = datetime.combine(p['fim'] + timedelta(days=1), datetime.min.time()) if p['fim'] else None
    conexao = current_app.config['db_session'].connection()
    resultado = extrai(conexao, p['tipos'], inicio, fim, p['exportacao'])
    with _trava:
        _cache[chave] = (agora, resultado)
    return resultado


def limpa_cache():
    with _trava:
        _cache.clear()


@bp_risco_historico.route('/', strict_slashes=False)  # sem redirect da barra final (proxy; ver CLAUDE.md)
@login_required
def index():
    contexto = {'erro': None, 'resumo': None, 'tabelas': None, 'dimensoes': DIMENSOES,
                'tipos_resultado': TIPOS_RESULTADO, 'linhas_por_dimensao': LINHAS_POR_DIMENSAO,
                'fichas_por_linha': FICHAS_POR_LINHA, 'hoje': date.today()}
    try:
        p = parametros()
        if request.args.get('pesquisar'):
            dados = extracao(p)
            contexto['resumo'] = resumo(dados)
            contexto['tabelas'] = indicadores(dados)
    except ParametrosInvalidos as err:
        contexto['erro'] = str(err)
        p = {'inicio': None, 'fim': None, 'tipos': TIPOS_PADRAO, 'minimo': MINIMO_PADRAO, 'exportacao': False}
    contexto['p'] = p
    return render_template('risco_historico.html', **contexto)


@bp_risco_historico.route('/planilha')
@login_required
def planilha():
    try:
        p = parametros()
    except ParametrosInvalidos as err:
        return str(err), 400
    descricao = {'tipos': ', '.join(TIPOS_RESULTADO[t] for t in p['tipos']),
                 'inicio': p['inicio'] or 'início', 'fim': p['fim'] or 'hoje',
                 'exportacao': 'incluída' if p['exportacao'] else 'excluída'}
    nome = f'risco_historico_fichas_{date.today():%Y%m%d}.xlsx'
    return send_file(
        io.BytesIO(planilha_xlsx(extracao(p), descricao)), as_attachment=True, download_name=nome,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )
