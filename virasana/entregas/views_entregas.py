# coding: utf-8
"""
Blueprint da consulta "Entregas por CNPJ".

  GET /entregas_cnpj/            -> tela (formulário + tabela de resultados)
  GET /entregas_cnpj/planilha    -> mesma consulta, em planilha Excel

Dados: `virasana/entregas/busca_entregas.py`.
"""
from datetime import date, datetime, timedelta

from flask import Blueprint, current_app, render_template, request, send_file
from flask_login import login_required

import io

from virasana.entregas.busca_entregas import (
    COLUNAS, DIAS_ANTES_DO_MANIFESTO, ParametrosInvalidos, busca_entregas, planilha_xlsx,
)

bp_entregas = Blueprint('entregas', __name__, url_prefix='/entregas_cnpj')

LIMITE_LINHAS_TELA = 1000


def _data(nome: str):
    valor = request.args.get(nome)
    if not valor:
        return None
    try:
        return datetime.strptime(valor, '%Y-%m-%d').date()
    except ValueError:
        raise ParametrosInvalidos(f'Data inválida: {valor}')


def _consulta():
    return busca_entregas(
        current_app.config['db_session'], request.args.get('cnpj'), _data('inicio'), _data('fim'),
        DIAS_ANTES_DO_MANIFESTO,
    )


@bp_entregas.route('/', strict_slashes=False)  # sem redirect da barra final (proxy; ver CLAUDE.md)
@login_required
def index():
    hoje = date.today()
    contexto = {
        'cnpj': request.args.get('cnpj', ''),
        'inicio': request.args.get('inicio') or (hoje - timedelta(days=30)).isoformat(),
        'fim': request.args.get('fim') or hoje.isoformat(),
        'colunas': COLUNAS, 'resultado': None, 'erro': None, 'limite': LIMITE_LINHAS_TELA,
    }
    if request.args.get('cnpj'):
        try:
            contexto['resultado'] = _consulta()
        except ParametrosInvalidos as err:
            contexto['erro'] = str(err)
    return render_template('entregas_cnpj.html', **contexto)


@bp_entregas.route('/planilha')
@login_required
def planilha():
    try:
        resultado = _consulta()
    except ParametrosInvalidos as err:
        return str(err), 400
    nome = f"entregas_{resultado['cnpj']}_{request.args.get('inicio')}_{request.args.get('fim')}.xlsx"
    return send_file(
        io.BytesIO(planilha_xlsx(resultado)), as_attachment=True, download_name=nome,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )
