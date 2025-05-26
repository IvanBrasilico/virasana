# exportacao_app.py
'''
from datetime import date, timedelta, datetime, time

import pandas as pd
from flask_wtf.csrf import generate_csrf
from werkzeug.utils import redirect

from ajna_commons.flask.log import logger
from bhadrasana.main import mongodb
from bhadrasana.models.apirecintos_risco import Pais
from flask import render_template, request, flash, url_for
from flask_login import login_required
from virasana.forms.filtros import FormFiltroData, FormFiltroEscaneamento
from virasana.integracao.mercante.mercantealchemy import Item
'''

from flask import Blueprint, render_template

exportacao_app = Blueprint(
    'exportacao_app',
    __name__,
    url_prefix='/exportacao'
)

def configure(app):
    app.register_blueprint(exportacao_app)

@exportacao_app.route('/', methods=['GET'])
def index():
    return render_template('exportacao.html')



'''
def get_imagens_container_data(mongodb, numero, inicio_scan, fim_scan, vazio=False) -> list:
    query = {
        'metadata.numeroinformado': numero,
        'metadata.contentType': 'image/jpeg',
        'metadata.dataescaneamento': {'$gte': inicio_scan, '$lte': fim_scan}
    }
    projection = {
        'metadata.numeroinformado': 1,
        'metadata.dataescaneamento': 1,
        'metadata.predictions.vazio': 1
    }

    cursor = (
        mongodb['fs.files']
        .find(query, projection)
        .sort('metadata.dataescaneamento', -1)
        .limit(10)
    )

    return list(cursor)

@exportacao_app.route('/stats', methods=['POST'])
def stats():
    numero = request.form.get('numero')
    start = request.form.get('start')
    end = request.form.get('end')

    if not start or not end:
        flash('Por favor, selecione datas válidas.')
        return redirect(url_for('exportacao'))

    inicio_scan = datetime.strptime(start, '%Y-%m-%d')
    fim_scan = datetime.strptime(end, '%Y-%m-%d')

    arquivos = get_imagens_container_data(mongodb, numero, inicio_scan, fim_scan)

    return render_template(
        'exportacao.html',
        arquivos=arquivos,
        csrf_token=generate_csrf()
    )
'''