# exportacao_app.py

from datetime import date, timedelta, datetime, time
from bhadrasana.main import mongodb
from flask import render_template, request, flash, url_for
from flask import Blueprint, render_template
from flask_wtf.csrf import generate_csrf

exportacao_app = Blueprint(
    'exportacao_app',
    __name__,
    url_prefix='/exportacao'
)

def configure(app):
    app.register_blueprint(exportacao_app)

@exportacao_app.route('/', methods=['GET'])
def index():
    return render_template(
        'exportacao.html',
        csrf_token=generate_csrf
    )




def get_imagens_container_data(mongodb, numero, inicio_scan, fim_scan, vazio=False) -> list:
    query = {
        'metadata.contentType': 'image/jpeg',
        'metadata.dataescaneamento': {'$gte': inicio_scan, '$lte': fim_scan}
    }

    # Adiciona o filtro por número apenas se fornecido
    if numero:
        query['metadata.numeroinformado'] = numero

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



def get_imagens_container_sem_data(mongodb, numero, vazio=False) -> list:
    query = {
        'metadata.contentType': 'image/jpeg',
    }

    # Adiciona o filtro por número apenas se fornecido
    if numero:
        query['metadata.numeroinformado'] = numero

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

    if not start and not end and numero:
        arquivos = get_imagens_container_sem_data(mongodb, numero)
        return render_template(
            'exportacao.html',
            arquivos=arquivos,
            csrf_token=generate_csrf
        )

    inicio_scan = datetime.strptime(start, '%Y-%m-%d')
    fim_scan = datetime.strptime(end, '%Y-%m-%d')

    arquivos = get_imagens_container_data(mongodb, numero, inicio_scan, fim_scan)

    return render_template(
        'exportacao.html',
        arquivos=arquivos,
        csrf_token=generate_csrf
    )
