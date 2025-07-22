import json

import gridfs
from flask import request, jsonify, flash, redirect, url_for, render_template

from virasana.forms.InspecaoNaoInvasiva import InspecaoNaoInvasivaForm
from virasana.views import csrf

# Dicionário para traduzir codigoRecinto
RECINTO_DICT = {
    "8931305": "TRANSBRASA",
}


def processar_inspecaonaoinvasiva(mongodb, json_original, arquivo_imagem):
    # 3) Montar metadata
    metadata = {}
    metadata['dataescaneamento'] = json_original.get('dataHoraOcorrencia')
    lista_conteineres = json_original.get('listaConteineresUld', [])
    numeroinformado = None
    if lista_conteineres and len(lista_conteineres) > 0:
        numeroinformado = lista_conteineres[0].get('numeroConteiner') or lista_conteineres[0].get('numero')
    metadata['numeroinformado'] = numeroinformado
    metadata['unidade'] = 'ALFSTS'
    codigo_recinto = json_original.get('codigoRecinto')
    recinto = RECINTO_DICT.get(codigo_recinto, 'RECINTO_NAO_ENCONTRADO')
    metadata['recinto'] = recinto

    # 4) Salvar arquivo no GridFS com metadata
    # arquivo_imagem.stream já é um arquivo-like object (bytes)
    fs = gridfs.GridFS(mongodb)
    file_id = fs.put(arquivo_imagem.stream, filename=arquivo_imagem.filename, metadata=metadata)


def configure(app):
    """Configura rotas para evento."""


    @app.route('/inspecaonaoinvasiva', methods=['GET', 'POST'])
    def inspecaonaoinvasiva():
        mongodb = app.config['mongodb']
        form = InspecaoNaoInvasivaForm()
        if form.validate_on_submit():
            json_data = {
                "dataHoraOcorrencia": form.dataHoraOcorrencia.data,
                "codigoRecinto": form.codigoRecinto.data,
                "listaConteineresUld": [
                    {"numeroConteiner": form.numeroConteiner.data}
                ]
            }

            try:
                file_id = processar_inspecaonaoinvasiva(mongodb, json_data, form.imagem.data)
                flash('Evento enviado com sucesso! File ID: ' + str(file_id), 'success')
            except Exception as e:
                flash('Erro ao enviar evento: ' + str(e), 'danger')

            return redirect(url_for('inspecaonaoinvasiva'))

        return render_template('inspecaonaoinvasiva.html', oform=form)

    @csrf.exempt
    @app.route('/api/inspecaonaoinvasiva', methods=['POST'])
    def api_inspecaonaoinvasiva():
        mongodb = app.config['mongodb']
        try:
            # 1) Receber JSON da forma multipart, campo 'json'
            if 'json' not in request.form:
                return jsonify({"error": "Campo form 'json' obrigatório"}), 400
            json_str = request.form['json']
            json_original = json.loads(json_str)

            # 2) Receber arquivo jpeg
            if 'imagem' not in request.files:
                return jsonify({"error": "Arquivo 'imagem' obrigatório"}), 400
            arquivo_imagem = request.files['imagem']

            # Verificar extensão do arquivo
            if not arquivo_imagem.filename.lower().endswith('.jpeg') and not arquivo_imagem.filename.lower().endswith(
                    '.jpg'):
                return jsonify({"error": "Arquivo deve ser .jpeg ou .jpg"}), 400

            file_id = processar_inspecaonaoinvasiva(mongodb, json_original, arquivo_imagem)
            return jsonify({"message": "Salvo com sucesso", "file_id": str(file_id)}), 201

        except Exception as e:
            return jsonify({"error": str(e)}), 500
