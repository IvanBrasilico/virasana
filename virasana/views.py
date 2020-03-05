"""Coleção de views da interface web do módulo Virasana.

Módulo Virasana é o Servidor de imagens e a interface para carga,
consulta e integração das imagens com outras bases.

"""
import json
import os
from base64 import b64encode
from datetime import date, datetime, timedelta
from json import JSONDecodeError
from sys import platform

import ajna_commons.flask.login as login_ajna
import ajna_commons.flask.user as user_ajna
import requests
from ajna_commons.flask.conf import (BSON_REDIS, DATABASE, logo, MONGODB_URI,
                                     PADMA_URL, SECRET, redisdb)
from ajna_commons.flask.log import logger
from ajna_commons.utils import ImgEnhance
from ajna_commons.utils.images import bytes_toPIL, mongo_image, PIL_tobytes, recorta_imagem
from ajna_commons.utils.sanitiza import mongo_sanitizar
from bson import json_util
from bson.objectid import ObjectId
from flask import (Flask, Response, abort, flash, jsonify, redirect,
                   render_template, request, url_for)
from flask_bootstrap import Bootstrap
from flask_login import current_user, login_required
# from flask_cors import CORS
from flask_nav import Nav
from flask_nav.elements import Navbar, Separator, Subgroup, View
from flask_wtf import FlaskForm
from flask_wtf.csrf import CSRFProtect
from gridfs import GridFS
from pymongo import MongoClient
from sqlalchemy.orm import sessionmaker
from wtforms import (BooleanField, DateField, FloatField, IntegerField,
                     PasswordField, SelectField, StringField)
from wtforms.validators import DataRequired, optional

from virasana.forms.auditoria import FormAuditoria, SelectAuditoria
from virasana.forms.filtros import FormFiltro
from virasana.integracao import (CHAVES_GRIDFS, carga, dict_to_html,
                                 dict_to_text, info_ade02, plot_bar_plotly,
                                 plot_pie_plotly, stats_resumo_imagens,
                                 summary,
                                 TIPOS_GRIDFS)
from virasana.integracao.due import due_mongo
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Item
from virasana.integracao.padma import consulta_padma
from virasana.models.anomalia_lote import get_conhecimentos_filtro, \
    get_ids_score_conhecimento_zscore
from virasana.models.auditoria import Auditoria
from virasana.models.image_search import ImageSearch
from virasana.models.models import Ocorrencias, Tags
from virasana.models.text_index import TextSearch
from virasana.workers.dir_monitor import BSON_DIR
from virasana.workers.tasks import raspa_dir, trata_bson

app = Flask(__name__, static_url_path='/static')
# app.jinja_env.filters['zip'] = zip
csrf = CSRFProtect(app)
Bootstrap(app)
nav = Nav()
nav.init_app(app)

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'static')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def configure_app(mongodb, mysql):
    """Configurações gerais e de Banco de Dados da Aplicação."""

    @app.route('/virasana/login', methods=['GET', 'POST'])
    def virasana_login():
        return login_ajna.login_view(request)
    login_ajna.login_manager.login_view = 'virasana_login'

    app.config['DEBUG'] = os.environ.get('DEBUG', 'None') == '1'
    if app.config['DEBUG'] is True:
        app.jinja_env.auto_reload = True
        app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.secret_key = SECRET
    app.config['SECRET_KEY'] = SECRET
    app.config['SESSION_TYPE'] = 'filesystem'
    login_ajna.configure(app)
    user_ajna.DBUser.dbsession = mongodb
    app.config['mongodb'] = mongodb
    app.config['mysql'] = mysql
    try:
        img_search = None
        img_search = ImageSearch(mongodb)
        app.config['img_search'] = img_search
    except (IOError, FileNotFoundError):
        pass
    app.config['text_search'] = TextSearch(mongodb)
    return app


stats_cache = {}


def allowed_file(filename, extensions):
    """Checa extensões permitidas."""
    return '.' in filename and \
           filename.rsplit('.', 1)[-1].lower() in extensions


@app.route('/')
def index():
    """View retorna index.html ou login se não autenticado."""
    # print(current_user)
    if current_user.is_authenticated:
        return render_template('index.html')
    else:
        return redirect(url_for('commons.login'))


def valid_file(file, extensions=['bson']):
    """Valida arquivo passado e retorna validade e mensagem."""
    if not file or file.filename == '' or not allowed_file(file.filename, extensions):
        if not file:
            mensagem = 'Arquivo nao informado'
        elif not file.filename:
            mensagem = 'Nome do arquivo vazio'
        else:
            mensagem = 'Nome de arquivo não permitido: ' + \
                       file.filename
            # print(file)
        return False, mensagem
    return True, None


@app.route('/uploadbson', methods=['GET', 'POST'])
@csrf.exempt
# @login_required
def upload_bson():
    """Função simplificada para upload do arquivo de uma extração.

    Ver API/Upload BSON
    """
    taskid = ''
    if request.method == 'POST':
        # check if the post request has the file part
        file = request.files.get('file')
        validfile, mensagem = valid_file(file)
        if not validfile:
            flash(mensagem)
            return redirect(request.url)
        content = file.read()
        if platform == 'win32':
            with MongoClient(host=MONGODB_URI) as conn:
                db = conn[DATABASE]
                trata_bson(content, db)
        else:
            # print('Escrevendo no REDIS')
            d = {'bson': b64encode(content).decode('utf-8'),
                 'filename': file.filename}
            redisdb.rpush(BSON_REDIS + file.filename, json.dumps(d))
            result = raspa_dir.delay(file.filename)
            taskid = result.id
            # print('taskid', taskid)
    if taskid:
        return redirect(url_for('list_files', taskid=taskid))
    return redirect(url_for('list_files'))


@app.route('/api/uploadbson', methods=['POST'])
@csrf.exempt
# @login_required
def api_upload():
    """Função para upload via API de um arquivo BSON.

    Coloca o arquivo numa queue do Banco de Dados Redis
    e inicia uma task Celery. O resultado do processamento efetivo do
    arquivo pode ser acompanhado na view
    py:func:`task_progress`

    Args:
        file: arquivo BSON gerado pelo AJNA e enviado via HTTP POST

    Returns:
        json['success']: True ou False
        json['taskid']: ID da task do celery a ser monitorada

    """
    sync = request.form.get('sync', 'True')
    todir = request.form.get('todir', 'False')
    data = {'success': False,
            'mensagem': 'Task iniciada',
            'taskid': ''}
    try:
        # ensure a bson was properly uploaded to our endpoint
        file = request.files.get('file')
        validfile, mensagem = valid_file(file)
        if not validfile:
            data['mensagem'] = mensagem
            return jsonify(data)

        # else
        if todir == 'True':
            # Apenas salva em sistema de arquivo para carga posterior
            with open(os.path.join(BSON_DIR, file.filename), 'b') as out:
                file.write(out)
            return jsonify(data)

        # else
        content = file.read()
        if sync == 'True' or platform == 'win32':
            with MongoClient(host=MONGODB_URI) as conn:
                db = conn[DATABASE]
                trata_bson(content, db)
            data['success'] = True
        else:
            d = {'bson': b64encode(content).decode('utf-8'),
                 'filename': file.filename}
            redisdb.rpush(BSON_REDIS + file.filename, json.dumps(d))
            result = raspa_dir.delay(file.filename)
            data['taskid'] = result.id
            data['success'] = True
    except Exception as err:
        logger.error(err, exc_info=True)
        data['mensagem'] = 'Excecao ' + str(err)

    return jsonify(data)


@app.route('/api/task/<taskid>')
# @login_required
def task_progress(taskid):
    """Retorna um json do progresso da celery task."""
    task = raspa_dir.AsyncResult(taskid)
    response = {'state': task.state}
    if task.info:
        response['current'] = task.info.get('current', ''),
        response['status'] = task.info.get('status', '')
    return jsonify(response)


@app.route('/list_files')
@login_required
def list_files():
    """Lista arquivos no banco MongoDB.

    Lista 10 arquivos mais recentes no banco MongoDB,
    por uploadDate mais recente.
    Se houver upload em andamento, informa.
    """
    db = app.config['mongodb']
    fs = GridFS(db)
    lista_arquivos = []
    for grid_data in fs.find().sort('uploadDate', -1).limit(10):
        lista_arquivos.append(grid_data.filename)
    taskid = request.args.get('taskid')
    task_info = None
    if taskid:
        task = raspa_dir.AsyncResult(taskid)
        task_info = task.info
    return render_template('importa_bson.html',
                           lista_arquivos=lista_arquivos,
                           task_info=task_info)


@app.route('/summary/<_id>')
# @login_required
def summarytext(_id=None):
    """Tela para exibição de um 'arquivo' do GridFS.

    Exibe os metadados associados a ele.
    """
    db = app.config['mongodb']
    fs = GridFS(db)
    grid_data = fs.get(ObjectId(_id))
    result = dict_to_text(summary(grid_data=grid_data)) + '\n' + \
             dict_to_text(carga.summary(grid_data=grid_data))
    return result


@app.route('/summaryhtml/<_id>')
@login_required
def summaryhtml(_id=None):
    """Tela para exibição de um 'arquivo' do GridFS.

    Exibe os metadados associados a ele.
    """
    db = app.config['mongodb']
    fs = GridFS(db)
    grid_data = fs.get(ObjectId(_id))
    result = dict_to_html(summary(grid_data=grid_data))
    return result


@app.route('/summaryjson/<_id>')
# @login_required
def summaryjson(_id=None):
    """Tela para exibição de um 'arquivo' do GridFS.

    Exibe os metadados associados a ele.
    """
    db = app.config['mongodb']
    fs = GridFS(db)
    grid_data = fs.get(ObjectId(_id))
    result = summary(grid_data=grid_data)
    result_carga = carga.summary(grid_data=grid_data)
    return jsonify({**result, **result_carga})


@app.route('/json/<_id>')
# @login_required
def json_get(_id=None):
    """Tela para exibição de um 'arquivo' do GridFS.

    Exibe os metadados associados a ele.
    """
    db = app.config['mongodb']
    fs = GridFS(db)
    grid_data = fs.get(ObjectId(_id))
    return json.dumps(grid_data.metadata,
                      sort_keys=True,
                      indent=4,
                      default=json_util.default)


class TagsForm(FlaskForm):
    tags = SelectField(u'Tags de usuário',
                       default=[0])


@app.route('/file/<_id>')
@app.route('/file')
@login_required
def file(_id=None):
    """Tela para exibição de um 'arquivo' do GridFS.

    Exibe o arquivo e os metadados associados a ele.
    """
    db = app.config['mongodb']
    fs = GridFS(db)
    tags_object = Tags(db)
    form_tags = TagsForm()
    form_tags.tags.choices = tags_object.tags_text
    if request.args.get('filename'):
        filename = mongo_sanitizar(request.args.get('filename'))
        logger.warn('Filename %s ' % filename)
        grid_data = fs.find_one({'filename': filename})
    else:
        if not _id:
            _id = request.args.get('_id')
        grid_data = fs.get(ObjectId(_id))
    # print(grid_data)
    if grid_data:
        summary_ = dict_to_html(summary(grid_data=grid_data))
        summary_carga = dict_to_html(carga.summary(grid_data=grid_data))
        tags = tags_object.list(_id)
        ocorrencias = Ocorrencias(db).list(_id)
    else:
        summary_ = summary_carga = 'Arquivo não encontrado.'
        ocorrencias = []
    return render_template('view_file.html',
                           myfile=grid_data,
                           summary=summary_,
                           summary_carga=summary_carga,
                           form_tags=form_tags, tags=tags,
                           ocorrencias=ocorrencias)


@login_required
@csrf.exempt
@app.route('/ocorrencia/add', methods=['POST', 'GET'])
def ocorrencia_add():
    """Função para inserção de ocorrência na imagem

    Faz update no fs.files, inserindo em um array o nome do usuário ativo
    e o texto da ocorrência passada.

    Args:
        _id: ObjectId do arquivo
        texto: String (texto)

    Returns:
        json['success']: True ou False

    """
    _id = request.values.get('_id')
    texto = request.values.get('texto')
    try:
        db = app.config['mongodb']
        ocorrencias = Ocorrencias(db)
        success = ocorrencias.add(_id=ObjectId(_id),
                                  usuario=current_user.id,
                                  texto=texto)
    except Exception as err:
        logger.error(err, exc_info=True)
        return jsonify({'erro': str(err)})
    return image_ocorrencia(_id, success)


@login_required
@csrf.exempt
@app.route('/ocorrencia/del', methods=['POST', 'GET'])
def ocorrencia_del():
    """Função para exclusão de ocorrência na imagem

    Faz update no fs.files, excluindo do array a id_ocorrencia

    Args:
        _id: ObjectId do arquivo
        id_ocorrencia: String (texto)

    Returns:
        image_ocorrencia, passando
        data['success']: True ou False

    """
    _id = request.values.get('_id')
    id_ocorrencia = request.values.get('id_ocorrencia')
    try:
        db = app.config['mongodb']
        ocorrencias = Ocorrencias(db)
        success = ocorrencias.delete(_id=ObjectId(_id),
                                     id_ocorrencia=id_ocorrencia)
    except Exception as err:
        logger.error(err, exc_info=True)
        return jsonify({'erro': str(err)})
    return image_ocorrencia(_id, success)


def image_ocorrencia(_id, success=True):
    """Função para listar ocorrências na imagem

    Args:
        _id: ObjectId do arquivo
        success: Falso se houve erro em operação anterior

    Returns:
        json['success']: True ou False

    """
    data = {'success': success}
    try:
        db = app.config['mongodb']
        ocorrencias = Ocorrencias(db)
        data['ocorrencias'] = ocorrencias.list(ObjectId(_id))
    except Exception as err:
        logger.error(err, exc_info=True)
        data['error'] = str(err)
        data['success'] = False
        # raise
    return jsonify(data)


@login_required
@csrf.exempt
@app.route('/tag/add', methods=['POST', 'GET'])
def tag_add():
    """Função para inserção de tag na imagem

    Faz update no fs.files, inserindo em um array com o nome do usuário ativo
    e a tag passada.

    Args:
        _id: ObjectId do arquivo
        tag: String (app usa lista de códigos com tupla (id, desc))

    Returns:
        json['success']: True ou False

    """
    _id = request.values.get('_id')
    tag = request.values.get('tag')
    data = {'success': False}
    try:
        db = app.config['mongodb']
        tags = Tags(db)
        data['success'] = tags.add(_id=ObjectId(_id),
                                   usuario=current_user.id,
                                   tag=tag)
        data['tags'] = tags.list(ObjectId(_id))
    except Exception as err:
        logger.error(err, exc_info=True)
        data['error'] = str(err)
        # raise
    return jsonify(data)


@login_required
@csrf.exempt
@app.route('/tag/del', methods=['POST', 'GET'])
def tag_del():
    """Função para exclusão de tag na imagem

    Faz update no fs.files, excluindo do array o nome do usuário ativo
    e a tag passada, se existir.

    Args:
        _id: ObjectId do arquivo
        tag: String (app usa lista de códigos com tupla (id, desc))

    Returns:
        json['success']: True ou False

    """
    _id = request.values.get('_id')
    tag = request.values.get('tag')
    data = {'success': False}
    try:
        db = app.config['mongodb']
        tags = Tags(db)
        data['success'] = tags.delete(_id=ObjectId(_id),
                                      usuario=current_user.id,
                                      tag=tag)
        data['tags'] = tags.list(ObjectId(_id))
    except Exception as err:
        logger.error(err, exc_info=True)
        data['error'] = str(err)
        # raise
    return jsonify(data)


@app.route('/grid_data', methods=['POST', 'GET'])
# @login_required
@csrf.exempt
def grid_data():
    """Executa uma consulta no banco.

    Monta um dicionário de consulta a partir dos argumentos do get.
    Se encontrar registro, retorna registro inteiro via JSON (metadados),
    o arquivo (campo content) fica em fs.chunks e é recuperado pela view
    image_id.
    """
    # TODO: Refatorar conversões de/para MongoDB - dict - JSON (Ver Bhadrasana, tem algo feito nesse sentido)
    db = app.config['mongodb']
    if request.method == 'POST':
        # print(request.json)
        query = request.json['query']
        projection = request.json['projection']
        query_processed = {}
        for key, value in query.items():
            if isinstance(value, dict):
                value_processed = {}
                for key2, value2 in value.items():
                    try:
                        value_processed[key2] = datetime.strptime(value2, '%Y-%m-%d  %H:%M:%S')
                    except:
                        value_processed[key2] = mongo_sanitizar(value2)
                query_processed[key] = value_processed
            else:
                try:
                    query_processed[key] = datetime.strptime(value, '%Y-%m-%d  %H:%M:%S')
                except:
                    query_processed[key] = mongo_sanitizar(value)

        # logger.warning(query)
        # logger.warning(query_processed)
        # query = {mongo_sanitizar(key): mongo_sanitizar(value)
        #          for key, value in query.items()}
        # projection = {mongo_sanitizar(key): mongo_sanitizar(value)
        #          for key, value in projection.items()}
        # logger.warning(projection)
        linhas = db['fs.files'].find(query_processed, projection)
        result = []
        for linha in linhas:
            dict_linha = {}
            for key, value in linha.items():
                if isinstance(value, dict):
                    value_processed = {}
                    for key2, value2 in value.items():
                        try:
                            value_processed[key2] = datetime.strptime(value2, '%Y-%m-%d  %H:%M:%S')
                        except:
                            value_processed[key2] = str(value2)
                    dict_linha[key] = value_processed
                else:
                    try:
                        dict_linha[key] = datetime.strptime(value, '%Y-%m-%d  %H:%M:%S')
                    except:
                        dict_linha[key] = str(value)
            result.append(dict_linha)

    else:
        filtro = {mongo_sanitizar(key): mongo_sanitizar(value)
                  for key, value in request.args.items()}
        logger.warning(filtro)
        linhas = db['fs.files'].find(filtro)
        result = [{'_id': str(linha['_id']),
                   'contentType': str(linha['metadata'].get('contentType'))
                   }
                  for linha in linhas]
    status_code = 404
    if len(result) > 0:
        status_code = 200
    return jsonify(result), status_code


@app.route('/dues/update', methods=['POST'])
# @login_required
@csrf.exempt
def dues_update():
    """Recebe um JSON no formato [{_id1: due1}, ..., {_idn: duen}] e grava

    """
    db = app.config['mongodb']
    if request.method == 'POST':
        due_mongo.update_due(db, request.json)
    return jsonify({'status': 'DUEs inseridas/atualizadas'}), 201


@app.route('/image')
@login_required
def image():
    """Executa uma consulta no banco.

    Monta um dicionário de consulta a partir dos argumentos do get.
    Se encontrar registro, chama image_id.
    """
    db = app.config['mongodb']
    filtro = {key: value for key, value in
              mongo_sanitizar(request.args.items())}
    linha = db['fs.files'].find_one(filtro, {'_id': 1})
    if linha:
        return image_id(linha['_id'])
    return ''


def get_contrast_and_color_(request):
    contrast = request.values.get('contrast', 'False').lower() in ('on', 'true')
    color = request.values.get('color', 'False').lower() in ('on', 'true')
    return contrast, color


@app.route('/image/<_id>')
def image_id(_id):
    """Recupera a imagem do banco e serializa para stream HTTP.

    Estes métodos dispensam autenticação, pois é necessário ter um _id válido.
    O padrão é retornar um bounding box desenhado.
    Para evitar o bbox, passar ?bboxes=False na url
    """
    db = app.config['mongodb']
    bboxes = request.args.get('bboxes', 'True').lower() == 'true'
    image = mongo_image(db, _id, bboxes=bboxes)
    if image:
        contrast, color = get_contrast_and_color_(request)
        if contrast or color:
            PILimage = bytes_toPIL(image)
            if contrast:
                PILimage = ImgEnhance.enhancedcontrast_cv2(PILimage)
            if color:
                PILimage = ImgEnhance.expand_tocolor(PILimage)
            image = PIL_tobytes(PILimage)
        return Response(response=image, mimetype='image/jpeg')
    return 'Sem Imagem'


def get_image(_id, n, pil=False):
    """Recupera, recorta a imagem do banco e retorna."""
    db = app.config['mongodb']
    fs = GridFS(db)
    _id = ObjectId(_id)
    if fs.exists(_id):
        grid_data = fs.get(_id)
        if n is not None:
            n = int(n)
            preds = grid_data.metadata.get('predictions')
            if preds:
                bboxes = [pred.get('bbox') for pred in preds]
                if len(bboxes) >= n + 1 and bboxes[n]:
                    image = grid_data.read()
                    image = recorta_imagem(image, bboxes[n], pil=pil)
                    return image
    return None


def do_mini(_id, n):
    """Recupera, recorta a imagem do banco e serializa para stream HTTP."""
    image = get_image(_id, n)
    if image:
        return Response(response=image, mimetype='image/jpeg')
    return 'Sem imagem'


@app.route('/mini1/<_id>')
def mini(_id, n=0):
    """Recorta a imagem do banco e serializa para stream HTTP."""
    return do_mini(_id, 0)


@app.route('/mini2/<_id>')
def mini2(_id):
    """Link para imagem do segundo contêiner, se houver."""
    return do_mini(_id, 1)


class ImgForm(FlaskForm):
    cutoff = IntegerField(u'Cut Off', default=10)
    equalize = BooleanField(u'Equalize')
    colorize = BooleanField(u'Colorize')
    alpha = FloatField(u'Alpha', default=10)
    beta = FloatField(u'Beta', default=10)
    equalize2 = BooleanField(u'Equalize')
    cv2 = BooleanField('cv2')


@app.route('/view_image/<_id>')
@app.route('/view_image', methods=['POST', 'GET'])
@login_required
def view_image(_id=None):
    """Tela para exibição de um 'arquivo' do GridFS.

    Exibe o arquivo e filtros para melhoria da visão.
    """
    imgform = ImgForm(request.form)
    if request.method == 'POST':
        _id = request.form['_id']
        imgform.validate_on_submit()
    return render_template('view_image.html', _id=_id, imgform=imgform)


@app.route('/contrast')
def img_contrast():
    _id = request.args.get('_id')
    n = request.args.get('n', 0)
    cutoff = request.args.get('cutoff', '10')
    equalize = request.args.get('equalize', False) == 'True'
    colorize = request.args.get('colorize', False) == 'True'
    cv2 = request.args.get('cv2', False) == 'True'
    image = get_image(_id, n, pil=True)
    if image:
        cutoff = int(cutoff)
        image = ImgEnhance.autocontrast(image, cutoff=cutoff,
                                        colorize=colorize, equalize=equalize,
                                        cv2=cv2)
        image = PIL_tobytes(image)
        return Response(response=image, mimetype='image/jpeg')
    return 'Sem imagem'


@app.route('/contrast_cv2/<_id>')
def contrast_cv2(_id, n=0):
    image = get_image(_id, n, pil=True)
    if image:
        image = ImgEnhance.enhancedcontrast_cv2(image)
        image = PIL_tobytes(image)
        return Response(response=image, mimetype='image/jpeg')
    return 'Sem imagem'


@app.route('/equalize/<_id>')
def equalize(_id, n=0):
    image = get_image(_id, n, pil=True)
    if image:
        image = ImgEnhance.equalize(image)
        image = PIL_tobytes(image)
        return Response(response=image, mimetype='image/jpeg')
    return 'Sem imagem'


@app.route('/colorize')
def colorize():
    _id = request.args.get('_id')
    n = request.args.get('n', 0)
    alpha = request.args.get('alpha', '12')
    beta = request.args.get('beta', '14')
    equalize = request.args.get('equalize', False) == 'True'
    image = get_image(_id, n, pil=True)
    if image:
        alpha = float(alpha) / 10.
        beta = float(beta) / 10.
        image = ImgEnhance.expand_tocolor(
            image, alpha=alpha, beta=beta, equalize=equalize)
        image = PIL_tobytes(image)
        return Response(response=image, mimetype='image/jpeg')
    return 'Sem imagem'


lista_ids = []
"""
Para testes de desempenho: o endpoint minitest retorna uma imagem aleatória
Se for passado o parâmetro 'mini', faz também recorte
Para habilitar, criar lista_ids descomentando linhas abaixo
lista_ids = [
    linha['_id'] for linha in
    db['fs.files'].find(
        {'metadata.contentType': 'image/jpeg'}, {'_id': 1}
    ).limit(1000)
]
"""


@app.route('/minitest')
def minitest():
    """Retorna uma imagem aleatória.

    Se for passado o parâmetro 'mini', faz também recorte
    Deixar desabilitado em produção.
    """
    if not lista_ids:
        return abort(404)
    import random
    _id = lista_ids[random.randint(0, 100)]
    n = request.args.get('mini')
    return do_mini(_id, n)


filtros = dict()


def campos_chave():
    """Retorna campos chave para montagem de filtro."""
    return CHAVES_GRIDFS + carga.CHAVES_CARGA + info_ade02.CHAVES_RECINTO + due_mongo.CHAVES_DUE


def campos_chave_tipos():
    """Retorna campos chave para montagem de filtro."""
    return {**TIPOS_GRIDFS, **carga.TIPOS_CARGA}


@app.route('/filtro_personalizado', methods=['GET', 'POST'])
@login_required
def filtro():
    """Configura filtro personalizado."""
    _, user_filtros = recupera_user_filtros()
    if user_filtros is None:
        return jsonify([])
    # print(request.form)
    # print(request.args)
    campo = request.args.get('campo')
    if campo:
        valor = request.args.get('valor')
        if valor:  # valor existe, adiciona
            if campos_chave_tipos().get(campo) == bool:
                valor = valor.lower()
                user_filtros[campo] = (valor == 's' or valor == 'true' or valor == 'sim')
            elif campos_chave_tipos().get(campo) == date:
                # print('DATE!!!', valor)
                ldata = datetime.strptime(valor, '%Y/%m/%d')
                start = datetime.combine(ldata, datetime.min.time())
                end = datetime.combine(ldata, datetime.max.time())
                user_filtros[campo] = {'$lte': end, '$gte': start}
            else:
                user_filtros[campo] = mongo_sanitizar(valor)
        else:  # valor não existe, exclui chave
            user_filtros.pop(campo)
    result = [{'campo': k, 'valor': v} for k, v in user_filtros.items()]
    return jsonify(result)


class FilesForm(FlaskForm):
    """Valida pesquisa de arquivos.

    Usa wtforms para facilitar a validação dos campos de pesquisa da tela
    search_files.html

    """
    numero = StringField(u'Número', validators=[optional()], default='')
    start = DateField('Start', validators=[optional()])
    end = DateField('End', validators=[optional()])
    alerta = BooleanField('Alerta', validators=[optional()], default=False)
    ranking = BooleanField('Ranking', validators=[optional()], default=False)
    pagina_atual = IntegerField('Pagina', default=1)
    filtro_auditoria = SelectField(u'Filtros de Auditoria',
                                   default=0)
    tag_usuario = BooleanField('Exclusivamente Tag do usuário',
                               validators=[optional()], default=False)
    filtro_tags = SelectField(u'Tags de usuário',
                              default=[0])
    texto_ocorrencia = StringField(u'Texto Ocorrência',
                                   validators=[optional()], default='')
    contrast = BooleanField()
    color = BooleanField()


def recupera_user_filtros():
    """Usa variável global para guardar filtros personalizados entre posts."""
    global filtros
    filtro = {}
    if filtros.get(current_user.id):
        user_filtros = filtros[current_user.id]
    else:
        user_filtros = dict()
        filtros[current_user.id] = user_filtros
    if user_filtros:  # Adiciona filtro personalizado se houver
        for campo, valor in user_filtros.items():
            filtro[campo] = valor
    return filtro, user_filtros


def valida_form_files(form, filtro, db):
    """Lê formulário e adiciona campos ao filtro se necessário."""
    order = None
    pagina_atual = None
    if form.validate():  # configura filtro básico
        numero = form.numero.data
        start = form.start.data
        end = form.end.data
        alerta = form.alerta.data
        ranking = form.ranking.data
        pagina_atual = form.pagina_atual.data
        filtro_escolhido = form.filtro_auditoria.data
        if filtro_escolhido and filtro_escolhido != '0':
            auditoria_object = Auditoria(db)
            filtro_auditoria = \
                auditoria_object.dict_auditoria.get(filtro_escolhido)
            # logger.debug(auditoria_object.dict_auditoria)
            # logger.debug(filtro_escolhido)
            # logger.debug(filtro_auditoria)
            if filtro_auditoria:
                filtro.update(filtro_auditoria.get('filtro'))
                order = filtro_auditoria.get('order')
        tag_escolhida = form.filtro_tags.data
        tag_usuario = form.tag_usuario.data
        # print('****************************', tag_escolhida)
        if tag_escolhida and tag_escolhida != '0':
            filtro_tag = {'tag': tag_escolhida}
            if tag_usuario:
                filtro_tag.update({'usuario': current_user.id})
            filtro.update({'metadata.tags': {'$elemMatch': filtro_tag}})
        texto_ocorrencia = form.texto_ocorrencia.data
        if texto_ocorrencia:
            filtro.update(
                {'metadata.ocorrencias': {'$exists': True},
                 'metadata.ocorrencias.texto': {'$regex': texto_ocorrencia}
                 })
        if numero == 'None':
            numero = None
        if start and end:
            start = datetime.combine(start, datetime.min.time())
            end = datetime.combine(end, datetime.max.time())
            filtro['metadata.dataescaneamento'] = {'$lte': end, '$gte': start}
        if numero:
            filtro['metadata.numeroinformado'] = \
                {'$regex': '^' + mongo_sanitizar(numero), '$options': 'i'}
        if alerta:
            filtro['metadata.xml.alerta'] = True
        if ranking:
            filtro['metadata.ranking'] = {'$exists': True, '$gte': .5}
            order = [('metadata.ranking', -1)]
    # print(filtro)
    return filtro, pagina_atual, order


@app.route('/files', methods=['GET', 'POST'])
@login_required
def files():
    """Recebe parâmetros, aplica no GridFS, retorna a lista de arquivos."""
    db = app.config['mongodb']
    PAGE_ROWS = 50
    PAGES = 100
    lista_arquivos = []
    campos = campos_chave()
    npaginas = 1
    pagina_atual = 1
    count = 0
    order = None
    tags_object = Tags(db)
    auditoria_object = Auditoria(db)
    form_files = FilesForm(start=date.today() - timedelta(days=10),
                           end=date.today())
    form_files.filtro_tags.choices = tags_object.tags_text
    form_files.filtro_auditoria.choices = auditoria_object.filtros_auditoria_desc
    filtro, user_filtros = recupera_user_filtros()
    if request.method == 'POST':
        # print('****************************', request.form)
        form_files = FilesForm(**request.form)
        form_files.filtro_tags.choices = tags_object.tags_text
        form_files.filtro_auditoria.choices = auditoria_object.filtros_auditoria_desc
        filtro, pagina_atual, order = valida_form_files(form_files, filtro, db)
    else:
        numero = request.args.get('numero')
        if numero:
            form_files = FilesForm(numero=numero)
            form_files.filtro_tags.choices = tags_object.tags_text
            form_files.filtro_auditoria.choices = auditoria_object.filtros_auditoria_desc
            filtro['metadata.numeroinformado'] = \
                {'$regex': '^' + mongo_sanitizar(numero), '$options': 'i'}
    if filtro:
        filtro['metadata.contentType'] = 'image/jpeg'
        if order is None:
            order = [('metadata.dataescaneamento', 1)]
        if pagina_atual is None:
            pagina_atual = 1

        projection = {'_id': 1, 'filename': 1,
                      'metadata.numeroinformado': 1,
                      'metadata.predictions': 1,
                      'metadata.pesagens': 1,
                      'metadata.dataescaneamento': 1,
                      'metadata.carga': 1}
        skip = (pagina_atual - 1) * PAGE_ROWS
        logger.debug(filtro)
        logger.debug(projection)
        logger.debug('order: %s' % order)
        logger.debug(PAGE_ROWS)
        logger.debug(skip)
        count = db['fs.files'].count_documents(filtro, limit=PAGES * PAGE_ROWS)
        # print(count)
        # count = 100
        npaginas = (count - 1) // PAGE_ROWS + 1
        # print('**Página:', pagina_atual, skip, type(skip))
        # print(count, skip)
        for grid_data in db['fs.files'] \
                .find(filter=filtro, projection=projection) \
                .sort(order) \
                .limit(PAGE_ROWS).skip(skip):
            linha = {}
            linha['_id'] = grid_data['_id']
            linha['filename'] = grid_data['filename']
            linha['dataescaneamento'] = datetime.strftime(grid_data['metadata'].get(
                'dataescaneamento'), '%-d/%m/%Y %H:%M:%S')
            linha['ncms'] = carga.get_dados_ncm(grid_data)
            linha['infocarga'] = carga.get_dados_conteiner(grid_data)
            linha['pesocarga'] = carga.get_peso_conteiner(grid_data)
            linha['infodue'] = due_mongo.get_dados(grid_data)
            linha['peso'] = carga.get_pesos(grid_data)
            linha['numero'] = grid_data['metadata'].get('numeroinformado')
            linha['conhecimento'] = carga.get_conhecimento(grid_data)
            lista_arquivos.append(linha)
        # print(lista_arquivos)
        if len(lista_arquivos) < 50:
            npaginas = pagina_atual
    return render_template('search_files.html',
                           paginated_files=lista_arquivos,
                           oform=form_files,
                           campos=campos,
                           filtros=user_filtros,
                           npaginas=npaginas,
                           nregistros=count)


@app.route('/lotes_anomalia', methods=['GET', 'POST'])
@login_required
def lotes_anomalia():
    """Recebe parâmetros, aplica no GridFS, retorna a lista de arquivos."""
    db = app.config['mongodb']
    PAGE_ROWS = 50
    PAGES = 100
    conhecimentos = []
    npaginas = 0
    count = 0
    campos = campos_chave()
    form = FormFiltro(start=date.today() - timedelta(days=10),
                      end=date.today())
    form.initialize(db)
    ## TODO: incluir lógica de user_filtros (filro personalizado) no form
    filtro, user_filtros = recupera_user_filtros()
    if request.method == 'POST':
        form = FormFiltro(**request.form)
        form.initialize(db)
        if form.valida():
            skip = (form.pagina_atual.data - 1) * PAGE_ROWS
            print('skip *****************', skip)
            # Merge user_filtros
            filtro = {**form.filtro, **filtro}
            conhecimentos_anomalia, query = get_conhecimentos_filtro(
                db, filtro, PAGE_ROWS, skip)
            count = db['fs.files'].count_documents(filtro, limit=PAGES * PAGE_ROWS)
            print(conhecimentos_anomalia)
            conhecimentos_idszscore = get_ids_score_conhecimento_zscore(
                db, conhecimentos_anomalia)
            npaginas = (count - 1) // PAGE_ROWS + 1
            # TODO: Refatorar para uma classe, modulo ou funções a lógica
            if conhecimentos_idszscore:
                idsnormais = []
                idnormal = idanomalo = None
                idsanomalos = []
                for conhecimento in conhecimentos_anomalia:
                    maxzscore = 0.
                    minzscore = 10.
                    for idzscore in conhecimentos_idszscore[conhecimento]:
                        _id = idzscore['_id']
                        zscore = idzscore['zscore']
                        if zscore > maxzscore:
                            maxzscore = zscore
                            idanomalo = _id
                        if zscore < minzscore:
                            minzscore = zscore
                            idnormal = _id
                    idsnormais.append(idnormal)
                    idsanomalos.append(idanomalo)
                for numero, normal, anomalo in zip(
                        conhecimentos_anomalia, idsnormais, idsanomalos):
                    conhecimento = {}
                    conhecimento['numero'] = numero
                    conhecimento['idnormal'] = normal
                    conhecimento['idanomalo'] = anomalo
                    conhecimentos.append(conhecimento)
    return render_template('search_lotes.html',
                           conhecimentos=conhecimentos,
                           oform=form,
                           npaginas=npaginas,
                           nregistros=count,
                           filtros=user_filtros,
                           campos=campos)


@app.route('/cemercante/<numero>')
@app.route('/cemercante', methods=['POST', 'GET'])
@login_required
def cemercante(numero=None):
    """Tela para exibição de um CE Mercante do GridFS.

    Exibe o CE Mercante e os arquivos associados a ele.
    """
    db = app.config['mongodb']
    sql = app.config['mysql']
    conhecimento = None
    imagens = []
    if request.method == 'POST':
        numero = request.form.get('numero')
        print(request.values)
        contrast, color = get_contrast_and_color_(request)
        print('################', contrast, color)
    if numero:
        Session = sessionmaker(bind=sql)
        session = Session()
        conhecimento = session.query(Conhecimento).filter(
            Conhecimento.numeroCEmercante == numero).one_or_none()
        containers = list(session.query(Item).filter(
            Item.numeroCEmercante == numero).all())
        # conhecimento = carga.Conhecimento.from_db(db, numero)
        # containers = carga.ListaContainerConhecimento.from_db(db, numero)
        idszscore = get_ids_score_conhecimento_zscore(db, [numero])[numero]
        # print(idszscore)
        imagens = [{'_id': str(item['_id']),
                    'container': item['container'],
                    'zscore': '{:0.1f}'.format(item['zscore'])}
                   for item in sorted(idszscore,
                                      key=lambda item: item['zscore'],
                                      reverse=True)
                   ]
        contrast, color = get_contrast_and_color_(request)
    return render_template('view_cemercante.html',
                           conhecimento=conhecimento,
                           containers=containers,
                           imagens=imagens,
                           color=color,
                           contrast=contrast)


class StatsForm(FlaskForm):
    """Valida datas da tela de estatísticas."""

    start = DateField('Start', validators=[optional()])
    end = DateField('End', validators=[optional()])


@app.route('/stats', methods=['GET', 'POST'])
@login_required
def stats():
    """Permite consulta as estatísticas do GridFS e integrações."""
    db = app.config['mongodb']
    global stats_cache
    form = StatsForm(request.form,
                     start=date.today() - timedelta(days=30),
                     end=date.today())
    if form.validate():
        start = datetime.combine(form.start.data, datetime.min.time())
        end = datetime.combine(form.end.data, datetime.max.time())
        stats_cache = stats_resumo_imagens(db, start, end)
        # logger.debug(stats_cache)
    return render_template('stats.html',
                           stats=stats_cache,
                           oform=form)


@app.route('/pie_plotly')
@login_required
def pie_plotly():
    """Renderiza HTML no pyplot e serializa via HTTP/HTML."""
    global stats_cache
    if stats_cache:
        stats = stats_cache['recinto']
        # print(stats)
        output = plot_pie_plotly(list(stats.values()), list(stats.keys()))
        return output
    return ''


@app.route('/bar_plotly')
@login_required
def bar_plotly():
    """Renderiza gráfico no plotly e serializa via HTTP/HTML."""
    global stats_cache
    # print('stats_cache', stats_cache)
    if stats_cache:
        recinto = request.args.get('recinto')
        stats = stats_cache['recinto_mes'].get(recinto)
        # print(stats_cache['recinto_mes'])
        if stats:
            output = plot_bar_plotly(list(stats.values()), list(stats.keys()))
            return output
    return ''


@app.route('/padma_proxy/<image_id>')
@login_required
def padma_proxy(image_id):
    """Teste. Envia uma imagem para padma teste e repassa retorno."""
    db = app.config['mongodb']
    fs = GridFS(db)
    _id = ObjectId(image_id)
    if fs.exists(_id):
        grid_out = fs.get(_id)
        image = grid_out.read()
        # filename = grid_out.filename
        data = {}
        data['file'] = image
        headers = {}
        # headers['Content-Type'] = 'image/jpeg'
        r = requests.post(PADMA_URL + '/teste',
                          files=data, headers=headers)
        result = r.text
    return result


@app.route('/similar')
@login_required
def similar_():
    """Chama view de índice de imagens similares por GET.

    Recebe _id e offset(página atual).
    Para possibilitar rolagem de página.

    """
    _id = request.args.get('_id', '')
    offset = int(request.args.get('offset', 0))
    return similar(_id, offset)


@app.route('/similar/<_id>')
@login_required
def similar(_id, offset=0):
    """Retorna índice de imagens similares."""
    img_search = app.config['img_search']
    most_similar = img_search.get_chunk(_id, offset)
    return render_template('similar_files.html',
                           ids=most_similar,
                           _id=_id,
                           offset=offset,
                           chunk=img_search.chunk)


@app.route('/similar_file', methods=['GET', 'POST'])
@login_required
def similar_file():
    """Retorna índice de imagens similares."""
    if request.method == 'POST':
        # check if the post request has the file part
        file = request.files.get('file')
        validfile, mensagem = valid_file(file,
                                         extensions=['jpg', 'jpeg', 'png'])
        if not validfile:
            flash(mensagem)
            return redirect(request.url)
        content = file.read()
        img_search = app.config['img_search']
        index = consulta_padma(content, 'index')
        # print(index)
        try:
            code = index.get('predictions')[0].get('code')
        except Exception as err:
            logger.error(err)
            code = None
        if code is not None:
            _ = img_search.get_chunk(file.filename, index=code)
            return redirect(url_for('similar', _id=file.filename))
        else:
            flash('Erro ao consultar index no PADMA. Checar logs do PADMA')
    return render_template('upload_file.html')


@app.route('/recarrega_imageindex')
@login_required
def recarrega_imageindex():
    """Recarrega image_index"""
    result = {}
    try:
        img_search = ImageSearch(app.config['mongodb'])
        app.config['img_search'] = img_search
        result['size'] = img_search.get_size()
        result['sucess'] = True
    except (IOError, FileNotFoundError) as err:
        logger.error(err)
        result['sucess'] = False
        result['erro'] = str(err)
    return jsonify(result)


@app.route('/text_search')
@login_required
def text_search():
    """Tela para busca textual."""
    return render_template('text_search.html')


@app.route('/vocabulary')
@login_required
def vocabulary_():
    """Chama view de índice de palavras similares por GET.

    Recebe partialword - primeiras letras a filtrar

    """
    partialword = request.args.get('partialword', '')
    return vocabulary(partialword)


@app.route('/vocabulary/<partialword>')
@login_required
def vocabulary(partialword):
    """Retorna índice de imagens similares."""
    text_search = app.config['text_search']
    palavras = text_search.get_palavras_como(partialword)
    return jsonify(palavras)


@app.route('/ranked_docs')
@login_required
def ranked_docs():
    """Usa text_search para retornar itens que contém as palavras da frase.

    Recebe partialword - primeiras letras a filtrar

    """
    phrase = request.args.get('phrase', '')
    pagina = int(request.args.get('pagina', '1'))
    text_search = app.config['text_search']
    docs = text_search.get_itens_frase(phrase)
    total = len(docs)
    npaginas = total // 100 + 1
    if pagina > npaginas:
        pagina = 1
    offset = (pagina - 1) * 100
    if offset >= total:
        offset = max(0, total - 1)
    return jsonify({'total': total, 'pagina': pagina,
                    'npaginas': npaginas,
                    'docs': docs[offset:offset + 100]})


@app.route('/recarrega_textindex')
@login_required
def recarrega_textindex():
    """Recarrega image_index"""
    result = {}
    try:
        app.config['text_search'] = TextSearch(app.config['mongodb'])
        result['sucess'] = True
    except Exception as err:
        logger.error(err)
        result['sucess'] = False
        result['erro'] = str(err)
    return jsonify(result)


class PasswordForm(FlaskForm):
    password = PasswordField('Nova Senha', validators=[DataRequired()])


@app.route('/account', methods=['GET', 'POST'])
@login_required
def account():
    """Permite ver usuário e mudar senha"""
    # TODO: colocar no blueprint de login
    form = PasswordForm()
    if request.method == 'POST':
        if form.validate_on_submit():
            user = current_user
            newpassword = form.password.data
            user.change_password(newpassword)
            flash('Senha alterada com successo!', 'success')
            return redirect(url_for('account'))

    return render_template('account.html', form=form)


@app.route('/auditoria', methods=['GET', 'POST'])
@login_required
def auditoria():
    """Permite editar e gravar filtro de auditoria"""
    # TODO: Passar lógica para caso de Uso ou para form e classe
    # TODO: Criar perfil para acesso restrito e SANITIZAR ENTRADA
    form = FormAuditoria()
    select = SelectAuditoria()
    auditoria = Auditoria(app.config['mongodb'])
    select.filtro_auditoria.choices = auditoria.filtros_auditoria_desc
    if request.method == 'POST':
        if form.validate_on_submit():
            filtro = json.loads(form.filtro.data)
            try:
                order = json.loads(form.order.data)
            except JSONDecodeError as err:
                order = form.order.data.split(',')
            filtro = filtro.replace('function', 'XXXX')
            order = order.replace('function', 'XXXX')
            form.descricao.data = mongo_sanitizar(form.descricao.data)
            auditoria.add_relatorio(form.id.data, filtro,
                                    order, form.descricao.data)
            flash('Filtro incluído!', 'success')
            return redirect(url_for('auditoria'))

    return render_template('auditoria.html',
                           form=form,
                           select=select)


@app.route('/select_auditoria', methods=['GET', 'POST'])
@login_required
def select_auditoria():
    """Permite editar e gravar filtro de auditoria"""
    # TODO: Passar lógica para caso de Uso ou para form e classe
    form = FormAuditoria()
    select = SelectAuditoria()
    auditoria = Auditoria(app.config['mongodb'])
    select.filtro_auditoria.choices = auditoria.filtros_auditoria_desc
    if request.method == 'POST':
        filtro_escolhido = select.filtro_auditoria.data
        # print(filtro_escolhido)
        if filtro_escolhido and filtro_escolhido != '0':
            filtro_auditoria = \
                auditoria.dict_auditoria.get(filtro_escolhido)
            # print(filtro_auditoria)
            form.id.data = filtro_escolhido
            form.order.data = json.dumps(filtro_auditoria.get('order'))
            form.filtro.data = json.dumps(filtro_auditoria.get('filtro'))
            form.descricao.data = filtro_auditoria.get('descricao')
    return render_template('auditoria.html',
                           form=form,
                           select=select)


@nav.navigation()
def mynavbar():
    """Menu da aplicação."""
    items = [View(logo, 'index'),
             View('Pesquisar arquivos', 'files'),
             View('Pesquisa lote com anomalia', 'lotes_anomalia'),
             View('Estatísticas', 'stats'),
             Subgroup(
                 'Outros',
                 View('Pesquisa imagem externa', 'similar_file'),
                 View('Pesquisa textual', 'text_search'),
                 Separator(),
                 View('Cadastra Filtro de Auditoria', 'auditoria'),
                 View('Mudar senha', 'account'),
                 Separator(),
                 View('Importar Bson', 'upload_bson'),
             ),
             ]
    if current_user.is_authenticated:
        items.append(View('Sair', 'commons.logout'))
    return Navbar(*items)


if __name__ == '__main__':
    # start the web server
    print('* Starting web service...')
    app.run(debug=app.config['DEBUG'])
