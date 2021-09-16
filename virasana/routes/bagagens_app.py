import os
from datetime import date, timedelta, datetime

import pandas as pd
from ajna_commons.flask.conf import tmpdir
from ajna_commons.flask.log import logger
from flask import render_template, request, flash
from flask_login import login_required, current_user
from virasana.conf import APP_PATH
from virasana.forms.filtros import FormFiltroBagagem, FormClassificacaoRisco
from virasana.integracao.bagagens.viajantesalchemy import DSI, ClassificacaoRisco, ClasseRisco
from virasana.integracao.mercante.mercantealchemy import Conhecimento
from virasana.usecases.bagagens_manager import get_bagagens
from werkzeug.utils import secure_filename, redirect


def get_planilha_valida(request, anexo_name='csv',
                        extensoes=['csv', 'xls', 'ods', 'xlsx']):
    planilha = request.files.get(anexo_name)
    if planilha is None:
        flash('Arquivo não repassado')
    if planilha.filename == '':
        flash('Nome de arquivo vazio')
    else:
        if '.' in planilha.filename:
            extensao = planilha.filename.rsplit('.', 1)[1].lower()
            if extensao in extensoes:
                return planilha
        flash('Extensão %s inválida/desconhecida' % extensao)
    return None


def configure(app):
    """Configura rotas para bagagem."""

    def lista_bagagens_html(mongodb, session, form: FormFiltroBagagem):
        start = datetime.combine(form.start.data, datetime.min.time())
        end = datetime.combine(form.end.data, datetime.max.time())
        bagagens, conteineres = get_bagagens(mongodb, session, start, end,
                                             portoorigem=form.portoorigem.data,
                                             cpf_cnpj=form.cpf_cnpj.data,
                                             numero_conteiner=form.conteiner.data,
                                             portodestino=form.portodestino.data,
                                             ncm=form.ncm.data,
                                             selecionados=form.selecionados.data,
                                             concluidos=form.concluidos.data,
                                             classificados=form.classificados.data,
                                             somente_sem_imagem=form.semimagem.data,
                                             filtrar_dsi=form.ordenar_dsi.data)
        if form.ordenar_dsi.data:
            bagagens = sorted(bagagens, key=lambda x: x.max_numero_dsi)
        if form.ordenar_rvf.data:
            bagagens = sorted(bagagens, key=lambda x: x.max_data_rvf)
        return bagagens, conteineres

    @app.route('/bagagens_redirect', methods=['GET'])
    @login_required
    def bagagens_redirect():
        mongodb = app.config['mongodb']
        session = app.config['db_session']
        lista_bagagens = []
        conteineres = []
        form = FormFiltroBagagem(request.args,
                                 start=date.today() - timedelta(days=30),
                                 end=date.today())
        try:
            lista_bagagens, conteineres = lista_bagagens_html(mongodb, session, form)
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('bagagens.html',
                               lista_bagagens=lista_bagagens,
                               conteineres=conteineres,
                               oform=form)

    def get_user_save_path():
        user_save_path = os.path.join(APP_PATH,
                                      app.config.get('STATIC_FOLDER', 'static'),
                                      current_user.name)
        if not os.path.exists(user_save_path):
            os.mkdir(user_save_path)
        return user_save_path

    @app.route('/bagagens', methods=['GET', 'POST'])
    @login_required
    def bagagens():
        mongodb = app.config['mongodb']
        session = app.config['db_session']
        lista_bagagens = []
        conteineres = []
        print(request.form)
        form = FormFiltroBagagem(request.form,
                                 start=date.today() - timedelta(days=30),
                                 end=date.today())
        try:
            if request.method == 'POST' and form.validate():
                lista_bagagens, conteineres = lista_bagagens_html(mongodb, session, form)
                # logger.debug(stats_cache)
                if request.form.get('btn_exportar'):
                    out_filename = 'ListaDSIs_{}.csv'.format(
                        datetime.strftime(datetime.now(), '%Y-%m-%dT%H-%M-%S')
                    )
                    lista_final = []
                    for item in lista_bagagens:
                        linha_risco = ['', '']
                        lista_dsis = []
                        for conhecimento in item.conhecimentos:
                            if conhecimento.classificacaorisco:
                                cl = conhecimento.classificacaorisco
                                linha_risco = [ClasseRisco(cl.classerisco).name, cl.descricao]
                            if conhecimento.dsis:
                                for dsi in conhecimento.dsis:
                                    lista_dsis.append([dsi.numero, dsi.consignatario])
                        for linha_dsi in lista_dsis:
                            lista_final.append([*linha_dsi, *linha_risco])
                    print(lista_final)
                    df = pd.DataFrame(lista_final,
                                      columns=['DSI', 'CPF', 'Classifição de Risco', 'Observação'])
                    df.to_csv(os.path.join(get_user_save_path(), out_filename), index=False)
                    return redirect('static/%s/%s' % (current_user.name, out_filename))
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('bagagens.html',
                               lista_bagagens=lista_bagagens,
                               conteineres=conteineres,
                               oform=form)

    @app.route('/importa_dsis', methods=['POST', 'GET'])
    @login_required
    def importacsv():
        """Importar arquivo com lista de DSI e CPF
        """
        session = app.config.get('db_session')
        lista_cpf = []
        if request.method == 'POST':
            try:
                csvf = get_planilha_valida(request, 'planilha')
                if csvf:
                    filename = secure_filename(csvf.filename)
                    save_name = os.path.join(tmpdir, filename)
                    csvf.save(save_name)
                    logger.info('CSV RECEBIDO: %s' % save_name)
                    if 'xlsx' in filename:
                        df = pd.read_excel(save_name, engine='openpyxl')
                    else:
                        df = pd.read_excel(save_name)
                    achou = False
                    # df = df.dropna()
                    for row in df.itertuples():
                        if achou is False:
                            if row[1] == 'DSI' and row[2] == 'CPF':
                                achou = True
                                continue
                        else:
                            logger.info('Recuperando row[1] "%s"' % row[1])
                            if row[1] is None:
                                break
                            dsi = ''.join([s for s in str(row[1]) if s.isdigit()])
                            cpf = ''.join([s for s in str(row[2]) if s.isdigit()])
                            if not dsi:
                                break
                            lista_cpf.append(cpf)
                            logger.info('Recuperando dsi "%s"' % dsi)
                            adsi = session.query(DSI).filter(DSI.numero == dsi).one_or_none()
                            if adsi is None:
                                adsi = DSI()
                                logger.info('Recuperando mercante cpf %s' % cpf)
                                ocemercante = session.query(Conhecimento).filter(Conhecimento.consignatario == cpf). \
                                    order_by(Conhecimento.dataEmissao.desc()).first()
                                adsi.numero = dsi
                                adsi.consignatario = cpf
                                if ocemercante is not None:
                                    adsi.numeroCEmercante = ocemercante.numeroCEmercante
                                adsi.data_registro = datetime.today()
                                session.add(adsi)
                    logger.info('Salvando %s dsis' % len(lista_cpf))
                    session.commit()
            except Exception as err:
                logger.error(str(err), exc_info=True)
                flash(str(err))
        inicio = datetime.strftime(datetime.today() - timedelta(days=120), '%Y-%m-%d')
        return redirect('bagagens_redirect?filtrar_dsi=1&cpf_cnpj=%s&start=%s' % (';'.join(lista_cpf), inicio))

    @app.route('/classificaCE', methods=['POST'])
    @login_required
    def classificaCE():
        session = app.config['db_session']
        form_classificacao = FormClassificacaoRisco()
        try:
            if request.method == 'POST' and form_classificacao.validate():
                classificacaorisco = session.query(ClassificacaoRisco). \
                    filter(ClassificacaoRisco.numeroCEmercante ==
                           form_classificacao.numeroCEmercante.data).one_or_none()
                if classificacaorisco is None:
                    classificacaorisco = ClassificacaoRisco()
                classificacaorisco.numeroCEmercante = form_classificacao.numeroCEmercante.data
                classificacaorisco.classerisco = form_classificacao.classerisco.data
                classificacaorisco.descricao = form_classificacao.descricao.data
                session.add(classificacaorisco)
                session.commit()
                flash('CE Classificado!')
            # logger.debug(stats_cache)
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('bagagens.html',
                               lista_bagagens=[], oform=FormFiltroBagagem())
