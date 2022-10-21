import io
import os
from datetime import timedelta, datetime
from random import randint, seed

import pandas as pd
from ajna_commons.flask.conf import tmpdir
from ajna_commons.flask.log import logger
from flask import render_template, request, flash
from flask_login import login_required, current_user
from virasana.conf import APP_PATH
from virasana.forms.filtros import FormFiltroBagagem, FormClassificacaoRisco
from virasana.integracao.bagagens.regra_vermelho_portaria import e_canal_vermelho
from virasana.integracao.bagagens.viajantesalchemy import DSI, ClassificacaoRisco, ClasseRisco, Pessoa
from virasana.integracao.mercante.mercantealchemy import Conhecimento
from virasana.usecases.bagagens_manager import get_bagagens
from werkzeug.utils import secure_filename, redirect

seed(13)


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
                                             filtrar_dsi=form.filtrar_dsi.data)
        if form.filtrar_dsi.data:
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
        form = FormFiltroBagagem(request.args)
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

    def exporta_dsis(lista_bagagens, data_inicio):
        out_filename = 'ListaDSIs_{}.xlsx'.format(
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
                        lista_dsis.append([dsi.numero, dsi.consignatario, dsi.data_registro])
            for linha_dsi in lista_dsis:
                lista_final.append([*linha_dsi, *linha_risco])
        print(lista_final)
        df = pd.DataFrame(lista_final,
                          columns=['DSI', 'CPF', 'Data Registro',
                                   'Classificação de Risco', 'Observação'])
        df = df.drop_duplicates()
        start = datetime.combine(data_inicio, datetime.min.time())
        df = df[df['Data Registro'] >= start]
        df.to_excel(os.path.join(get_user_save_path(), out_filename), index=False)
        return out_filename

    @app.route('/bagagens', methods=['GET', 'POST'])
    @login_required
    def bagagens():
        mongodb = app.config['mongodb']
        session = app.config['db_session']
        lista_bagagens = []
        conteineres = []
        oform = FormFiltroBagagem(request.values)
        print('*****************************')
        print(request.values)
        print(oform.start.data, oform.end.data)
        try:
            if (request.method == 'GET' and request.values.get('filtrar_dsi')) or \
                    (request.method == 'POST'):  # Listar por GET somente no caso de redirect do Importa DSI
                lista_bagagens, conteineres = lista_bagagens_html(mongodb, session, oform)
                # logger.debug(stats_cache)
                if request.values.get('exportar'):
                    planilha = exporta_dsis(lista_bagagens, oform.start.data)
                    print(planilha)
                    return redirect(
                        'static/%s/%s' % (current_user.name, planilha))
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('bagagens.html',
                               lista_bagagens=lista_bagagens,
                               conteineres=conteineres,
                               oform=oform)

    @app.route('/importa_dsisold', methods=['POST', 'GET'])
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
        return redirect('bagagens?filtrar_dsi=1&cpf_cnpj=%s&start=%s' % (';'.join(lista_cpf), inicio))

    def classifica_ce(session,
                      numeroCEmercante: str,
                      classerisco: int = ClasseRisco.VERDE.value,
                      descricao='',
                      user_name='',
                      bloquear_edicao=False):
        classificacaorisco = session.query(ClassificacaoRisco). \
            filter(ClassificacaoRisco.numeroCEmercante ==
                   numeroCEmercante).one_or_none()
        #  Se edição bloqueado, só permite criação de novo, senão apagaria classificação manual
        if bloquear_edicao and classificacaorisco is not None:
            return
        if classificacaorisco is None:
            classificacaorisco = ClassificacaoRisco()
        classificacaorisco.numeroCEmercante = numeroCEmercante
        classificacaorisco.classerisco = classerisco
        classificacaorisco.descricao = descricao
        classificacaorisco.user_name = user_name
        session.add(classificacaorisco)
        print(f'Classificando risco {numeroCEmercante} {classerisco} {descricao}')

    PERCENT = 2

    def pseudo_random(num_dsi: int, data_registro: datetime):
        numero_dsi = int(num_dsi)
        divisor = 100 / PERCENT
        dia = data_registro.day
        selecionado = (numero_dsi % (divisor + randint(-5, 5))) == abs(dia + randint(-5, 21))
        return selecionado

    def classifica_aleatoriamente(session, numeroCEmercante: str,
                                  num_dsi: str, data_registro: datetime,
                                  user_name):
        if e_canal_vermelho(num_dsi, data_registro):
            classifica_ce(session, numeroCEmercante,
                          ClasseRisco.VERMELHO.value, 'Classificação aleatória')
            return 'VERMELHO'
        classifica_ce(session, numeroCEmercante, ClasseRisco.VERDE.value)
        return 'VERDE'

    def le_linha_csvportal(row, session, user_name, lista_dsis_sem_ce):
        # session = app.config.get('db_session')
        recinto = row.get('Número do Recinto Aduaneiro')
        ce = row.get(' Conhecimento de Carga', '').strip()
        operacao = row.get(' Código Natureza da Operação', '').strip()
        dsi = row.get(' Número DSI')
        cpf = str(row.get(' Número Importador', '')).strip().zfill(11)
        nome = row.get(' Nome Importador', '').strip()
        data = row['data']
        canal = classifica_aleatoriamente(session, ce, str(dsi), data, user_name)
        apessoa = session.query(Pessoa).filter(Pessoa.cpf == cpf).one_or_none()
        if apessoa is None:
            logger.info(f'Adicionando pessoa {cpf, nome}')
            apessoa = Pessoa()
            apessoa.cpf = cpf
        apessoa.nome = nome
        session.add(apessoa)
        adsi = session.query(DSI).filter(DSI.numero == dsi).one_or_none()
        if adsi is None:
            adsi = DSI()
            logger.info(f'Adicionando DSI {dsi, data, ce}')
            adsi.numero = dsi
            adsi.consignatario = cpf
        adsi.numeroCEmercante = ce
        adsi.data_registro = data
        session.add(adsi)
        cemercante = session.query(Conhecimento).filter(Conhecimento.numeroCEmercante == ce).one_or_none()
        if cemercante is None:
            lista_dsis_sem_ce.append(f'DSI {dsi} - canal {canal} NÃO será exibida. CE-Mercante {ce} não encontrado.')
        print(recinto, ce, dsi, cpf, nome, data)

    @app.route('/importa_dsis', methods=['POST', 'GET'])
    @login_required
    def importacsvportal():
        """Importar arquivo com lista de DSIs a selecionar do Portal Único
        """
        session = app.config.get('db_session')
        inicio = fim = datetime.strftime(datetime.today() - timedelta(days=1), '%Y-%m-%d')
        if request.method == 'POST':
            try:
                csvf = get_planilha_valida(request, 'csv')
                if csvf:
                    df = pd.read_csv(io.StringIO(csvf.stream.read().decode('utf-8')))
                    logger.info(df.columns)
                    df = df[df[' Código Natureza da Operação'].isin([9, 10])]
                    df['data'] = pd.to_datetime(df[' Data de Registro'], format=' %d/%m/%Y %H:%M')
                    df_maior_2022 = df[df['data'] >= datetime(2022, 1, 1)]
                    inicio = datetime.strftime(df_maior_2022.data.min(), '%Y-%m-%d')
                    fim = datetime.strftime(df_maior_2022.data.max(), '%Y-%m-%d')
                    lista_dsis_sem_ce = []
                    df_maior_2022.apply(lambda x: le_linha_csvportal(x, session, current_user.name, lista_dsis_sem_ce),
                                        axis=1)
                    session.commit()
                    # Alertar sobre DSIs que não serão exibidas a seguir
                    flash('<br'.join(lista_dsis_sem_ce))
            except Exception as err:
                session.rollback()
                logger.error(str(err), exc_info=True)
                flash(str(err))
        return redirect('bagagens?filtrar_dsi=1&start=%s&end=%s' % (inicio, fim))

    @app.route('/classificaCE', methods=['POST'])
    @login_required
    def classificaCE():
        session = app.config['db_session']
        form_classificacao = FormClassificacaoRisco()
        try:
            if request.method == 'POST' and form_classificacao.validate():
                classifica_ce(session,
                              form_classificacao.numeroCEmercante.data,
                              form_classificacao.classerisco.data,
                              form_classificacao.descricao.data,
                              current_user.name)
                session.commit()
                flash('CE Classificado!')
            # logger.debug(stats_cache)
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('bagagens.html',
                               lista_bagagens=[], oform=FormFiltroBagagem())
