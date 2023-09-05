import os
from datetime import datetime

import pandas as pd
from ajna_commons.flask.log import logger
from bhadrasana.models.apirecintos import AcessoVeiculo
from bhadrasana.models.apirecintos_risco import Motorista
from bhadrasana.models.ovrmanager import get_recintos_api
from flask import render_template, request, flash
from flask_login import login_required, current_user
from virasana.views import get_user_save_path
from virasana.forms.filtros import FormFiltroAPIRecintos
from virasana.usecases.apirecintos_manager import get_eventos, Missao
from werkzeug.utils import redirect


def configure(app):
    """Configura rotas para evento."""

    def lista_eventos_html(mongodb, session, form: FormFiltroAPIRecintos):
        # start = datetime.combine(form.start.data, datetime.min.time())
        # end = datetime.combine(form.end.data, datetime.max.time())
        form.validate()
        eventos, count_missao = get_eventos(mongodb, session, form.start.data, form.end.data, form.placa.data,
                                            form.numeroConteiner.data, form.cpfMotorista.data,
                                            form.motoristas_de_risco_select.data,
                                            form.codigoRecinto.data, form.tempo_permanencia.data,
                                            Missao().get_descricao_missao(int(form.missao.data)))
        return eventos, count_missao

    @app.route('/eventos_redirect', methods=['GET'])
    @login_required
    def eventos_redirect():
        mongodb = app.config['mongodb']
        session = app.config['db_session']
        lista_eventos = []
        count_missao = {}
        form = FormFiltroAPIRecintos(request.args)
        try:
            lista_eventos, count_missao = lista_eventos_html(mongodb, session, form)
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('eventos.html',
                               lista_eventos=lista_eventos,
                               count_missao=count_missao,
                               oform=form)

    @app.route('/eventos', methods=['GET', 'POST'])
    @login_required
    def eventos():
        mongodb = app.config['mongodb']
        session = app.config['db_session']
        lista_eventos = []
        count_missao = {}
        recintos = get_recintos_api(session)
        missoes = Missao().get_tipos_missao()
        oform = FormFiltroAPIRecintos(request.values, recintos=recintos, missoes=missoes)
        try:
            if request.method == 'POST':
                lista_eventos, count_missao = lista_eventos_html(mongodb, session, oform)
                print('************', request.values)
                if request.values.get('planilha'):
                    titulos = ['recinto', 'missao',
                               'motorista.cpf', 'motorista.nome', 'motorista.risco',
                               'entrada.placa', 'entrada.numeroConteiner', 'entrada.cnpjTransportador',
                               'entrada.numeroDeclaracao', 'entrada.numeroConhecimento', 'entrada.listaNfe',
                               'saida.placa', 'saida.numeroConteiner', 'saida.cnpjTransportador',
                               'saida.numeroDeclaracao', 'saida.numeroConhecimento', 'saida.listaNfe']
                    linhas = []
                    for evento in lista_eventos:
                        motorista: Motorista = evento['motorista']
                        entrada: AcessoVeiculo = evento['entrada']
                        saida: AcessoVeiculo = evento['saida']
                        if saida is None:
                            saida = AcessoVeiculo()
                        linha = [evento['recinto'], evento['missao'],
                                 motorista.cpf, motorista.nome, motorista.get_risco(),
                                 entrada.placa, entrada.numeroConteiner, entrada.cnpjTransportador,
                                 entrada.numeroDeclaracao, entrada.numeroConhecimento, entrada.listaNfe,
                                 saida.placa, saida.numeroConteiner, saida.cnpjTransportador,
                                 saida.numeroDeclaracao, saida.numeroConhecimento, saida.listaNfe]
                        linhas.append(linha)
                    if len(linhas) > 0:
                        df = pd.DataFrame(linhas)
                        df.columns = titulos
                        out_filename = '{}_{}.xlsx'.format('PesquisaAPIRecintos_',
                                                          datetime.strftime(datetime.now(), '%Y-%m-%dT%H-%M-%S'))
                        print(os.path.join(get_user_save_path(app), out_filename))
                        df.to_excel(os.path.join(get_user_save_path(app), out_filename), index=False)
                        return redirect('static/%s/%s' % (current_user.name, out_filename))
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('eventos.html',
                               lista_eventos=lista_eventos,
                               count_missao=count_missao,
                               oform=oform)
