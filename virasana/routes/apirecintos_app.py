from datetime import datetime

from ajna_commons.flask.log import logger
from flask import render_template, request, flash
from flask_login import login_required
from virasana.forms.filtros import FormFiltroAPIRecintos
from virasana.usecases.apirecintos_manager import get_eventos


def configure(app):
    """Configura rotas para evento."""

    def lista_eventos_html(mongodb, session, form: FormFiltroAPIRecintos):
        # start = datetime.combine(form.start.data, datetime.min.time())
        # end = datetime.combine(form.end.data, datetime.max.time())
        eventos = get_eventos(mongodb, session, form.start.data, form.end.data, form.placa.data,
                              form.numeroConteiner.data, form.cpfMotorista.data)
        return eventos

    @app.route('/eventos_redirect', methods=['GET'])
    @login_required
    def eventos_redirect():
        mongodb = app.config['mongodb']
        session = app.config['db_session']
        lista_eventos = []
        form = FormFiltroAPIRecintos(request.args)
        try:
            lista_eventos = lista_eventos_html(mongodb, session, form)
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('eventos.html',
                               lista_eventos=lista_eventos,
                               oform=form)

    @app.route('/eventos', methods=['GET', 'POST'])
    @login_required
    def eventos():
        mongodb = app.config['mongodb']
        session = app.config['db_session']
        lista_eventos = []
        oform = FormFiltroAPIRecintos(request.values)
        try:
            if request.method == 'POST':  # Listar por GET somente no caso de redirect do Importa DSI
                lista_eventos = lista_eventos_html(mongodb, session, oform)
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('eventos.html',
                               lista_eventos=lista_eventos,
                               oform=oform)
