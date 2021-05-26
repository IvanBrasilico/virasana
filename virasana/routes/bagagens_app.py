from datetime import date, timedelta, datetime

from ajna_commons.flask.log import logger
from flask import render_template, request, flash
from flask_login import login_required
from forms.filtros import FormFiltroData


def configure(app):
    """Configura rotas para bagagem."""

    @app.route('/virasana/bagagens', methods=['GET', 'POST'])
    @login_required
    def bagagens():
        session = app.config['db_session']
        headers = []
        lista_bagagens = []
        form = FormFiltroData(request.form,
                              start=date.today() - timedelta(days=30),
                              end=date.today())
        try:
            if request.method == 'POST' and form.validate():
                start = datetime.combine(form.start.data, datetime.min.time())
                end = datetime.combine(form.end.data, datetime.max.time())
                headers, lista_bagagens = get_bagagens(session, start, end)
                # logger.debug(stats_cache)
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('bagagens.html',
                               headers=headers,
                               lista_bagagens=lista_bagagens,
                               oform=form)
