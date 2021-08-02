from datetime import date, timedelta, datetime

from ajna_commons.flask.log import logger
from flask import render_template, request, flash
from flask_login import login_required
from virasana.forms.filtros import FormFiltroData, FormFiltroConformidade
from virasana.integracao.risco.conformidade_alchemy import get_isocode_groups_choices, get_isocode_sizes_choices
from virasana.integracao.risco.conformidade_manager import get_conformidade, get_conformidade_recinto, \
    get_completude_mercante


def configure(app):
    """Configura rotas para bagagem."""

    @app.route('/conformidade', methods=['GET', 'POST'])
    @login_required
    def conformidade():
        """Permite consulta o tabelão de estatísticas de conformidade."""
        session = app.config['db_session']
        headers = []
        lista_conformidade = []
        form = FormFiltroData(request.form,
                              start=date.today() - timedelta(days=30),
                              end=date.today())
        try:
            if request.method == 'POST' and form.validate():
                start = datetime.combine(form.start.data, datetime.min.time())
                end = datetime.combine(form.end.data, datetime.max.time())
                headers, lista_conformidade = get_conformidade(session, start, end)
                # logger.debug(stats_cache)
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('conformidade.html',
                               headers=headers,
                               lista_conformidade=lista_conformidade,
                               oform=form)

    @app.route('/conformidade_recinto', methods=['GET', 'POST'])
    @login_required
    def conformidade_recinto():
        """Permite consulta o tabelão de estatísticas de conformidade."""
        session = app.config['db_session']
        npaginas = 0
        headers = []
        conformidade = []
        lista_conformidade = []
        print(request.values)
        form = FormFiltroConformidade(request.values,
                                      start=date.today() - timedelta(days=30),
                                      end=date.today())
        form.isocode_group.choices = get_isocode_groups_choices(session)
        form.isocode_size.choices = get_isocode_sizes_choices(session)
        try:
            if form.validate():
                start = datetime.combine(form.start.data, datetime.min.time())
                end = datetime.combine(form.end.data, datetime.max.time())
                headers, conformidade = get_conformidade(session, start, end,
                                                         form.recinto.data,
                                                         isocode_group=form.isocode_group.data,
                                                         isocode_size=form.isocode_size.data)
                lista_conformidade, npaginas = get_conformidade_recinto(session,
                                                                        recinto=form.recinto.data,
                                                                        datainicio=start,
                                                                        datafim=end,
                                                                        order=form.order.data,
                                                                        reverse=form.reverse.data,
                                                                        isocode_group=form.isocode_group.data,
                                                                        isocode_size=form.isocode_size.data,
                                                                        paginaatual=form.pagina_atual.data)
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('conformidade_recinto.html',
                               lista_conformidade=lista_conformidade,
                               oform=form,
                               npaginas=npaginas,
                               headers=headers,
                               conformidade=conformidade)

    @app.route('/completude', methods=['GET', 'POST'])
    @login_required
    def completude():
        """Permite consulta o tabelão de estatísticas de conformidade."""
        session = app.config['db_session']
        headers = []
        lista_completude = []
        form = FormFiltroData(request.form,
                              start=date.today() - timedelta(days=30),
                              end=date.today())
        try:
            if request.method == 'POST' and form.validate():
                start = datetime.strftime(form.start.data, '%Y-%m-%d')
                end = datetime.strftime(form.end.data + timedelta(days=1), '%Y-%m-%d')
                headers, lista_completude = get_completude_mercante(session, start, end)
                # logger.debug(stats_cache)
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('completude.html',
                               headers=headers,
                               lista_completude=lista_completude,
                               oform=form)
