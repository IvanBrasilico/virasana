from datetime import date, timedelta, datetime

from ajna_commons.flask.log import logger
from flask import render_template, request, flash
from flask_login import login_required
from virasana.forms.filtros import FormFiltroBagagem
from virasana.usecases.bagagens_manager import get_bagagens


def configure(app):
    """Configura rotas para bagagem."""

    @app.route('/bagagens', methods=['GET', 'POST'])
    @login_required
    def bagagens():
        mongodb = app.config['mongodb']
        session = app.config['db_session']
        headers = []
        lista_bagagens = []
        form = FormFiltroBagagem(request.form,
                                 start=date.today() - timedelta(days=30),
                                 end=date.today())
        try:
            if request.method == 'POST' and form.validate():
                start = datetime.combine(form.start.data, datetime.min.time())
                end = datetime.combine(form.end.data, datetime.max.time())
                lista_bagagens = get_bagagens(mongodb, session, start, end,
                                              portoorigem=form.portoorigem.data,
                                              cpf_cnpj=form.cpf_cnpj.data,
                                              numero_conteiner=form.conteiner.data,
                                              selecionados=form.selecionados.data,
                                              descartados=form.descartados.data,
                                              somente_sem_imagem=form.semimagem.data)
                # logger.debug(stats_cache)
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('bagagens.html',
                               headers=headers,
                               lista_bagagens=lista_bagagens,
                               oform=form)


"""
                <div class="col-sm-12">
                    {% if conhecimento.viagens2 %}
                    {% for viagem in conhecimento.viagens %}
                    <div class="col-sm-2">&nbsp;</div>
                    <div class="col-sm-10">
                        <div class="col-sm-4">
                            {{ viagem.data_chegada }}
                        </div>
                        <div class="col-sm-2">
                            {{ viagem.origem }}
                        </div>
                        <div class="col-sm-2">
                            {{ viagem.destino }}
                        </div>
                        <div class="col-sm-4">
                            {{ viagem.localizador }}
                        </div>
                    </div>
                    {% endfor %}
                    {% endif %}
                </div>
                {% endfor %}


"""