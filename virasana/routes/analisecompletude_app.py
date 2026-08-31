from datetime import time, datetime

from flask import request, render_template, flash

from bhadrasana.models.ovr import Recinto
from virasana.forms.analisecompletude_form import AnaliseCompletudeFiltroForm
from virasana.models.analise_completude_service import query_completude_detalhe, query_completude_agregada


def configure(app):
    def _intervalo_do_form(form):
        inicio = datetime.combine(form.datainicio.data, time.min)
        fim = datetime.combine(form.datafim.data, time.max)
        return inicio, fim

    @app.route('/analise/completude-escaneamentos-recintos-secos', methods=['GET'])
    def analise_completude_recintos_secos():
        session = app.config.get('db_session')
        form = AnaliseCompletudeFiltroForm(request.args)
        if not form.validate():
            for campo, erros in form.errors.items():
                for erro in erros:
                    flash(f'{campo}: {erro}', 'warning')

        data_inicio, data_fim = _intervalo_do_form(form)
        resultados = query_completude_agregada(session, data_inicio, data_fim)
        return render_template(
            'analise_completude_agregada.html',
            oform=form,
            resultados=resultados,
            data_inicio=form.datainicio.data,
            data_fim=form.datafim.data,
        )

    @app.route('/analise/completude-escaneamentos-recintos-secos/<cod_recinto>', methods=['GET'])
    def analise_completude_recintos_secos_detalhe(cod_recinto):
        session = app.config.get('db_session')
        form = AnaliseCompletudeFiltroForm(request.args)
        if not form.validate():
            for campo, erros in form.errors.items():
                for erro in erros:
                    flash(f'{campo}: {erro}', 'warning')

        data_inicio, data_fim = _intervalo_do_form(form)
        linhas = query_completude_detalhe(session, data_inicio, data_fim, cod_recinto)
        recinto = session.query(Recinto).filter(Recinto.cod_siscomex == cod_recinto).one_or_none()
        return render_template(
            'analise_completude_detalhe.html',
            oform=form,
            cod_recinto=cod_recinto,
            nome_recinto=recinto.nome if recinto else '(recinto não cadastrado)',
            linhas=linhas,
            data_inicio=form.datainicio.data,
            data_fim=form.datafim.data,
        )
