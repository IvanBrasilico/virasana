from datetime import date, timedelta, datetime, time

import pandas as pd
from ajna_commons.flask.log import logger
from bhadrasana.models.apirecintos_risco import Pais
from flask import render_template, request, flash
from flask_login import login_required
from virasana.forms.filtros import FormFiltroData
from virasana.integracao.mercante.mercantealchemy import Item


def get_imagens_container_data(mongodb, numero, inicio_scan, fim_scan, vazio=False) -> list:
    query = {'metadata.numeroinformado': numero,
             'metadata.contentType': 'image/jpeg',
             'metadata.dataescaneamento': {'$gte': inicio_scan, '$lte': fim_scan}
             }
    projection = {'metadata.numeroinformado': 1,
                  'metadata.dataescaneamento': 1,
                  'metadata.predictions.vazio': 1}
    cursor = mongodb['fs.files'].find(query, projection)
    return list(cursor)


def get_escaneamentos_obrigatorios(engine, inicio, fim, porto_origem):
    SQL_ESCANEAMENTOS = \
        '''
        SELECT c.numeroCEmercante, substring(c.portoDestFinal, 1, 2) as pais_porto_final, 
        substring(m.portoDescarregamento, 1, 2) as pais_porto_baldeacao 
        FROM conhecimentosresumo c INNER JOIN manifestosresumo m ON c.manifestoCE = m.numero
        WHERE c.dataEmissao between '{}' and '{}'
        AND c.portoOrigemCarga = '{}'
        AND (
        (substring(c.portoDestFinal, 1, 2) IN (SELECT sigla FROM risco_paises WHERE escaneamento is True))
        OR 
        (substring(m.portoDescarregamento, 1, 2) IN (SELECT sigla FROM risco_paises WHERE escaneamento is True))
        )
        '''.format(inicio, fim, porto_origem)
    # print(SQL_ESCANEAMENTOS)
    return pd.read_sql(SQL_ESCANEAMENTOS, engine)


def completa_nome_pais(session, sigla):
    lpais = session.query(Pais).filter(Pais.sigla == sigla).one_or_none()
    if lpais is None:
        return sigla
    return f'{lpais.sigla} - {lpais.nome}'


def get_embarques_sem_imagem(mongodb, session, inicio, fim, porto_origem):
    inicio_scan = datetime.combine(inicio - timedelta(days=10), time.min)
    fim_scan = datetime.combine(fim + timedelta(days=10), time.max)
    vazios = []
    semimagem = []
    comimagem = []
    logger.info(f'Checando embarques (emissão CE) entre {inicio} e {fim} com escaneamentos'
                f' entre {inicio_scan} e {fim_scan}')
    df_escaneamentos_obrigatorios = get_escaneamentos_obrigatorios(session.get_bind(), inicio, fim, porto_origem)
    # print(df_escaneamentos_obrigatorios)
    for row in df_escaneamentos_obrigatorios.itertuples(index=False):
        cemercante = row.numeroCEmercante
        nome_pais_porto_final = completa_nome_pais(session, row.pais_porto_final)
        if row.pais_porto_baldeacao == row.pais_porto_final:
            nome_pais_porto_baldeacao = nome_pais_porto_final
        else:
            nome_pais_porto_baldeacao = completa_nome_pais(session, row.pais_porto_baldeacao)
        itens = session.query(Item).filter(Item.numeroCEmercante == cemercante).all()
        for item in itens:
            imagens = get_imagens_container_data(mongodb, item.codigoConteiner, inicio_scan, fim_scan)
            if len(imagens) == 0:
                semimagem.append((cemercante, item.codigoConteiner,
                                  nome_pais_porto_final, nome_pais_porto_baldeacao))
                continue
            else:
                vazio = True
                for imagem in imagens:  # Procura imagem que não seja de vazio.
                    if imagem['metadata']['predictions'][0]['vazio'] is False:
                        vazio = False
                        break
                if vazio:
                    vazios.append((cemercante, item.codigoConteiner,
                                   nome_pais_porto_final, nome_pais_porto_baldeacao))
                    continue
            comimagem.append((cemercante, item.codigoConteiner,
                              nome_pais_porto_final, nome_pais_porto_baldeacao,
                              imagens[0]['_id']))
    return semimagem, vazios, comimagem


def configure(app):
    """Configura rotas para bagagem."""

    @app.route('/confere_escaneamento', methods=['GET', 'POST'])
    @login_required
    def confere_escaneamento():
        """Permite consulta o tabelão de estatísticas de conformidade."""
        session = app.config['db_session']
        mongodb = app.config['mongodb']
        semimagem = []
        vazios = []
        comimagem = []
        form = FormFiltroData(request.form,
                              start=date.today() - timedelta(days=1),
                              end=date.today())
        try:
            if request.method == 'POST':
                semimagem, vazios, comimagem = get_embarques_sem_imagem(mongodb, session,
                                                                        form.start.data,
                                                                        form.end.data,
                                                                        'BRSSZ')
                # logger.debug(stats_cache)
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('confere_escaneamento.html',
                               semimagem=semimagem,
                               vazios=vazios,
                               comimagem=comimagem,
                               oform=form)
