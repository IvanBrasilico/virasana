from datetime import date, timedelta, datetime, time

import pandas as pd
from ajna_commons.flask.log import logger
from bhadrasana.models.apirecintos_risco import Pais
from flask import render_template, request, flash
from flask_login import login_required
from virasana.forms.filtros import FormFiltroData, FormFiltroEscaneamento
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
        SELECT c.numeroCEmercante, c.portoDestFinal as porto_final, 
        m.portoDescarregamento as porto_baldeacao 
        FROM conhecimentosresumo c INNER JOIN manifestosresumo m ON c.manifestoCE = m.numero
        WHERE c.dataEmissao between '{}' and '{}'
        AND c.portoOrigemCarga = '{}'
        AND (
        (substring(c.portoDestFinal, 1, 2) IN (SELECT sigla FROM risco_paises WHERE escaneamento is True))
        OR 
        (substring(m.portoDescarregamento, 1, 2) IN (SELECT sigla FROM risco_paises WHERE escaneamento is True))
        ) limit 5000
        '''.format(inicio, fim, porto_origem)
    # print(SQL_ESCANEAMENTOS)
    return pd.read_sql(SQL_ESCANEAMENTOS, engine)

def get_escaneamentos_obrigatorios_impo(engine, inicio, fim, porto_destino):
    SQL_ESCANEAMENTOS = \
        '''
        SELECT c.numeroCEmercante, c.portoDestFinal as porto_final, m.portoDescarregamento as porto_baldeacao
        FROM conhecimentosresumo c INNER JOIN manifestosresumo m ON c.manifestoCE = m.numero
        INNER JOIN escalamanifestoresumo em ON em.manifesto = m.numero
        INNER JOIN escalasresumo e ON e.numeroDaEscala = em.escala
        WHERE e.dataPrevistaAtracacao between '{}' and '{}'
        AND c.tipoTrafego = '5'
        AND (c.portoDestFinal = '{}' or m.portoDescarregamento = '{}') limit 5000
        '''.format(inicio, fim, porto_destino, porto_destino)
    # print(SQL_ESCANEAMENTOS)
    return pd.read_sql(SQL_ESCANEAMENTOS, engine)


def completa_nome_pais(session, sigla_porto):
    lpais = session.query(Pais).filter(Pais.sigla ==  sigla_porto[:2]).one_or_none()
    if lpais is None:
        return sigla_porto
    return f'{sigla_porto} - {lpais.nome}'


def get_embarques_sem_imagem(mongodb, session, inicio, fim, porto, sentido='EXPO'):
    inicio_scan = datetime.combine(inicio - timedelta(days=10), time.min)
    fim_scan = datetime.combine(fim + timedelta(days=10), time.max)
    vazios = []
    semimagem = []
    comimagem = []
    logger.info(f'Checando embarques (emissão CE) entre {inicio} e {fim} com escaneamentos'
                f' entre {inicio_scan} e {fim_scan}')
    if sentido == 'EXPO':
        df_escaneamentos_obrigatorios = get_escaneamentos_obrigatorios(session.get_bind(), inicio, fim, porto)
    else:
        df_escaneamentos_obrigatorios = get_escaneamentos_obrigatorios_impo(session.get_bind(), inicio,
                                                                            fim, porto)
    # print(df_escaneamentos_obrigatorios)
    for row in df_escaneamentos_obrigatorios.itertuples(index=False):
        cemercante = row.numeroCEmercante
        nome_pais_porto_final = completa_nome_pais(session, row.porto_final)
        if row.porto_baldeacao == row.porto_final:
            nome_pais_porto_baldeacao = nome_pais_porto_final
        else:
            nome_pais_porto_baldeacao = completa_nome_pais(session, row.porto_baldeacao)
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
                    evazio = True
                    predictions = imagem['metadata'].get('predictions')
                    if predictions and isinstance(predictions, list):
                        evazio = predictions[0].get('vazio', True)
                    if evazio is False:
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
        form = FormFiltroEscaneamento(request.form,
                              start=date.today() - timedelta(days=1),
                              end=date.today())
        try:
            if request.method == 'POST':
                semimagem, vazios, comimagem = get_embarques_sem_imagem(mongodb, session,
                                                                        form.start.data,
                                                                        form.end.data,
                                                                        'BRSSZ',
                                                                        form.sentido.data)
                # logger.debug(stats_cache)
        except Exception as err:
            flash(err)
            logger.error(err, exc_info=True)
        return render_template('confere_escaneamento.html',
                               semimagem=semimagem,
                               vazios=vazios,
                               comimagem=comimagem,
                               oform=form)
