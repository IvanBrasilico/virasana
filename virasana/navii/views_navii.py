# coding: utf-8
"""
Blueprint Flask do NAVI-I — Análise de risco na importação.

Expõe:
  GET /navii/            -> página HTML (filtros + cards com rolagem infinita)
  GET /navii/extrato?CE=  -> extrato do CE: dados do CE uma vez e um card por contêiner (extrato_navii.py)
  GET /navii/api/ces      -> JSON paginado (consumido pelo JS da página); na
                             primeira página (sem cursor) traz também `total`
                             de contêineres que o filtro encontrou na base; `tempos`
                             (segundos) mostra à tela quanto custou cada parte
  GET /navii/api/sugestoes -> autocompletar de porto, NCM e recinto
  GET|POST|DELETE /navii/api/filtros -> filtros salvos do usuário (MongoDB,
                             ver `filtros_salvos.py`)
  GET|POST /navii/api/ficha -> abrir Ficha pelo card (tratamento do risco, `ficha_navii.py`): GET prévia
                             para o diálogo; POST abre (ou acrescenta o contêiner à Ficha aberta do CE)
  GET /navii/fichas        -> aba "Fichas em acompanhamento" (`acompanhamento_fichas.py`)
  GET /navii/api/fichas    -> JSON da aba (?minhas=1&navii=1&verificadas=1)
  POST /navii/api/uso      -> cards vistos de uma pesquisa (segundos visíveis e interações), em lotes do navegador
  GET /navii/estatisticas  -> aba "Estatísticas de uso" (`estatisticas_uso.py`); JSON em /navii/api/estatisticas
                             (?tipo=mes|trimestre|semestre|ano&codigo=2026-10), planilha em
                             /navii/estatisticas/planilha; POST /navii/api/estatisticas/universo calcula os pedaços
                             do universo que faltam (a tela chama até zerar)
  GET /navii/texto         -> aba "Busca por texto": o mesmo navii.html em modo texto (?q= abre já buscando)
  GET /navii/api/texto     -> ?q=&de=&ate=&pagina=: CEs do índice do Meilisearch (`texto_navii.py`), na ordem de
                             relevância, com os cards (o mesmo caminho da busca, pelo filtro `ce`) e os trechos

A tela, a pesquisa e o extrato deixam uma linha `NAVII_USO` no log do servidor (`uso_navii.py`). Para as
estatísticas de uso, cada pesquisa fica no Mongo com os cards enviados e os vistos (`coleta_uso.py`): a 1ª página
devolve o id da pesquisa (`pesquisa`), que o navegador manda nas páginas seguintes, nos vistos e no "Abrir Ficha".

Depende de `busca_ces_navii`, `conta_conteineres_navii` e `monta_cards_navii`,
definidos em `busca_navii.py`.
"""
import io
import os
from datetime import date, datetime, timedelta
from time import perf_counter
from typing import List, Optional

import requests
from flask import Blueprint, current_app, jsonify, render_template, request, send_file, url_for
from flask_login import current_user, login_required
from werkzeug.routing import BuildError

from virasana.analytic_scripts.ciclo_de_vida import estagios_ciclo
from virasana.navii import (
    acompanhamento_fichas, coleta_uso, dta_navii, estatisticas_uso, extrato_navii, ficha_navii, filtros_salvos,
    referencias, risco_navii, texto_navii, uso_navii,
)
from virasana.navii.busca_navii import (
    CAMPOS_ESCOLHA, CAMPOS_LISTA, CAMPOS_MARCAVEIS, CAMPOS_PESO, MINIMO_OCULTAR, ROTULOS_CORAD, TIPOS_JANELA_VALIDOS,
    busca_ces_navii,
    conta_conteineres_navii, monta_cards_navii, normaliza_filtros, PORTO_ESCALA_PADRAO, _janela_efetiva,
    _janela_previsao, _periodo_navii, sugere_recintos,
)

bp_navii = Blueprint(
    'navii', __name__,
    url_prefix='/navii',
    template_folder='templates',
)

# Aba "Busca por texto" (plano em CONTEXTO.md, "Busca por texto"; F2 em 19/10/2026)
MINIMO_TEXTO = 5                 # caracteres para disparar a busca (decisão do usuário: 5, não 3)
CES_POR_PAGINA_TEXTO = 10
MAX_CARDS_TEXTO = 300            # cards por página (CE com muitos itens: o resto fica no extrato)
DIAS_TEXTO = (15, 30)            # período padrão: previsão de atracação de hoje -15 a +30 dias
TIMEOUT_TEXTO = 10               # s: o Meilisearch é local; mais que isso é falha
JANELA_TEXTO = 'texto'           # a "janela" das pesquisas desta aba na coleta de uso


def _imagem_url_template() -> str:
    """URL da miniatura da imagem (rota `/mini1/<_id>` de virasana/views.py),
    com 'imagem_id' no lugar do id — o JS troca pelo id de cada imagem.

    Em produção a app roda sob o prefixo /virasana atrás de proxy: a URL
    precisa sair do `url_for`/`script_root`, nunca fixa. Sem a rota registrada
    (testes, app isolada), monta pelo `script_root`.
    """
    try:
        return url_for('mini', _id='imagem_id')
    except BuildError:
        return request.script_root + '/mini1/imagem_id'


def _imagem_cheia_template() -> str:
    """URL da imagem completa (rota `/image/<_id>` de virasana/views.py), com 'imagem_id' no lugar do id."""
    try:
        return url_for('image_id', _id='imagem_id')
    except BuildError:
        return request.script_root + '/image/imagem_id'


def _serializa_valor(v):
    if isinstance(v, datetime):
        return v.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(v, dict):
        return {k: _serializa_valor(sub) for k, sub in v.items()}
    if isinstance(v, list):
        return [_serializa_valor(sub) for sub in v]
    if isinstance(v, (set, tuple)):
        return sorted(v)
    return v


def _serializa_card(card: dict) -> dict:
    """Converte datetimes (inclusive dentro da linha do tempo e dos escaneamentos) para string, para virar JSON."""
    return {k: _serializa_valor(v) for k, v in card.items()}


def _defaults_periodo():
    """Sugestão de período inicial (pré-preenchida na tela) para cada janela."""
    inicio_prevista, fim_prevista = _janela_previsao()
    inicio_efetiva, fim_efetiva = _janela_efetiva()
    return {
        'prevista': {'inicio': inicio_prevista, 'fim': fim_prevista},
        'efetiva': {'inicio': inicio_efetiva.strftime('%Y-%m-%d'), 'fim': fim_efetiva.strftime('%Y-%m-%d')},
    }


def _contexto_da_tela(modo_texto: bool = False) -> dict:
    """Variáveis do navii.html — a aba de análise e a de busca por texto (o mesmo template, `modo_texto`)."""
    return dict(
        imagem_url_template=_imagem_url_template(),
        defaults_periodo=_defaults_periodo(),
        porto_escala_padrao=PORTO_ESCALA_PADRAO,
        tipos_bl=referencias.tipos_bl(),
        estagios=estagios_ciclo.lista(),
        filtros_iniciais=None if modo_texto else _filtros_iniciais(),
        extrato_url=url_for('navii.extrato'),
        filtros_risco=risco_navii.ROTULOS_FILTRO,
        rotulos_corad=ROTULOS_CORAD,
        opcoes_tem_dta=dta_navii.OPCOES_TEM_DTA,
        risco_parametros_url=_url_parametros_risco(),
        segundos_lenta=uso_navii.SEGUNDOS_LENTA,
        minimo_ocultar=MINIMO_OCULTAR,
        ficha_url=url_for('navii.api_ficha'),
        uso_url=url_for('navii.api_uso'),
        modo_texto=modo_texto,
        texto_url=url_for('navii.api_texto'),
        minimo_texto=MINIMO_TEXTO,
        periodo_texto=periodo_texto_padrao(),
        texto_inicial=' '.join((request.args.get('q') or '').split())[:200] if modo_texto else '',
    )


@bp_navii.route('/', strict_slashes=False)  # sem redirect da barra final (proxy; ver CLAUDE.md)
@login_required
def index():
    # aberta por link (completude, card, extrato...): os parâmetros mostram de onde veio
    uso_navii.registra('tela', _usuario(), link=request.args.to_dict(flat=False) or None)
    return render_template('navii.html', **_contexto_da_tela())


@bp_navii.route('/texto', strict_slashes=False)  # sem redirect da barra final (proxy)
@login_required
def texto():
    """Aba "Busca por texto": o navii.html em modo texto (campo de texto e período no lugar dos filtros)."""
    uso_navii.registra('tela', _usuario(), aba='texto', link=request.args.to_dict(flat=False) or None)
    return render_template('navii.html', **_contexto_da_tela(modo_texto=True))


def periodo_texto_padrao(hoje: Optional[date] = None) -> dict:
    hoje = hoje or date.today()
    return {'inicio': (hoje - timedelta(days=DIAS_TEXTO[0])).isoformat(),
            'fim': (hoje + timedelta(days=DIAS_TEXTO[1])).isoformat()}


@bp_navii.app_template_global('url_extrato_ce')
def url_extrato_ce(numero: str) -> str:
    """Link para o extrato do CE, para qualquer tela da app que mostre um número de CE (com o prefixo da app).
    Nos templates: `{% if url_extrato_ce is defined %}` — só existe com o NAVI-I registrado."""
    return url_for('navii.extrato', CE=numero)


@bp_navii.route('/extrato', strict_slashes=False)  # sem redirect da barra final (proxy)
@login_required
def extrato():
    """Extrato do CE (?CE=número): os dados do CE uma vez e um card por contêiner, com imagem e eventos."""
    numero = extrato_navii.normaliza_ce(request.args.get('CE') or request.args.get('ce'))
    dados, segundos = None, None
    if numero:
        dados, segundos = _cronometrado(extrato_navii.monta_extrato, current_app.config['db_session'], numero)
    uso_navii.registra('extrato', _usuario(), segundos, ce=numero or request.args.get('CE') or '',
                       conteineres=len(dados['conteineres']) if dados else 0)
    return render_template(
        'navii_extrato.html', numero=numero, dados=dados,
        extrato_url=url_for('navii.extrato'), navii_url=url_for('navii.index'),
        imagem_url_template=_imagem_url_template(), imagem_cheia_template=_imagem_cheia_template(),
        risco_parametros_url=_url_parametros_risco(),
    )


def _url_parametros_risco() -> Optional[str]:
    """Página dos parâmetros do risco por CE (memória de cálculo do card), se o blueprint estiver registrado."""
    try:
        return url_for('risco_historico.parametros_em_uso')
    except BuildError:
        return None


def _data_valida(valor: Optional[str]) -> bool:
    try:
        datetime.strptime(valor or '', '%Y-%m-%d')
    except ValueError:
        return False
    return True


def _filtros_iniciais() -> Optional[dict]:
    """Tela aberta por um LINK (ex.: contêiner na completude ou no card): janela, período e filtros da URL, no
    lugar dos padrões da tela (inclusive o porto de atracação sugerido). None quando a URL não traz filtros."""
    filtros = _filtros_da_requisicao()
    tipo_janela = request.args.get('tipo_janela')
    inicio, fim = filtros.pop('data_inicio', None), filtros.pop('data_fim', None)
    if not filtros and tipo_janela not in TIPOS_JANELA_VALIDOS:
        return None
    return {
        'tipo_janela': tipo_janela if tipo_janela in TIPOS_JANELA_VALIDOS else 'efetiva',
        'periodo': {'inicio': inicio, 'fim': fim} if _data_valida(inicio) and _data_valida(fim) else None,
        'filtros': normaliza_filtros(filtros),
    }


def _filtros_da_requisicao() -> dict:
    """Filtros da busca a partir da query string da API."""
    filtros = {}
    for campo in CAMPOS_LISTA:  # repetidos (?ncm=8471&ncm=6109) ou separados por vírgula
        valores = [v for v in request.args.getlist(campo) if v.strip()]
        if valores:
            filtros[campo] = valores
    # peso: a normalização (busca_navii) converte e descarta o inválido
    for campo in ('produto', 'empresa') + CAMPOS_ESCOLHA + CAMPOS_PESO:
        valor = (request.args.get(campo) or '').strip()
        if valor:
            filtros[campo] = valor
    for campo in CAMPOS_MARCAVEIS:
        if request.args.get(campo) in ('1', 'true', 'True'):
            filtros[campo] = True
    ncms_max = request.args.get('ncms_max', type=int)
    if ncms_max:
        filtros['ncms_max'] = ncms_max
    data_inicio, data_fim = request.args.get('data_inicio'), request.args.get('data_fim')
    if data_inicio and data_fim:
        filtros['data_inicio'] = data_inicio
        filtros['data_fim'] = data_fim
    return filtros


@bp_navii.route('/api/ces')
@login_required
def api_ces():
    session = current_app.config['db_session']
    inicio = perf_counter()

    tipo_janela = request.args.get('tipo_janela', 'efetiva')
    cursor_id = request.args.get('cursor_id', type=int)
    limit = request.args.get('limit', default=20, type=int)
    limit = max(1, min(limit, 50))
    filtros = _filtros_da_requisicao()

    try:
        resultados, t_consulta = _cronometrado(
            busca_ces_navii,
            session,
            tipo_janela=tipo_janela,
            filtros=filtros,
            cursor_id=cursor_id,
            limit=limit,
        )
        # Contagem só na primeira página: nas seguintes a tela já tem o total.
        total, t_contagem = (None, None) if cursor_id else _cronometrado(
            conta_conteineres_navii, session, tipo_janela, filtros)
    except ValueError as err:
        uso_navii.registra('pesquisa', _usuario(), perf_counter() - inicio, janela=tipo_janela, filtros=filtros,
                           erro=str(err))
        return jsonify({'erro': str(err)}), 400

    montados, t_cards = _cronometrado(monta_cards_navii, session, resultados)
    cards = [_serializa_card(card) for card in montados]
    avisos = []   # códigos de OCULTAR ignorados (curtos ou que esconderiam um filtro positivo): a tela mostra
    normaliza_filtros(filtros, avisos)
    next_cursor = cards[-1]['item_id'] if len(cards) == limit else None
    tempos = {'consulta': t_consulta, 'contagem': t_contagem, 'cards': t_cards}
    pesquisa = _registra_pesquisa(session, cursor_id, tipo_janela, filtros, total, tempos, cards)

    uso_navii.registra('pesquisa', _usuario(), perf_counter() - inicio, janela=tipo_janela,
                       pagina='1' if not cursor_id else 'seguinte', filtros=filtros, total=total, n_cards=len(cards),
                       t_consulta=t_consulta, t_contagem=t_contagem, t_cards=t_cards)
    return jsonify({
        'cards': cards,
        'next_cursor': next_cursor,
        'total': total,
        'tempos': tempos,
        'tipo_janela': tipo_janela,
        'filtros': filtros,
        'avisos': avisos,
        'pesquisa': pesquisa,
    })


def _coleta(registro, padrao=None):
    """Chama a coleta de uso (`coleta_uso.py`) com a coleção do Mongo, sem nunca derrubar a tela (como o log de
    uso): sem Mongo configurado ou com falha, devolve `padrao`."""
    db = current_app.config.get('mongodb')
    if db is None or not _usuario():
        return padrao
    try:
        return registro(coleta_uso.colecao(db))
    except Exception:  # noqa: a coleta de uso é acessória
        current_app.logger.warning('NAVI-I: falha na coleta de uso', exc_info=True)
        return padrao


def _setor(session, usuario) -> Optional[str]:
    try:
        return coleta_uso.setor_do_usuario(session, usuario)
    except Exception:  # noqa: sem o setor, a pesquisa é gravada assim mesmo
        current_app.logger.warning('NAVI-I: setor do usuário no Fichas não lido', exc_info=True)
        return None


def _registra_pesquisa(session, cursor_id, tipo_janela: str, filtros: dict, total, tempos: dict,
                       cards: list) -> Optional[str]:
    """Coleta de uso da aba de análise (ver `_registra_na_coleta`)."""
    return _registra_na_coleta(session, not cursor_id, tipo_janela, _periodo_navii(tipo_janela, filtros),
                               normaliza_filtros(filtros), total, tempos, cards)


def _registra_na_coleta(session, primeira: bool, janela: str, periodo, filtros: dict, total, tempos: dict,
                        cards: list) -> Optional[str]:
    """Coleta de uso: a 1ª página grava a pesquisa (com a aba, a pesquisa anterior e a origem que o navegador manda)
    e devolve o id; as seguintes acrescentam os cards à pesquisa do parâmetro `pesquisa` e o devolvem."""
    usuario = _usuario()
    if not primeira:
        pesquisa = request.args.get('pesquisa')
        return pesquisa if _coleta(lambda col: coleta_uso.registra_pagina(col, pesquisa, usuario, cards)) else None
    setor = _setor(session, usuario) if usuario else None
    return _coleta(lambda col: coleta_uso.registra_pesquisa(
        col, usuario, setor, request.args.get('aba'), request.args.get('anterior'), request.args.get('origem'),
        janela, periodo, filtros, total, tempos, cards))


def _cliente_texto():
    """O cliente de busca do Meilisearch: o da configuração da app (`navii_meili`, testes e demonstração) ou o do
    ambiente (chave de busca); None sem a chave."""
    cliente = current_app.config.get('navii_meili')
    if cliente is not None:
        return cliente
    if not os.environ.get(texto_navii.VARIAVEL_BUSCA):
        return None
    return texto_navii.cliente_do_ambiente(texto_navii.VARIAVEL_BUSCA, timeout=TIMEOUT_TEXTO)


def _dia_ou_nada(valor: Optional[str]) -> Optional[date]:
    """Data AAAA-MM-DD da tela; vazio = sem limite; inválida: ValueError."""
    return datetime.strptime(valor, '%Y-%m-%d').date() if valor else None


def _numero_ce(card: dict) -> str:
    return (card.get('numeroCEmercante') or '').strip()


def cards_do_texto(session, achados: List[texto_navii.Achado]):
    """(cards, sem_card, cortado, tempos) dos CEs achados pela busca por texto: os cards pelo caminho da busca (filtro
    `ce`, sem limite de data), na ordem de relevância dos CEs e, dentro do CE, na da busca (Item.ID decrescente), cada
    um com os trechos do seu CE (HTML já escapado); `sem_card`: os CEs sem item no Mercante (sem card); `cortado`: a
    página passou de MAX_CARDS_TEXTO cards."""
    if not achados:
        return [], [], False, {'consulta': 0.0, 'cards': 0.0}
    ordem = {achado.ce: posicao for posicao, achado in enumerate(achados)}
    trechos = {achado.ce: [{'campo': campo, 'html': texto_navii.trecho_html(trecho)} for campo, trecho in
                           achado.trechos] for achado in achados}
    resultados, t_consulta = _cronometrado(busca_ces_navii, session, tipo_janela='prevista',
                                           filtros={'ce': list(ordem)}, limit=MAX_CARDS_TEXTO)
    montados, t_cards = _cronometrado(monta_cards_navii, session, resultados)
    cards = sorted((_serializa_card(card) for card in montados),
                   key=lambda card: ordem.get(_numero_ce(card), len(ordem)))      # estável: mantém a ordem no CE
    for card in cards:
        card['trechos'] = trechos.get(_numero_ce(card), [])
    com_card = {_numero_ce(card) for card in cards}
    sem_card = [{'ce': achado.ce, 'referencia': achado.referencia.isoformat() if achado.referencia else None,
                 'trechos': trechos[achado.ce]} for achado in achados if achado.ce not in com_card]
    return cards, sem_card, len(resultados) >= MAX_CARDS_TEXTO, {'consulta': t_consulta, 'cards': t_cards}


@bp_navii.route('/api/texto')
@login_required
def api_texto():
    """Busca por texto: ?q= (pelo menos MINIMO_TEXTO caracteres), de/ate (AAAA-MM-DD, previsão de atracação de
    referência; vazios = sem limite) e pagina (0, 1...). Uma página = até CES_POR_PAGINA_TEXTO CEs do Meilisearch,
    com os cards (`cards_do_texto`). Sem o Meilisearch (ou sem a chave), 503: só esta aba fica sem busca."""
    inicio, usuario = perf_counter(), _usuario()
    texto = ' '.join((request.args.get('q') or '').split())[:200]
    if len(texto) < MINIMO_TEXTO:
        return jsonify({'erro': f'Digite pelo menos {MINIMO_TEXTO} caracteres.'}), 400
    try:
        de, ate = _dia_ou_nada(request.args.get('de')), _dia_ou_nada(request.args.get('ate'))
    except ValueError:
        return jsonify({'erro': 'Data inválida (use AAAA-MM-DD).'}), 400
    pagina = max(0, request.args.get('pagina', default=0, type=int))
    cliente = _cliente_texto()
    if cliente is None:
        return jsonify({'erro': 'A busca por texto não está configurada neste servidor '
                                f'({texto_navii.VARIAVEL_BUSCA}).'}), 503
    try:
        resultado, t_busca = _cronometrado(texto_navii.busca, cliente, texto, de, ate, CES_POR_PAGINA_TEXTO,
                                           pagina * CES_POR_PAGINA_TEXTO)
    except (requests.RequestException, texto_navii.ErroMeili) as erro:
        current_app.logger.warning(f'NAVI-I: busca por texto indisponível: {erro}')
        uso_navii.registra('texto', usuario, perf_counter() - inicio, texto=texto, erro='meilisearch')
        return jsonify({'erro': 'A busca por texto está indisponível no momento; as outras abas do NAVI-I '
                                'funcionam normalmente.'}), 503
    session = current_app.config['db_session']
    cards, sem_card, cortado, tempos = cards_do_texto(session, resultado.achados)
    tempos['busca'] = t_busca
    vistos = (pagina + 1) * CES_POR_PAGINA_TEXTO
    proxima = pagina + 1 if len(resultado.achados) == CES_POR_PAGINA_TEXTO and vistos < resultado.total else None
    periodo = (de.isoformat() if de else None, ate.isoformat() if ate else None)
    pesquisa = _registra_na_coleta(session, pagina == 0, JANELA_TEXTO, periodo, {'texto': texto}, resultado.total,
                                   tempos, cards)
    uso_navii.registra('texto', usuario, perf_counter() - inicio, pagina=pagina + 1, texto=texto, de=periodo[0],
                       ate=periodo[1], total=resultado.total, n_ces=len(resultado.achados), n_cards=len(cards),
                       t_busca=t_busca, t_consulta=tempos['consulta'], t_cards=tempos['cards'])
    avisos = ['Página com muitos contêineres: alguns CEs aparecem só como linha, sem card — veja o extrato.'] \
        if cortado else []
    return jsonify({'cards': cards, 'sem_card': sem_card, 'proxima': proxima, 'total': resultado.total,
                    'tempos': tempos, 'pesquisa': pesquisa, 'avisos': avisos})


@bp_navii.route('/api/uso', methods=['POST'])
@login_required
def api_uso():
    """Cards vistos de uma pesquisa — {pesquisa, vistos: [{item, s: segundos visíveis acumulados, x: interações}]},
    em lotes do navegador (`fetch keepalive`). O servidor só aceita cards que enviou ao usuário nessa pesquisa."""
    usuario = _usuario()
    if not usuario:
        return jsonify({'erro': 'usuário não identificado'}), 401
    dados = request.get_json(silent=True)
    if not isinstance(dados, dict):
        return jsonify({'erro': 'corpo JSON esperado'}), 400
    aceitos = _coleta(lambda col: coleta_uso.registra_vistos(col, dados.get('pesquisa'), usuario, dados.get('vistos')),
                      padrao=0)
    if aceitos is None:
        return jsonify({'erro': 'pesquisa não encontrada'}), 404
    return jsonify({'aceitos': aceitos})


def _cronometrado(funcao, *args, **kwargs):
    """(resultado, segundos) da chamada: a tela mostra ao usuário quanto custou cada consulta."""
    inicio = perf_counter()
    resultado = funcao(*args, **kwargs)
    return resultado, round(perf_counter() - inicio, 3)


def _usuario():
    """Identificação do usuário logado (dono dos filtros salvos)."""
    return getattr(current_user, 'id', None)


@bp_navii.route('/api/sugestoes')
@login_required
def api_sugestoes():
    """Autocompletar dos campos da tela: ?campo=porto|ncm|recinto&q=texto."""
    campo, termo = request.args.get('campo'), request.args.get('q', '')
    if campo == 'porto':
        sugestoes = referencias.sugere_portos(termo)
    elif campo == 'ncm':
        sugestoes = referencias.sugere_ncm(termo)
    elif campo == 'recinto':
        sugestoes = sugere_recintos(current_app.config['db_session'], termo)
    else:
        return jsonify({'erro': 'campo inválido'}), 400
    return jsonify({'sugestoes': sugestoes})


@bp_navii.route('/fichas', strict_slashes=False)  # sem redirect da barra final (proxy)
@login_required
def fichas():
    """Aba "Fichas em acompanhamento": o tratamento do risco no Fichas (dados pela API, `api_fichas`)."""
    uso_navii.registra('fichas', _usuario(), link=request.args.to_dict(flat=False) or None)
    return render_template('navii_fichas.html', api_url=url_for('navii.api_fichas'),
                           navii_url=url_for('navii.index'), extrato_url=url_for('navii.extrato'),
                           imagem_url_template=_imagem_url_template())


@bp_navii.route('/api/fichas')
@login_required
def api_fichas():
    """Fichas de repressão na importação em acompanhamento (ver `acompanhamento_fichas.py`)."""
    inicio = perf_counter()
    usuario = _usuario()
    if not usuario:
        return jsonify({'erro': 'usuário não identificado'}), 401
    opcoes = {nome: request.args.get(nome) in ('1', 'true') for nome in ('minhas', 'navii', 'verificadas')}
    dados = acompanhamento_fichas.fichas_em_acompanhamento(current_app.config['db_session'], usuario, **opcoes)
    uso_navii.registra('fichas', usuario, perf_counter() - inicio, pagina='api', total=dados['total'],
                       alertas=dados['alertas'], **{k: '1' for k, v in opcoes.items() if v})
    return jsonify(dados)


MAX_PENDENTES = 2000          # dias do universo por chamada (um ano e o anterior, nas duas janelas)
ORCAMENTO_UNIVERSO = 5.0      # segundos por chamada: um dia frio leva até ~7,5 s; cabe nos 30 s do gunicorn


@bp_navii.route('/estatisticas', strict_slashes=False)  # sem redirect da barra final (proxy)
@login_required
def estatisticas():
    """Aba "Estatísticas de uso" (dados pela API, `api_estatisticas`; cálculo em `estatisticas_uso.py`)."""
    uso_navii.registra('estatisticas', _usuario(), link=request.args.to_dict(flat=False) or None)
    return render_template('navii_estatisticas.html', api_url=url_for('navii.api_estatisticas'),
                           universo_url=url_for('navii.api_estatisticas_universo'),
                           planilha_url=url_for('navii.planilha_estatisticas'), navii_url=url_for('navii.index'),
                           fichas_url=url_for('navii.fichas'))


def _porto_estatisticas() -> str:
    return current_app.config.get('navii_porto_estatisticas') or estatisticas_uso.PORTO_PADRAO


def _dados_estatisticas():
    """(dados, None) ou (None, resposta de erro): 401 sem usuário, 503 sem o Mongo, 400 com período inválido."""
    usuario = _usuario()
    if not usuario:
        return None, (jsonify({'erro': 'usuário não identificado'}), 401)
    db = current_app.config.get('mongodb')
    if db is None:
        return None, (jsonify({'erro': 'Banco do uso (Mongo) não configurado no servidor.'}), 503)
    try:
        return estatisticas_uso.estatisticas(current_app.config['db_session'], db, usuario,
                                             request.args.get('tipo') or 'mes', request.args.get('codigo'),
                                             _porto_estatisticas()), None
    except ValueError as err:
        return None, (jsonify({'erro': str(err)}), 400)


@bp_navii.route('/api/estatisticas')
@login_required
def api_estatisticas():
    """As estatísticas de uso do período para o usuário logado (o acesso por usuário é decidido no servidor)."""
    inicio = perf_counter()
    dados, erro = _dados_estatisticas()
    if erro:
        return erro
    uso_navii.registra('estatisticas', _usuario(), perf_counter() - inicio, pagina='api',
                       periodo=dados['periodo']['codigo'], pendentes=len(dados['pendentes']))
    return jsonify(dados)


@bp_navii.route('/api/estatisticas/universo', methods=['POST'])
@login_required
def api_estatisticas_universo():
    """Calcula, até o orçamento de segundos, os pedaços do universo que a tela recebeu em `pendentes`; devolve os
    `restantes` (a tela chama de novo até zerar)."""
    inicio = perf_counter()
    usuario = _usuario()
    if not usuario:
        return jsonify({'erro': 'usuário não identificado'}), 401
    db = current_app.config.get('mongodb')
    if db is None:
        return jsonify({'erro': 'Banco do uso (Mongo) não configurado no servidor.'}), 503
    corpo = request.get_json(silent=True)
    pendentes = corpo.get('pendentes') if isinstance(corpo, dict) else None
    if not isinstance(pendentes, list) or len(pendentes) > MAX_PENDENTES:
        return jsonify({'erro': f'Lista de pendentes inválida (no máximo {MAX_PENDENTES}).'}), 400
    try:
        pedacos = [estatisticas_uso.Pedaco.de_dict(p) for p in pendentes]
    except ValueError as err:
        return jsonify({'erro': str(err)}), 400
    restantes = estatisticas_uso.calcula_pendencias(
        current_app.config['db_session'], estatisticas_uso.colecao_universo(db), _porto_estatisticas(), pedacos,
        datetime.now(), orcamento=ORCAMENTO_UNIVERSO)
    uso_navii.registra('estatisticas', usuario, perf_counter() - inicio, pagina='universo',
                       calculados=len(pedacos) - len(restantes), restantes=len(restantes))
    return jsonify({'restantes': [p.como_dict() for p in restantes], 'calculados': len(pedacos) - len(restantes)})


@bp_navii.route('/estatisticas/planilha')
@login_required
def planilha_estatisticas():
    """A aba em planilha Excel (só as linhas por usuário que quem baixa pode ver)."""
    dados, erro = _dados_estatisticas()
    if erro:
        return erro
    uso_navii.registra('estatisticas', _usuario(), pagina='planilha', periodo=dados['periodo']['codigo'])
    return send_file(io.BytesIO(estatisticas_uso.planilha_xlsx(dados)), as_attachment=True,
                     download_name=f"estatisticas_navii_{dados['periodo']['codigo']}.xlsx",
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@bp_navii.route('/api/ficha', methods=['GET', 'POST'])
@login_required
def api_ficha():
    """Abrir Ficha pelo card — o tratamento do risco (`ficha_navii.py`; regra no Fichas, `navii_ficha.py`).

    GET ?ce=&conteiner= -> prévia para o diálogo (o que será feito, impedimento, Fichas do CE, textos);
    POST {ce, conteiner, resumo, bloqueio, agendada, pesquisa} -> abre a Ficha, ou acrescenta o contêiner à Ficha de
    repressão aberta do CE. O servidor remonta tudo pelo card (do cliente vêm só o resumo e as caixas); a Ficha fica
    registrada na pesquisa de onde veio (coleta de uso).
    """
    inicio = perf_counter()
    usuario = _usuario()
    if not usuario:
        return jsonify({'erro': 'usuário não identificado'}), 401
    dados = (request.get_json(silent=True) or {}) if request.method == 'POST' else request.args
    ce, conteiner = (dados.get('ce') or '').strip(), (dados.get('conteiner') or '').strip()
    try:
        regra = ficha_navii.regra_do_fichas()
    except Exception:  # noqa: o módulo do Fichas não carregou — só esta função fica indisponível
        current_app.logger.exception('NAVI-I: módulo do Fichas (navii_ficha) não carregou')
        return jsonify({'erro': 'Abertura de Ficha indisponível: o módulo do Fichas não carregou.'}), 503
    session = current_app.config['db_session']
    card = ficha_navii.card_do_conteiner(session, ce, conteiner) if ce and conteiner else None
    if card is None:
        return jsonify({'erro': 'Contêiner não encontrado neste CE.'}), 404
    mongo_fichas = ficha_navii.mongodb_fichas(current_app.config)
    if request.method == 'GET':
        return jsonify(_previa_ficha(regra, session, usuario, card, mongo_fichas))
    if mongo_fichas is None:
        return jsonify({'erro': 'Banco de imagens do Fichas (MONGODB_RISCO) não configurado no servidor.'}), 503
    try:
        resultado = regra.abre_ficha_navii(session, mongo_fichas, usuario, ficha_navii.dados_da_ficha(
            regra, current_app.config.get('mongodb'), card, dados.get('resumo'), dados.get('bloqueio'),
            dados.get('agendada')))
    except regra.ERROS_DE_NEGOCIO as err:
        uso_navii.registra('ficha', usuario, perf_counter() - inicio, ce=ce, conteiner=conteiner, erro=str(err))
        return jsonify({'erro': str(err)}), 400
    uso_navii.registra('ficha', usuario, perf_counter() - inicio, ce=ce, conteiner=conteiner,
                       resultado=resultado.acao, ficha=resultado.ovr_id)
    _coleta(lambda col: coleta_uso.registra_acao(col, dados.get('pesquisa'), usuario, {
        't': 'ficha', 'ficha': resultado.ovr_id, 'resultado': resultado.acao, 'ce': ce, 'c': conteiner}))
    return jsonify({'acao': resultado.acao, 'ovr_id': resultado.ovr_id, 'rvf_id': resultado.rvf_id,
                    'eventos': resultado.eventos, 'imagens': resultado.imagens,
                    'url': ficha_navii.url_da_ficha(resultado.ovr_id)})


def _previa_ficha(regra, session, usuario: str, card: dict, mongo_fichas) -> dict:
    previa = regra.previa(session, usuario, card['numeroCEmercante'], card['numeroConteiner'])
    if mongo_fichas is None and not previa['impedimento']:
        previa['impedimento'] = 'Banco de imagens do Fichas (MONGODB_RISCO) não configurado no servidor.'
    for ficha in [previa['ficha'], *previa['outras']]:
        if ficha:
            ficha['url'] = ficha_navii.url_da_ficha(ficha['id'])
    return {**previa, 'ce': card['numeroCEmercante'], 'conteiner': card['numeroConteiner'],
            'resumo': ficha_navii.resumo_padrao(card), 'descricao': ficha_navii.descricao_carga(card),
            'memoria': ficha_navii.memoria_risco(card), 'limiteResumo': ficha_navii.TAMANHO_RESUMO,
            'imagens': min(len(card.get('imagens_ids') or []), ficha_navii.MAX_IMAGENS)}


@bp_navii.route('/api/filtros', methods=['GET', 'POST', 'DELETE'])
@login_required
def api_filtros_salvos():
    """Filtros salvos do usuário logado.

    GET lista; POST {nome, tipo_janela, filtros} cria ou substitui; DELETE
    ?nome= apaga. Todas devolvem a lista atualizada em `filtros_salvos`.
    """
    usuario = _usuario()
    if not usuario:
        return jsonify({'erro': 'usuário não identificado'}), 401
    col = filtros_salvos.colecao(current_app.config['mongodb'])
    try:
        if request.method == 'POST':
            dados = request.get_json(silent=True) or {}
            filtros_salvos.salva(col, usuario, dados.get('nome'), dados.get('tipo_janela'), dados.get('filtros'))
        elif request.method == 'DELETE':
            if not filtros_salvos.apaga(col, usuario, request.args.get('nome')):
                return jsonify({'erro': 'filtro não encontrado'}), 404
    except filtros_salvos.FiltroInvalido as err:
        return jsonify({'erro': str(err)}), 400
    salvos = [{**d, 'atualizado': _serializa_valor(d.get('atualizado')), 'criado': _serializa_valor(d.get('criado'))}
              for d in filtros_salvos.lista(col, usuario)]
    return jsonify({'filtros_salvos': salvos})
