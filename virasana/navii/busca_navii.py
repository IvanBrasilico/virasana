# coding: utf-8
"""
Camada de dados (SQLAlchemy) do NAVI-I — Análise de risco na importação.

Busca os contêineres declarados em CEs mercante (`Item`) dentro de uma janela
de atracação (prevista ou efetiva), para alimentar os cards em rolagem
infinita. Cada card = um contêiner declarado (CE + contêiner).

O Ciclo de Vida (API Recintos) é OPCIONAL: muitas vezes ele não localiza os
eventos (desembarque, saída, escaneamento...) e, na previsão de atracação, a
carga ainda nem desembarcou. Por isso a busca parte do CE declarado e o ciclo
é apenas cruzado quando existir — filtros que dependem dele ("somente com
imagem", "entregue ao importador") restringem aos contêineres que o têm.

Suporta filtros sucessivos (NCM, rota, porto de baldeação do manifesto,
produto na descrição do CE, recinto, tipo de BL — portos, NCM, recinto e tipo de
BL aceitam lista —, empresa, estágio do ciclo de vida (no operador, em trânsito,
em recinto seco, entregue... — ver `estagios_ciclo`), quantidade máxima de NCMs
distintos, somente com imagem de escaneamento, sem contêiner de potencial OOG),
contagem total do filtro e paginação por cursor (Item.ID decrescente).

Família do CE (sombra do ciclo de vida, 01–02/10/2026): o contêiner consta nos
itens do master E do(s) filhote(s), e o ciclo de vida guarda um só desses CEs.
Por isso o ciclo do card é procurado na FAMÍLIA do CE (o próprio, o master, os
filhotes e os irmãos), e o card mostra o tipo de BL e os filhotes do master. A
janela "efetiva" e os filtros que dependem do ciclo ligam pelo CE gravado no ciclo
E, no ciclo de vida v2, por qualquer CE da família (`ciclodevida_ciclo_ce`) — ver
`LIGACAO_FAMILIA` (02/10/2026).
A janela de dias (`_janela_previsao`/`_janela_efetiva`) é apenas a *sugestão*
de período inicial — `data_inicio`/`data_fim` em `filtros` sobrescrevem essa
sugestão.

A montagem da query (`monta_query_navii`) é separada da execução para que o
script `diagnostico_sql.py` rode EXPLAIN exatamente sobre as queries da tela.

Enriquecimento cruza também com dados do repo irmão bhadrasana2
(`bhadrasana.models.laudo`, tabela `laudo_empresas`) para resolver CNPJ→nome
de empresa, e com as tabelas locais de `virasana/navii/referencias` (posição
NCM, porto, país, navio — geradas de fontes públicas).

Fora de escopo por enquanto (sem fonte de dados/implementação ainda):
  - Filtros em linguagem natural (hoje são campos estruturados sucessivos).
  - Filtros subjetivos automáticos por empresa e sócios — não há fonte de
    dados de sócios no modelo atual.
  - Resolução de CPF (bagagens_pessoas) — embarcador/consignatário do
    Conhecimento só guardam CNPJ ou nome estrangeiro, nunca CPF.
"""
import os
import re
import sys
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, NamedTuple, Optional, Set, Tuple

from sqlalchemy import (
    BigInteger, Column, MetaData, Table, VARCHAR, and_, false, func, join, literal, not_, or_, select, union,
)
from sqlalchemy.orm import selectinload, undefer_group

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')

from virasana.integracao.mercante.mercantealchemy import (
    Conhecimento, Enumerado, Escala, EscalaManifesto, Item, Manifesto, NCMItem,
)
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import (
    CicloDeVidaCE, CicloDeVidaImportacao, CicloDeVidaImportacaoEscaneamentos, logger,
)
from virasana.analytic_scripts.ciclo_de_vida import estagios_ciclo
from bhadrasana.models.laudo import Empresa
from bhadrasana.models.riscocorad import MatrizCorad
from virasana.navii import dta_navii, fichas_ovr, referencias, risco_navii, tabelas_opcionais
from virasana.analytic_scripts.ciclo_de_vida.dados_conteiner import expressao_oog

# Tabela `ovr_recintos` do repo irmão bhadrasana2 (`bhadrasana.models.ovr.Recinto`)
# declarada aqui só com as colunas lidas pelo NAVI-I. Importar o model de lá
# carregaria o registry inteiro do OVR (relationship com RVF etc.), que não
# configura sem os demais models do bhadrasana — e só lemos código -> nome.
# `test_navii_busca_sqlite.TestTabelaRecintos` garante que as colunas batem.
tabela_recintos = Table(
    'ovr_recintos', MetaData(),
    Column('id', BigInteger, primary_key=True),
    Column('nome', VARCHAR(50)),
    Column('cod_siscomex', VARCHAR(20)),
    Column('cod_carga', VARCHAR(20)),
)

# Janelas de atracação (em dias, a partir de hoje) — usadas só como SUGESTÃO
# de período inicial na tela; o usuário pode escolher outro intervalo.
JANELA_PREVISTA_MIN_DIAS = 3
JANELA_PREVISTA_MAX_DIAS = 15
JANELA_EFETIVA_MIN_DIAS = 5
JANELA_EFETIVA_MAX_DIAS = 15

TIPOS_JANELA_VALIDOS = ('prevista', 'efetiva')

# Conhecimento.tipoTrafego (tabela Enumerado): 1=Interior, 3=Cabotagem,
# 5=lci (longo curso importação), 7=lce (longo curso exportação), 9=Passagem.
# O NAVI-I exclui APENAS a exportação: cabotagem e passagem interessam à
# análise de risco (pode-se mandar descer e escanear), mesmo sem imagem.
TIPO_TRAFEGO_EXPORTACAO = '7'

# Porto de atracação sugerido na tela (filtro `porto_escala`). Provisório: no
# futuro virá da unidade/equipe de trabalho do usuário logado.
PORTO_ESCALA_PADRAO = 'BRSSZ'

# Ligação contêiner <-> ciclo de vida também pela FAMÍLIA do CE (`ciclodevida_ciclo_ce`, ciclo de vida v2): a
# janela "efetiva" e os filtros do ciclo passam a achar o card do master quando o ciclo foi gravado com o
# filhote (e vice-versa). Custa um segundo acesso indexado por candidato quando o primeiro não acha. Para
# medir/desligar sem mudar o código: NAVII_LIGACAO_FAMILIA=0 (ver diagnostico_sql.py --sem-familia).
LIGACAO_FAMILIA = os.environ.get('NAVII_LIGACAO_FAMILIA', '1') != '0'
# BL único (Conhecimento.tipoBLConhecimento '10', ~72% dos CEs) não tem master nem filhotes: a família dele é só
# ele mesmo, que é o CE gravado no ciclo — o segundo acesso (pela família) é pulado. Medido em produção em
# 02/10/2026: o segundo acesso por candidato era a maior parte do custo da família.
TIPO_BL_UNICO = '10'

# Filtros que aceitam uma LISTA de códigos (vale qualquer um deles: "OU").
# Chegam como lista ou como texto separado por vírgula/ponto e vírgula/espaço.
CAMPOS_LISTA = ('porto_escala', 'porto_origem', 'porto_destino', 'porto_baldeacao', 'ncm', 'recinto', 'tipo_bl',
                'estagio', 'conteiner', 'ce', 'dta', 'ncm_ocultar', 'porto_origem_ocultar')
# Filtros "OCULTAR" (pedido do usuário, 16/10/2026): campo -> campo base (os códigos são normalizados como os do
# base). Escondem o card só quando ele SABE que bate — sem a informação, o card aparece (sistema de risco: nunca
# sonegar informação). NCM: oculta só se TODOS os NCMs do contêiner batem. Ver `_criterio_ncm_ocultar`,
# `_criterio_porto_ocultar` e `_resolve_conflitos_ocultar`.
CAMPOS_OCULTAR = {'ncm_ocultar': 'ncm', 'porto_origem_ocultar': 'porto_origem'}
ROTULOS_OCULTAR = {'ncm_ocultar': 'NCM', 'porto_origem_ocultar': 'origem'}
MINIMO_OCULTAR = 2   # caracteres: capítulo do NCM, sigla do país
# Filtros de marcar (sim/não). `concluido` é o filtro antigo "entregue ao importador" (links e filtros salvos
# antigos): `normaliza_filtros` o converte no estágio "entregue". `so_com_desembarque` (16/10/2026): sem os contêineres
# ainda sem desembarque localizado — na tela, a caixa "Incluir os ainda sem desembarque" DESMARCADA.
CAMPOS_MARCAVEIS = ('concluido', 'com_imagem', 'sem_oog', 'so_com_desembarque')
# Filtros de escolha única. `risco`: faixa do risco histórico por CE (risco_navii.FILTROS_RISCO). `corad`: nível
# do slider "Risco CORAD" (0 a CORAD_MAX; ver `_criterio_corad`). `tem_dta`: 'sim'/'nao' (dta_navii).
CAMPOS_ESCOLHA = ('risco', 'corad', 'tem_dta')
# Matriz de risco CORAD (`matriz_risco_corad`, classe `MatrizCorad` do Fichas): classificacao_aniita_imp de 1 (alto)
# a 4 por raiz de CNPJ. Rótulos do slider (pedido do usuário, 15/10/2026); 0 = não consta (risco não apurado).
CORAD_MAX = 4
ROTULOS_CORAD = ('Não consta', 'Alto', 'Médio', 'Baixo', 'Muito baixo')
# A tabela (Core), não a classe: a classe divide a base ORM com os modelos do Fichas, e usá-la pelo ORM faria o
# SQLAlchemy configurar todos eles (inclusive os que este processo não carrega).
MATRIZ_CORAD = MatrizCorad.__table__
_CAMPOS_PORTO = ('porto_escala', 'porto_origem', 'porto_destino', 'porto_baldeacao')


def _janela_previsao() -> Tuple[str, str]:
    """Sugestão de janela de PREVISÃO de atracação: 3 a 15 dias no futuro.

    Retorna strings 'YYYY-MM-DD' porque `Escala.dataPrevistaAtracacao` é
    armazenado como VARCHAR(10) nesse formato (não é um DateTime real).
    """
    hoje = datetime.now()
    inicio = hoje + timedelta(days=JANELA_PREVISTA_MIN_DIAS)
    fim = hoje + timedelta(days=JANELA_PREVISTA_MAX_DIAS)
    return inicio.strftime('%Y-%m-%d'), fim.strftime('%Y-%m-%d')


def _janela_efetiva() -> Tuple[datetime, datetime]:
    """Sugestão de janela de desembarque EFETIVO: 5 a 15 dias no passado.

    Retorna datetimes reais para comparar contra
    `CicloDeVidaImportacao.data_desembarque` (coluna DateTime, populada a
    partir de `EmbarqueDesembarque.dataHoraOcorrencia` — evento específico do
    contêiner, não da escala/navio).
    """
    hoje = datetime.now()
    inicio = hoje - timedelta(days=JANELA_EFETIVA_MAX_DIAS)
    fim = hoje - timedelta(days=JANELA_EFETIVA_MIN_DIAS)
    return inicio, fim


def _parse_data(valor: str) -> datetime:
    return datetime.strptime(valor, '%Y-%m-%d')


def _periodo_navii(tipo_janela: str, filtros: dict) -> Tuple[Optional[str], Optional[str]]:
    """Período ('YYYY-MM-DD', 'YYYY-MM-DD') efetivamente usado na busca:
    `data_inicio`/`data_fim` dos filtros (se vierem os dois) ou a sugestão da
    janela. Com o filtro `ce` ou `dta` e sem datas, (None, None): sem limite de data (pedido do usuário,
    14/10/2026 — todos os cards do CE / da DTA)."""
    if filtros.get('data_inicio') and filtros.get('data_fim'):
        return filtros['data_inicio'], filtros['data_fim']
    if filtros.get('ce') or filtros.get('dta'):
        return None, None
    if tipo_janela == 'prevista':
        return _janela_previsao()
    inicio, fim = _janela_efetiva()
    return inicio.strftime('%Y-%m-%d'), fim.strftime('%Y-%m-%d')


def _mesmo_conteiner_do_ciclo():
    """Critério de ligação Item (contêiner declarado) <-> CicloDeVidaImportacao pelo CE gravado no ciclo."""
    return and_(
        CicloDeVidaImportacao.numeroCEmercante == Item.numeroCEmercante,
        CicloDeVidaImportacao.numeroConteiner == Item.codigoConteiner,
    )


def _mesmo_conteiner_pela_familia():
    """Ligação Item <-> ciclo por QUALQUER CE da família gravada pelo ciclo de vida v2 (origem
    `_CICLO_E_FAMILIA`): o card do master acha o ciclo gravado com o filhote, e vice-versa."""
    return and_(
        CicloDeVidaCE.numeroCEmercante == Item.numeroCEmercante,
        CicloDeVidaImportacao.numeroConteiner == Item.codigoConteiner,
    )


_CICLO_E_FAMILIA = join(CicloDeVidaCE, CicloDeVidaImportacao,
                        CicloDeVidaCE.ciclo_de_vida_id == CicloDeVidaImportacao.ID)
_CICLO_E_ESCANEAMENTOS = join(CicloDeVidaImportacao, CicloDeVidaImportacaoEscaneamentos,
                              CicloDeVidaImportacaoEscaneamentos.ciclo_de_vida_id == CicloDeVidaImportacao.ID)
_FAMILIA_E_ESCANEAMENTOS = join(_CICLO_E_FAMILIA, CicloDeVidaImportacaoEscaneamentos,
                                CicloDeVidaImportacaoEscaneamentos.ciclo_de_vida_id == CicloDeVidaImportacao.ID)


def _existe(origem, *criterios):
    """Condição "existe ao menos uma linha em `origem` para este contêiner",
    verificada POR CANDIDATO da janela.

    É uma subquery escalar correlacionada com LIMIT 1 de propósito. Com
    `IN (subquery)`/`EXISTS` o MariaDB pode converter em semijoin e passar a
    DIRIGIR a busca pela subquery — e daí chegar em `itensresumo` só pelo
    índice do código do contêiner. Como há milhões de itens com contêiner
    vazio (carga solta, granel), isso foi medido em produção em até 16 minutos
    (filtro por NCM completo). Com LIMIT o banco não faz essa conversão: a
    janela dirige, e cada filtro é um acesso indexado por candidato.
    """
    return select(literal(1)).select_from(origem).where(*criterios).limit(1).scalar_subquery().isnot(None)


def _existe_ciclo(*criterios, escaneamentos: bool = False):
    """`_existe` para o ciclo de vida do contêiner: pelo CE gravado no ciclo (ciclos antigos e v2) OU, com
    `LIGACAO_FAMILIA`, por qualquer CE da família (v2). São dois acessos indexados por candidato — o segundo só
    quando o primeiro não acha e o CE não é BL único (`TIPO_BL_UNICO`; quem chama tem o `Conhecimento` do item
    na consulta). `escaneamentos`: os critérios também olham os escaneamentos do ciclo.

    O teste do BL único fica FORA da subconsulta da família, de propósito (medições de 02/10/2026): assim o
    MariaDB antecipa "tem ciclo pelo CE ou pela família" para a leitura de `itensresumo` e só junta o CE dos
    candidatos que passam (na contagem, ~17 mil de 247 mil); o atalho vale no ramo da atracação prevista. Com
    o teste DENTRO da subconsulta, ela dependia do CE: a junção passou a ser feita para todos os candidatos e
    a contagem ficou ~0,7 s mais lenta."""
    direto = _existe(_CICLO_E_ESCANEAMENTOS if escaneamentos else CicloDeVidaImportacao,
                     _mesmo_conteiner_do_ciclo(), *criterios)
    if not LIGACAO_FAMILIA:
        return direto
    pela_familia = _existe(_FAMILIA_E_ESCANEAMENTOS if escaneamentos else _CICLO_E_FAMILIA,
                           _mesmo_conteiner_pela_familia(), *criterios)
    pode_ter_familia = or_(Conhecimento.tipoBLConhecimento.is_(None), Conhecimento.tipoBLConhecimento != TIPO_BL_UNICO)
    return or_(direto, and_(pode_ter_familia, pela_familia))


def _tem_desembarque_localizado():
    """O contêiner tem desembarque localizado pelo ciclo de vida (pelo CE ou pela família gravada pelo ciclo).
    Critério ÚNICO da janela efetiva (sem ele, o contêiner entra pela atracação prevista), do filtro
    `so_com_desembarque` e do selo "sem desembarque" do card (`_itens_com_desembarque`): o card nunca contradiz a
    janela — a família que o card usa para achar o ciclo (`busca_familias_navii`, do Mercante) pode divergir da
    gravada pelo ciclo de vida."""
    return _existe_ciclo(CicloDeVidaImportacao.data_desembarque.isnot(None))


def _itens_com_desembarque(session, ids: List[int]) -> Set[int]:
    """IDs dos itens (da página) com desembarque localizado — `_tem_desembarque_localizado`, por candidato."""
    if not ids:
        return set()
    q = (session.query(Item.ID)
         .join(Conhecimento, Conhecimento.numeroCEmercante == Item.numeroCEmercante)
         .filter(Item.ID.in_(ids), _tem_desembarque_localizado()))
    return {id_ for id_, in q}


def _lista_de_codigos(valor, separadores: str = r'[,;\s]+') -> List[str]:
    """'CNSHA, cnngb;SGSIN' ou ['CNSHA', 'CNNGB SGSIN'] -> ['CNSHA', 'cnngb', 'SGSIN']."""
    if valor is None:
        return []
    partes = valor if isinstance(valor, (list, tuple, set)) else [valor]
    codigos = []
    for parte in partes:
        codigos.extend(p for p in re.split(separadores, str(parte)) if p.strip())
    return codigos


def _normaliza_codigo(campo: str, codigo: str) -> str:
    """Um código de um campo de CAMPOS_LISTA ('' = descartar)."""
    if campo in _CAMPOS_PORTO:
        return codigo.upper()
    if campo in ('ncm', 'tipo_bl', 'ce'):
        return re.sub(r'\D', '', codigo)
    if campo in ('conteiner', 'dta'):
        return re.sub(r'[^A-Z0-9]', '', codigo.upper())
    if campo == 'estagio':
        return codigo.lower() if codigo.lower() in estagios_ciclo.ESTAGIOS else ''
    return codigo


def _normaliza_escolhas(filtros: dict) -> dict:
    """`risco` só com uma faixa conhecida; `corad` como inteiro de 0 a CORAD_MAX (senão, sem o filtro)."""
    if 'risco' in filtros and filtros['risco'] not in risco_navii.FILTROS_RISCO:
        del filtros['risco']
    if 'tem_dta' in filtros and filtros['tem_dta'] not in dta_navii.OPCOES_TEM_DTA:
        del filtros['tem_dta']
    if 'corad' in filtros:
        try:
            nivel = int(filtros['corad'])
        except (TypeError, ValueError):
            nivel = -1
        if 0 <= nivel <= CORAD_MAX:
            filtros['corad'] = nivel
        else:
            del filtros['corad']
    return filtros


def _normaliza_oculto(campo: str, codigo: str, avisos: List[str]) -> str:
    """Um código de filtro OCULTAR ('' = descartar). Só letras e dígitos — um curinga ('%', '_') no LIKE
    esconderia TUDO — e ao menos MINIMO_OCULTAR caracteres (capítulo do NCM, sigla do país): um prefixo de 1
    caractere esconderia capítulos/países inteiros por engano."""
    codigo = re.sub(r'[^A-Z0-9]', '', _normaliza_codigo(CAMPOS_OCULTAR[campo], codigo).upper())
    if codigo and len(codigo) < MINIMO_OCULTAR:
        avisos.append(f'Ocultar {ROTULOS_OCULTAR[campo]} {codigo}: ignorado — use ao menos {MINIMO_OCULTAR} '
                      f'caracteres ({"o capítulo" if campo == "ncm_ocultar" else "a sigla do país"}).')
        return ''
    return codigo


def _resolve_conflitos_ocultar(filtros: dict, avisos: List[str]) -> dict:
    """Código OCULTAR que é igual ou prefixo de um código do filtro positivo do mesmo campo esconderia o que o
    usuário pediu para ver (ex.: filtrar NCM 84713012 e ocultar 8471): é ignorado, com aviso — filtrar prevalece,
    para não sonegar informação. O contrário é um refinamento e vale (filtrar 8471, ocultar 84713012)."""
    for oculto, base in CAMPOS_OCULTAR.items():
        positivos = filtros.get(base) or []
        validos = []
        for codigo in filtros.get(oculto) or []:
            cobertos = [p for p in positivos if p.startswith(codigo)]
            if cobertos:
                avisos.append(f'Ocultar {ROTULOS_OCULTAR[oculto]} {codigo}: ignorado — esconderia o filtro '
                              f'{ROTULOS_OCULTAR[oculto]} {", ".join(cobertos)}.')
            else:
                validos.append(codigo)
        if validos:
            filtros[oculto] = validos
        else:
            filtros.pop(oculto, None)
    return filtros


def normaliza_filtros(filtros: Optional[dict], avisos: Optional[List[str]] = None) -> dict:
    """Padroniza o que o usuário digitou: os campos de CAMPOS_LISTA viram
    listas sem repetição, códigos de porto em maiúsculas e NCM só com dígitos
    (a base guarda '84713012'; o usuário pode digitar '8471.30.12'). Sem isso,
    'brssz' ou '8471.30' não achariam nada. Lista vazia remove o filtro.
    Estágios desconhecidos são descartados; o filtro antigo `concluido` vira o estágio "entregue".
    Filtros OCULTAR: ver `_normaliza_oculto` e `_resolve_conflitos_ocultar`; o que for ignorado vai para `avisos`
    (a tela mostra)."""
    avisos = [] if avisos is None else avisos
    filtros = _normaliza_escolhas(dict(filtros or {}))
    if filtros.pop('concluido', None):
        filtros['estagio'] = _lista_de_codigos(filtros.get('estagio')) + [estagios_ciclo.ENTREGUE]
    for campo in CAMPOS_LISTA:
        if campo not in filtros:
            continue
        codigos = []
        # o número do contêiner costuma vir com espaço ("ABCU 123456-7"): só vírgula/ponto e vírgula separam
        for codigo in _lista_de_codigos(filtros[campo], r'[,;]+' if campo == 'conteiner' else r'[,;\s]+'):
            if campo in CAMPOS_OCULTAR:
                codigo = _normaliza_oculto(campo, codigo, avisos)
            else:
                codigo = _normaliza_codigo(campo, codigo)
            if codigo and codigo not in codigos:
                codigos.append(codigo)
        if codigos:
            filtros[campo] = codigos
        else:
            del filtros[campo]
    return _resolve_conflitos_ocultar(filtros, avisos)


def _porto_ou_pais(coluna, valor: str):
    """Filtro de porto que aceita código parcial: com menos de 5 caracteres
    ('CN', 'US', 'BRS') casa pelo prefixo — na prática, filtro por país."""
    valor = valor.strip().upper()
    if len(valor) < 5:
        return coluna.like(valor.replace('%', '').replace('_', '') + '%')
    return coluna == valor


def _algum_porto(coluna, valores):
    """`_porto_ou_pais` para uma lista (vale qualquer um): os códigos completos
    viram um único IN; os parciais (país), um LIKE cada."""
    if isinstance(valores, str):
        valores = [valores]
    valores = [v.strip().upper() for v in valores]
    completos = [v for v in valores if len(v) >= 5]
    criterios = [_porto_ou_pais(coluna, v) for v in valores if len(v) < 5]
    if completos:
        criterios.insert(0, coluna.in_(completos) if len(completos) > 1 else coluna == completos[0])
    return criterios[0] if len(criterios) == 1 else or_(*criterios)


def _criterio_porto_ocultar(coluna, valores):
    """OCULTAR porto/país: esconde só o card cujo porto BATE; porto vazio (NULL) continua visível — sem o
    `IS NULL`, o `NOT (coluna LIKE ...)` do SQL daria NULL e o card sumiria (não sonegar informação)."""
    return or_(coluna.is_(None), not_(_algum_porto(coluna, valores)))


def _criterio_ncm_ocultar(ocultos: List[str]):
    """OCULTAR NCM (decisão do usuário, 16/10/2026): esconde o card só quando TODOS os NCMs declarados para o
    contêiner batem com algum código oculto — basta um NCM não oculto para o card aparecer. Sem NCM declarado, o
    card aparece; NCM vazio (NULL ou '') conta como não oculto. Mesma chave (CE + contêiner) do filtro positivo e
    dos NCMs do card; cada lado é um acesso indexado por candidato (`_existe`)."""
    chave = (NCMItem.numeroCEMercante == Item.numeroCEmercante, NCMItem.codigoConteiner == Item.codigoConteiner)
    bate = or_(*[NCMItem.identificacaoNCM.like(f'{ncm}%') for ncm in ocultos])
    return or_(not_(_existe(NCMItem, *chave, bate)),
               _existe(NCMItem, *chave, or_(NCMItem.identificacaoNCM.is_(None), not_(bate))))


def _manifestos_com_atracacao_prevista(inicio: str, fim: str, porto=None):
    """Subquery dos manifestos com escala de data PREVISTA de atracação no
    período (VARCHAR 'YYYY-MM-DD', comparação textual).

    Com `porto` (código ou lista), só valem as escalas nesse(s) porto(s) (Escala.portoDaEscala): um
    manifesto tem uma escala por porto em que o navio atraca, e a data que
    interessa é a da atracação no porto do usuário — não em outro porto.

    Usada sempre como `Conhecimento.manifestoCE.in_(...)` (semijoin) e nunca
    como JOIN: um manifesto costuma ter várias escalas, e o JOIN repetiria o
    contêiner uma vez por escala dentro do período.
    """
    return (
        select(EscalaManifesto.manifesto)
        .join(Escala, Escala.numeroDaEscala == EscalaManifesto.escala)
        .where(
            Escala.dataPrevistaAtracacao >= inicio,
            Escala.dataPrevistaAtracacao <= fim,
            *([_algum_porto(Escala.portoDaEscala, porto)] if porto else []),
        )
    )


def _itens_com_desembarque_efetivo(inicio: datetime, fim: datetime) -> list:
    """Consultas dos IDs de Item cujo ciclo de vida registra desembarque do contêiner no período: pelo CE
    gravado no ciclo e, com `LIGACAO_FAMILIA`, pelos OUTROS CEs da família (o principal já é o do ciclo).
    Partem dos ciclos do período (índice `ix_cdv_data_ce_conteiner`) — a família entra pela chave única
    (ciclo, CE). STRAIGHT_JOIN (só MySQL/MariaDB): sem ele, o otimizador lia a `ciclodevida_ciclo_ce` INTEIRA
    (custo que cresce com a tabela, não com o período — medição de 02/10/2026)."""
    no_periodo = CicloDeVidaImportacao.data_desembarque.between(inicio, fim)
    consultas = [select(Item.ID.label('item_id')).select_from(CicloDeVidaImportacao)
                 .join(Item, _mesmo_conteiner_do_ciclo()).where(no_periodo)]
    if LIGACAO_FAMILIA:
        consultas.append(
            select(Item.ID.label('item_id')).select_from(CicloDeVidaImportacao)
            .join(CicloDeVidaCE, CicloDeVidaCE.ciclo_de_vida_id == CicloDeVidaImportacao.ID)
            .join(Item, _mesmo_conteiner_pela_familia())
            .where(no_periodo, CicloDeVidaCE.principal.is_(False))
            .prefix_with('STRAIGHT_JOIN', dialect='mysql'))
    return consultas


def _janela_por_candidato(tipo_janela: str, inicio: str, fim: str, porto=None, incluir_previsao: bool = True):
    """A mesma janela de `_filtra_janela`, como CONDIÇÃO verificada por candidato (sem a tabela derivada):
    para quando outro filtro já escolhe poucos itens — o contêiner, pelo índice de `codigoConteiner`."""
    prevista = Conhecimento.manifestoCE.in_(_manifestos_com_atracacao_prevista(inicio, fim, porto))
    if tipo_janela == 'prevista':
        return prevista
    inicio_dt = _parse_data(inicio)
    fim_dt = _parse_data(fim) + timedelta(days=1, seconds=-1)  # dia final inteiro
    efetiva = _existe_ciclo(CicloDeVidaImportacao.data_desembarque.between(inicio_dt, fim_dt))
    if not incluir_previsao:
        return efetiva
    return or_(efetiva, and_(not_(_tem_desembarque_localizado()), prevista))


def _filtra_janela(q, tipo_janela: str, inicio: Optional[str], fim: Optional[str], porto=None,
                   criterios_ce=(), conteineres=(), ces: Optional[List[str]] = None, incluir_previsao: bool = True):
    """Restringe a query (Item, Conhecimento) aos contêineres da janela.

    - 'prevista': manifesto do CE com escala de data prevista de atracação no
      período.
    - 'efetiva': desembarque do contêiner (ciclo de vida) no período; para os
      contêineres SEM desembarque localizado pelo ciclo de vida, cai para a
      data prevista de atracação da escala (fallback combinado com o usuário).
      As duas origens são unidas (UNION, sem repetição) em uma tabela derivada
      de Item.ID — um OR entre elas impediria o banco de partir dos índices
      de data.

    `porto` (filtro `porto_escala`) restringe as escalas ao porto de
    atracação. Não se aplica ao desembarque efetivo, por decisão do usuário:
    o ciclo de vida é o desembarque real, e em análise de risco é melhor
    exibir a mais (ex.: contêiner previsto para outro porto que desceu aqui,
    ou o contrário) do que deixar casos de fora.

    `criterios_ce` são os filtros que só dependem do Conhecimento (tráfego,
    portos, empresa). Quem chama os aplica na query principal; aqui eles são
    repetidos DENTRO do ramo da atracação prevista, para que CEs descartados
    (ex.: exportação, ~1/3 dos manifestos) não custem a leitura dos seus itens
    nem a verificação do ciclo de vida — a tabela derivada é recalculada a
    cada página, e em Santos são ~200 mil contêineres por janela de 11 dias.

    `conteineres` (filtro `conteiner`, 13/10/2026) e `ces` (filtro `ce`, 14/10/2026): o índice do contêiner ou do
    CE em `itensresumo` escolhe os poucos itens e a janela vira condição por candidato (`_janela_por_candidato`) —
    não calcula a janela inteira. Sem período (só com `ce`/`dta`; ver `_periodo_navii`), não há condição de data.
    `ces` None = sem filtro de CE; lista vazia = nenhum CE (ex.: DTA não encontrada).

    `incluir_previsao` False (filtro `so_com_desembarque`, 16/10/2026): a janela efetiva fica só com o desembarque no
    período, sem o ramo da atracação prevista (e mais leve).
    """
    if conteineres or ces is not None:
        if ces is not None:
            q = q.filter(Item.numeroCEmercante.in_(ces) if ces else false())
        if conteineres:
            q = q.filter(Item.codigoConteiner.in_(conteineres))
        if not (inicio and fim):
            return q
        return q.filter(_janela_por_candidato(tipo_janela, inicio, fim, porto, incluir_previsao))
    if tipo_janela == 'prevista':
        return q.filter(Conhecimento.manifestoCE.in_(_manifestos_com_atracacao_prevista(inicio, fim, porto)))
    inicio_dt = _parse_data(inicio)
    fim_dt = _parse_data(fim) + timedelta(days=1, seconds=-1)  # dia final inteiro
    sem_desembarque_localizado = not_(_tem_desembarque_localizado())
    itens_pela_atracacao_prevista = (
        select(Item.ID.label('item_id'))
        .select_from(Item)
        .join(Conhecimento, Conhecimento.numeroCEmercante == Item.numeroCEmercante)
        .where(
            Conhecimento.manifestoCE.in_(_manifestos_com_atracacao_prevista(inicio, fim, porto)),
            *criterios_ce,
            sem_desembarque_localizado,
        )
    )
    consultas = _itens_com_desembarque_efetivo(inicio_dt, fim_dt)
    if incluir_previsao:
        consultas.append(itens_pela_atracacao_prevista)
    janela = (union(*consultas) if len(consultas) > 1 else consultas[0]).subquery('janela')
    return q.join(janela, janela.c.item_id == Item.ID)


def monta_query_navii(
    session,
    tipo_janela: str = 'efetiva',
    filtros: Optional[dict] = None,
    exigir_escaneamento: bool = False,
):
    """Monta (sem executar, sem ordenar/paginar) a query (Item, Conhecimento)
    dos contêineres declarados que atendem à janela e aos filtros.

    Args:
        session: sessão SQLAlchemy.
        tipo_janela: 'prevista' (Escala.dataPrevistaAtracacao) ou 'efetiva'
            (CicloDeVidaImportacao.data_desembarque, com fallback para
            Escala.dataPrevistaAtracacao quando o ciclo não localizou o
            desembarque — ver `_filtra_janela`).
        filtros: dict opcional com filtros sucessivos:
            Os campos de CAMPOS_LISTA aceitam lista (ou texto separado por
            vírgula) e valem para QUALQUER um dos valores.
            - ncm (str | lista): prefixo do código NCM declarado NO CONTÊINER (ex.: '8471')
            - produto (str): busca textual na descrição da mercadoria do CE
              (Conhecimento.descricao). O `NCMItem.descritivo` não serve: na
              base real traz números de BL/contêiner, não o produto.
            - porto_origem / porto_destino (str): código do porto de origem/destino
              da carga (Conhecimento — o que o embarcador declarou). Os filtros
              de porto aceitam código parcial: 'CN' ou 'US' filtram pelo PAÍS
              (os 2 primeiros caracteres do código do porto são a sigla do país)
            - porto_escala (str): porto de ATRACAÇÃO do navio (Escala.portoDaEscala)
              — só as escalas nesse porto contam para a data prevista de
              atracação. A tela sugere PORTO_ESCALA_PADRAO; vazio = qualquer porto.
            - porto_baldeacao (str): código do porto de CARREGAMENTO do Manifesto
              (Manifesto.portoCarregamento) — quando difere do porto de origem do
              Conhecimento, indica que houve transbordo/baldeação antes do Brasil.
            - conteiner (str | lista): número do contêiner (só letras e dígitos, em maiúsculas) — todos os
              cards do contêiner no período, com os escaneamentos de cada um; a busca parte do contêiner
            - ce (str | lista): número do CE Mercante (só dígitos) — os cards do CE; a busca parte do CE e, sem
              data_inicio/data_fim, não há limite de data
            - recinto (str): código (cod_siscomex) do recinto de desembarque OU
              de armazenagem do ciclo de vida — exige ciclo de vida. Filtro
              secundário: o terminal do Manifesto (cod_carga) não é usado, só
              existe para operadores portuários e deixaria recintos de fora.
            - empresa (str): busca textual em embarcador ou consignatário
            - tipo_bl (str | lista): tipo de BL do CE (Conhecimento.tipoBLConhecimento:
              10 BL único, 11 MBL master, 12 HBL house, 15 MHBL — ver referencias)
            - sem_oog (bool): exclui os contêineres de potencial OOG (open top e
              plataforma/flat rack, pelo Item.isoCode — podem não passar no escâner)
            - estagio (str | lista): estágio do contêiner no ciclo de vida (no_operador, em_transito,
              no_seco, entregue, embarcado, incompleto — ver `estagios_ciclo`) — exige ciclo de vida.
              `concluido` (bool, filtro antigo) = estágio "entregue".
            - ncms_max (int): quantidade máxima de NCMs distintos declarados no CE
              ("até N NCMs distintos"; CE sem NCM declarado conta como 0)
            - com_imagem (bool): equivalente a `exigir_escaneamento=True`
            - so_com_desembarque (bool): só contêineres com desembarque localizado pelo ciclo de vida — na janela
              efetiva, sem o ramo da atracação prevista (`_tem_desembarque_localizado`)
            - dta (str | lista): número da DTA (só letras e dígitos) — os cards dos CEs da DTA; sem
              data_inicio/data_fim, sem limite de data (como `ce`)
            - tem_dta (str): 'sim' (CE com DTA) ou 'nao' (sem DTA) — ver `dta_navii`
            - risco (str): faixa do risco histórico por CE — 'alto', 'medio' (médio ou alto) ou 'sinal' (com
              algum sinal); ver `risco_navii`
            - corad (int 0..4): Risco CORAD do importador, inclusivo — 0 só os que não constam da matriz;
              N os que não constam e os de classificação <= N (1 alto ... 4 muito baixo); ver `_criterio_corad`
            - data_inicio / data_fim (str 'YYYY-MM-DD'): sobrescrevem a janela
              sugerida por `_janela_previsao`/`_janela_efetiva` para esse
              `tipo_janela`. Os dois precisam vir juntos. Sem eles e com `ce`, sem limite de data.
        exigir_escaneamento: se True, só contêineres cujo ciclo de vida já tem
            ao menos um escaneamento com imagem associada. Padrão False: o
            ciclo de vida/imagem são opcionais.

    Filtros que dependem de outras tabelas nunca são JOINs (multiplicariam
    linhas): são verificados por candidato com `_existe`, o que dispensa
    DISTINCT, deixa a contagem exata e mantém o plano de execução previsível.

    CEs de exportação (tipoTrafego 7) são sempre excluídos; importação,
    cabotagem, passagem e tráfego não informado são exibidos.
    """
    filtros = normaliza_filtros(filtros)
    if tipo_janela not in TIPOS_JANELA_VALIDOS:
        raise ValueError(f"tipo_janela inválido: {tipo_janela!r}. Use 'prevista' ou 'efetiva'.")
    inicio, fim = _periodo_navii(tipo_janela, filtros)

    q = (
        session.query(Item, Conhecimento)
        .join(Conhecimento, Conhecimento.numeroCEmercante == Item.numeroCEmercante)
    )
    # Filtros que só dependem do Conhecimento: valem para a query principal e
    # também dentro da janela (ver `_filtra_janela`).
    criterios_ce = _criterios_ce(filtros)
    q = _filtra_janela(q, tipo_janela, inicio, fim, filtros.get('porto_escala'), criterios_ce,
                       filtros.get('conteiner'), _ces_alvo(session, filtros),
                       incluir_previsao=not filtros.get('so_com_desembarque'))
    q = q.filter(*criterios_ce)
    if filtros.get('so_com_desembarque') and tipo_janela == 'prevista':   # na efetiva, a própria janela já tira
        q = q.filter(_tem_desembarque_localizado())
    if filtros.get('risco'):   # risco histórico por CE: faixa verificada por candidato
        q = q.filter(risco_navii.criterio(session, filtros['risco'], _existe))
    if filtros.get('tem_dta'):
        q = q.filter(dta_navii.criterio_tem_dta(session, filtros['tem_dta'], _existe))
    return _filtra_por_candidato(q, filtros, exigir_escaneamento)


def _ces_alvo(session, filtros: dict) -> Optional[List[str]]:
    """CEs que dirigem a busca: os do filtro `ce` e/ou os das DTAs do filtro `dta` (os dois: os comuns).
    None = sem esses filtros."""
    ces = set(filtros['ce']) if filtros.get('ce') else None
    if filtros.get('dta'):
        das_dtas = set(dta_navii.ces_das_dtas(session, filtros['dta']))
        ces = das_dtas if ces is None else ces & das_dtas
    return sorted(ces) if ces is not None else None


def _criterios_ce(filtros: dict) -> list:
    """Critérios que só dependem do Conhecimento (tráfego, portos, produto, empresa)."""
    criterios_ce = [or_(
        Conhecimento.tipoTrafego != TIPO_TRAFEGO_EXPORTACAO,
        Conhecimento.tipoTrafego.is_(None),
    )]
    if filtros.get('porto_origem'):
        criterios_ce.append(_algum_porto(Conhecimento.portoOrigemCarga, filtros['porto_origem']))
    if filtros.get('porto_origem_ocultar'):
        criterios_ce.append(_criterio_porto_ocultar(Conhecimento.portoOrigemCarga, filtros['porto_origem_ocultar']))
    if filtros.get('porto_destino'):
        criterios_ce.append(_algum_porto(Conhecimento.portoDestFinal, filtros['porto_destino']))
    if filtros.get('porto_baldeacao'):
        criterios_ce.append(Conhecimento.manifestoCE.in_(
            select(Manifesto.numero).where(_algum_porto(Manifesto.portoCarregamento, filtros['porto_baldeacao']))
        ))
    if filtros.get('tipo_bl'):
        criterios_ce.append(Conhecimento.tipoBLConhecimento.in_(filtros['tipo_bl']))
    if filtros.get('produto'):
        criterios_ce.append(Conhecimento.descricao.ilike(f"%{filtros['produto'].strip()}%"))
    if filtros.get('empresa'):
        termo = f"%{filtros['empresa']}%"
        criterios_ce.append(or_(
            Conhecimento.embarcador.ilike(termo),
            Conhecimento.consignatario.ilike(termo),
        ))
    if filtros.get('corad') is not None:
        criterios_ce.append(_criterio_corad(filtros['corad']))
    return criterios_ce


def _criterio_corad(nivel: int):
    """Slider "Risco CORAD", INCLUSIVO (pedido do usuário): nível 0 = só os importadores que não constam da
    matriz; nível N = os que não constam e os de classificação <= N. Como a raiz é única na matriz, é o mesmo
    que "não há linha da raiz com classificação acima de N" — uma condição só, verificada por candidato pela chave
    única de `cod_emp_interv_rad`. Não consta: raiz fora da matriz, classificação vazia ou consignatário que não é
    CNPJ (CPF, nome estrangeiro, vazio)."""
    consignatario = func.trim(Conhecimento.consignatario)
    classificado_acima = _existe(
        MATRIZ_CORAD,
        MATRIZ_CORAD.c.cod_emp_interv_rad == func.substr(consignatario, 1, 8),
        MATRIZ_CORAD.c.classificacao_aniita_imp > nivel,
    )
    return or_(Conhecimento.consignatario.is_(None), func.length(consignatario) != 14, not_(classificado_acima))


def _filtra_por_candidato(q, filtros: dict, exigir_escaneamento: bool):
    """Filtros em outras tabelas (imagem, NCM, recinto, entregue, qtde. de
    NCMs), verificados por candidato da janela com `_existe`/subquery."""
    if exigir_escaneamento or filtros.get('com_imagem'):
        q = q.filter(_existe_ciclo(CicloDeVidaImportacaoEscaneamentos.imagem_id.isnot(None), escaneamentos=True))

    if filtros.get('ncm'):
        q = q.filter(_existe(
            NCMItem,
            NCMItem.numeroCEMercante == Item.numeroCEmercante,
            NCMItem.codigoConteiner == Item.codigoConteiner,
            or_(*[NCMItem.identificacaoNCM.like(f'{ncm}%') for ncm in filtros['ncm']]),
        ))
    if filtros.get('ncm_ocultar'):
        q = q.filter(_criterio_ncm_ocultar(filtros['ncm_ocultar']))

    if filtros.get('sem_oog'):
        q = q.filter(or_(Item.isoCode.is_(None), not_(expressao_oog(Item.isoCode))))

    if filtros.get('recinto'):
        recintos = filtros['recinto']
        q = q.filter(_existe_ciclo(or_(
            CicloDeVidaImportacao.codigoRecintoDesembarque.in_(recintos),
            CicloDeVidaImportacao.codigoRecintoArmazenagem.in_(recintos),
        )))

    if filtros.get('estagio'):
        q = q.filter(_existe_ciclo(or_(*[estagios_ciclo.criterio(e) for e in filtros['estagio']])))

    if filtros.get('ncms_max'):
        # Subquery correlacionada por CE (usa ix_ncmitem_chave) — em vez de
        # agrupar a tabela ncmitemresumo inteira a cada busca.
        qtd_ncms = (
            select(func.count(func.distinct(NCMItem.identificacaoNCM)))
            .where(NCMItem.numeroCEMercante == Conhecimento.numeroCEmercante)
            .scalar_subquery()
        )
        q = q.filter(qtd_ncms <= int(filtros['ncms_max']))

    return q


class ResultadoNavii(NamedTuple):
    """Uma linha da busca: o contêiner declarado e o que foi possível cruzar."""
    item: Item
    conhecimento: Conhecimento
    ciclo: Optional[CicloDeVidaImportacao]
    manifesto: Optional[Manifesto]
    embarcacao: Optional[str]
    data_prevista_atracacao: Optional[str]
    familia: Optional[dict] = None   # ver `busca_familias_navii`
    sem_desembarque: Optional[bool] = None   # `_itens_com_desembarque`; None = deduzir do ciclo (extrato)


def busca_ces_navii(
    session,
    tipo_janela: str = 'efetiva',
    filtros: Optional[dict] = None,
    exigir_escaneamento: bool = False,
    cursor_id: Optional[int] = None,
    limit: int = 20,
) -> List[ResultadoNavii]:
    """Busca uma página de contêineres declarados para a tela do NAVI-I.

    Janela/filtros: ver `monta_query_navii`. `cursor_id` é o último Item.ID já
    exibido (rolagem infinita: busca sempre ID menor que o cursor).

    Returns:
        Lista de `ResultadoNavii`, ordenada por Item.ID decrescente (mais
        recentes primeiro). `ciclo`, `manifesto`, `embarcacao` e
        `data_prevista_atracacao` podem ser None — o ciclo de vida é opcional
        e Manifesto/Escala podem ainda não ter sido recebidos.

    Observação de escopo:
        Filtros "subjetivos" (empresa e sócios cruzados com listas de risco) não
        estão implementados aqui — dependem de uma fonte de dados de sócios que
        ainda não existe no modelo atual.
    """
    q = monta_query_navii(session, tipo_janela, filtros, exigir_escaneamento)
    if cursor_id:
        q = q.filter(Item.ID < cursor_id)
    linhas = q.order_by(Item.ID.desc()).limit(limit).all()

    familias = busca_familias_navii(session, [ce for _, ce in linhas])
    ciclos = busca_ciclos_navii(session, [item for item, _ in linhas],
                                {numero: dados['familia'] for numero, dados in familias.items()})
    filtros_normais = normaliza_filtros(filtros)
    manifestos = busca_manifestos_navii(
        session, [ce.manifestoCE for _, ce in linhas], filtros_normais.get('porto_escala'),
        periodo=_periodo_navii(tipo_janela, filtros_normais),
    )
    com_desembarque = _itens_com_desembarque(session, [item.ID for item, _ in linhas])
    resultado = []
    for item, ce in linhas:
        dados_manifesto = manifestos.get(ce.manifestoCE, {})
        resultado.append(ResultadoNavii(
            item=item,
            conhecimento=ce,
            ciclo=ciclos.get((item.numeroCEmercante, item.codigoConteiner)),
            manifesto=dados_manifesto.get('manifesto'),
            embarcacao=dados_manifesto.get('embarcacao'),
            data_prevista_atracacao=dados_manifesto.get('dataPrevistaAtracacao'),
            familia=familias.get(ce.numeroCEmercante),
            sem_desembarque=item.ID not in com_desembarque,
        ))
    logger.debug(  # o uso da tela vai para o log com o selo NAVII_USO (uso_navii.py)
        f'busca_ces_navii: janela={tipo_janela} filtros={filtros} '
        f'cursor={cursor_id} -> {len(resultado)} contêineres retornados'
    )
    return resultado


def conta_conteineres_navii(
    session,
    tipo_janela: str = 'efetiva',
    filtros: Optional[dict] = None,
    exigir_escaneamento: bool = False,
) -> int:
    """Total de contêineres declarados que atendem à janela/filtros (todas as
    páginas) — exibido na tela junto com os cards já carregados."""
    q = monta_query_navii(session, tipo_janela, filtros, exigir_escaneamento)
    return q.with_entities(func.count(Item.ID)).scalar() or 0


def itens_na_janela(session, tipo_janela: str, inicio: str, fim: str, porto, itens) -> set:
    """Dos `itens` (Item.ID), os que estão na janela do período no porto da escala, sem outros filtros — a mesma
    regra da contagem da tela (`conta_conteineres_navii` com só período e porto), verificada por candidato: barata
    para poucos itens. Usada na cobertura das estatísticas de uso (`estatisticas_uso.py`)."""
    if not itens:
        return set()
    consulta = (
        session.query(Item.ID)
        .join(Conhecimento, Conhecimento.numeroCEmercante == Item.numeroCEmercante)
        .filter(Item.ID.in_(list(itens)), _janela_por_candidato(tipo_janela, inicio, fim, porto), *_criterios_ce({}))
    )
    return {item for (item,) in consulta}


def _master_do(conhecimento: Conhecimento) -> Optional[str]:
    """CE master do conhecimento; o CE único costuma repetir o próprio número — não é vínculo."""
    master = conhecimento.numeroCEMaster
    return master if master and master != conhecimento.numeroCEmercante else None


def busca_familias_navii(session, conhecimentos: List[Conhecimento]) -> Dict[str, dict]:
    """Família master/filhote dos CEs da página, em lote (índice de numeroCEMaster):

        {CE: {'master': CE master | None, 'filhotes': [CEs filhotes deste CE],
              'familia': {o CE, o master, os filhotes e os irmãos (filhotes do mesmo master)}}}

    A família serve para achar o ciclo de vida (que guarda um só dos CEs) e para o card mostrar os
    filhotes de um master.
    """
    masters = {ce.numeroCEmercante: _master_do(ce) for ce in conhecimentos if ce.numeroCEmercante}
    if not masters:
        return {}
    filhotes: Dict[str, List[str]] = {}
    chaves = set(masters) | {m for m in masters.values() if m}
    for filhote, master in (session.query(Conhecimento.numeroCEmercante, Conhecimento.numeroCEMaster)
                            .filter(Conhecimento.numeroCEMaster.in_(chaves))):
        if filhote != master:
            filhotes.setdefault(master, []).append(filhote)
    familias = {}
    for numero, master in masters.items():
        proprios = sorted(filhotes.get(numero, []))
        familias[numero] = {'master': master, 'filhotes': proprios,
                            'familia': {numero, *proprios, *([master] + filhotes.get(master, []) if master else [])}}
    return familias


def busca_ciclos_navii(session, itens: List[Item],
                       familias: Optional[Dict[str, set]] = None) -> Dict[Tuple[str, str], CicloDeVidaImportacao]:
    """Ciclos de vida dos contêineres da página, em lote, por (CE do item, contêiner).

    `familias` ({CE: CEs da família}, ver `busca_familias_navii`): o ciclo pode ter sido gravado com o
    master, um filhote ou um irmão — vale qualquer CE da família com o mesmo contêiner, preferindo o
    do próprio CE. Sem `familias`, só o CE exato. Com mais de um ciclo (ex.: vários desembarques
    informados), prevalece o mais recente (maior ID). Contêiner sem ciclo não aparece no dict.
    """
    familias = familias or {}
    pares = [(item.numeroCEmercante, item.codigoConteiner) for item in itens
             if item.numeroCEmercante and item.codigoConteiner]
    if not pares:
        return {}
    ces = {ce for ce, _ in pares} | {c for ce, _ in pares for c in familias.get(ce, ())}
    por_chave = {}
    for ciclo in (session.query(CicloDeVidaImportacao)
                  .options(undefer_group('v2'), selectinload(CicloDeVidaImportacao.escaneamentos).undefer_group('v2'))
                  .filter(CicloDeVidaImportacao.numeroConteiner.in_({c for _, c in pares}),
                          CicloDeVidaImportacao.numeroCEmercante.in_(ces))
                  .order_by(CicloDeVidaImportacao.ID)):
        por_chave[(ciclo.numeroCEmercante, ciclo.numeroConteiner)] = ciclo
    resultado = {}
    for ce, conteiner in pares:
        da_familia = [por_chave[(c, conteiner)] for c in familias.get(ce, ()) if (c, conteiner) in por_chave]
        ciclo = por_chave.get((ce, conteiner)) or max(da_familia, key=lambda c: c.ID, default=None)
        if ciclo is not None:
            resultado[(ce, conteiner)] = ciclo
    return resultado


def _prefere_escala(nova: str, atual: Optional[str], periodo) -> bool:
    """Qual data prevista mostrar no card: a de uma escala DENTRO do período da pesquisa (a que trouxe o card pela
    janela) vence a de fora; no mesmo grupo, a maior."""
    def dentro(data):
        return bool(periodo and periodo[0] and periodo[1] and periodo[0] <= data[:10] <= periodo[1])
    if not atual:
        return True
    if dentro(nova) != dentro(atual):
        return dentro(nova)
    return nova > atual


def busca_manifestos_navii(
    session, numeros_manifesto: Iterable[str], porto=None, periodo: Optional[Tuple[str, str]] = None,
) -> Dict[str, dict]:
    """Manifesto + dados da escala (embarcação, data prevista de atracação) dos
    CEs da página, em lote, por número do manifesto. Com `porto` (código ou
    lista), considera só as escalas nesse(s) porto(s) de atracação (mesmo
    critério da janela).

    Returns:
        {numero: {'manifesto': Manifesto | None, 'embarcacao': str | None,
                  'dataPrevistaAtracacao': str | None}}
        Com mais de uma escala para o mesmo manifesto, usa a de maior data
        prevista de atracação — com `periodo` ('AAAA-MM-DD', 'AAAA-MM-DD'), a maior DENTRO do período, se houver
        (16/10/2026: a maior de todas podia cair fora do período e confundir o card sem desembarque).
    """
    numeros = {n for n in numeros_manifesto if n}
    if not numeros:
        return {}
    resultado = {n: {'manifesto': None, 'embarcacao': None, 'dataPrevistaAtracacao': None}
                 for n in numeros}
    for manifesto in session.query(Manifesto).filter(Manifesto.numero.in_(numeros)).all():
        resultado[manifesto.numero]['manifesto'] = manifesto
    q_escalas = (
        session.query(EscalaManifesto.manifesto, Escala.embarcacao, Escala.dataPrevistaAtracacao)
        .join(Escala, Escala.numeroDaEscala == EscalaManifesto.escala)
        .filter(EscalaManifesto.manifesto.in_(numeros))
    )
    if porto:
        q_escalas = q_escalas.filter(_algum_porto(Escala.portoDaEscala, porto))
    escalas = q_escalas.all()
    for numero, embarcacao, data_prevista in escalas:
        dados = resultado.get(numero)
        if dados is None:
            continue
        if data_prevista and _prefere_escala(data_prevista, dados['dataPrevistaAtracacao'], periodo):
            dados['dataPrevistaAtracacao'] = data_prevista
            dados['embarcacao'] = embarcacao or dados['embarcacao']
        elif not dados['embarcacao']:
            dados['embarcacao'] = embarcacao
    return resultado


def busca_nomes_recintos(session, codigos: Iterable[str], campo: str = 'cod_siscomex') -> Dict[str, str]:
    """Resolve código de recinto -> nome em lote via `ovr_recintos` (repo
    bhadrasana2, `bhadrasana.models.ovr.Recinto`).

    `campo='cod_siscomex'` para os códigos da API Recintos (ciclo de vida);
    `campo='cod_carga'` para o terminal de descarregamento do Manifesto
    (mesmo mapeamento usado em `integracao/risco/conformidade_manager.py`).
    Códigos sem correspondência não aparecem no dict.
    """
    codigos = {c for c in codigos if c}
    if not codigos:
        return {}
    coluna = tabela_recintos.c[campo]
    linhas = session.execute(
        select(coluna, tabela_recintos.c.nome).where(coluna.in_(codigos))
    ).all()
    return {codigo: nome for codigo, nome in linhas if nome}


def sugere_recintos(session, termo: str, limite: int = 15) -> List[dict]:
    """Sugestões para o campo Recinto: código Siscomex (o que o ciclo de vida
    usa) que começa pelo termo ou nome que o contém. [{'codigo', 'descricao'}]"""
    termo = (termo or '').strip().replace('%', '').replace('_', '')
    if not termo:
        return []
    codigo, nome = tabela_recintos.c.cod_siscomex, tabela_recintos.c.nome
    linhas = session.execute(
        select(codigo, func.max(nome))
        .where(codigo.isnot(None), codigo != '', or_(codigo.like(f'{termo}%'), nome.ilike(f'%{termo}%')))
        .group_by(codigo)
        .order_by(func.max(nome))
        .limit(limite)
    ).all()
    return [{'codigo': c, 'descricao': n or ''} for c, n in linhas]


def _texto_para_float(valor: Optional[str]) -> Optional[float]:
    """Converte campos texto do Mercante (ex.: Item.pesoBruto/tara) para float,
    sem derrubar a consulta quando o dado vier vazio/sujo (é XML de terceiros).
    """
    if valor is None:
        return None
    try:
        return float(str(valor).strip().replace(',', '.'))
    except (TypeError, ValueError):
        return None


def busca_itens_navii(session, numeros_ce: List[str]) -> Dict[str, dict]:
    """Enriquecimento em lote com os contêineres e NCMs declarados de cada CE
    — para a seção "Contêineres" do card e para o peso bruto total / contagem
    de NCMs distintos exibidos no cabeçalho do card.

    Além de Item/NCMItem:
      - cada NCM leva a descrição oficial da sua POSIÇÃO (4 dígitos), das
        tabelas locais de referência, ao lado do texto livre declarado no
        manifesto (`NCMItem.descritivo`) — os dois interessam à análise. A
        tabela `laudo_ncm` deixou de ser usada (descrições ruins).
      - `Enumerado.tipoItemCarga`: decodifica o tipo de carga do contêiner
        (`Item.tipoItemCarga`), hoje não decodificado em nenhum outro lugar.

    Feito à parte da busca principal (que filtra/pagina) para não repetir os
    joins com Item/NCMItem em toda consulta — só roda para os CEs da página
    atual carregada na tela.

    Returns:
        Dict por numeroCEmercante:
            {
              'pesoBrutoTotal': float | None,
              'qtdNcmsDistintos': int,
              'conteineres': [
                  {'codigoConteiner': str, 'isoCode': str, 'tipoConteiner': dict | None,
                   'pesoBruto': float | None, 'tara': float | None, 'lacre': str,
                   'tipoCarga': str | None,
                   'ncms': [{'identificacaoNCM': str, 'descritivo': str,
                             'descricaoPosicao': str | None}, ...]},
                  ...
              ],
            }
    """
    numeros_ce = [n for n in numeros_ce if n]
    if not numeros_ce:
        return {}

    # numeroCEmercante -> codigoConteiner -> dict do contêiner
    por_ce: Dict[str, Dict[str, dict]] = {}

    itens = (
        session.query(Item)
        .filter(Item.numeroCEmercante.in_(numeros_ce))
        .all()
    )
    codigos_tipo_carga = {item.tipoItemCarga for item in itens if item.tipoItemCarga}
    tipos_carga: Dict[str, str] = {}
    if codigos_tipo_carga:
        for enumerado in (
            session.query(Enumerado)
            .filter(Enumerado.id.in_(codigos_tipo_carga))
            .all()
        ):
            if enumerado.tipoItemCarga:
                tipos_carga[enumerado.id] = enumerado.tipoItemCarga

    for item in itens:
        conteineres = por_ce.setdefault(item.numeroCEmercante, {})
        conteineres[item.codigoConteiner] = {
            'codigoConteiner': item.codigoConteiner,
            'isoCode': item.isoCode,
            'tipoConteiner': referencias.tipo_conteiner(item.isoCode),
            'pesoBruto': _texto_para_float(item.pesoBruto),
            'tara': _texto_para_float(item.tara),
            'lacre': item.lacre,
            'tipoCarga': tipos_carga.get(item.tipoItemCarga),
            'ncms': [],
        }

    ncm_itens = (
        session.query(NCMItem)
        .filter(NCMItem.numeroCEMercante.in_(numeros_ce))
        .all()
    )

    for ncm_item in ncm_itens:
        conteineres = por_ce.setdefault(ncm_item.numeroCEMercante, {})
        conteiner = conteineres.get(ncm_item.codigoConteiner)
        if conteiner is None:
            # NCM declarado sem Item correspondente carregado (dado incompleto) —
            # cria uma entrada mínima para não perder a informação do NCM.
            conteiner = conteineres.setdefault(ncm_item.codigoConteiner, {
                'codigoConteiner': ncm_item.codigoConteiner,
                'isoCode': None, 'tipoConteiner': None, 'pesoBruto': None, 'tara': None, 'lacre': None,
                'tipoCarga': None, 'ncms': [],
            })
        conteiner['ncms'].append({
            'identificacaoNCM': ncm_item.identificacaoNCM,
            'descritivo': ncm_item.descritivo,  # texto livre declarado no manifesto
            'descricaoPosicao': referencias.descricao_posicao_ncm(ncm_item.identificacaoNCM),
        })

    resultado: Dict[str, dict] = {}
    for numero_ce, conteineres in por_ce.items():
        lista_conteineres = list(conteineres.values())
        pesos = [c['pesoBruto'] for c in lista_conteineres if c['pesoBruto'] is not None]
        todos_ncms = {ncm['identificacaoNCM'] for c in lista_conteineres for ncm in c['ncms']}
        resultado[numero_ce] = {
            'pesoBrutoTotal': sum(pesos) if pesos else None,
            'qtdNcmsDistintos': len(todos_ncms),
            'conteineres': lista_conteineres,
        }
    return resultado


def busca_nomes_empresas(session, valores: List[str]) -> Dict[str, str]:
    """Resolve CNPJ -> nome de empresa em lote, cruzando com `laudo_empresas`
    (repo bhadrasana2, `bhadrasana.models.laudo.Empresa`).

    `valores` é a lista de `Conhecimento.embarcador`/`.consignatario` da
    página atual — esses campos guardam o CNPJ (empresa BR) ou um nome
    estrangeiro (ver `virasana.integracao.mercante.mercante.Embarcador`/
    `Consignatario`, cujo `__repr__` prioriza o CNPJ). Valores que não forem
    CNPJs conhecidos simplesmente não aparecem no dict de retorno — quem
    chama deve manter o valor original nesse caso.

    É um match exato (sem normalização de máscara): tanto o CNPJ gravado no
    Conhecimento quanto `Empresa.cnpj` vêm da mesma cadeia de dados da
    Receita, sempre sem pontuação.
    """
    valores = [v for v in valores if v]
    if not valores:
        return {}

    return {
        empresa.cnpj: empresa.nome
        for empresa in session.query(Empresa).filter(Empresa.cnpj.in_(valores)).all()
        if empresa.nome
    }


def _dados_bl_e_familia(conhecimento: Conhecimento, ciclo: Optional[CicloDeVidaImportacao],
                        familia: Optional[dict]) -> dict:
    """Tipo de BL (sigla + descrição curta), filhotes do master e de que CE da família veio o ciclo."""
    tipo_bl = referencias.descreve_tipo_bl(conhecimento.tipoBLConhecimento) or {}
    outro_ce = ciclo is not None and ciclo.numeroCEmercante != conhecimento.numeroCEmercante
    return {
        'tipoBL': tipo_bl.get('sigla'),
        'tipoBLCodigo': tipo_bl.get('codigo'),
        'tipoBLDescricao': tipo_bl.get('descricao'),
        'filhotes': (familia or {}).get('filhotes', []),
        # o ciclo de vida foi achado por outro CE da família (master/filhote/irmão)
        'cicloPorCE': ciclo.numeroCEmercante if outro_ce else None,
    }


def _dados_tipo_conteiner(item: Optional[Item]) -> dict:
    """Tipo do contêiner declarado no Mercante (Item.isoCode), com descrição e potencial OOG."""
    iso_code = item.isoCode if item is not None else None
    return {'isoCode': iso_code, 'tipoConteiner': referencias.tipo_conteiner(iso_code)}


def _linha_do_tempo(ciclo: Optional[CicloDeVidaImportacao], nomes_recintos: Dict[str, str]) -> dict:
    """Etapas do ciclo (API Recintos) — cada uma com data None quando o evento ainda não ocorreu ("se
    houver") ou não há ciclo. v2: entrada no seco implícita (deduzida de inspeção/pesagem: `implicita`) e
    embarque (passagem/baldeação)."""
    def etapa(data_attr: str, recinto_attr: Optional[str] = None) -> dict:
        codigo = getattr(ciclo, recinto_attr) if ciclo and recinto_attr else None
        return {'data': getattr(ciclo, data_attr) if ciclo else None, 'recinto': codigo,
                'nomeRecinto': nomes_recintos.get(codigo)}

    return {
        'desembarque': etapa('data_desembarque', 'codigoRecintoDesembarque'),
        'saida': etapa('data_saida', 'codigoRecintoDesembarque'),
        'entradaRecintoSeco': {**etapa('data_entrada_seco', 'codigoRecintoArmazenagem'),
                               'implicita': ciclo.entrada_seco_implicita_de if ciclo else None},
        'saidaRecintoSeco': etapa('data_saida_seco', 'codigoRecintoArmazenagem'),
        'embarque': etapa('data_embarque'),
    }


def _escaneamentos_do_card(ciclo: Optional[CicloDeVidaImportacao], nomes_recintos: Dict[str, str]) -> List[dict]:
    """Escaneamentos ligados ao ciclo, em ordem de data; v2: recinto, confiança, motivo e escaneamento fora
    (terceirizado, antes do TRA). Ciclos antigos só têm a data e a imagem."""
    if ciclo is None:
        return []
    return [{'data': e.data_escaneamento, 'recinto': e.codigoRecinto,
             'nomeRecinto': nomes_recintos.get(e.codigoRecinto),
             'confianca': e.confianca, 'motivo': e.motivo, 'fora': bool(e.fora), 'temImagem': e.imagem_id is not None}
            for e in sorted(ciclo.escaneamentos, key=lambda e: e.data_escaneamento or datetime.min)]


def _dados_situacao(ciclo: Optional[CicloDeVidaImportacao]) -> dict:
    """Estágio (para o selo e o filtro), situação/fluxo/motivo da v2 e `concluido` = entregue ao importador."""
    estagio = estagios_ciclo.estagio_do_ciclo(ciclo)
    return {
        'concluido': estagio == estagios_ciclo.ENTREGUE,
        'estagio': estagios_ciclo.descreve(estagio),
        'situacao': ciclo.situacao if ciclo else None,
        'fluxo': ciclo.fluxo if ciclo else None,
        'motivo': ciclo.motivo if ciclo else None,
    }


def monta_card_navii(
    ciclo: Optional[CicloDeVidaImportacao],
    conhecimento: Conhecimento,
    embarcacao: Optional[str] = None,
    itens: Optional[dict] = None,
    manifesto: Optional[Manifesto] = None,
    nome_embarcador: Optional[str] = None,
    nome_consignatario: Optional[str] = None,
    item: Optional[Item] = None,
    data_prevista_atracacao: Optional[str] = None,
    nomes_recintos: Optional[Dict[str, str]] = None,
    nomes_terminais: Optional[Dict[str, str]] = None,
    familia: Optional[dict] = None,
    fichas: Optional[List[dict]] = None,
    sem_desembarque: Optional[bool] = None,
    hoje: Optional[date] = None,
) -> dict:
    """Monta o dicionário de dados de um card: dados do mercante (inclusive
    vínculo Master/House, manifesto e contêineres/NCMs declarados), a linha do
    tempo do ciclo de vida (API Recintos) e as referências às imagens de
    escaneamento (já carregadas via `ciclo.escaneamentos`, lazy='selectin'). Do ciclo de vida
    v2 (colunas carregadas por `busca_ciclos_navii`): estágio, situação/motivo, entrada no seco
    implícita, embarque e os escaneamentos com recinto, confiança e "fora".

    `ciclo` pode ser None: o contêiner foi declarado no CE mas o ciclo de vida
    não foi localizado (ainda não desembarcou, ou a API Recintos não trouxe os
    eventos) — o card sai com `cicloDeVidaLocalizado=False`, linha do tempo
    vazia e sem imagens. `item` é o contêiner declarado que originou o card.

    `itens` é o dict retornado por `busca_itens_navii` para este CE (ou None
    quando não há Item/NCMItem carregados para ele). `nome_embarcador`/
    `nome_consignatario` vêm de `busca_nomes_empresas` (ou None se o valor
    guardado não bateu com nenhum CNPJ conhecido). `nomes_recintos`/
    `nomes_terminais` vêm de `busca_nomes_recintos` (por cod_siscomex/cod_carga).
    `familia` vem de `busca_familias_navii` (filhotes do master); o ciclo pode ter vindo de outro CE da
    família (`cicloPorCE`). `fichas`: Fichas OVR do CE, da família ou abertas pelo contêiner
    (`fichas_ovr.fichas_do`), inclusive as FMAs (abandono).

    `sem_desembarque` (16/10/2026): o contêiner ainda não tem desembarque localizado — na janela efetiva, o card
    entrou pela atracação prevista. Vem de `_itens_com_desembarque` (o mesmo critério da janela); None = deduzir do
    ciclo. Com `previsaoAtracacao` (`_previsao_atracacao`, relativa a `hoje`), a tela diz se o navio ainda vai
    chegar ou se a previsão já passou sem desembarque informado.
    """
    if sem_desembarque is None:
        sem_desembarque = ciclo is None or ciclo.data_desembarque is None
    nomes_recintos = nomes_recintos or {}
    nomes_terminais = nomes_terminais or {}
    imagens = [e.imagem_id for e in ciclo.escaneamentos if e.imagem_id is not None] if ciclo else []
    itens = itens or {'pesoBrutoTotal': None, 'qtdNcmsDistintos': 0, 'conteineres': []}
    numero_ce_master = _master_do(conhecimento)

    dados_manifesto = None
    if manifesto is not None:
        dados_manifesto = {
            'numero': manifesto.numero,
            'portoCarregamento': manifesto.portoCarregamento,
            'nomePortoCarregamento': referencias.descreve_porto(manifesto.portoCarregamento),
            'portoDescarregamento': manifesto.portoDescarregamento,
            'nomePortoDescarregamento': referencias.descreve_porto(manifesto.portoDescarregamento),
            'numeroViagem': manifesto.numeroViagem,
        }

    terminal = manifesto.codigoTerminalDescarregamento if manifesto is not None else None
    numero_conteiner = (ciclo.numeroConteiner if ciclo else None) or (item.codigoConteiner if item else None)

    return {
        'item_id': item.ID if item is not None else None,
        'ciclo_de_vida_id': ciclo.ID if ciclo else None,
        # False = contêiner declarado no CE, mas sem ciclo de vida localizado
        # (ainda não desembarcou ou a API Recintos não trouxe os eventos).
        'cicloDeVidaLocalizado': ciclo is not None,
        'numeroConteiner': numero_conteiner,
        'numeroCEmercante': conhecimento.numeroCEmercante,
        'numeroCEMaster': numero_ce_master,
        'imagens_ids': imagens,
        'embarcador': conhecimento.embarcador,
        'nomeEmbarcador': nome_embarcador,
        'consignatario': conhecimento.consignatario,
        'nomeConsignatario': nome_consignatario,
        'descricaoMercadoria': conhecimento.descricao,
        'portoOrigemCarga': conhecimento.portoOrigemCarga,
        'nomePortoOrigemCarga': referencias.descreve_porto(conhecimento.portoOrigemCarga),
        'portoDestFinal': conhecimento.portoDestFinal,
        'nomePortoDestFinal': referencias.descreve_porto(conhecimento.portoDestFinal),
        'tipoTrafego': conhecimento.get_tipoTrafego(),
        **_dados_bl_e_familia(conhecimento, ciclo, familia),
        **_dados_tipo_conteiner(item),
        'numConhecimento': conhecimento.numConhecimento,
        'embarcacao': embarcacao,  # número IMO (ou inscrição nacional), não o nome
        'nomeEmbarcacao': referencias.nome_navio(embarcacao),
        'manifesto': dados_manifesto,
        'dataPrevistaAtracacao': data_prevista_atracacao,
        'semDesembarque': sem_desembarque,
        'previsaoAtracacao': _previsao_atracacao(data_prevista_atracacao, hoje),
        'terminalDescarregamento': {'codigo': terminal, 'nome': nomes_terminais.get(terminal)} if terminal else None,
        'pesoBrutoTotal': itens['pesoBrutoTotal'],
        'qtdNcmsDistintos': itens['qtdNcmsDistintos'],
        'conteineres': itens['conteineres'],
        **_dados_situacao(ciclo),
        'linhaDoTempo': _linha_do_tempo(ciclo, nomes_recintos),
        'escaneamentos': _escaneamentos_do_card(ciclo, nomes_recintos),
        'fichasOvr': fichas or [],
    }


def _previsao_atracacao(data_prevista: Optional[str], hoje: Optional[date] = None) -> Optional[dict]:
    """{'data': 'AAAA-MM-DD', 'dias': até a previsão (negativo = já passou), 'situacao': 'futura' | 'hoje' |
    'vencida'}; None sem data válida."""
    try:
        prevista = datetime.strptime((data_prevista or '')[:10], '%Y-%m-%d').date()
    except ValueError:
        return None
    dias = (prevista - (hoje or date.today())).days
    return {'data': prevista.isoformat(), 'dias': dias,
            'situacao': 'futura' if dias > 0 else ('hoje' if dias == 0 else 'vencida')}


def monta_cards_navii(session, resultados: List[ResultadoNavii], hoje: Optional[date] = None) -> List[dict]:
    """Enriquece em lote (itens/NCMs, nomes de empresas e de recintos) uma
    página de `busca_ces_navii` e monta os cards — uma query por tipo de
    dado, só para os contêineres da página."""
    itens_por_ce = busca_itens_navii(session, [r.conhecimento.numeroCEmercante for r in resultados])
    nomes_empresas = busca_nomes_empresas(
        session,
        [v for r in resultados for v in (r.conhecimento.embarcador, r.conhecimento.consignatario)],
    )
    nomes_recintos = busca_nomes_recintos(session, _recintos_dos_ciclos(resultados))
    nomes_terminais = busca_nomes_recintos(
        session,
        [r.manifesto.codigoTerminalDescarregamento for r in resultados if r.manifesto is not None],
        campo='cod_carga',
    )
    familias = {r.conhecimento.numeroCEmercante: (r.familia or {}).get('familia', set()) for r in resultados}
    fichas_por_ce, fichas_por_conteiner = fichas_ovr.busca_fichas(
        session, {ce for numero, familia in familias.items() for ce in (numero, *familia)},
        [r.item.codigoConteiner for r in resultados if r.item is not None])
    cards = [
        monta_card_navii(
            r.ciclo, r.conhecimento, r.embarcacao,
            itens_por_ce.get(r.conhecimento.numeroCEmercante), r.manifesto,
            nomes_empresas.get(r.conhecimento.embarcador),
            nomes_empresas.get(r.conhecimento.consignatario),
            item=r.item,
            data_prevista_atracacao=r.data_prevista_atracacao,
            nomes_recintos=nomes_recintos,
            nomes_terminais=nomes_terminais,
            familia=r.familia,
            fichas=fichas_ovr.fichas_do(r.conhecimento.numeroCEmercante,
                                        r.item.codigoConteiner if r.item is not None else None,
                                        fichas_por_ce, fichas_por_conteiner,
                                        familias.get(r.conhecimento.numeroCEmercante, ())),
            sem_desembarque=r.sem_desembarque,
            hoje=hoje,
        )
        for r in resultados
    ]
    _acrescenta_risco_dta_corad(session, cards)
    return cards


def _recintos_dos_ciclos(resultados: List[ResultadoNavii]) -> List[str]:
    codigos = []
    for r in resultados:
        if r.ciclo:
            codigos += [r.ciclo.codigoRecintoDesembarque, r.ciclo.codigoRecintoArmazenagem]
            codigos += [e.codigoRecinto for e in r.ciclo.escaneamentos]
    return codigos


def _acrescenta_risco_dta_corad(session, cards: List[dict]):
    """Risco histórico do CE (None: não calculado — CE antes do início ou sem as tabelas), DTAs do CE e classe
    CORAD do consignatário (None: matriz ausente no banco)."""
    ces = {card['numeroCEmercante'] for card in cards}
    riscos, dtas = risco_navii.risco_dos_ces(session, ces), dta_navii.dtas_dos_ces(session, ces)
    corad = busca_corad(session, [card['consignatario'] for card in cards])
    for card in cards:
        card['risco'] = riscos.get(card['numeroCEmercante'])
        card['dtas'] = dtas.get(card['numeroCEmercante'], [])
        card['corad'] = corad.get(card['consignatario']) if corad else None


def _raiz_cnpj(consignatario: Optional[str]) -> Optional[str]:
    valor = (consignatario or '').strip()
    return valor[:8] if re.fullmatch(r'\d{14}', valor) else None


def busca_corad(session, consignatarios: Iterable[str]) -> Dict[str, dict]:
    """{consignatário: {'classe', 'rotulo'}} pela matriz CORAD (classe None = não consta: raiz fora da matriz, sem
    classificação ou consignatário que não é CNPJ). {} sem a tabela no banco."""
    consignatarios = {c for c in consignatarios if c}
    if not consignatarios or not tabelas_opcionais.existe(session, MATRIZ_CORAD.name):
        return {}
    raizes = {c: _raiz_cnpj(c) for c in consignatarios}
    classes = {}
    if any(raizes.values()):   # .all(): dict() trataria o resultado como mapeamento (ele tem keys())
        classes = dict(session.connection().execute(
            select(MATRIZ_CORAD.c.cod_emp_interv_rad, MATRIZ_CORAD.c.classificacao_aniita_imp)
            .where(MATRIZ_CORAD.c.cod_emp_interv_rad.in_(sorted({r for r in raizes.values() if r})))).all())
    resultado = {}
    for consignatario, raiz in raizes.items():
        classe = classes.get(raiz) if raiz else None
        if classe is not None and not 1 <= classe <= CORAD_MAX:
            classe = None
        resultado[consignatario] = {'classe': classe, 'rotulo': ROTULOS_CORAD[classe or 0]}
    return resultado


if __name__ == '__main__':  # pragma: no-cover
    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    filtros_exemplo = {'recinto': '8931356'}
    print('Total:', conta_conteineres_navii(session, 'efetiva', filtros_exemplo))
    for card in monta_cards_navii(session, busca_ces_navii(session, 'efetiva', filtros_exemplo, limit=10)):
        print(card)
