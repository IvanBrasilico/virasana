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
produto na descrição do CE, recinto — portos, NCM e recinto aceitam lista —,
empresa, ciclo concluído/"entregue ao importador", quantidade máxima de NCMs
distintos, somente com imagem de escaneamento), contagem total do filtro e paginação por cursor (Item.ID decrescente).
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
import re
import sys
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, NamedTuple, Optional, Tuple

from sqlalchemy import (
    BigInteger, Column, MetaData, Table, VARCHAR, and_, func, join, literal, not_, or_, select, union,
)

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')

from virasana.integracao.mercante.mercantealchemy import (
    Conhecimento, Enumerado, Escala, EscalaManifesto, Item, Manifesto, NCMItem,
)
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import (
    CicloDeVidaImportacao, CicloDeVidaImportacaoEscaneamentos, logger,
)
from bhadrasana.models.laudo import Empresa
from virasana.navii import referencias

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

# Filtros que aceitam uma LISTA de códigos (vale qualquer um deles: "OU").
# Chegam como lista ou como texto separado por vírgula/ponto e vírgula/espaço.
CAMPOS_LISTA = ('porto_escala', 'porto_origem', 'porto_destino', 'porto_baldeacao', 'ncm', 'recinto')
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


def _periodo_navii(tipo_janela: str, filtros: dict) -> Tuple[str, str]:
    """Período ('YYYY-MM-DD', 'YYYY-MM-DD') efetivamente usado na busca:
    `data_inicio`/`data_fim` dos filtros (se vierem os dois) ou a sugestão da
    janela."""
    if filtros.get('data_inicio') and filtros.get('data_fim'):
        return filtros['data_inicio'], filtros['data_fim']
    if tipo_janela == 'prevista':
        return _janela_previsao()
    inicio, fim = _janela_efetiva()
    return inicio.strftime('%Y-%m-%d'), fim.strftime('%Y-%m-%d')


def _mesmo_conteiner_do_ciclo():
    """Critério de ligação Item (contêiner declarado) <-> CicloDeVidaImportacao."""
    return and_(
        CicloDeVidaImportacao.numeroCEmercante == Item.numeroCEmercante,
        CicloDeVidaImportacao.numeroConteiner == Item.codigoConteiner,
    )


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


def _existe_ciclo(*criterios, origem=CicloDeVidaImportacao):
    """`_existe` para o ciclo de vida do contêiner (mesmo CE e contêiner)."""
    return _existe(origem, _mesmo_conteiner_do_ciclo(), *criterios)


def _lista_de_codigos(valor) -> List[str]:
    """'CNSHA, cnngb;SGSIN' ou ['CNSHA', 'CNNGB SGSIN'] -> ['CNSHA', 'cnngb', 'SGSIN']."""
    if valor is None:
        return []
    partes = valor if isinstance(valor, (list, tuple, set)) else [valor]
    codigos = []
    for parte in partes:
        codigos.extend(p for p in re.split(r'[,;\s]+', str(parte)) if p)
    return codigos


def normaliza_filtros(filtros: Optional[dict]) -> dict:
    """Padroniza o que o usuário digitou: os campos de CAMPOS_LISTA viram
    listas sem repetição, códigos de porto em maiúsculas e NCM só com dígitos
    (a base guarda '84713012'; o usuário pode digitar '8471.30.12'). Sem isso,
    'brssz' ou '8471.30' não achariam nada. Lista vazia remove o filtro."""
    filtros = dict(filtros or {})
    for campo in CAMPOS_LISTA:
        if campo not in filtros:
            continue
        codigos = []
        for codigo in _lista_de_codigos(filtros[campo]):
            if campo in _CAMPOS_PORTO:
                codigo = codigo.upper()
            elif campo == 'ncm':
                codigo = re.sub(r'\D', '', codigo)
            if codigo and codigo not in codigos:
                codigos.append(codigo)
        if codigos:
            filtros[campo] = codigos
        else:
            del filtros[campo]
    return filtros


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


def _itens_com_desembarque_efetivo(inicio: datetime, fim: datetime):
    """IDs de Item cujo ciclo de vida registra desembarque do contêiner no período."""
    return (
        select(Item.ID.label('item_id'))
        .select_from(CicloDeVidaImportacao)
        .join(Item, _mesmo_conteiner_do_ciclo())
        .where(CicloDeVidaImportacao.data_desembarque.between(inicio, fim))
    )


def _filtra_janela(q, tipo_janela: str, inicio: str, fim: str, porto=None,
                   criterios_ce=()):
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
    """
    if tipo_janela == 'prevista':
        return q.filter(Conhecimento.manifestoCE.in_(_manifestos_com_atracacao_prevista(inicio, fim, porto)))
    inicio_dt = _parse_data(inicio)
    fim_dt = _parse_data(fim) + timedelta(days=1, seconds=-1)  # dia final inteiro
    sem_desembarque_localizado = not_(_existe_ciclo(CicloDeVidaImportacao.data_desembarque.isnot(None)))
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
    janela = union(
        _itens_com_desembarque_efetivo(inicio_dt, fim_dt),
        itens_pela_atracacao_prevista,
    ).subquery('janela')
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
            - recinto (str): código (cod_siscomex) do recinto de desembarque OU
              de armazenagem do ciclo de vida — exige ciclo de vida. Filtro
              secundário: o terminal do Manifesto (cod_carga) não é usado, só
              existe para operadores portuários e deixaria recintos de fora.
            - empresa (str): busca textual em embarcador ou consignatário
            - concluido (bool): só contêineres com ciclo de vida concluído
              ("entregue ao importador") — exige ciclo de vida
            - ncms_max (int): quantidade máxima de NCMs distintos declarados no CE
              ("até N NCMs distintos"; CE sem NCM declarado conta como 0)
            - com_imagem (bool): equivalente a `exigir_escaneamento=True`
            - data_inicio / data_fim (str 'YYYY-MM-DD'): sobrescrevem a janela
              sugerida por `_janela_previsao`/`_janela_efetiva` para esse
              `tipo_janela`. Os dois precisam vir juntos.
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
    criterios_ce = [or_(
        Conhecimento.tipoTrafego != TIPO_TRAFEGO_EXPORTACAO,
        Conhecimento.tipoTrafego.is_(None),
    )]
    if filtros.get('porto_origem'):
        criterios_ce.append(_algum_porto(Conhecimento.portoOrigemCarga, filtros['porto_origem']))
    if filtros.get('porto_destino'):
        criterios_ce.append(_algum_porto(Conhecimento.portoDestFinal, filtros['porto_destino']))
    if filtros.get('porto_baldeacao'):
        criterios_ce.append(Conhecimento.manifestoCE.in_(
            select(Manifesto.numero).where(_algum_porto(Manifesto.portoCarregamento, filtros['porto_baldeacao']))
        ))
    if filtros.get('produto'):
        criterios_ce.append(Conhecimento.descricao.ilike(f"%{filtros['produto'].strip()}%"))
    if filtros.get('empresa'):
        termo = f"%{filtros['empresa']}%"
        criterios_ce.append(or_(
            Conhecimento.embarcador.ilike(termo),
            Conhecimento.consignatario.ilike(termo),
        ))
    q = _filtra_janela(q, tipo_janela, inicio, fim, filtros.get('porto_escala'), criterios_ce)
    q = q.filter(*criterios_ce)

    if exigir_escaneamento or filtros.get('com_imagem'):
        q = q.filter(_existe_ciclo(
            CicloDeVidaImportacaoEscaneamentos.imagem_id.isnot(None),
            origem=join(
                CicloDeVidaImportacao, CicloDeVidaImportacaoEscaneamentos,
                CicloDeVidaImportacaoEscaneamentos.ciclo_de_vida_id == CicloDeVidaImportacao.ID,
            ),
        ))

    if filtros.get('ncm'):
        q = q.filter(_existe(
            NCMItem,
            NCMItem.numeroCEMercante == Item.numeroCEmercante,
            NCMItem.codigoConteiner == Item.codigoConteiner,
            or_(*[NCMItem.identificacaoNCM.like(f'{ncm}%') for ncm in filtros['ncm']]),
        ))

    if filtros.get('recinto'):
        recintos = filtros['recinto']
        q = q.filter(_existe_ciclo(or_(
            CicloDeVidaImportacao.codigoRecintoDesembarque.in_(recintos),
            CicloDeVidaImportacao.codigoRecintoArmazenagem.in_(recintos),
        )))

    if filtros.get('concluido'):
        q = q.filter(_existe_ciclo(CicloDeVidaImportacao.concluido.is_(True)))

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

    ciclos = busca_ciclos_navii(session, [item for item, _ in linhas])
    manifestos = busca_manifestos_navii(
        session, [ce.manifestoCE for _, ce in linhas], normaliza_filtros(filtros).get('porto_escala'),
    )
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
        ))
    logger.info(
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


def busca_ciclos_navii(session, itens: List[Item]) -> Dict[Tuple[str, str], CicloDeVidaImportacao]:
    """Ciclos de vida dos contêineres da página, em lote, por (CE, contêiner).

    Quando há mais de um ciclo para o mesmo contêiner/CE (ex.: vários
    desembarques informados), prevalece o mais recente (maior ID).
    Contêiner sem ciclo simplesmente não aparece no dict.
    """
    numeros_ce = {item.numeroCEmercante for item in itens if item.numeroCEmercante}
    if not numeros_ce:
        return {}
    ciclos = (
        session.query(CicloDeVidaImportacao)
        .filter(CicloDeVidaImportacao.numeroCEmercante.in_(numeros_ce))
        .order_by(CicloDeVidaImportacao.ID)
        .all()
    )
    return {(ciclo.numeroCEmercante, ciclo.numeroConteiner): ciclo for ciclo in ciclos}


def busca_manifestos_navii(
    session, numeros_manifesto: Iterable[str], porto=None,
) -> Dict[str, dict]:
    """Manifesto + dados da escala (embarcação, data prevista de atracação) dos
    CEs da página, em lote, por número do manifesto. Com `porto` (código ou
    lista), considera só as escalas nesse(s) porto(s) de atracação (mesmo
    critério da janela).

    Returns:
        {numero: {'manifesto': Manifesto | None, 'embarcacao': str | None,
                  'dataPrevistaAtracacao': str | None}}
        Com mais de uma escala para o mesmo manifesto, usa a de maior data
        prevista de atracação.
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
        if data_prevista and (dados['dataPrevistaAtracacao'] or '') < data_prevista:
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
                  {'codigoConteiner': str, 'isoCode': str,
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
                'isoCode': None, 'pesoBruto': None, 'tara': None, 'lacre': None,
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
) -> dict:
    """Monta o dicionário de dados de um card: dados do mercante (inclusive
    vínculo Master/House, manifesto e contêineres/NCMs declarados), a linha do
    tempo do ciclo de vida (API Recintos) e as referências às imagens de
    escaneamento (já carregadas via `ciclo.escaneamentos`, lazy='selectin').

    `ciclo` pode ser None: o contêiner foi declarado no CE mas o ciclo de vida
    não foi localizado (ainda não desembarcou, ou a API Recintos não trouxe os
    eventos) — o card sai com `cicloDeVidaLocalizado=False`, linha do tempo
    vazia e sem imagens. `item` é o contêiner declarado que originou o card.

    `itens` é o dict retornado por `busca_itens_navii` para este CE (ou None
    quando não há Item/NCMItem carregados para ele). `nome_embarcador`/
    `nome_consignatario` vêm de `busca_nomes_empresas` (ou None se o valor
    guardado não bateu com nenhum CNPJ conhecido). `nomes_recintos`/
    `nomes_terminais` vêm de `busca_nomes_recintos` (por cod_siscomex/cod_carga).
    """
    nomes_recintos = nomes_recintos or {}
    nomes_terminais = nomes_terminais or {}
    imagens = [e.imagem_id for e in ciclo.escaneamentos if e.imagem_id is not None] if ciclo else []
    itens = itens or {'pesoBrutoTotal': None, 'qtdNcmsDistintos': 0, 'conteineres': []}
    numero_ce_master = conhecimento.numeroCEMaster
    if numero_ce_master == conhecimento.numeroCEmercante:
        numero_ce_master = None  # CE único costuma repetir o próprio número; não é um vínculo real

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

    def etapa(data_attr: str, recinto_attr: str) -> dict:
        codigo = getattr(ciclo, recinto_attr) if ciclo else None
        return {
            'data': getattr(ciclo, data_attr) if ciclo else None,
            'recinto': codigo,
            'nomeRecinto': nomes_recintos.get(codigo),
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
        'tipoBL': conhecimento.get_tipoBL(),
        'numConhecimento': conhecimento.numConhecimento,
        'embarcacao': embarcacao,  # número IMO (ou inscrição nacional), não o nome
        'nomeEmbarcacao': referencias.nome_navio(embarcacao),
        'manifesto': dados_manifesto,
        'dataPrevistaAtracacao': data_prevista_atracacao,
        'terminalDescarregamento': {'codigo': terminal, 'nome': nomes_terminais.get(terminal)} if terminal else None,
        'pesoBrutoTotal': itens['pesoBrutoTotal'],
        'qtdNcmsDistintos': itens['qtdNcmsDistintos'],
        'conteineres': itens['conteineres'],
        'concluido': bool(ciclo and ciclo.concluido),
        # Linha do tempo do ciclo de vida (API Recintos) — cada etapa pode ser
        # None quando o evento ainda não ocorreu ("se houver") ou quando não
        # há ciclo de vida para o contêiner.
        'linhaDoTempo': {
            'desembarque': etapa('data_desembarque', 'codigoRecintoDesembarque'),
            'saida': etapa('data_saida', 'codigoRecintoDesembarque'),
            'entradaRecintoSeco': etapa('data_entrada_seco', 'codigoRecintoArmazenagem'),
            'saidaRecintoSeco': etapa('data_saida_seco', 'codigoRecintoArmazenagem'),
        },
    }


def monta_cards_navii(session, resultados: List[ResultadoNavii]) -> List[dict]:
    """Enriquece em lote (itens/NCMs, nomes de empresas e de recintos) uma
    página de `busca_ces_navii` e monta os cards — uma query por tipo de
    dado, só para os contêineres da página."""
    itens_por_ce = busca_itens_navii(session, [r.conhecimento.numeroCEmercante for r in resultados])
    nomes_empresas = busca_nomes_empresas(
        session,
        [v for r in resultados for v in (r.conhecimento.embarcador, r.conhecimento.consignatario)],
    )
    nomes_recintos = busca_nomes_recintos(
        session,
        [c for r in resultados if r.ciclo
         for c in (r.ciclo.codigoRecintoDesembarque, r.ciclo.codigoRecintoArmazenagem)],
    )
    nomes_terminais = busca_nomes_recintos(
        session,
        [r.manifesto.codigoTerminalDescarregamento for r in resultados if r.manifesto is not None],
        campo='cod_carga',
    )
    return [
        monta_card_navii(
            r.ciclo, r.conhecimento, r.embarcacao,
            itens_por_ce.get(r.conhecimento.numeroCEmercante), r.manifesto,
            nomes_empresas.get(r.conhecimento.embarcador),
            nomes_empresas.get(r.conhecimento.consignatario),
            item=r.item,
            data_prevista_atracacao=r.data_prevista_atracacao,
            nomes_recintos=nomes_recintos,
            nomes_terminais=nomes_terminais,
        )
        for r in resultados
    ]


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
