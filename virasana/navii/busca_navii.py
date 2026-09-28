# coding: utf-8
"""
Camada de dados (SQLAlchemy) do NAVI-I — Análise de risco na importação.

Busca CEs mercante dentro de uma janela de atracação (prevista ou efetiva),
cruza com o Ciclo de Vida (API Recintos) e exige que já existam escaneamentos
com imagem associada, para alimentar os cards em rolagem infinita.

Suporta filtros sucessivos (NCM, rota, produto, recinto, empresa) e
paginação por cursor (ID decrescente).

Fora de escopo por enquanto (sem fonte de dados/implementação ainda):
  - Filtros em linguagem natural (hoje são campos estruturados sucessivos).
  - Filtros subjetivos automáticos por empresa e sócios — não há fonte de
    dados de sócios no modelo atual.
"""
import sys
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

from sqlalchemy import or_, and_

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')

from virasana.integracao.mercante.mercantealchemy import (
    Conhecimento, Escala, EscalaManifesto, Item, Manifesto, NCMItem,
)
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import (
    CicloDeVidaImportacao, CicloDeVidaImportacaoEscaneamentos, logger,
)

# Janelas de atracação (em dias, a partir de hoje)
JANELA_PREVISTA_MIN_DIAS = 3
JANELA_PREVISTA_MAX_DIAS = 15
JANELA_EFETIVA_MIN_DIAS = 5
JANELA_EFETIVA_MAX_DIAS = 15

TIPOS_JANELA_VALIDOS = ('prevista', 'efetiva')


def _janela_previsao() -> Tuple[str, str]:
    """Janela de PREVISÃO de atracação: 3 a 15 dias no futuro.

    Retorna strings 'YYYY-MM-DD' porque `Escala.dataPrevistaAtracacao` é
    armazenado como VARCHAR(10) nesse formato (não é um DateTime real).
    """
    hoje = datetime.now()
    inicio = hoje + timedelta(days=JANELA_PREVISTA_MIN_DIAS)
    fim = hoje + timedelta(days=JANELA_PREVISTA_MAX_DIAS)
    return inicio.strftime('%Y-%m-%d'), fim.strftime('%Y-%m-%d')


def _janela_efetiva() -> Tuple[datetime, datetime]:
    """Janela de desembarque EFETIVO: 5 a 15 dias no passado.

    Retorna datetimes reais para comparar contra
    `CicloDeVidaImportacao.data_desembarque` (coluna DateTime, populada a
    partir de `EmbarqueDesembarque.dataHoraOcorrencia` — evento específico do
    contêiner, não da escala/navio).
    """
    hoje = datetime.now()
    inicio = hoje - timedelta(days=JANELA_EFETIVA_MAX_DIAS)
    fim = hoje - timedelta(days=JANELA_EFETIVA_MIN_DIAS)
    return inicio, fim


def busca_ces_navii(
    session,
    tipo_janela: str = 'efetiva',
    filtros: Optional[dict] = None,
    exigir_escaneamento: Optional[bool] = None,
    cursor_id: Optional[int] = None,
    limit: int = 20,
) -> List[Tuple[CicloDeVidaImportacao, Conhecimento]]:
    """Busca CEs mercante na janela de atracação informada, para a tela do NAVI-I.

    Args:
        session: sessão SQLAlchemy.
        tipo_janela: 'prevista' (3-15 dias no futuro, via Escala.dataPrevistaAtracacao)
            ou 'efetiva' (5-15 dias no passado, via CicloDeVidaImportacao.data_desembarque).
        filtros: dict opcional com filtros sucessivos:
            - ncm (str): prefixo do código NCM (ex.: '8471')
            - porto_origem / porto_destino (str): código do porto (rota)
            - produto (str): busca textual na descrição do item (NCMItem.descritivo)
            - recinto (str): código do recinto de desembarque OU de armazenagem
            - empresa (str): busca textual em embarcador ou consignatário
        exigir_escaneamento: se True, só retorna CEs cujo Ciclo de Vida já tem ao
            menos um escaneamento com imagem associada ("recém escaneados"). Se
            None (padrão), o comportamento é condicional à janela: True para
            'efetiva' (já desembarcou, é razoável já ter sido escaneado) e False
            para 'prevista' (a carga ainda não chegou, exigir escaneamento
            devolveria pouco ou nada). Pode ser sobrescrito explicitamente.
        cursor_id: ID do último CicloDeVidaImportacao.ID já exibido na tela, para
            paginação por rolagem infinita (busca sempre ID menor que o cursor).
        limit: tamanho da página.

    Returns:
        Lista de tuplas (CicloDeVidaImportacao, Conhecimento), ordenada por
        CicloDeVidaImportacao.ID decrescente (mais recentes primeiro).

    Observação de escopo:
        Na janela 'efetiva', a data usada é `data_desembarque` do próprio
        Ciclo de Vida — evento específico do contêiner (EmbarqueDesembarque),
        não a atracação do navio como um todo (Escala), que é menos precisa e
        armazenada como string. A ligação com "recém escaneados" continua
        sendo pela EXISTÊNCIA de escaneamento com imagem, não pela data do
        escaneamento cair dentro da janela — se quiser exigir isso também, é
        só avisar para adicionarmos o filtro em
        CicloDeVidaImportacaoEscaneamentos.data_escaneamento.

        Filtros "subjetivos" (empresa e sócios cruzados com listas de risco) não
        estão implementados aqui — dependem de uma fonte de dados de sócios que
        ainda não existe no modelo atual.
    """
    filtros = filtros or {}
    if tipo_janela not in TIPOS_JANELA_VALIDOS:
        raise ValueError(f"tipo_janela inválido: {tipo_janela!r}. Use 'prevista' ou 'efetiva'.")
    if exigir_escaneamento is None:
        exigir_escaneamento = (tipo_janela == 'efetiva')

    q = (
        session.query(CicloDeVidaImportacao, Conhecimento)
        .join(Conhecimento, Conhecimento.numeroCEmercante == CicloDeVidaImportacao.numeroCEmercante)
    )

    if tipo_janela == 'prevista':
        inicio, fim = _janela_previsao()
        q = (
            q.join(Manifesto, Manifesto.numero == Conhecimento.manifestoCE)
             .join(EscalaManifesto, EscalaManifesto.manifesto == Manifesto.numero)
             .join(Escala, Escala.numeroDaEscala == EscalaManifesto.escala)
             .filter(
                 Escala.dataPrevistaAtracacao.isnot(None),
                 Escala.dataPrevistaAtracacao >= inicio,
                 Escala.dataPrevistaAtracacao <= fim,
             )
        )
    else:  # 'efetiva' -> usa data_desembarque (por contêiner, real DateTime)
        inicio, fim = _janela_efetiva()
        q = q.filter(
            CicloDeVidaImportacao.data_desembarque.isnot(None),
            CicloDeVidaImportacao.data_desembarque >= inicio,
            CicloDeVidaImportacao.data_desembarque <= fim,
        )

    if exigir_escaneamento:
        q = q.filter(
            CicloDeVidaImportacao.escaneamentos.any(
                CicloDeVidaImportacaoEscaneamentos.imagem_id.isnot(None)
            )
        )

    # Filtros que dependem de Item / NCMItem (join único, sob demanda)
    precisa_item = bool(filtros.get('ncm') or filtros.get('produto'))
    if precisa_item:
        q = q.join(Item, Item.numeroCEmercante == Conhecimento.numeroCEmercante)
        precisa_ncmitem = bool(filtros.get('ncm') or filtros.get('produto'))
        if precisa_ncmitem:
            q = q.join(
                NCMItem,
                and_(
                    NCMItem.numeroCEMercante == Item.numeroCEmercante,
                    NCMItem.codigoConteiner == Item.codigoConteiner,
                ),
            )
        if filtros.get('ncm'):
            q = q.filter(NCMItem.identificacaoNCM.like(f"{filtros['ncm']}%"))
        if filtros.get('produto'):
            q = q.filter(NCMItem.descritivo.ilike(f"%{filtros['produto']}%"))

    if filtros.get('porto_origem'):
        q = q.filter(Conhecimento.portoOrigemCarga == filtros['porto_origem'])

    if filtros.get('porto_destino'):
        q = q.filter(Conhecimento.portoDestFinal == filtros['porto_destino'])

    if filtros.get('recinto'):
        recinto = filtros['recinto']
        q = q.filter(or_(
            CicloDeVidaImportacao.codigoRecintoDesembarque == recinto,
            CicloDeVidaImportacao.codigoRecintoArmazenagem == recinto,
        ))

    if filtros.get('empresa'):
        termo = f"%{filtros['empresa']}%"
        q = q.filter(or_(
            Conhecimento.embarcador.ilike(termo),
            Conhecimento.consignatario.ilike(termo),
        ))

    if cursor_id:
        q = q.filter(CicloDeVidaImportacao.ID < cursor_id)

    q = q.distinct().order_by(CicloDeVidaImportacao.ID.desc()).limit(limit)

    resultado = q.all()
    logger.info(
        f'busca_ces_navii: janela={tipo_janela} [{inicio}..{fim}] filtros={filtros} '
        f'-> {len(resultado)} CEs retornados'
    )
    return resultado


def monta_card_navii(ciclo: CicloDeVidaImportacao, conhecimento: Conhecimento) -> dict:
    """Monta o dicionário de dados de um card para a tela: dados do mercante,
    do ciclo de vida (API Recintos) e as referências às imagens de escaneamento
    (já carregadas via `ciclo.escaneamentos`, lazy='selectin').
    """
    imagens = [e.imagem_id for e in ciclo.escaneamentos if e.imagem_id is not None]
    return {
        'ciclo_de_vida_id': ciclo.ID,
        'numeroConteiner': ciclo.numeroConteiner,
        'numeroCEmercante': ciclo.numeroCEmercante,
        'codigoRecintoDesembarque': ciclo.codigoRecintoDesembarque,
        'codigoRecintoArmazenagem': ciclo.codigoRecintoArmazenagem,
        'data_desembarque': ciclo.data_desembarque,
        'concluido': ciclo.concluido,
        'imagens_ids': imagens,
        'embarcador': conhecimento.embarcador,
        'consignatario': conhecimento.consignatario,
        'portoOrigemCarga': conhecimento.portoOrigemCarga,
        'portoDestFinal': conhecimento.portoDestFinal,
        'tipoTrafego': conhecimento.get_tipoTrafego(),
        'tipoBL': conhecimento.get_tipoBL(),
        'numConhecimento': conhecimento.numConhecimento,
    }


if __name__ == '__main__':  # pragma: no-cover
    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    resultados = busca_ces_navii(
        session,
        tipo_janela='efetiva',
        filtros={'recinto': '8931356'},
        limit=10,
    )
    for ciclo, ce in resultados:
        print(monta_card_navii(ciclo, ce))
