# coding: utf-8
"""
Camada de dados (SQLAlchemy) do NAVI-I — Análise de risco na importação.

Busca CEs mercante dentro de uma janela de atracação (prevista ou efetiva),
cruza com o Ciclo de Vida (API Recintos) e exige que já existam escaneamentos
com imagem associada, para alimentar os cards em rolagem infinita.

Suporta filtros sucessivos (NCM, rota, porto de baldeação do manifesto,
produto, recinto, empresa, ciclo concluído/"entregue ao importador",
quantidade máxima de NCMs distintos) e paginação por cursor (ID decrescente).
A janela de dias (`_janela_previsao`/`_janela_efetiva`) é apenas a *sugestão*
de período inicial — `data_inicio`/`data_fim` em `filtros` sobrescrevem essa
sugestão.

Enriquecimento cruza também com dados do repo irmão bhadrasana2
(`bhadrasana.models.laudo`, tabelas `laudo_empresas`/`laudo_ncm`) para
resolver CNPJ→nome de empresa e código NCM→descrição oficial.

Fora de escopo por enquanto (sem fonte de dados/implementação ainda):
  - Filtros em linguagem natural (hoje são campos estruturados sucessivos).
  - Filtros subjetivos automáticos por empresa e sócios — não há fonte de
    dados de sócios no modelo atual.
  - Resolução de CPF (bagagens_pessoas) — embarcador/consignatário do
    Conhecimento só guardam CNPJ ou nome estrangeiro, nunca CPF.
"""
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from sqlalchemy import and_, func, or_

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
from bhadrasana.models.laudo import NCM as LaudoNCM

# Janelas de atracação (em dias, a partir de hoje) — usadas só como SUGESTÃO
# de período inicial na tela; o usuário pode escolher outro intervalo.
JANELA_PREVISTA_MIN_DIAS = 3
JANELA_PREVISTA_MAX_DIAS = 15
JANELA_EFETIVA_MIN_DIAS = 5
JANELA_EFETIVA_MAX_DIAS = 15

TIPOS_JANELA_VALIDOS = ('prevista', 'efetiva')


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


def busca_ces_navii(
    session,
    tipo_janela: str = 'efetiva',
    filtros: Optional[dict] = None,
    exigir_escaneamento: Optional[bool] = None,
    cursor_id: Optional[int] = None,
    limit: int = 20,
) -> List[Tuple[CicloDeVidaImportacao, Conhecimento, Optional[str], Optional[Manifesto]]]:
    """Busca CEs mercante na janela de atracação informada, para a tela do NAVI-I.

    Args:
        session: sessão SQLAlchemy.
        tipo_janela: 'prevista' (via Escala.dataPrevistaAtracacao)
            ou 'efetiva' (via CicloDeVidaImportacao.data_desembarque).
        filtros: dict opcional com filtros sucessivos:
            - ncm (str): prefixo do código NCM (ex.: '8471')
            - porto_origem / porto_destino (str): código do porto de origem/destino
              da carga (Conhecimento — o que o embarcador declarou)
            - porto_baldeacao (str): código do porto de CARREGAMENTO do Manifesto
              (Manifesto.portoCarregamento) — quando difere do porto de origem do
              Conhecimento, indica que houve transbordo/baldeação antes do Brasil.
            - produto (str): busca textual na descrição do item (NCMItem.descritivo)
            - recinto (str): código do recinto de desembarque OU de armazenagem
            - empresa (str): busca textual em embarcador ou consignatário
            - concluido (bool): se True, só CEs com ciclo de vida concluído
              ("entregue ao importador")
            - ncms_max (int): quantidade máxima de NCMs distintos declarados no CE
              ("até N NCMs distintos")
            - data_inicio / data_fim (str 'YYYY-MM-DD'): sobrescrevem a janela
              sugerida por `_janela_previsao`/`_janela_efetiva` para esse
              `tipo_janela`. Os dois precisam vir juntos.
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
        Lista de tuplas (CicloDeVidaImportacao, Conhecimento, nome_embarcacao,
        Manifesto), ordenada por CicloDeVidaImportacao.ID decrescente (mais
        recentes primeiro). `nome_embarcacao` e `Manifesto` podem ser None
        quando a Escala/Manifesto do CE ainda não foi recebida/casada (LEFT JOIN).

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

    # Manifesto/Escala (nome da embarcação, dados de manifesto e porto de
    # baldeação, exibidos no card) são sempre trazidos via LEFT JOIN,
    # independente da janela — em 'prevista' também são usados para filtrar a
    # data prevista de atracação.
    q = (
        session.query(CicloDeVidaImportacao, Conhecimento, Escala.embarcacao, Manifesto)
        .join(Conhecimento, Conhecimento.numeroCEmercante == CicloDeVidaImportacao.numeroCEmercante)
        .outerjoin(Manifesto, Manifesto.numero == Conhecimento.manifestoCE)
        .outerjoin(EscalaManifesto, EscalaManifesto.manifesto == Manifesto.numero)
        .outerjoin(Escala, Escala.numeroDaEscala == EscalaManifesto.escala)
    )

    data_inicio_str = filtros.get('data_inicio')
    data_fim_str = filtros.get('data_fim')
    tem_intervalo_customizado = bool(data_inicio_str and data_fim_str)

    if tipo_janela == 'prevista':
        if tem_intervalo_customizado:
            inicio, fim = data_inicio_str, data_fim_str
        else:
            inicio, fim = _janela_previsao()
        q = q.filter(
            Escala.dataPrevistaAtracacao.isnot(None),
            Escala.dataPrevistaAtracacao >= inicio,
            Escala.dataPrevistaAtracacao <= fim,
        )
    else:  # 'efetiva' -> usa data_desembarque (por contêiner, real DateTime)
        if tem_intervalo_customizado:
            inicio = _parse_data(data_inicio_str)
            # inclui o dia final inteiro (até 23:59:59)
            fim = _parse_data(data_fim_str) + timedelta(days=1, seconds=-1)
        else:
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

    if filtros.get('porto_baldeacao'):
        q = q.filter(Manifesto.portoCarregamento == filtros['porto_baldeacao'])

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

    if filtros.get('concluido'):
        q = q.filter(CicloDeVidaImportacao.concluido.is_(True))

    if filtros.get('ncms_max'):
        subq_ncms = (
            session.query(
                NCMItem.numeroCEMercante.label('ce'),
                func.count(func.distinct(NCMItem.identificacaoNCM)).label('qtd_ncms'),
            )
            .group_by(NCMItem.numeroCEMercante)
            .subquery()
        )
        q = (
            q.join(subq_ncms, subq_ncms.c.ce == Conhecimento.numeroCEmercante)
             .filter(subq_ncms.c.qtd_ncms <= int(filtros['ncms_max']))
        )

    if cursor_id:
        q = q.filter(CicloDeVidaImportacao.ID < cursor_id)

    q = q.distinct().order_by(CicloDeVidaImportacao.ID.desc()).limit(limit)

    resultado = q.all()
    logger.info(
        f'busca_ces_navii: janela={tipo_janela} [{inicio}..{fim}] filtros={filtros} '
        f'-> {len(resultado)} CEs retornados'
    )
    return resultado


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

    Além de Item/NCMItem, cruza em mais duas queries em lote:
      - `laudo_ncm` (repo bhadrasana2): descrição oficial do NCM, usada no
        lugar de `NCMItem.descritivo` (texto livre declarado no manifesto,
        muitas vezes incompleto) quando existe uma correspondência.
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
                   'ncms': [{'identificacaoNCM': str, 'descritivo': str}, ...]},
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
    codigos_ncm = {ncm_item.identificacaoNCM for ncm_item in ncm_itens if ncm_item.identificacaoNCM}
    descricoes_laudo: Dict[str, str] = {}
    if codigos_ncm:
        for laudo_ncm in (
            session.query(LaudoNCM)
            .filter(LaudoNCM.title.in_(codigos_ncm))
            .all()
        ):
            if laudo_ncm.contents:
                descricoes_laudo[laudo_ncm.title] = laudo_ncm.contents

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
        # Prioridade: descrição oficial do laudo_ncm; se não houver, cai para o
        # texto livre declarado no manifesto (NCMItem.descritivo).
        descricao = descricoes_laudo.get(ncm_item.identificacaoNCM) or ncm_item.descritivo
        conteiner['ncms'].append({
            'identificacaoNCM': ncm_item.identificacaoNCM,
            'descritivo': descricao,
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
    ciclo: CicloDeVidaImportacao,
    conhecimento: Conhecimento,
    embarcacao: Optional[str] = None,
    itens: Optional[dict] = None,
    manifesto: Optional[Manifesto] = None,
    nome_embarcador: Optional[str] = None,
    nome_consignatario: Optional[str] = None,
) -> dict:
    """Monta o dicionário de dados de um card: dados do mercante (inclusive
    vínculo Master/House, manifesto e contêineres/NCMs declarados), a linha do
    tempo do ciclo de vida (API Recintos) e as referências às imagens de
    escaneamento (já carregadas via `ciclo.escaneamentos`, lazy='selectin').

    `itens` é o dict retornado por `busca_itens_navii` para este CE (ou None
    quando não há Item/NCMItem carregados para ele). `nome_embarcador`/
    `nome_consignatario` vêm de `busca_nomes_empresas` (ou None se o valor
    guardado não bateu com nenhum CNPJ conhecido).
    """
    imagens = [e.imagem_id for e in ciclo.escaneamentos if e.imagem_id is not None]
    itens = itens or {'pesoBrutoTotal': None, 'qtdNcmsDistintos': 0, 'conteineres': []}
    numero_ce_master = conhecimento.numeroCEMaster
    if numero_ce_master == conhecimento.numeroCEmercante:
        numero_ce_master = None  # CE único costuma repetir o próprio número; não é um vínculo real

    dados_manifesto = None
    if manifesto is not None:
        dados_manifesto = {
            'numero': manifesto.numero,
            'portoCarregamento': manifesto.portoCarregamento,
            'portoDescarregamento': manifesto.portoDescarregamento,
            'numeroViagem': manifesto.numeroViagem,
        }

    return {
        'ciclo_de_vida_id': ciclo.ID,
        'numeroConteiner': ciclo.numeroConteiner,
        'numeroCEmercante': ciclo.numeroCEmercante,
        'numeroCEMaster': numero_ce_master,
        'imagens_ids': imagens,
        'embarcador': conhecimento.embarcador,
        'nomeEmbarcador': nome_embarcador,
        'consignatario': conhecimento.consignatario,
        'nomeConsignatario': nome_consignatario,
        'descricaoMercadoria': conhecimento.descricao,
        'portoOrigemCarga': conhecimento.portoOrigemCarga,
        'portoDestFinal': conhecimento.portoDestFinal,
        'tipoTrafego': conhecimento.get_tipoTrafego(),
        'tipoBL': conhecimento.get_tipoBL(),
        'numConhecimento': conhecimento.numConhecimento,
        'embarcacao': embarcacao,
        'manifesto': dados_manifesto,
        'pesoBrutoTotal': itens['pesoBrutoTotal'],
        'qtdNcmsDistintos': itens['qtdNcmsDistintos'],
        'conteineres': itens['conteineres'],
        'concluido': ciclo.concluido,
        # Linha do tempo do ciclo de vida (API Recintos) — cada etapa pode ser
        # None quando o evento ainda não ocorreu ("se houver").
        'linhaDoTempo': {
            'desembarque': {
                'data': ciclo.data_desembarque,
                'recinto': ciclo.codigoRecintoDesembarque,
            },
            'saida': {
                'data': ciclo.data_saida,
                'recinto': ciclo.codigoRecintoDesembarque,
            },
            'entradaRecintoSeco': {
                'data': ciclo.data_entrada_seco,
                'recinto': ciclo.codigoRecintoArmazenagem,
            },
            'saidaRecintoSeco': {
                'data': ciclo.data_saida_seco,
                'recinto': ciclo.codigoRecintoArmazenagem,
            },
        },
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
    numeros = [ce.numeroCEmercante for _, ce, _, _ in resultados]
    itens_por_ce = busca_itens_navii(session, numeros)
    nomes = busca_nomes_empresas(
        session,
        [v for ciclo, ce, _, _ in resultados for v in (ce.embarcador, ce.consignatario)],
    )
    for ciclo, ce, embarcacao, manifesto in resultados:
        print(monta_card_navii(
            ciclo, ce, embarcacao, itens_por_ce.get(ce.numeroCEmercante), manifesto,
            nomes.get(ce.embarcador), nomes.get(ce.consignatario),
        ))
