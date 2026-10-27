# coding: utf-8
"""
Imagem de escaneamento do card pelo CONTÊINER e pela DATA, sem ciclo de vida — o "fallback" (plano aprovado em
20/10/2026: `analytic_scripts/ciclo_de_vida/CICLO_DE_VIDA.md`, "Fora de Santos e pesagens", D2).

O ciclo de vida só cobre os operadores de Santos (escopo do v2); fora dele — ou com o ciclo sem imagem —, o card
ficava sem imagem mesmo com a imagem no Mongo (ex.: GSLU2053602, CE 132605338073101, escaneado no Rio). Regra do
usuário (01/10/2026): contêiner + data NUNCA bastam, é preciso validar que é a MESMA operação. Por isso:

  - só para os cards SEM imagem vinda do ciclo;
  - janela da operação do CE: da previsão de atracação −2 dias a +30 dias (sem previsão, a data do desembarque do
    ciclo; sem nenhuma das duas, nada), CORTADA no 1º evento do contêiner que abre outra operação: depois da 1ª SAÍDA
    do contêiner (gate), um embarque, um novo desembarque ou uma entrada com o contêiner vazio. Antes da saída, o
    embarque não corta (passagem/baldeação: o contêiner ainda está na mesma viagem);
  - prefere as INSPEÇÕES da API do contêiner na janela, CHEIAS (a de vazio informado sai), e a imagem de cada uma
    pela defasagem do recinto (`imagens_ciclo.escolhe_imagem`) — confiança média;
  - sem inspeção com imagem, as imagens do Mongo do contêiner na janela, só por `numeroinformado` +
    `dataescaneamento` (regra do usuário) — confiança baixa; a imagem perto de uma inspeção VAZIA sai;
  - o card diz a origem (`imagemFallback`). O filtro "Somente com imagem" continua só pelo ciclo.

Custo: por página, uma consulta a cada tabela de eventos (pelo índice do contêiner) e uma ao Mongo, só para os cards
sem imagem.
"""
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, NamedTuple, Optional, Tuple

from virasana.navii import busca_navii  # noqa: F401  (ajusta o sys.path antes dos modelos do bhadrasana)
from bhadrasana.models.apirecintos import AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva  # noqa: I100
from sqlalchemy import select

from virasana.analytic_scripts.ciclo_de_vida.imagens_ciclo import DEFASAGEM_HORAS, escolhe_imagem

DIAS_ANTES, DIAS_DEPOIS = 2, 30
MAXIMO_IMAGENS = 8
FOLGA_VAZIO = timedelta(hours=1)          # imagem tão perto de uma inspeção vazia (com a defasagem) é dela
FOLGA_MONGO = timedelta(hours=max(DEFASAGEM_HORAS.values()) + 1)   # o Mongo está à frente da API (fuso)
SAIDA, ENTRADA, EMBARQUE, DESEMBARQUE = 'saída', 'entrada', 'embarque', 'desembarque'
POR_INSPECAO, SO_IMAGEM = 'inspeção', 'imagem'
MEDIA, BAIXA = 'média', 'baixa'


class EventoDoConteiner(NamedTuple):
    data: datetime
    tipo: str
    vazio: Optional[bool] = None


class Inspecao(NamedTuple):
    data: datetime
    recinto: str
    vazio: Optional[bool]


def _dia(valor) -> Optional[date]:
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    try:
        return datetime.strptime(str(valor or '')[:10], '%Y-%m-%d').date()
    except ValueError:
        return None


def janela(referencia) -> Optional[Tuple[datetime, datetime]]:
    """(início, fim) da operação a partir da data de referência (previsão de atracação ou desembarque)."""
    dia = _dia(referencia)
    if dia is None:
        return None
    inicio = datetime(dia.year, dia.month, dia.day) - timedelta(days=DIAS_ANTES)
    return inicio, datetime(dia.year, dia.month, dia.day) + timedelta(days=DIAS_DEPOIS + 1, seconds=-1)


def corta(inicio: datetime, fim: datetime, eventos: Iterable[EventoDoConteiner]) -> datetime:
    """O fim da janela, cortado no 1º evento que abre outra operação depois da 1ª saída (gate) do contêiner."""
    saiu = False
    for evento in sorted(eventos, key=lambda e: e.data):
        if evento.data < inicio:
            continue
        if evento.data > fim:
            break
        if saiu and (evento.tipo in (EMBARQUE, DESEMBARQUE) or (evento.tipo == ENTRADA and evento.vazio)):
            return evento.data
        if evento.tipo == SAIDA:
            saiu = True
    return fim


def _perto_de_vazia(data_imagem: datetime, vazias: List[Inspecao]) -> bool:
    for inspecao in vazias:
        for horas in (DEFASAGEM_HORAS.get(inspecao.recinto, 3), 0):
            if abs(data_imagem - (inspecao.data + timedelta(hours=horas))) <= FOLGA_VAZIO:
                return True
    return False


def escolhe(inicio: datetime, fim: datetime, inspecoes: Iterable[Inspecao], imagens: List[Tuple[datetime, str]],
            nomes: Optional[Dict[str, str]] = None) -> Optional[dict]:
    """As imagens do contêiner na janela, com a origem e a confiança; None se não houver. `imagens`:
    [(dataescaneamento, id)] do Mongo."""
    inspecoes = [i for i in inspecoes if inicio <= i.data <= fim]
    ids, primeira = [], None
    for inspecao in sorted((i for i in inspecoes if not i.vazio), key=lambda i: i.data):
        imagem = escolhe_imagem(imagens, inspecao.data, inspecao.recinto)
        if imagem and imagem not in ids:
            ids.append(imagem)
            primeira = primeira or inspecao
    if ids:
        nome = (nomes or {}).get(primeira.recinto)
        return {'imagens': ids[:MAXIMO_IMAGENS], 'origem': POR_INSPECAO, 'confianca': MEDIA,
                'detalhe': f'inspeção no {primeira.recinto}{" " + nome if nome else ""} em '
                           f'{primeira.data:%d/%m/%Y %H:%M}'}
    vazias = [i for i in inspecoes if i.vazio]
    soltas = sorted((d, i) for d, i in imagens if inicio <= d <= fim + FOLGA_MONGO and not _perto_de_vazia(d, vazias))
    if soltas:
        return {'imagens': [i for _, i in soltas][:MAXIMO_IMAGENS], 'origem': SO_IMAGEM, 'confianca': BAIXA,
                'detalhe': f'imagem de {soltas[0][0]:%d/%m/%Y %H:%M}, sem inspeção transmitida'}
    return None


# ---------------------------------------------------------------------- banco e Mongo
def _texto(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def eventos_dos_conteineres(session, conteineres: List[str], de: datetime, ate: datetime
                            ) -> Tuple[Dict[str, List[EventoDoConteiner]], Dict[str, List[Inspecao]]]:
    """({contêiner: eventos de fronteira}, {contêiner: inspeções}) no intervalo — uma consulta por tabela."""
    eventos, inspecoes = {}, {}
    for conteiner, data, tipo in session.execute(select(
            EmbarqueDesembarque.numeroConteiner, EmbarqueDesembarque.dataHoraOcorrencia,
            EmbarqueDesembarque.embarqueDesembarque).where(
            EmbarqueDesembarque.numeroConteiner.in_(conteineres), EmbarqueDesembarque.dataHoraOcorrencia >= de,
            EmbarqueDesembarque.dataHoraOcorrencia <= ate)):
        eventos.setdefault(_texto(conteiner), []).append(
            EventoDoConteiner(data, EMBARQUE if _texto(tipo) == 'E' else DESEMBARQUE))
    for conteiner, data, direcao, vazio in session.execute(select(
            AcessoVeiculo.numeroConteiner, AcessoVeiculo.dataHoraOcorrencia, AcessoVeiculo.direcao,
            AcessoVeiculo.vazioConteiner).where(
            AcessoVeiculo.numeroConteiner.in_(conteineres), AcessoVeiculo.operacao == 'C',
            AcessoVeiculo.dataHoraOcorrencia >= de, AcessoVeiculo.dataHoraOcorrencia <= ate)):
        eventos.setdefault(_texto(conteiner), []).append(
            EventoDoConteiner(data, SAIDA if _texto(direcao) == 'S' else ENTRADA, vazio))
    for conteiner, data, recinto, vazio in session.execute(select(
            InspecaoNaoInvasiva.numeroConteiner, InspecaoNaoInvasiva.dataHoraOcorrencia,
            InspecaoNaoInvasiva.codigoRecinto, InspecaoNaoInvasiva.vazio).where(
            InspecaoNaoInvasiva.numeroConteiner.in_(conteineres), InspecaoNaoInvasiva.dataHoraOcorrencia >= de,
            InspecaoNaoInvasiva.dataHoraOcorrencia <= ate)):
        inspecoes.setdefault(_texto(conteiner), []).append(Inspecao(data, _texto(recinto), vazio))
    return eventos, inspecoes


def imagens_dos_conteineres(mongo_db, conteineres: List[str], de: datetime, ate: datetime
                            ) -> Dict[str, List[Tuple[datetime, str]]]:
    """{contêiner: [(dataescaneamento, id)]} — uma consulta, só por `numeroinformado` + `dataescaneamento`."""
    imagens = {}
    for documento in mongo_db['fs.files'].find(
            {'metadata.numeroinformado': {'$in': conteineres}, 'metadata.dataescaneamento': {'$gte': de, '$lte': ate}},
            {'metadata.numeroinformado': 1, 'metadata.dataescaneamento': 1}):
        metadata = documento.get('metadata') or {}
        imagens.setdefault(_texto(metadata.get('numeroinformado')), []).append(
            (metadata['dataescaneamento'].replace(tzinfo=None), str(documento['_id'])))
    return imagens


def _referencia(card: dict):
    """Previsão de atracação do card; sem ela, a data do desembarque do ciclo."""
    desembarque = ((card.get('linhaDoTempo') or {}).get('desembarque') or {}).get('data')
    return card.get('dataPrevistaAtracacao') or desembarque


def completa_cards(session, mongo_db, cards: List[dict]) -> int:
    """Põe a imagem do fallback nos cards sem imagem do ciclo (`imagens_ids` e `imagemFallback`); devolve quantos
    ganharam imagem."""
    pedidos = []
    for card in cards:
        conteiner = _texto(card.get('numeroConteiner'))
        intervalo = janela(_referencia(card))
        if not card.get('imagens_ids') and conteiner and intervalo:
            pedidos.append((card, conteiner) + intervalo)
    if not pedidos or mongo_db is None:
        return 0
    conteineres = sorted({conteiner for _, conteiner, _, _ in pedidos})
    de, ate = min(p[2] for p in pedidos), max(p[3] for p in pedidos)
    eventos, inspecoes = eventos_dos_conteineres(session, conteineres, de, ate)
    imagens = imagens_dos_conteineres(mongo_db, conteineres, de, ate + FOLGA_MONGO)
    nomes = busca_navii.busca_nomes_recintos(session, {i.recinto for lista in inspecoes.values() for i in lista})
    ganharam = 0
    for card, conteiner, inicio, fim in pedidos:
        fim = corta(inicio, fim, eventos.get(conteiner, []))
        achado = escolhe(inicio, fim, inspecoes.get(conteiner, []), imagens.get(conteiner, []), nomes)
        if achado:
            card['imagens_ids'] = achado.pop('imagens')
            card['imagemFallback'] = achado
            ganharam += 1
    return ganharam
