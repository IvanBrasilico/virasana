# coding: utf-8
"""
Por que o card de um contêiner não aparece no NAVI-I? Diagnóstico de UM contêiner, só leitura (21/10/2026).

Atenção ao período: o extrato do CE busca SEM limite de data; a tela, com o filtro de contêiner, continua presa ao
período e à janela (o sugerido na janela efetiva é de 15 a 5 dias atrás). O script refaz o caminho da tela com o
código ATUAL, etapa por etapa:

  1. Mercante: os itens (CE + contêiner) com o número do contêiner — o card nasce do item — e os contêineres de cada
     CE informado (--ce), como estão gravados (repr: espaço e hífen aparecem);
  2. o CE de cada item: tráfego (exportação nunca vira card), tipo de BL, master e filhotes, manifesto e escalas
     (porto e data prevista de atracação);
  3. ciclo de vida: os ciclos do contêiner (com qualquer CE), a família gravada pelo ciclo e como cada item liga a
     eles — pelo CE do ciclo ou pela família gravada (o que a BUSCA usa) e pela família do Mercante (o que o CARD
     usa) — e as pesagens dos ciclos;
  4. API Recintos: desembarques/embarques e inspeções do contêiner (devia haver ciclo?);
  5. as buscas da tela (`monta_query_navii`) em vários cenários — período sugerido com e sem o porto padrão, o
     período informado (--inicio/--fim), um período largo, a janela prevista, pelo CE (como o extrato) e os filtros
     informados (--filtros) — e, quando o item fica de fora, o MOTIVO (as condições da janela, uma a uma, e cada
     filtro sozinho);
  6. o card como a API o monta (`monta_cards_navii`) e serializa, gravado em JSON ao lado do log (para reproduzir
     a tela). Uma falha ao montar ou serializar aparece com o traceback.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    D=virasana/navii/diagnostico_card.py
    virasana-venv/bin/python $D --conteiner HMMU5471507 --ce 152605332273790 --saida card_hmmu.log
    virasana-venv/bin/python $D --conteiner HMMU5471507 --inicio 2026-10-01 --fim 2026-10-21 \\
        --filtros '{"porto_escala": "BRSSZ", "so_com_desembarque": true}'
"""
import argparse
import json
import sys
import traceback
from datetime import date, datetime, timedelta
from typing import Dict, List, NamedTuple, Optional, Set, Tuple

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, undefer_group

from bhadrasana.models.apirecintos import EmbarqueDesembarque, InspecaoNaoInvasiva
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import CicloDeVidaCE, CicloDeVidaImportacao
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Escala, EscalaManifesto, Item
from virasana.navii import pesagem_navii
from virasana.navii.busca_navii import (
    JANELA_EFETIVA_MAX_DIAS, JANELA_EFETIVA_MIN_DIAS, PORTO_ESCALA_PADRAO, TIPO_BL_UNICO, TIPO_TRAFEGO_EXPORTACAO,
    _existe_ciclo, _manifestos_com_atracacao_prevista, _periodo_navii, _tem_desembarque_localizado,
    busca_ces_navii, busca_ciclos_navii, busca_familias_navii, monta_cards_navii, monta_query_navii,
    normaliza_filtros,
)
from virasana.navii.mercante_por_id import limita_tempo

DIAS_LARGO = 365        # período largo: um ano para trás...
DIAS_PREVISAO = 30      # ... e 30 dias para a frente (atracação prevista)
DIAS_API = 120          # eventos da API Recintos: dias para trás
LIMITE_EVENTOS = 40
LIMITE_CARDS = 50
LIMITE_SEGUNDOS = 120   # max_statement_time de cada consulta (MariaDB)
# campos que definem o universo do cenário; os outros são os filtros testados um a um quando o item fica de fora
CAMPOS_DA_JANELA = ('conteiner', 'ce', 'data_inicio', 'data_fim', 'porto_escala')


class Cenario(NamedTuple):
    descricao: str
    janela: str
    filtros: dict


class Saida:
    """Escreve na tela e no arquivo."""

    def __init__(self, arquivo=None):
        self.arquivo = arquivo
        self.linhas: List[str] = []

    def __call__(self, texto: str = ''):
        self.linhas.append(texto)
        print(texto)
        if self.arquivo:
            self.arquivo.write(texto + '\n')

    def titulo(self, texto: str):
        self()
        self(texto)
        self('=' * len(texto))


def _de_ate(periodo: dict) -> str:
    return f'{periodo["data_inicio"]} a {periodo["data_fim"]}'


def periodo_sugerido(hoje: date) -> dict:
    """O período que a tela sugere na janela efetiva (`busca_navii._janela_efetiva`), relativo a `hoje`."""
    return {'data_inicio': (hoje - timedelta(days=JANELA_EFETIVA_MAX_DIAS)).isoformat(),
            'data_fim': (hoje - timedelta(days=JANELA_EFETIVA_MIN_DIAS)).isoformat()}


def periodo_largo(hoje: date) -> dict:
    return {'data_inicio': (hoje - timedelta(days=DIAS_LARGO)).isoformat(),
            'data_fim': (hoje + timedelta(days=DIAS_PREVISAO)).isoformat()}


def cenarios(conteiner: str, ces: List[str], hoje: date, periodo: Optional[dict] = None,
             extras: Optional[dict] = None, janela_extras: str = 'efetiva') -> List[Cenario]:
    """As buscas da tela que interessam ao contêiner: do jeito que a tela abre até sem limite de data."""
    base = {'conteiner': [conteiner]}
    porto = {'porto_escala': [PORTO_ESCALA_PADRAO]}
    sugerido, largo = periodo_sugerido(hoje), periodo_largo(hoje)
    lista = [Cenario(f'tela aberta: período sugerido ({_de_ate(sugerido)}) e porto {PORTO_ESCALA_PADRAO}',
                     'efetiva', {**base, **sugerido, **porto}),
             Cenario('período sugerido, qualquer porto', 'efetiva', {**base, **sugerido})]
    if periodo:
        lista += [Cenario(f'período informado ({_de_ate(periodo)}) e porto {PORTO_ESCALA_PADRAO}', 'efetiva',
                          {**base, **periodo, **porto}),
                  Cenario('período informado, qualquer porto', 'efetiva', {**base, **periodo})]
    lista += [Cenario(f'período largo ({_de_ate(largo)})', 'efetiva', {**base, **largo}),
              Cenario(f'janela PREVISTA, período largo ({_de_ate(largo)})', 'prevista', {**base, **largo})]
    lista += [Cenario(f'pelo CE {ce}, sem data (como o extrato)', 'efetiva', {'ce': [ce]}) for ce in ces]
    if extras is not None:
        lista.append(Cenario(f'filtros informados (--filtros), janela {janela_extras}', janela_extras,
                             {**(periodo or sugerido), **extras, **base}))
    return lista


def ids_da_busca(session, janela: str, filtros: dict, ids: List[int]) -> Set[int]:
    """Quais destes itens a busca da tela (`monta_query_navii`) devolve."""
    q = monta_query_navii(session, janela, filtros)
    return {id_ for id_, in q.with_entities(Item.ID).filter(Item.ID.in_(ids))}


def _vale(session, item_id: int, *condicoes) -> bool:
    """O item, junto com o seu CE (como na busca), atende às condições."""
    return (session.query(Item.ID).join(Conhecimento, Conhecimento.numeroCEmercante == Item.numeroCEmercante)
            .filter(Item.ID == item_id, *condicoes).first()) is not None


def _prevista(inicio: str, fim: str, porto):
    return Conhecimento.manifestoCE.in_(_manifestos_com_atracacao_prevista(inicio, fim, porto))


def _motivo_prevista(session, item_id: int, inicio: str, fim: str, porto) -> Tuple[bool, str]:
    if _vale(session, item_id, _prevista(inicio, fim, porto)):
        return True, 'entra pela atracação PREVISTA no período'
    if porto and _vale(session, item_id, _prevista(inicio, fim, None)):
        return False, (f'há escala com atracação prevista no período, mas em outro porto (filtro "Porto de '
                       f'atracação" = {", ".join(porto)})')
    return False, f'nenhuma escala do manifesto com atracação prevista entre {inicio} e {fim}'


def _motivo_efetiva(session, item_id: int, inicio: str, fim: str, porto,
                    so_com_desembarque: bool) -> Tuple[bool, str]:
    inicio_dt = datetime.strptime(inicio, '%Y-%m-%d')
    fim_dt = datetime.strptime(fim, '%Y-%m-%d') + timedelta(days=1, seconds=-1)
    if _vale(session, item_id, _existe_ciclo(CicloDeVidaImportacao.data_desembarque.between(inicio_dt, fim_dt))):
        return True, 'entra pelo DESEMBARQUE no período'
    if _vale(session, item_id, _tem_desembarque_localizado()):
        return False, (f'tem desembarque localizado pelo ciclo de vida, mas FORA do período {inicio} a {fim} — com '
                       'desembarque localizado, a data prevista de atracação não conta (datas na seção 3)')
    if so_com_desembarque:
        return False, 'sem desembarque localizado, e o filtro "só com desembarque" tira'
    aceita, motivo = _motivo_prevista(session, item_id, inicio, fim, porto)
    return aceita, 'sem desembarque localizado; ' + motivo


def motivo_da_janela(session, item_id: int, cenario: Cenario) -> Tuple[bool, str]:
    """(a JANELA do cenário aceita o item?, por quê) — as condições de `_filtra_janela`, uma a uma."""
    filtros = normaliza_filtros(cenario.filtros)
    inicio, fim = _periodo_navii(cenario.janela, filtros)
    if not _vale(session, item_id):
        return False, 'o CE do item não está no Mercante (conhecimentosresumo): sem o CE, nunca há card'
    if _vale(session, item_id, Conhecimento.tipoTrafego == TIPO_TRAFEGO_EXPORTACAO):
        return False, 'CE de EXPORTAÇÃO (tipoTrafego 7): nunca vira card'
    if not (inicio and fim):
        return True, 'sem limite de data: a janela aceita'
    porto = filtros.get('porto_escala')
    if cenario.janela == 'prevista':
        return _motivo_prevista(session, item_id, inicio, fim, porto)
    return _motivo_efetiva(session, item_id, inicio, fim, porto, bool(filtros.get('so_com_desembarque')))


def filtros_que_tiram(session, item_id: int, cenario: Cenario) -> List[str]:
    """Os filtros do cenário (fora os da janela) que, SOZINHOS com a janela, tiram o item."""
    fixos = {campo: valor for campo, valor in cenario.filtros.items() if campo in CAMPOS_DA_JANELA}
    return [campo for campo, valor in cenario.filtros.items()
            if campo not in CAMPOS_DA_JANELA
            and item_id not in ids_da_busca(session, cenario.janela, {**fixos, campo: valor}, [item_id])]


def _quem_tira(session, item_id: int, cenario: Cenario) -> str:
    """A janela aceita, mas a busca não devolve: os filtros que tiram (ou a combinação deles)."""
    tiram = filtros_que_tiram(session, item_id, cenario)
    if tiram:
        return '; mas o(s) filtro(s) ' + ', '.join(tiram) + ' tira(m)'
    return '; a busca não o devolve por outra combinação dos filtros'


def avalia_cenario(session, cenario: Cenario, itens: List[Item]) -> Dict[int, tuple]:
    """{item: (entra, motivo)} para os itens do contêiner que o cenário pode trazer (no cenário do CE, os desse
    CE)."""
    if 'ce' in cenario.filtros:
        itens = [item for item in itens if item.numeroCEmercante in cenario.filtros['ce']]
    if not itens:
        return {}
    try:
        achados = ids_da_busca(session, cenario.janela, cenario.filtros, [item.ID for item in itens])
    except ValueError as erro:   # ex.: filtro de pesagem sem a tabela
        return {item.ID: (False, f'a busca responde com erro: {erro}') for item in itens}
    resultado = {}
    for item in itens:
        aceita, motivo = motivo_da_janela(session, item.ID, cenario)
        if aceita and item.ID not in achados:
            motivo += _quem_tira(session, item.ID, cenario)
        resultado[item.ID] = (item.ID in achados, motivo)
    return resultado


class DiagnosticoCard:

    def __init__(self, session, saida: Saida, hoje: Optional[date] = None):
        self.session = session
        self.escreve = saida
        self.hoje = hoje or date.today()

    # 1. Mercante
    def itens(self, conteiner: str, ces: List[str]) -> List[Item]:
        self.escreve.titulo(f'1. Mercante: itens do contêiner {conteiner}')
        itens = self.session.query(Item).filter(Item.codigoConteiner == conteiner).order_by(Item.ID).all()
        if not itens:
            self.escreve('  NENHUM item com este número exato: sem item no Mercante, não há card (o card nasce do '
                         'item CE + contêiner).')
        for item in itens:
            self._escreve_item(item)
        for ce in ces:
            self.escreve(f'  Contêineres declarados no CE {ce}:')
            do_ce = self.session.query(Item).filter(Item.numeroCEmercante == ce).order_by(Item.ID).all()
            for item in do_ce or []:
                self._escreve_item(item, '    ')
            if not do_ce:
                self.escreve('    nenhum item (CE sem contêiner no Mercante)')
        return itens

    def _escreve_item(self, item: Item, recuo: str = '  '):
        self.escreve(f'{recuo}item {item.ID}: CE {item.numeroCEmercante} contêiner {item.codigoConteiner!r} '
                     f'iso {item.isoCode} peso {item.pesoBruto} tipo {item.tipoItemCarga} '
                     f'criado {item.create_date} alterado {item.last_modified}')

    # 2. CEs
    def ces(self, numeros: List[str]) -> Dict[str, Optional[Conhecimento]]:
        self.escreve.titulo('2. CEs: tráfego, tipo de BL, família e escalas')
        conhecimentos = {}
        for numero in numeros:
            conhecimento = self.session.query(Conhecimento).filter(
                Conhecimento.numeroCEmercante == numero).one_or_none()
            conhecimentos[numero] = conhecimento
            if conhecimento is None:
                self.escreve(f'  CE {numero}: NÃO está em conhecimentosresumo — o item não vira card')
                continue
            self._escreve_ce(conhecimento)
        return conhecimentos

    def _escreve_ce(self, ce: Conhecimento):
        familia = busca_familias_navii(self.session, [ce]).get(ce.numeroCEmercante, {})
        exportacao = ' (EXPORTAÇÃO: nunca vira card)' if ce.tipoTrafego == TIPO_TRAFEGO_EXPORTACAO else ''
        self.escreve(f'  CE {ce.numeroCEmercante}: tráfego {ce.tipoTrafego}{exportacao} tipo de BL '
                     f'{ce.tipoBLConhecimento} master {familia.get("master")} filhotes {familia.get("filhotes")} '
                     f'manifesto {ce.manifestoCE} origem {ce.portoOrigemCarga} destino {ce.portoDestFinal} '
                     f'emissão {ce.dataEmissao}')
        escalas = (self.session.query(EscalaManifesto.escala, Escala.portoDaEscala, Escala.dataPrevistaAtracacao,
                                      Escala.dataEfetivaPrimeiraAtracacao, Escala.embarcacao)
                   .join(Escala, Escala.numeroDaEscala == EscalaManifesto.escala)
                   .filter(EscalaManifesto.manifesto == ce.manifestoCE).all())
        for escala, porto, prevista, efetiva, embarcacao in escalas:
            self.escreve(f'    escala {escala}: porto {porto} atracação prevista {prevista} efetiva {efetiva} '
                         f'embarcação {embarcacao}')
        if not escalas:
            self.escreve('    nenhuma escala do manifesto (sem desembarque, o card não entra pela previsão)')

    # 3. Ciclo de vida
    def ciclos(self, conteiner: str, itens: List[Item], conhecimentos: Dict[str, Optional[Conhecimento]]):
        self.escreve.titulo(f'3. Ciclo de vida do contêiner {conteiner} (todos os CEs)')
        ciclos = (self.session.query(CicloDeVidaImportacao).options(undefer_group('v2'))
                  .filter(CicloDeVidaImportacao.numeroConteiner == conteiner)
                  .order_by(CicloDeVidaImportacao.ID).all())
        familias = self._familias_gravadas([c.ID for c in ciclos])
        if not ciclos:
            self.escreve('  NENHUM ciclo: sem desembarque localizado; na janela efetiva o card só entra pela '
                         'atracação prevista (ver a seção 4: há desembarque na API?)')
        for ciclo in ciclos:
            self._escreve_ciclo(ciclo, familias.get(ciclo.ID, []))
        self._pesagens([c.ID for c in ciclos])
        for item in itens:
            self._ligacao(item, ciclos, familias, conhecimentos.get(item.numeroCEmercante))

    def _familias_gravadas(self, ids: List[int]) -> Dict[int, list]:
        familias: Dict[int, list] = {}
        if ids:
            for linha in (self.session.query(CicloDeVidaCE).filter(CicloDeVidaCE.ciclo_de_vida_id.in_(ids))
                          .order_by(CicloDeVidaCE.ID)):
                familias.setdefault(linha.ciclo_de_vida_id, []).append(linha)
        return familias

    def _escreve_ciclo(self, ciclo: CicloDeVidaImportacao, familia: list):
        imagens = sum(1 for e in ciclo.escaneamentos if e.imagem_id)
        self.escreve(f'  ciclo {ciclo.ID} ({ciclo.versao_heuristica}): CE {ciclo.numeroCEmercante} '
                     f'desembarque {ciclo.data_desembarque} em {ciclo.codigoRecintoDesembarque} '
                     f'saída {ciclo.data_saida} armazenagem {ciclo.codigoRecintoArmazenagem} '
                     f'entrada no seco {ciclo.data_entrada_seco} embarque {ciclo.data_embarque}')
        self.escreve(f'    situação {ciclo.situacao} fluxo {ciclo.fluxo} confiança do CE {ciclo.confianca_ce} '
                     f'motivo {ciclo.motivo!r}; processado em {ciclo.processado_em}; escaneamentos '
                     f'{len(ciclo.escaneamentos)} ({imagens} com imagem)')
        if familia:
            self.escreve('    família gravada: ' + ', '.join(
                f'{f.numeroCEmercante} ({f.papel}{", principal" if f.principal else ""})' for f in familia))

    def _pesagens(self, ids: List[int]):
        if not ids or not pesagem_navii.tem_tabela(self.session):
            return
        for ciclo_id, dados in sorted(pesagem_navii.pesagens_dos_ciclos(self.session, ids).items()):
            principal = dados['principal'] or {}
            self.escreve(f'  pesagens do ciclo {ciclo_id}: {len(dados["pesagens"])}; principal: '
                         f'{principal.get("operacao")} {principal.get("diagnostico")} '
                         f'{principal.get("diferencaPct")}%')

    def _ligacao(self, item: Item, ciclos: list, familias: Dict[int, list], ce: Optional[Conhecimento]):
        direto = [c.ID for c in ciclos if c.numeroCEmercante == item.numeroCEmercante]
        pela_familia = [c.ID for c in ciclos if c.ID not in direto
                        and any(f.numeroCEmercante == item.numeroCEmercante for f in familias.get(c.ID, []))]
        self.escreve(f'  item {item.ID} (CE {item.numeroCEmercante}): ciclos pelo CE do ciclo {direto or "-"}; '
                     f'pela família gravada {pela_familia or "-"}')
        if ce is None:
            return
        if pela_familia and not direto and ce.tipoBLConhecimento == TIPO_BL_UNICO:
            self.escreve('    ATENÇÃO: o CE é BL único (tipo 10) e só liga pela família gravada — a BUSCA não '
                         'procura pela família de BL único (TIPO_BL_UNICO): esse desembarque não conta na janela')
        familia = busca_familias_navii(self.session, [ce]).get(ce.numeroCEmercante, {})
        ciclo = busca_ciclos_navii(self.session, [item], {ce.numeroCEmercante: familia.get('familia', set())}
                                   ).get((item.numeroCEmercante, item.codigoConteiner))
        self.escreve(f'    o CARD mostra o ciclo {ciclo.ID if ciclo else "nenhum"} (família do Mercante)')

    # 4. API Recintos
    def eventos_api(self, conteiner: str):
        desde = datetime.combine(self.hoje - timedelta(days=DIAS_API), datetime.min.time())
        self.escreve.titulo(f'4. API Recintos: desembarques/embarques e inspeções desde {desde:%Y-%m-%d}')
        for modelo, nome, extra in ((EmbarqueDesembarque, 'D/E', EmbarqueDesembarque.embarqueDesembarque),
                                    (InspecaoNaoInvasiva, 'inspeção', InspecaoNaoInvasiva.vazio)):
            linhas = (self.session.query(modelo.id, modelo.dataHoraOcorrencia, modelo.codigoRecinto,
                                         modelo.tipoOperacao, extra, modelo.numeroConhecimento)
                      .filter(modelo.numeroConteiner == conteiner, modelo.dataHoraOcorrencia >= desde)
                      .order_by(modelo.dataHoraOcorrencia).limit(LIMITE_EVENTOS).all())
            for id_, data, recinto, operacao, valor, ce in linhas:
                rotulo = f'{nome} {valor}' if nome == 'D/E' else f'{nome} vazio={valor}'
                self.escreve(f'  {data} {rotulo} recinto {recinto} CE {ce} (id {id_}, operação {operacao})')
            if not linhas:
                self.escreve(f'  nenhum evento de {nome}')

    # 5. Buscas da tela
    def buscas(self, lista: List[Cenario], itens: List[Item]) -> Dict[str, Dict[int, tuple]]:
        self.escreve.titulo('5. Buscas da tela (código atual), item a item')
        resultados = {}
        for cenario in lista:
            resultado = avalia_cenario(self.session, cenario, itens)
            resultados[cenario.descricao] = resultado
            self.escreve(f'  [{cenario.janela}] {cenario.descricao}')
            self.escreve(f'    filtros: {json.dumps(cenario.filtros, ensure_ascii=False)}')
            for item_id, (entra, motivo) in resultado.items():
                self.escreve(f'    item {item_id}: {"ENTRA" if entra else "FORA"} — {motivo}')
            if not resultado:
                self.escreve('    nenhum item do contêiner neste cenário')
        return resultados

    # 6. Cards
    def cards(self, conteiner: str, ces: List[str], caminho_json: Optional[str]) -> List[dict]:
        self.escreve.titulo('6. O card como a API monta (período largo e pelos CEs)')
        try:
            cards = self._monta_cards(conteiner, ces)
            texto = serializa_como_a_api(cards)
        except Exception:  # noqa: o diagnóstico mostra a falha que a tela teria
            self.escreve('  FALHA ao montar ou serializar o card (a API daria erro):')
            for linha in traceback.format_exc().splitlines():
                self.escreve('    ' + linha)
            return []
        for card in cards:
            pesagem = (card.get('pesagem') or {}).get('principal') or {}
            self.escreve(f'  card item {card["item_id"]}: CE {card["numeroCEmercante"]} ciclo '
                         f'{card["ciclo_de_vida_id"]} sem desembarque {card["semDesembarque"]} imagens '
                         f'{len(card["imagens_ids"])} escaneamentos {len(card["escaneamentos"])} estágio '
                         f'{card.get("estagio")} pesagem {pesagem.get("diagnostico")}')
        if caminho_json:
            with open(caminho_json, 'w', encoding='utf-8') as arquivo:
                arquivo.write(texto)
            self.escreve(f'  cards gravados em {caminho_json} ({len(cards)})')
        return cards

    def _monta_cards(self, conteiner: str, ces: List[str]) -> List[dict]:
        resultados = busca_ces_navii(self.session, 'efetiva',
                                     {'conteiner': [conteiner], **periodo_largo(self.hoje)}, limit=LIMITE_CARDS)
        vistos = {r.item.ID for r in resultados}
        for ce in ces:
            for r in busca_ces_navii(self.session, 'efetiva', {'ce': [ce]}, limit=LIMITE_CARDS):
                if r.item.codigoConteiner == conteiner and r.item.ID not in vistos:
                    resultados.append(r)
                    vistos.add(r.item.ID)
        return monta_cards_navii(self.session, resultados, hoje=self.hoje)


def serializa_como_a_api(cards: List[dict]) -> str:
    """O JSON dos cards como `api_ces` o devolve (`_serializa_card` + o JSON do Flask)."""
    from flask import Flask, json as flask_json
    from virasana.navii.views_navii import _serializa_card
    with Flask(__name__).app_context():
        return flask_json.dumps([_serializa_card(card) for card in cards], ensure_ascii=False, indent=1)


def resumo(saida: Saida, resultados: Dict[str, Dict[int, tuple]]):
    saida.titulo('Resumo')
    for descricao, por_item in resultados.items():
        situacao = ', '.join(f'item {item_id} {"ENTRA" if entra else "FORA"}'
                             for item_id, (entra, _) in por_item.items()) or 'sem item'
        saida(f'  {descricao}: {situacao}')


def diagnostica(session, conteiner: str, ces: List[str], saida: Saida, hoje: Optional[date] = None,
                periodo: Optional[dict] = None, extras: Optional[dict] = None, janela_extras: str = 'efetiva',
                caminho_json: Optional[str] = None) -> dict:
    """Roda as seis etapas; devolve os itens, os resultados das buscas e os cards (para os testes)."""
    diagnostico = DiagnosticoCard(session, saida, hoje)
    itens = diagnostico.itens(conteiner, ces)
    numeros = sorted({item.numeroCEmercante for item in itens} | set(ces))
    conhecimentos = diagnostico.ces(numeros)
    diagnostico.ciclos(conteiner, itens, conhecimentos)
    diagnostico.eventos_api(conteiner)
    resultados = diagnostico.buscas(cenarios(conteiner, ces, diagnostico.hoje, periodo, extras, janela_extras),
                                    itens)
    cards = diagnostico.cards(conteiner, ces, caminho_json)
    resumo(saida, resultados)
    return {'itens': [item.ID for item in itens], 'buscas': resultados, 'cards': cards}


def _numero(texto: str, digitos: bool = False) -> str:
    texto = texto.upper()
    return ''.join(c for c in texto if c.isdigit()) if digitos else ''.join(c for c in texto if c.isalnum())


def _argumentos(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--conteiner', required=True, help='número do contêiner')
    parser.add_argument('--ce', nargs='*', default=[], help='CE(s) Mercante do contêiner (opcional)')
    parser.add_argument('--inicio', help='início do período usado na tela (AAAA-MM-DD)')
    parser.add_argument('--fim', help='fim do período usado na tela (AAAA-MM-DD)')
    parser.add_argument('--filtros', help='filtros da tela em JSON (ex.: os de um filtro salvo); o contêiner e o '
                                          'período entram sozinhos')
    parser.add_argument('--janela', default='efetiva', choices=('efetiva', 'prevista'),
                        help='janela dos --filtros (padrão: efetiva)')
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    parser.add_argument('--saida', help='arquivo do log (padrão: diagnostico_card_<contêiner>.log); os cards vão '
                                        'para o mesmo nome com .json')
    args = parser.parse_args(argv)
    if bool(args.inicio) != bool(args.fim):
        parser.error('--inicio e --fim precisam vir juntos')
    return args


def main(argv=None):  # pragma: no cover - depende do banco real
    from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import uri_do_banco
    args = _argumentos(argv)
    conteiner = _numero(args.conteiner)
    ces = [_numero(ce, digitos=True) for ce in args.ce]
    periodo = {'data_inicio': args.inicio, 'data_fim': args.fim} if args.inicio else None
    extras = json.loads(args.filtros) if args.filtros else None
    caminho = args.saida or f'diagnostico_card_{conteiner}.log'
    engine = create_engine(uri_do_banco(args))
    limita_tempo(engine, LIMITE_SEGUNDOS)
    session = sessionmaker(bind=engine)()
    with open(caminho, 'w', encoding='utf-8') as arquivo:
        saida = Saida(arquivo)
        saida(f'Diagnóstico do card do contêiner {conteiner} — {datetime.now():%Y-%m-%d %H:%M:%S}')
        saida(f'Banco: {engine.url.render_as_string(hide_password=True)}')
        diagnostica(session, conteiner, ces, saida, periodo=periodo, extras=extras, janela_extras=args.janela,
                    caminho_json=caminho.rsplit('.', 1)[0] + '.json')
    print(f'Diagnóstico gravado em {caminho}')


if __name__ == '__main__':  # pragma: no cover
    main()
