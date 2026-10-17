# coding: utf-8
"""
FICHAS EM ACOMPANHAMENTO — a aba do NAVI-I que acompanha o TRATAMENTO do risco no Fichas (fase 2 do plano de
17/10/2026; `CONTEXTO.md`, "Tratamento do risco: Fichas pelo NAVI-I"). Somente leitura.

Quais Fichas: de Repressão na importação (`tipooperacao = 1`) ainda NÃO encerradas (fase < 3) e SEM verificação física
informada (evento especial 2, não excluído) — o fim do acompanhamento —, dos SETORES do usuário (o dele e os
subordinados, como o "Minhas Fichas" do Fichas) e as em que ele aparece (abriu, responsável, auditor). Opções: só as
minhas; só as abertas pelo NAVI-I (flag NAVI-I); incluir as com verificação física informada.

Por Ficha: fase, status (último evento), pessoas, dias desde a abertura, bloqueio de CE e verificação agendada/informada
(informação — medição de 17/10/2026: auditor e bloqueio não servem de alerta) e os últimos eventos; por contêiner (os
das RVFs; sem RVF com contêiner, os do CE no Mercante), o ciclo de vida: estágio, onde está, previsão de atracação e o
último escaneamento. ALERTA (o único): contêiner que já SAIU (entregue ou reembarcado) sem verificação física informada.
Os CPFs nos motivos dos eventos viram o nome do usuário (ou saem mascarados).

As tabelas do Fichas são declaradas aqui só com as colunas lidas (como em `fichas_ovr`): os models do bhadrasana
carregariam o registry inteiro do Fichas; `test_navii_acompanhamento` confere as colunas contra os models.
"""
import re
import unicodedata
from collections import defaultdict
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import (BigInteger, Boolean, CHAR, Column, DateTime, Integer, MetaData, Table, VARCHAR, or_, select)

from virasana.analytic_scripts.ciclo_de_vida import estagios_ciclo
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Item
from virasana.navii import fichas_ovr
from virasana.navii.busca_navii import (
    _previsao_atracacao, busca_ciclos_navii, busca_familias_navii, busca_manifestos_navii, busca_nomes_recintos,
)

TIPO_REPRESSAO_IMPORTACAO = 1
FASE_ENCERRADA = 3
VERIFICACAO_INFORMADA = 2          # EventoEspecial.RVF
FLAG_NAVII = 'NAVI-I'
LIMITE_CARDS = 200
LIMITE_CONSULTA = 2000             # Fichas lidas antes do filtro da verificação (proteção)
EVENTOS_NO_CARD = 4
CONTEINERES_DO_CE = 20             # sem RVF com contêiner: no máximo isto do CE
SAIU = (estagios_ciclo.ENTREGUE, estagios_ciclo.EMBARCADO)
CPF_NO_TEXTO = re.compile(r'(?<!\d)\d{11}(?!\d)')

_metadata = MetaData()
OVRS = Table('ovr_ovrs', _metadata,
             Column('id', BigInteger, primary_key=True), Column('numero', VARCHAR(10)), Column('ano', VARCHAR(4)),
             Column('tipooperacao', Integer), Column('numeroCEmercante', VARCHAR(15)), Column('fase', Integer),
             Column('tipoevento_id', BigInteger), Column('setor_id', CHAR(15)), Column('user_name', VARCHAR(14)),
             Column('responsavel_cpf', VARCHAR(15)), Column('cpfauditorresponsavel', VARCHAR(15)),
             Column('datahora', DateTime))
EVENTOS = Table('ovr_eventos', _metadata,
                Column('id', BigInteger, primary_key=True), Column('ovr_id', BigInteger),
                Column('tipoevento_id', BigInteger), Column('motivo', VARCHAR(200)), Column('create_date', DateTime),
                Column('excluido', Boolean), Column('meramente_informativo', Boolean))
TIPOS_EVENTO = Table('ovr_tiposevento', _metadata,
                     Column('id', BigInteger, primary_key=True), Column('nome', VARCHAR(50)),
                     Column('eventoespecial', Integer))
RVFS = Table('ovr_verificacoesfisicas', _metadata,
             Column('id', BigInteger, primary_key=True), Column('ovr_id', BigInteger),
             Column('numerolote', VARCHAR(20)))
FLAGS = Table('ovr_flags', _metadata, Column('id', BigInteger, primary_key=True), Column('nome', VARCHAR(100)))
FLAGS_OVR = Table('ovr_flags_ovr', _metadata, Column('rvf_id', BigInteger), Column('flag_id', BigInteger))
USUARIOS = Table('ovr_usuarios', _metadata,
                 Column('cpf', CHAR(15), primary_key=True), Column('nome', CHAR(50)), Column('setor_id', CHAR(15)))
SETORES = Table('ovr_setores', _metadata,
                Column('id', CHAR(15), primary_key=True), Column('nome', CHAR(50)), Column('pai_id', CHAR(15)))


def _texto(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def _sem_acento(texto: Optional[str]) -> str:
    sem = ''.join(c for c in unicodedata.normalize('NFD', texto or '') if unicodedata.category(c) != 'Mn')
    return ' '.join(sem.lower().split())


def normaliza_conteiner(numero) -> str:
    return ''.join(c for c in _texto(numero).upper() if c.isalnum())


def _iso(valor) -> Optional[str]:
    if isinstance(valor, datetime):
        return valor.isoformat(timespec='minutes')
    if valor is None:
        return None
    return str(valor)[:16]


def _data(valor) -> Optional[datetime]:
    if valor is None or isinstance(valor, datetime):
        return valor
    return datetime.fromisoformat(str(valor)[:19])


# ---------------------------------------------------------------- usuário e setores

def descendentes(pais: Dict[str, Optional[str]], raiz: str) -> Set[str]:
    """O setor e todos os subordinados (pelo `pai_id`), sem repetir — um ciclo no cadastro não trava."""
    filhos = defaultdict(list)
    for setor, pai in pais.items():
        if pai:
            filhos[pai].append(setor)
    vistos, pendentes = set(), [raiz]
    while pendentes:
        setor = pendentes.pop()
        if setor in vistos:
            continue
        vistos.add(setor)
        pendentes.extend(filhos.get(setor, ()))
    return vistos


def usuario_e_setores(conexao, cpf: str) -> Tuple[Optional[dict], Dict[str, str], Dict[str, str]]:
    """({cpf, nome, setor}, {setor do usuário: nome}, {todo setor: nome}) no Fichas; (None, {}, {}) sem cadastro.

    Os setores do usuário são o dele e os subordinados; o nome de TODOS serve aos cards das Fichas em que ele aparece
    fora dos seus setores."""
    linha = conexao.execute(select(USUARIOS.c.nome, USUARIOS.c.setor_id).where(USUARIOS.c.cpf == cpf)).first()
    if linha is None:
        return None, {}, {}
    nomes, pais = {}, {}
    for id_, nome, pai in conexao.execute(select(SETORES.c.id, SETORES.c.nome, SETORES.c.pai_id)):
        nomes[_texto(id_)], pais[_texto(id_)] = _texto(nome), _texto(pai) or None
    setor = _texto(linha.setor_id)
    setores = descendentes(pais, setor) if setor else set()
    return ({'cpf': cpf, 'nome': _texto(linha.nome), 'setor': nomes.get(setor, setor)},
            {s: nomes.get(s, s) for s in setores}, nomes)


# ---------------------------------------------------------------- leitura das Fichas

def _fichas(conexao, cpf: str, setores: Set[str], minhas: bool, navii: bool) -> List:
    eu = or_(OVRS.c.user_name == cpf, OVRS.c.responsavel_cpf == cpf, OVRS.c.cpfauditorresponsavel == cpf)
    escopo = eu if minhas or not setores else or_(OVRS.c.setor_id.in_(sorted(setores)), eu)
    consulta = (select(OVRS)
                .where(OVRS.c.tipooperacao == TIPO_REPRESSAO_IMPORTACAO, OVRS.c.fase < FASE_ENCERRADA, escopo)
                .order_by(OVRS.c.id.desc()).limit(LIMITE_CONSULTA))
    if navii:
        com_flag = select(FLAGS_OVR.c.rvf_id).join(FLAGS, FLAGS.c.id == FLAGS_OVR.c.flag_id).where(
            FLAGS.c.nome == FLAG_NAVII)
        consulta = consulta.where(OVRS.c.id.in_(com_flag))
    return conexao.execute(consulta).all()


def _eventos(conexao, ids: List[int]) -> Dict[int, list]:
    """{Ficha: [eventos válidos (não excluídos), do mais antigo ao mais novo]} com o tipo (nome, especial)."""
    por_ficha = defaultdict(list)
    if not ids:
        return por_ficha
    consulta = (select(EVENTOS.c.ovr_id, EVENTOS.c.create_date, EVENTOS.c.motivo, EVENTOS.c.excluido,
                       EVENTOS.c.meramente_informativo, TIPOS_EVENTO.c.nome, TIPOS_EVENTO.c.eventoespecial)
                .select_from(EVENTOS.outerjoin(TIPOS_EVENTO, TIPOS_EVENTO.c.id == EVENTOS.c.tipoevento_id))
                .where(EVENTOS.c.ovr_id.in_(ids)).order_by(EVENTOS.c.create_date, EVENTOS.c.id))
    for linha in conexao.execute(consulta):
        if not linha.excluido:
            por_ficha[linha.ovr_id].append(SimpleNamespace(
                data=_data(linha.create_date), motivo=_texto(linha.motivo),
                informativo=bool(linha.meramente_informativo), tipo=_texto(linha.nome),
                especial=linha.eventoespecial, chave=_sem_acento(linha.nome)))
    return por_ficha


def _rvfs_e_flags(conexao, ids: List[int]) -> Tuple[Dict[int, List[str]], Set[int]]:
    conteineres = defaultdict(list)
    if not ids:
        return conteineres, set()
    for ovr_id, lote in conexao.execute(select(RVFS.c.ovr_id, RVFS.c.numerolote).where(RVFS.c.ovr_id.in_(ids))
                                        .order_by(RVFS.c.id)):
        numero = normaliza_conteiner(lote)
        if numero and numero not in conteineres[ovr_id]:
            conteineres[ovr_id].append(numero)
    navii = {ovr_id for ovr_id, in conexao.execute(
        select(FLAGS_OVR.c.rvf_id).join(FLAGS, FLAGS.c.id == FLAGS_OVR.c.flag_id)
        .where(FLAGS.c.nome == FLAG_NAVII, FLAGS_OVR.c.rvf_id.in_(ids)))}
    return conteineres, navii


def _nomes(conexao, cpfs: Set[str]) -> Dict[str, str]:
    cpfs = sorted(c for c in cpfs if c)
    if not cpfs:
        return {}
    return {_texto(cpf): _texto(nome) for cpf, nome in
            conexao.execute(select(USUARIOS.c.cpf, USUARIOS.c.nome).where(USUARIOS.c.cpf.in_(cpfs)))}


def _mascara_cpf(cpf: str) -> str:
    return f'{cpf[:3]}.***.***-{cpf[-2:]}'


def _pessoa(cpf, nomes: Dict[str, str]) -> Optional[str]:
    cpf = _texto(cpf)
    return (nomes.get(cpf) or _mascara_cpf(cpf)) if cpf else None


def _motivo(texto: str, nomes: Dict[str, str]) -> str:
    """CPF no motivo (ex.: "De: Nenhum Para: <cpf>") vira o nome do usuário, ou sai mascarado."""
    texto = CPF_NO_TEXTO.sub(lambda m: nomes.get(m.group(0)) or _mascara_cpf(m.group(0)), texto or '')
    return texto if len(texto) <= 160 else texto[:159] + '…'


# ---------------------------------------------------------------- ciclo de vida dos contêineres

def _conteineres_dos_ces(session, ces: List[str]) -> Dict[str, List[str]]:
    resultado = defaultdict(list)
    if not ces:
        return resultado
    for ce, conteiner in (session.query(Item.numeroCEmercante, Item.codigoConteiner)
                          .filter(Item.numeroCEmercante.in_(ces)).order_by(Item.ID)):
        numero = normaliza_conteiner(conteiner)
        if numero and numero not in resultado[ce] and len(resultado[ce]) < CONTEINERES_DO_CE:
            resultado[ce].append(numero)
    return resultado


def _onde(ciclo, nomes_recintos: Dict[str, str]) -> Optional[dict]:
    if ciclo is None:
        return None
    codigo = ciclo.codigoRecintoArmazenagem if ciclo.data_entrada_seco else ciclo.codigoRecintoDesembarque
    return {'codigo': codigo, 'nome': nomes_recintos.get(codigo)} if codigo else None


def _ultimo_escaneamento(ciclo) -> Optional[dict]:
    escaneamentos = sorted(ciclo.escaneamentos, key=lambda e: e.data_escaneamento or datetime.min) if ciclo else []
    if not escaneamentos:
        return None
    ultimo = escaneamentos[-1]
    com_imagem = [e.imagem_id for e in escaneamentos if e.imagem_id]
    return {'data': _iso(ultimo.data_escaneamento), 'recinto': ultimo.codigoRecinto,
            'imagem': com_imagem[-1] if com_imagem else None}


def _parametros_navii(numero: str, ce: str, ciclo, previsao: Optional[dict]) -> dict:
    """Link para o card do contêiner no NAVI-I, na janela de ±1 dia do desembarque (efetiva) ou, sem desembarque, da
    previsão de atracação (como o da completude): sem período, a tela usaria o padrão e o contêiner antigo sumiria."""
    parametros = {'conteiner': numero, **({'ce': ce} if ce else {})}
    desembarque = _data(ciclo.data_desembarque) if ciclo is not None else None
    if desembarque is not None:
        tipo, base = 'efetiva', desembarque.date()
    elif previsao:
        tipo, base = 'prevista', date.fromisoformat(previsao['data'])
    else:
        return parametros
    return {**parametros, 'tipo_janela': tipo, 'data_inicio': f'{base - timedelta(days=1):%Y-%m-%d}',
            'data_fim': f'{base + timedelta(days=1):%Y-%m-%d}'}


def _conteiner(numero: str, ce: str, ciclo, previsao: Optional[dict], nomes_recintos: Dict[str, str],
               verificada: bool) -> dict:
    estagio = estagios_ciclo.estagio_do_ciclo(ciclo)
    sem_desembarque = ciclo is None or ciclo.data_desembarque is None
    # só a saída DEFINITIVA: a do porto rumo ao seco (data_saida, com o contêiner no seco) não é saída do controle
    saida = (ciclo.data_saida_seco or ciclo.data_saida) if estagio in SAIU else None
    return {'numero': numero, 'estagio': estagios_ciclo.descreve(estagio), 'onde': _onde(ciclo, nomes_recintos),
            'desembarque': _iso(ciclo.data_desembarque) if ciclo else None, 'saida': _iso(saida),
            'semDesembarque': sem_desembarque, 'previsao': previsao if sem_desembarque else None,
            'escaneamento': _ultimo_escaneamento(ciclo), 'navii': _parametros_navii(numero, ce, ciclo, previsao),
            'alerta': estagio in SAIU and not verificada}


def _ciclos(session, pares: List[Tuple[str, str]]):
    """({(CE, contêiner): ciclo}, {CE: previsão de atracação}, {recinto: nome}) — as buscas do NAVI-I, em lote."""
    ces = sorted({ce for ce, _ in pares if ce})
    conhecimentos = session.query(Conhecimento).filter(Conhecimento.numeroCEmercante.in_(ces)).all() if ces else []
    familias = busca_familias_navii(session, conhecimentos)
    itens = [SimpleNamespace(numeroCEmercante=ce, codigoConteiner=c) for ce, c in pares if ce]
    ciclos = busca_ciclos_navii(session, itens, {n: d['familia'] for n, d in familias.items()})
    manifestos = busca_manifestos_navii(session, [c.manifestoCE for c in conhecimentos])
    previsoes = {c.numeroCEmercante: _previsao_atracacao(
        manifestos.get(c.manifestoCE, {}).get('dataPrevistaAtracacao')) for c in conhecimentos}
    recintos = {r for ciclo in ciclos.values()
                for r in (ciclo.codigoRecintoDesembarque, ciclo.codigoRecintoArmazenagem)}
    return ciclos, previsoes, busca_nomes_recintos(session, recintos)


# ---------------------------------------------------------------- montagem

def _marcos(eventos: list) -> dict:
    """Bloqueio de CE (o primeiro), verificação agendada (a última) e verificação física informada (a primeira)."""
    def primeiro(regra):
        return next((e.data for e in eventos if regra(e)), None)

    def ultimo(regra):
        return next((e.data for e in reversed(eventos) if regra(e)), None)
    return {'bloqueio': _iso(primeiro(lambda e: e.chave.startswith('bloqueio de ce'))),
            'agendada': _iso(ultimo(lambda e: e.chave == 'verificacao fisica agendada')),
            'verificacao': _iso(primeiro(lambda e: e.especial == VERIFICACAO_INFORMADA))}


def _card(linha, eventos: list, conteineres: List[dict], navii: bool, nomes: Dict[str, str],
          setores_nomes: Dict[str, str], agora: datetime) -> dict:
    aberta = _data(linha.datahora)
    return {
        'id': linha.id, 'url': fichas_ovr.URL_FICHA.format(linha.id), 'numero': _texto(linha.numero) or None,
        'ce': _texto(linha.numeroCEmercante) or None, 'fase': linha.fase,
        'faseDescricao': fichas_ovr.FASES[linha.fase] if linha.fase is not None and 0 <= linha.fase < 5 else None,
        'status': eventos[-1].tipo if eventos else None, 'navii': navii,
        'aberta': _iso(aberta), 'dias': (agora - aberta).days if aberta else None,
        'setor': setores_nomes.get(_texto(linha.setor_id), _texto(linha.setor_id)) or None,
        'abriu': _pessoa(linha.user_name, nomes), 'responsavel': _pessoa(linha.responsavel_cpf, nomes),
        'auditor': _pessoa(linha.cpfauditorresponsavel, nomes), **_marcos(eventos),
        'eventos': [{'data': _iso(e.data), 'tipo': e.tipo, 'motivo': _motivo(e.motivo, nomes),
                     'informativo': e.informativo} for e in reversed(eventos[-EVENTOS_NO_CARD:])],
        'conteineres': conteineres, 'alerta': any(c['alerta'] for c in conteineres),
    }


def fichas_em_acompanhamento(session, cpf: str, minhas: bool = False, navii: bool = False,
                             verificadas: bool = False, agora: Optional[datetime] = None) -> dict:
    """{'usuario', 'setores', 'fichas' (cards; os com alerta primeiro), 'total', 'alertas', 'aviso'}."""
    agora = agora or datetime.now()
    conexao = session.connection()
    usuario, setores, nomes_setores = usuario_e_setores(conexao, cpf)
    if usuario is None:
        return {'usuario': None, 'setores': [], 'fichas': [], 'total': 0, 'alertas': 0,
                'aviso': 'Seu usuário não tem cadastro no Fichas: peça o cadastro ao supervisor.'}
    linhas = _fichas(conexao, cpf, set(setores), minhas, navii)
    eventos = _eventos(conexao, [linha.id for linha in linhas])
    if not verificadas:
        linhas = [linha for linha in linhas if not _verificada(eventos.get(linha.id, []))]
    cards = _monta_cards(session, conexao, linhas, eventos, nomes_setores, agora)
    cards.sort(key=lambda c: (not c['alerta'], -(c['id'])))
    aviso = f'Mostrando as {LIMITE_CARDS} primeiras de {len(cards)}.' if len(cards) > LIMITE_CARDS else None
    return {'usuario': usuario, 'setores': sorted(setores.values()), 'fichas': cards[:LIMITE_CARDS],
            'total': len(cards), 'alertas': sum(c['alerta'] for c in cards), 'aviso': aviso}


def _verificada(eventos: list) -> bool:
    return _marcos(eventos)['verificacao'] is not None


def _pares(session, linhas, lotes: Dict[int, List[str]]) -> Dict[int, List[Tuple[str, str]]]:
    """{Ficha: [(CE, contêiner)]}: os contêineres das RVFs; sem RVF com contêiner, os do CE no Mercante."""
    sem_lote = sorted({_texto(linha.numeroCEmercante) for linha in linhas
                       if not lotes.get(linha.id) and linha.numeroCEmercante})
    do_ce = _conteineres_dos_ces(session, sem_lote)
    pares = {}
    for linha in linhas:
        ce = _texto(linha.numeroCEmercante)
        pares[linha.id] = [(ce, c) for c in (lotes.get(linha.id) or do_ce.get(ce, []))]
    return pares


def _cpfs(linhas, eventos: Dict[int, list]) -> Set[str]:
    """Os CPFs a trocar por nome: das pessoas da Ficha e os citados nos motivos dos eventos."""
    pessoas = {_texto(v) for linha in linhas
               for v in (linha.user_name, linha.responsavel_cpf, linha.cpfauditorresponsavel)}
    citados = {cpf for lista in eventos.values() for e in lista for cpf in CPF_NO_TEXTO.findall(e.motivo)}
    return pessoas | citados


def _monta_cards(session, conexao, linhas, eventos: Dict[int, list], nomes_setores: Dict[str, str],
                 agora: datetime) -> List[dict]:
    lotes, com_flag = _rvfs_e_flags(conexao, [linha.id for linha in linhas])
    pares = _pares(session, linhas, lotes)
    ciclos, previsoes, nomes_recintos = _ciclos(session, [p for lista in pares.values() for p in lista])
    nomes = _nomes(conexao, _cpfs(linhas, eventos))
    cards = []
    for linha in linhas:
        evs = eventos.get(linha.id, [])
        verificada = _verificada(evs)
        conteineres = [_conteiner(c, ce, ciclos.get((ce, c)), previsoes.get(ce), nomes_recintos, verificada)
                       for ce, c in pares[linha.id]]
        cards.append(_card(linha, evs, conteineres, linha.id in com_flag, nomes, nomes_setores, agora))
    return cards
