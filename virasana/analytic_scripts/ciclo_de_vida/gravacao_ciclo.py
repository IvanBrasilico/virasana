# coding: utf-8
"""
Gravação do Ciclo de Vida v2 nas tabelas do ciclo de vida, com as colunas de `ddl_ciclo_de_vida_v2.sql`.

As tabelas são declaradas aqui no SQLAlchemy Core (não no model ORM `ciclo_de_vida.py`, que o loop
antigo e as telas usam e fica como está); `test_servico_ciclo_de_vida` confere que batem com a DDL.

Por ciclo (chave: `desembarque_id`, única): atualiza a linha que já existe — inclusive a criada pelo loop
antigo, que passa a `versao_heuristica = 'v2'` — ou insere. As ligações de confiança BAIXA (CE declarado
de outra família) NÃO são gravadas: ficam só no motivo/alertas da sombra. Os escaneamentos do ciclo são
substituídos pelos da v2 (mantendo a imagem já achada); a família de CEs vai para `ciclodevida_ciclo_ce`.

Escaneamento só com IMAGEM (15/10/2026, decisão do usuário): numa passagem por outro recinto ou na chegada ao seco
sem inspeção transmitida, a imagem do contêiner no Mongo nesse horário é gravada como escaneamento com
`escaneamento_id` NEGATIVO (derivado do id da imagem — `imagens_ciclo.id_da_imagem`), confiança média e motivo
"imagem do escâner sem inspeção transmitida". Sem DDL: `escaneamento_id < 0` = não veio da API (conformidade).

Pesagens (P1, 20/10/2026; `ddl_pesagens_ciclo.sql`): as linhas de `pesagens_ciclo.linhas_do_ciclo` vão para
`ciclodevida_pesagem`, substituindo as do ciclo (dado derivado). Só com `pesagens=True` — o serviço liga quando a
tabela existe no banco.
"""
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sqlalchemy import (
    BigInteger, Boolean, CHAR, Column, DateTime, Integer, MetaData, Numeric, Table, UniqueConstraint, VARCHAR,
    bindparam, func, select,
)

from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
from virasana.analytic_scripts.ciclo_de_vida.ces_do_ciclo import CEsResolvidos
from virasana.analytic_scripts.ciclo_de_vida.dados_conteiner import peso_menos_tara, potencial_oog
from virasana.analytic_scripts.ciclo_de_vida.imagens_ciclo import (
    SEM_INSPECAO, Janela, id_da_imagem, imagens_das_inspecoes, imagens_sem_inspecao, janelas_sem_inspecao,
)

VERSAO = 'v2'
FORA = 'escaneamento fora'

metadata = MetaData()
_ID = BigInteger().with_variant(Integer, 'sqlite')

CICLOS = Table(
    'ciclodevida_ciclo_de_vida_importacao', metadata,
    Column('ID', _ID, primary_key=True, autoincrement=True),
    Column('codigoRecintoDesembarque', CHAR(7)), Column('numeroConteiner', CHAR(11)),
    Column('numeroCEmercante', CHAR(15)), Column('portoDestFinal', CHAR(7)),
    Column('desembarque_id', BigInteger, unique=True), Column('data_desembarque', DateTime),
    Column('saida_id', BigInteger), Column('data_saida', DateTime), Column('codigoRecintoArmazenagem', CHAR(7)),
    Column('entrada_seco_id', BigInteger), Column('data_entrada_seco', DateTime),
    Column('saida_seco_id', BigInteger), Column('data_saida_seco', DateTime), Column('concluido', Boolean),
    Column('create_date', DateTime, server_default=func.current_timestamp()), Column('last_modified', DateTime),
    # v2 (ddl_ciclo_de_vida_v2.sql)
    Column('situacao', VARCHAR(12)), Column('fluxo', VARCHAR(12)), Column('motivo', VARCHAR(200)),
    Column('confianca_ce', VARCHAR(5)), Column('vazio', Boolean), Column('numeroManifesto', CHAR(15)),
    Column('escala', CHAR(15)), Column('tipoConteiner', CHAR(4)), Column('oog', Boolean),
    Column('peso_balanca', Numeric(9, 2)), Column('peso_manifesto', Numeric(11, 2)), Column('tara', Numeric(9, 2)),
    Column('peso_menos_tara', Numeric(11, 2)), Column('embarque_id', BigInteger), Column('data_embarque', DateTime),
    Column('entrada_seco_implicita_de', VARCHAR(10)), Column('versao_heuristica', VARCHAR(10)),
    Column('processado_em', DateTime),
)
ESCANEAMENTOS = Table(
    'ciclodevida_ciclo_de_vida_importacao_escaneamentos', metadata,
    Column('ID', _ID, primary_key=True, autoincrement=True),
    Column('ciclo_de_vida_id', BigInteger, nullable=False), Column('escaneamento_id', BigInteger, nullable=False),
    Column('data_escaneamento', DateTime, nullable=False), Column('imagem_id', CHAR(24)),
    # v2
    Column('confianca', VARCHAR(5)), Column('motivo', VARCHAR(200)), Column('codigoRecinto', CHAR(7)),
    Column('fora', Boolean),
)
CICLO_CE = Table(
    'ciclodevida_ciclo_ce', metadata,
    Column('ID', _ID, primary_key=True, autoincrement=True),
    Column('ciclo_de_vida_id', BigInteger, nullable=False), Column('numeroCEmercante', CHAR(15), nullable=False),
    Column('papel', VARCHAR(8), nullable=False), Column('principal', Boolean, nullable=False),
    Column('confianca', VARCHAR(5)), Column('create_date', DateTime, server_default=func.current_timestamp()),
)
MARCAS = Table(
    'ciclodevida_marca_dagua', metadata,
    Column('tabela', VARCHAR(60), primary_key=True), Column('ultimo_id', BigInteger, nullable=False),
    Column('atualizado_em', DateTime, nullable=False),
)
PESAGENS = Table(
    'ciclodevida_pesagem', metadata,
    Column('ID', _ID, primary_key=True, autoincrement=True),
    Column('ciclo_de_vida_id', BigInteger, nullable=False), Column('pesagem_id', BigInteger, nullable=False),
    Column('data', DateTime, nullable=False), Column('codigoRecinto', CHAR(7)), Column('placa', VARCHAR(10)),
    Column('semirreboque', VARCHAR(10)), Column('operacao', VARCHAR(60), nullable=False),
    Column('principal', Boolean, nullable=False), Column('peso_bruto', Numeric(11, 2)),
    Column('captura_automatica', Boolean), Column('tara_pesagem_id', BigInteger),
    Column('tara_conjunto', Numeric(11, 2)), Column('origem_tara_conjunto', VARCHAR(25)),
    Column('tara_conteiner', Numeric(9, 2)), Column('origem_tara_conteiner', VARCHAR(20)),
    Column('peso_medido', Numeric(11, 2)), Column('peso_declarado', Numeric(11, 2)),
    Column('origem_declarado', VARCHAR(30)), Column('numeroCEmercante', CHAR(15)),
    Column('diferenca_kg', Numeric(11, 2)), Column('diferenca_pct', Numeric(12, 2)), Column('razao', Numeric(12, 4)),
    Column('classificacao', VARCHAR(60)), Column('diagnostico', VARCHAR(20)), Column('detalhe', VARCHAR(120)),
    Column('confirmacao', VARCHAR(80)), Column('divisao', Integer), Column('fora_da_divisao', Boolean),
    Column('create_date', DateTime, server_default=func.current_timestamp()),
    UniqueConstraint('ciclo_de_vida_id', 'pesagem_id', name='uq_pesagem_ciclo'),
)
_COLUNAS_PESAGEM = [c.name for c in PESAGENS.c if c.name not in ('ID', 'ciclo_de_vida_id', 'create_date')]
_TEXTOS_PESAGEM = {c.name: c.type.length for c in PESAGENS.c if isinstance(c.type, (CHAR, VARCHAR))}


@dataclass
class ItemGravacao:
    """Um ciclo calculado, pronto para gravar."""
    conteiner: str
    ciclo: ldt.CicloCalculado
    ces: CEsResolvidos
    linha: dict
    pesagens: List[dict] = field(default_factory=list)    # pesagens_ciclo.linhas_do_ciclo


# ---------------------------------------------------------------------- montagem das linhas (pura)

def _valida(ligacao: Optional[ldt.Ligacao]) -> Optional[ldt.Ligacao]:
    """A ligação, se não for de confiança baixa (essas não são gravadas)."""
    return ligacao if ligacao is not None and ligacao.confianca != ldt.BAIXA else None


def _etapa(ligacao: Optional[ldt.Ligacao]):
    ligacao = _valida(ligacao)
    return (ligacao.evento.id, ligacao.evento.data) if ligacao else (None, None)


def _texto(valor, tamanho: int) -> Optional[str]:
    return str(valor)[:tamanho] if valor else None


def _pesos(dados: dict) -> dict:
    peso, tara, manifesto = dados.get('peso'), dados.get('tara'), dados.get('manifesto')
    liquido = peso_menos_tara(peso, tara)
    if liquido is None:   # a balança quase nunca pesa (sombra: ~75% sem peso ou zerada)
        liquido = peso_menos_tara(manifesto, tara)
    return {'peso_balanca': peso, 'peso_manifesto': manifesto, 'tara': tara, 'peso_menos_tara': liquido}


def _entrada_seco(ciclo: ldt.CicloCalculado) -> dict:
    entrada = _valida(ciclo.entrada_seco)
    if entrada is None:
        return {'codigoRecintoArmazenagem': None, 'entrada_seco_id': None, 'data_entrada_seco': None,
                'entrada_seco_implicita_de': None}
    implicita = entrada.evento.implicita_de
    return {'codigoRecintoArmazenagem': entrada.evento.recinto,
            'entrada_seco_id': None if implicita else entrada.evento.id,   # id de inspeção/pesagem não é acesso
            'data_entrada_seco': entrada.evento.data, 'entrada_seco_implicita_de': implicita}


def linha_ciclo(conteiner: str, ciclo: ldt.CicloCalculado, ces: CEsResolvidos, dados: dict, info: Dict[str, dict],
                manifesto_vazio: Optional[str], tipo: Optional[str], agora: datetime) -> dict:
    """Valores da linha do ciclo (colunas antigas e v2).

    dados: o desembarque na API (peso, tara, peso de manifesto, escala); info: o Mercante dos CEs (porto de
    destino do principal); manifesto_vazio: manifesto em que o contêiner consta vazio (se consta).
    """
    saida_id, data_saida = _etapa(ciclo.saida)
    saida_seco_id, data_saida_seco = _etapa(ciclo.saida_seco)
    embarque_id, data_embarque = _etapa(ciclo.embarque)
    return {
        'codigoRecintoDesembarque': ciclo.desembarque.recinto, 'numeroConteiner': conteiner,
        'numeroCEmercante': ces.principal,
        'portoDestFinal': _texto((info.get(ces.principal) or {}).get('porto_destino'), 7),
        'desembarque_id': ciclo.desembarque.id, 'data_desembarque': ciclo.desembarque.data,
        'saida_id': saida_id, 'data_saida': data_saida, **_entrada_seco(ciclo),
        'saida_seco_id': saida_seco_id, 'data_saida_seco': data_saida_seco,
        'embarque_id': embarque_id, 'data_embarque': data_embarque,
        'concluido': ciclo.situacao != ldt.ABERTO,
        'situacao': ciclo.situacao, 'fluxo': ciclo.fluxo, 'motivo': _texto(ciclo.motivo, 200),
        'confianca_ce': ces.confianca,
        'vazio': bool(manifesto_vazio), 'numeroManifesto': _texto(manifesto_vazio, 15),
        'escala': _texto(dados.get('escala'), 15), 'tipoConteiner': _texto(tipo, 4), 'oog': potencial_oog(tipo),
        **_pesos(dados),
        'versao_heuristica': VERSAO, 'processado_em': agora, 'last_modified': agora,
    }


def linhas_escaneamentos(ciclo: ldt.CicloCalculado) -> Dict[int, dict]:
    """{id da inspeção: valores} das inspeções ligadas (menos as de confiança baixa)."""
    linhas = {}
    for ligacao in ciclo.escaneamentos:
        if _valida(ligacao) is None:
            continue
        linhas[ligacao.evento.id] = {
            'escaneamento_id': ligacao.evento.id, 'data_escaneamento': ligacao.evento.data,
            'confianca': ligacao.confianca, 'motivo': _texto('; '.join(ligacao.motivos), 200),
            'codigoRecinto': _texto(ligacao.evento.recinto, 7),
            'fora': any(FORA in motivo for motivo in ligacao.motivos),
        }
    return linhas


def linhas_so_imagem(achadas: List[Tuple[Janela, datetime, str]]) -> Dict[int, dict]:
    """{id negativo: valores} dos escaneamentos só com imagem — o escâner escaneou e a inspeção não foi transmitida
    (passagem: "escaneamento fora"; chegada ao seco: na etapa). Confiança média."""
    linhas = {}
    for janela, horario, imagem_id in achadas:
        onde = (f'na passagem pelo recinto {janela.recinto} ({FORA})' if janela.passagem
                else 'na chegada ao recinto seco')
        linhas[id_da_imagem(imagem_id)] = {
            'escaneamento_id': id_da_imagem(imagem_id), 'data_escaneamento': horario, 'imagem_id': imagem_id,
            'confianca': ldt.MEDIA, 'motivo': _texto(f'{SEM_INSPECAO}; {onde}', 200),
            'codigoRecinto': _texto(janela.recinto, 7), 'fora': janela.passagem,
        }
    return linhas


def _inclui(valores: dict, id_: Optional[int], ciclo_id: int, inserir: list, atualizar: list):
    if id_ is None:
        inserir.append({**valores, 'ciclo_de_vida_id': ciclo_id})
    else:
        atualizar.append({**valores, 'b_id': id_})


def linha_pesagem(valores: dict, ciclo_id: int) -> dict:
    """Uma linha de `ciclodevida_pesagem` (as colunas que faltam ficam nulas; textos cortados no tamanho)."""
    linha = {coluna: valores.get(coluna) for coluna in _COLUNAS_PESAGEM}
    for coluna, tamanho in _TEXTOS_PESAGEM.items():
        if linha[coluna] is not None:
            linha[coluna] = str(linha[coluna])[:tamanho]
    return {**linha, 'principal': bool(linha['principal']), 'ciclo_de_vida_id': ciclo_id}


def linhas_ces(ces: CEsResolvidos) -> List[dict]:
    """Os CEs da família com o contêiner (papel master/filhote/simples), marcando o principal."""
    return [{'numeroCEmercante': ce, 'papel': papel, 'principal': ce == ces.principal, 'confianca': ces.confianca}
            for ce, papel in sorted(ces.ces.items())]


# ---------------------------------------------------------------------- gravação

class Gravador:
    """Grava, numa conexão (dentro da transação de quem chama), um lote de ciclos calculados."""

    def __init__(self, conexao, mongo_db=None, pesagens: bool = False):
        self.conexao, self.mongo_db, self.pesagens = conexao, mongo_db, pesagens
        self.estatisticas = Counter()

    def grava(self, itens: List[ItemGravacao]) -> Counter:
        if not itens:
            return self.estatisticas
        self._apaga_remocoes([d.id for item in itens for d in item.ciclo.remocoes])
        ids = self._grava_ciclos(itens)
        self._grava_escaneamentos(itens, ids)
        self._grava_ces(itens, ids)
        if self.pesagens:
            self._grava_pesagens(itens, ids)
        return self.estatisticas

    def _apaga_remocoes(self, desembarques: List[int]):
        """Apaga o ciclo (com escaneamentos e CEs) gravado para um desembarque que se revelou REMOÇÃO A BORDO —
        ele continua no ciclo do primeiro desembarque. É dado derivado: refazer o contêiner o recria se preciso."""
        if not desembarques:
            return
        ids = [id_ for id_, in self.conexao.execute(
            select(CICLOS.c.ID).where(CICLOS.c.desembarque_id.in_(desembarques)))]
        if ids:
            self.conexao.execute(ESCANEAMENTOS.delete().where(ESCANEAMENTOS.c.ciclo_de_vida_id.in_(ids)))
            self.conexao.execute(CICLO_CE.delete().where(CICLO_CE.c.ciclo_de_vida_id.in_(ids)))
            if self.pesagens:
                self.conexao.execute(PESAGENS.delete().where(PESAGENS.c.ciclo_de_vida_id.in_(ids)))
            self.conexao.execute(CICLOS.delete().where(CICLOS.c.ID.in_(ids)))
        self.estatisticas['ciclos de remoção a bordo apagados'] += len(ids)

    def _ids_por_desembarque(self, desembarques) -> Dict[int, int]:
        return dict(self.conexao.execute(
            select(CICLOS.c.desembarque_id, CICLOS.c.ID).where(CICLOS.c.desembarque_id.in_(list(desembarques)))).all())

    def _grava_ciclos(self, itens: List[ItemGravacao]) -> Dict[int, int]:
        """Atualiza ou insere; devolve {desembarque_id: ID do ciclo}."""
        existentes = self._ids_por_desembarque({i.linha['desembarque_id'] for i in itens})
        novos = [i.linha for i in itens if i.linha['desembarque_id'] not in existentes]
        atualizar = [{**i.linha, 'b_id': existentes[i.linha['desembarque_id']]} for i in itens
                     if i.linha['desembarque_id'] in existentes]
        if novos:
            self.conexao.execute(CICLOS.insert(), novos)
        if atualizar:
            self.conexao.execute(CICLOS.update().where(CICLOS.c.ID == bindparam('b_id')), atualizar)
        self.estatisticas.update({'ciclos inseridos': len(novos), 'ciclos atualizados': len(atualizar)})
        return {**existentes, **self._ids_por_desembarque({linha['desembarque_id'] for linha in novos})}

    def _grava_escaneamentos(self, itens: List[ItemGravacao], ids: Dict[int, int]):
        atuais: Dict[int, Dict[int, tuple]] = {}
        for id_, ciclo_id, escaneamento_id, imagem_id in self.conexao.execute(
                select(ESCANEAMENTOS.c.ID, ESCANEAMENTOS.c.ciclo_de_vida_id, ESCANEAMENTOS.c.escaneamento_id,
                       ESCANEAMENTOS.c.imagem_id).where(ESCANEAMENTOS.c.ciclo_de_vida_id.in_(list(ids.values())))):
            atuais.setdefault(ciclo_id, {})[escaneamento_id] = (id_, imagem_id)
        inserir, atualizar, apagar = [], [], []
        for item in itens:
            ciclo_id = ids[item.linha['desembarque_id']]
            self._compara_escaneamentos(item, ciclo_id, atuais.get(ciclo_id, {}), inserir, atualizar, apagar)
        if apagar:
            self.conexao.execute(ESCANEAMENTOS.delete().where(ESCANEAMENTOS.c.ID.in_(apagar)))
        if inserir:
            self.conexao.execute(ESCANEAMENTOS.insert(), inserir)
        if atualizar:
            self.conexao.execute(ESCANEAMENTOS.update().where(ESCANEAMENTOS.c.ID == bindparam('b_id')), atualizar)
        self.estatisticas.update({'escaneamentos inseridos': len(inserir), 'escaneamentos atualizados': len(atualizar),
                                  'escaneamentos removidos': len(apagar),
                                  'com imagem': sum(1 for v in inserir + atualizar if v['imagem_id'])})

    def _compara_escaneamentos(self, item: ItemGravacao, ciclo_id: int, existentes: Dict[int, tuple],
                               inserir: list, atualizar: list, apagar: list):
        """Separa as inspeções do ciclo em inserir/atualizar/apagar, mantendo a imagem já achada; acrescenta as
        imagens sem inspeção transmitida (id negativo — ver `imagens_ciclo`). Sem o Mongo, as que já existem ficam."""
        novos = linhas_escaneamentos(item.ciclo)
        imagens = self._imagens(item, novos, existentes)
        for escaneamento_id, valores in novos.items():
            id_, imagem = existentes.get(escaneamento_id, (None, None))
            valores = {**valores, 'imagem_id': imagem or imagens.get(escaneamento_id)}
            _inclui(valores, id_, ciclo_id, inserir, atualizar)
        manter = set(novos)
        if self.mongo_db is None:
            manter |= {escaneamento_id for escaneamento_id in existentes if escaneamento_id < 0}
        else:
            so_imagem = linhas_so_imagem(imagens_sem_inspecao(self.mongo_db, item.conteiner,
                                                              janelas_sem_inspecao(item.ciclo)))
            for escaneamento_id, valores in so_imagem.items():
                _inclui(valores, existentes.get(escaneamento_id, (None, None))[0], ciclo_id, inserir, atualizar)
            manter |= set(so_imagem)
            self.estatisticas['escaneamentos só com imagem (sem inspeção na API)'] += len(so_imagem)
        apagar += [id_ for escaneamento_id, (id_, _) in existentes.items() if escaneamento_id not in manter]

    def _imagens(self, item: ItemGravacao, novos: Dict[int, dict], existentes: Dict[int, tuple]) -> Dict[int, str]:
        """Imagens no Mongo só das inspeções que ainda não têm imagem gravada."""
        sem_imagem = [lig.evento for lig in item.ciclo.escaneamentos
                      if lig.evento.id in novos and not existentes.get(lig.evento.id, (None, None))[1]]
        return imagens_das_inspecoes(self.mongo_db, item.conteiner, sem_imagem)

    def _grava_ces(self, itens: List[ItemGravacao], ids: Dict[int, int]):
        self.conexao.execute(CICLO_CE.delete().where(CICLO_CE.c.ciclo_de_vida_id.in_(list(ids.values()))))
        linhas = [{**linha, 'ciclo_de_vida_id': ids[item.linha['desembarque_id']]}
                  for item in itens for linha in linhas_ces(item.ces)]
        if linhas:
            self.conexao.execute(CICLO_CE.insert(), linhas)
        self.estatisticas['CEs da família gravados'] += len(linhas)

    def _grava_pesagens(self, itens: List[ItemGravacao], ids: Dict[int, int]):
        """Substitui as pesagens dos ciclos do lote (dado derivado: refazer o ciclo refaz as pesagens)."""
        self.conexao.execute(PESAGENS.delete().where(PESAGENS.c.ciclo_de_vida_id.in_(list(ids.values()))))
        linhas = [linha_pesagem(valores, ids[item.linha['desembarque_id']])
                  for item in itens for valores in item.pesagens]
        if linhas:
            self.conexao.execute(PESAGENS.insert(), linhas)
        self.estatisticas['pesagens gravadas'] += len(linhas)
        self.estatisticas.update(f'pesagem principal: {linha["diagnostico"]}' for linha in linhas if linha['principal'])
