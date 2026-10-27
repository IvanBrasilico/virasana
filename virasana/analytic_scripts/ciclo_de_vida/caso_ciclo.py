# coding: utf-8
"""
CASO do ciclo de vida (21/10/2026): extrai de um banco TUDO o que o ciclo de vida lê para alguns CEs e contêineres,
reprocessa com o código ATUAL num SQLite em memória e mostra por que cada desembarque ficou com o CE que ficou.
No banco de origem, somente leitura (SELECT por colunas indexadas).

Extração (--ces e/ou --conteineres; os contêineres dos itens dos CEs entram sozinhos):
  - Mercante: conhecimentos (os CEs pedidos, os que têm os contêineres nos itens, os masters, os filhotes e os
    declarados nos eventos), itens, manifestos, escalas (escalamanifestoresumo + escalasresumo, inclusive as declaradas
    nos desembarques) e contêineres vazios;
  - API Recintos: embarques/desembarques, acessos (inclusive agendamentos), inspeções e pesagens dos contêineres desde
    --desde — só as colunas que o ciclo de vida e a análise usam: sem CPF, CNPJ ou nome (motorista, transportador,
    consignatário), que é o que precisa ficar fora de dados publicados no repositório (usuário, 22/10/2026); a placa
    fica (com --apelida-placas, vira apelido estável, P000001...);
  - recintos (código, nome, operador, unidade);
  - o ciclo de vida GRAVADO dos contêineres: ciclos, CEs (família), escaneamentos e pesagens.
  O JSON (--json) é a entrada do --reprocessa e dos testes de regressão (`virasana/tests/dados/`).

Reprocesso (logo depois da extração, ou só ele com --reprocessa caso.json): carrega o JSON num SQLite em memória e
  1. refaz a escolha do CE de cada desembarque como o serviço faz (`CargaCiclo` + `resolve_ces`), mostrando os eventos
     ligados ao ciclo, os candidatos (master, tráfego, portos de origem e destino, manifesto e o porto de carregamento
     dele, as datas das escalas e a distância ao desembarque), os CEs declarados, a família escolhida, a confiança e o
     motivo;
  2. roda o serviço (`Servico.processa`, relógio na hora da extração) e compara o CE principal de cada desembarque
     com o gravado.

Lote (--casos, 22/10/2026): um caso por linha do arquivo ("nome contêiner [contêiner ...]  # por quê"), cada um
gravado em --pasta como <nome>.json e <nome>.log — o corpus de casos reais dos testes de regressão
(`virasana/tests/dados/casos_ciclo/`, `test_casos_reais_ciclo.py`).

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    C=virasana/analytic_scripts/ciclo_de_vida/caso_ciclo.py
    virasana-venv/bin/python $C --casos virasana/tests/dados/casos_ciclo/casos.txt --desde 2026-08-01 \
        --pasta casos_ciclo
    virasana-venv/bin/python $C --ces 072603341989805 152605316468892 --desde 2026-08-01 --saida caso_caau.log \\
        --json caso_caau.json
    virasana-venv/bin/python $C --reprocessa caso_caau.json --saida reprocesso_caau.log
"""
import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Set, Tuple

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')

from sqlalchemy import DateTime, bindparam, create_engine, inspect, select, text
from sqlalchemy.pool import StaticPool

from bhadrasana.models.apirecintos import AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva, PesagemVeiculo
from virasana.analytic_scripts.ciclo_de_vida import gravacao_ciclo as gc
from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
from virasana.analytic_scripts.ciclo_de_vida.carga_ciclo import DIAS_PORTOS, CargaCiclo, ConsultaSQL
from virasana.analytic_scripts.ciclo_de_vida.ces_do_ciclo import (
    JANELA_ESCALA_DIAS, datas_do_candidato, normaliza_ce, resolve_ces,
)
from virasana.analytic_scripts.ciclo_de_vida.heuristica_ciclo import declarados_do_ciclo, processa_conteiner
from virasana.integracao.mercante.mercantealchemy import (
    Conhecimento, ConteinerVazio, Escala, EscalaManifesto, Item, Manifesto,
)
from virasana.navii.busca_navii import tabela_recintos

DIAS_PADRAO = 120
LIMITE_SEGUNDOS = 120   # max_statement_time de cada consulta (MariaDB)
EVENTO = ('id', 'numeroConteiner', 'dataHoraOcorrencia', 'dataHoraTransmissao', 'codigoRecinto', 'tipoOperacao')
# tabela -> (modelo do SQLite do reprocesso, colunas extraídas)
TABELAS = {
    'conhecimentosresumo': (Conhecimento, (
        'numeroCEmercante', 'numeroCEMaster', 'tipoTrafego', 'tipoBLConhecimento', 'manifestoCE', 'portoOrigemCarga',
        'portoDestFinal', 'paisDestinoFinalMercante', 'dataEmissao', 'codigoTerminalCarregamento', 'numConhecimento')),
    'itensresumo': (Item, (
        'numeroCEmercante', 'codigoConteiner', 'numeroSequencialItemCarga', 'tipoItemCarga', 'isoCode', 'pesoBruto',
        'tara', 'indicadorUsoParcial')),
    'manifestosresumo': (Manifesto, (
        'numero', 'tipoTrafego', 'portoCarregamento', 'portoDescarregamento', 'codigoTerminalCarregamento',
        'codigoTerminalDescarregamento', 'numeroViagem', 'numeroImoDPC', 'dataInicioOperacao', 'dataEncerramento')),
    'escalamanifestoresumo': (EscalaManifesto, ('ID', 'manifesto', 'escala')),
    'escalasresumo': (Escala, (
        'numeroDaEscala', 'portoDaEscala', 'dataPrevistaAtracacao', 'dataPrevistaDesatracacao',
        'dataEfetivaPrimeiraAtracacao', 'dataEfetivaPrimeiraDesatracacao', 'embarcacao', 'numeroViagemDoArmador',
        'tipoOperacaoPrevista')),
    'conteinervazioresumo': (ConteinerVazio, ('manifesto', 'idConteinerVazio', 'isoConteinerVazio',
                                              'taraConteinerVazio')),
    'apirecintos_embarquedesembarque': (EmbarqueDesembarque, EVENTO + (
        'embarqueDesembarque', 'numeroConhecimento', 'tipoConhecimento', 'pesoBrutoBalanca', 'taraConteiner',
        'pesoBrutoManifesto', 'tipoConteiner', 'escala', 'viagem', 'cargaSolta')),
    'apirecintos_acessosveiculo': (AcessoVeiculo, EVENTO + (
        'operacao', 'direcao', 'vazioConteiner', 'numeroConhecimento', 'tipoConhecimento', 'tipoConteiner', 'placa',
        'placaSemirreboque', 'portoDescarregamento', 'navio', 'tipoDeclaracao', 'numeroDeclaracao')),
    'apirecintos_inspecoesnaoinvasivas': (InspecaoNaoInvasiva, EVENTO + (
        'vazio', 'numeroConhecimento', 'tipoConhecimento', 'tipoConteiner', 'placa')),
    'apirecintos_pesagensveiculo': (PesagemVeiculo, EVENTO + (
        'placa', 'placaSemirreboque', 'pesoBrutoBalanca', 'taraConjunto', 'capturaAutoPeso')),
}
EVENTOS = ('apirecintos_embarquedesembarque', 'apirecintos_acessosveiculo', 'apirecintos_inspecoesnaoinvasivas',
           'apirecintos_pesagensveiculo')
CHAVES = {
    'conhecimentosresumo': ('numeroCEmercante',),
    'itensresumo': ('numeroCEmercante', 'codigoConteiner', 'numeroSequencialItemCarga'),
    'manifestosresumo': ('numero',),
    'escalamanifestoresumo': ('ID',),
    'escalasresumo': ('numeroDaEscala',),
    'conteinervazioresumo': ('manifesto', 'idConteinerVazio'),
}
COLUNAS_RECINTOS = ('id', 'nome', 'cod_siscomex', 'e_operador', 'cod_unidade')
PLACAS = ('placa', 'placaSemirreboque', 'semirreboque')


# ---------------------------------------------------------------------- utilidades

def _texto(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def _data(valor) -> Optional[datetime]:
    if valor is None or isinstance(valor, datetime):
        return valor
    return datetime.fromisoformat(str(valor))


def _fmt(valor) -> str:
    data = _data(valor)
    return data.strftime('%d/%m/%Y %H:%M') if data else '-'


def json_padrao(valor):
    """Serializa datas e decimais do banco no JSON do caso."""
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()
    if isinstance(valor, Decimal):
        return float(valor)
    raise TypeError(f'{type(valor).__name__} não serializável')


class Saida:
    """Escreve na tela e, se houver, no arquivo."""

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


# ---------------------------------------------------------------------- extração

class Extracao:
    """Lê do banco de origem (só SELECT, por colunas indexadas) as linhas do caso, sem repetir."""

    def __init__(self, conexao, desde: datetime):
        self.conexao, self.desde = conexao, desde
        self.tabelas: Dict[str, Dict[tuple, dict]] = defaultdict(dict)

    def linhas(self, tabela: str) -> List[dict]:
        return list(self.tabelas[tabela].values())

    def le(self, tabela: str, onde: str, params: dict) -> List[dict]:
        listas = [nome for nome, valor in params.items() if isinstance(valor, (list, set, tuple))]
        if any(not params[nome] for nome in listas):
            return []
        comando = text(f'SELECT {", ".join(TABELAS[tabela][1])} FROM {tabela} WHERE {onde}'  # nosec B608 - fixos
                       ).bindparams(*[bindparam(nome, expanding=True) for nome in listas])
        params = {nome: sorted(valor) if nome in listas else valor for nome, valor in params.items()}
        linhas = [dict(linha._mapping) for linha in self.conexao.execute(comando, params)]
        chave = CHAVES.get(tabela, ('id',))
        for linha in linhas:
            self.tabelas[tabela][tuple(linha[c] for c in chave)] = linha
        return linhas

    def extrai(self, ces: Set[str], conteineres: Set[str]) -> List[str]:
        """Lê tudo; devolve os contêineres do caso (os pedidos e os dos itens dos CEs)."""
        conteineres = set(conteineres)
        for linha in self.le('itensresumo', 'numeroCEmercante IN :ces', {'ces': ces}):
            if _texto(linha['codigoConteiner']):
                conteineres.add(_texto(linha['codigoConteiner']))
        self.le('itensresumo', 'codigoConteiner IN :conteineres', {'conteineres': conteineres})
        for tabela in EVENTOS:
            self.le(tabela, 'numeroConteiner IN :conteineres AND dataHoraOcorrencia >= :desde',
                    {'conteineres': conteineres, 'desde': self.desde})
        self.le('conteinervazioresumo', 'idConteinerVazio IN :conteineres', {'conteineres': conteineres})
        self._conhecimentos(ces)
        self._manifestos_e_escalas()
        return sorted(conteineres)

    def _conhecimentos(self, ces: Set[str]):
        """Os pedidos, os dos itens, os declarados nos eventos, os masters e os filhotes deles (irmãos)."""
        declarados = {normaliza_ce(linha.get('numeroConhecimento')) for tabela in EVENTOS
                      for linha in self.linhas(tabela)} - {None}
        todos = set(ces) | {_texto(linha['numeroCEmercante']) for linha in self.linhas('itensresumo')} | declarados
        linhas = self.le('conhecimentosresumo', 'numeroCEmercante IN :ces', {'ces': todos - {''}})
        masters = {_texto(linha['numeroCEMaster']) for linha in linhas} - {''}
        self.le('conhecimentosresumo', 'numeroCEmercante IN :ces', {'ces': masters - todos})
        self.le('conhecimentosresumo', 'numeroCEMaster IN :masters', {'masters': (masters | todos) - {''}})

    def _manifestos_e_escalas(self):
        manifestos = ({_texto(linha['manifestoCE']) for linha in self.linhas('conhecimentosresumo')}
                      | {_texto(linha['manifesto']) for linha in self.linhas('conteinervazioresumo')}) - {''}
        declaradas = {_texto(linha['escala']) for linha in self.linhas('apirecintos_embarquedesembarque')} - {''}
        self.le('escalamanifestoresumo', 'manifesto IN :manifestos', {'manifestos': manifestos})
        self.le('escalamanifestoresumo', 'escala IN :escalas', {'escalas': declaradas})
        ligacoes = self.linhas('escalamanifestoresumo')
        self.le('manifestosresumo', 'numero IN :manifestos',
                {'manifestos': manifestos | ({_texto(linha['manifesto']) for linha in ligacoes} - {''})})
        self.le('escalasresumo', 'numeroDaEscala IN :escalas',
                {'escalas': declaradas | ({_texto(linha['escala']) for linha in ligacoes} - {''})})

    def recintos(self) -> List[dict]:
        return [dict(linha._mapping) for linha in self.conexao.execute(text(
            f'SELECT {", ".join(COLUNAS_RECINTOS)} FROM ovr_recintos '  # nosec B608 - colunas fixas
            "WHERE cod_siscomex IS NOT NULL AND cod_siscomex <> ''"))]

    def gravado(self, conteineres: List[str]) -> dict:
        """O ciclo de vida gravado dos contêineres (todas as versões) com a família, os escaneamentos e as pesagens."""
        ciclos = [dict(linha._mapping) for linha in self.conexao.execute(
            select(gc.CICLOS).where(gc.CICLOS.c.numeroConteiner.in_(conteineres)).order_by(gc.CICLOS.c.ID))]
        ids = [ciclo['ID'] for ciclo in ciclos]
        resultado = {'ciclos': ciclos}
        tabelas = [('ces', gc.CICLO_CE), ('escaneamentos', gc.ESCANEAMENTOS)]
        if inspect(self.conexao).has_table(gc.PESAGENS.name):
            tabelas.append(('pesagens', gc.PESAGENS))
        for nome, tabela in tabelas:
            resultado[nome] = [dict(linha._mapping) for linha in self.conexao.execute(
                select(tabela).where(tabela.c.ciclo_de_vida_id.in_(ids)).order_by(tabela.c.ID))] if ids else []
        return resultado


def apelida_placas(caso: dict) -> int:
    """Troca as placas por apelidos estáveis (a mesma placa, o mesmo apelido); devolve quantas placas havia."""
    apelidos: Dict[str, str] = {}

    def apelido(placa):
        chave = _texto(placa).upper()
        return apelidos.setdefault(chave, f'P{len(apelidos) + 1:06d}') if chave else placa

    linhas = [linha for lista in caso['tabelas'].values() for linha in lista] + caso['gravado'].get('pesagens', [])
    for linha in linhas:
        for campo in PLACAS:
            if campo in linha:
                linha[campo] = apelido(linha[campo])
    return len(apelidos)


def extrai_caso(conexao, ces: Iterable[str], conteineres: Iterable[str], desde: datetime,
                agora: Optional[datetime] = None, apelidar_placas: bool = False) -> dict:
    """O caso (JSON-serializável com `json_padrao`) lido da conexão."""
    extracao = Extracao(conexao, desde)
    ces = {ce for ce in (normaliza_ce(ce) for ce in ces) if ce}
    todos = extracao.extrai(ces, {_texto(c).upper() for c in conteineres if _texto(c)})
    agora = agora or datetime.now()
    recintos = {_texto(linha['codigoRecinto']) for linha in extracao.linhas('apirecintos_embarquedesembarque')}
    portos = CargaCiclo(ConsultaSQL(conexao), agora, agora).portos_dos_recintos(
        agora - timedelta(days=DIAS_PORTOS), recintos - {''})
    caso = {'meta': {'ces': sorted(ces), 'conteineres': todos, 'desde': desde, 'agora': agora,
                     'portos_dos_recintos': portos},
            'tabelas': {tabela: extracao.linhas(tabela) for tabela in TABELAS if extracao.linhas(tabela)},
            'recintos': extracao.recintos(),
            'gravado': extracao.gravado(todos)}
    if apelidar_placas:
        caso['meta']['placas'] = apelida_placas(caso)
    return json.loads(json.dumps(caso, default=json_padrao))


# ---------------------------------------------------------------------- banco do reprocesso

def _tipos(tabela, linha: dict) -> dict:
    """Datas do JSON (texto ISO) de volta a datetime nas colunas DateTime."""
    return {campo: _data(valor) if valor is not None and isinstance(tabela.c[campo].type, DateTime) else valor
            for campo, valor in linha.items()}


def banco_do_caso(caso: dict):
    """SQLite em memória com as tabelas que o ciclo de vida lê, preenchidas com o caso (o gravado fica de fora)."""
    engine = create_engine('sqlite://', poolclass=StaticPool, connect_args={'check_same_thread': False})
    for modelo, _ in TABELAS.values():
        modelo.__table__.create(engine)
    tabela_recintos.create(engine)
    gc.metadata.create_all(engine)
    with engine.begin() as conexao:
        conexao.execute(text('ALTER TABLE ovr_recintos ADD COLUMN e_operador BOOLEAN'))
        conexao.execute(text('ALTER TABLE ovr_recintos ADD COLUMN cod_unidade VARCHAR(7)'))
        for recinto in caso['recintos']:
            conexao.execute(text('INSERT INTO ovr_recintos (id, nome, cod_siscomex, e_operador, cod_unidade) '
                                 'VALUES (:id, :nome, :cod_siscomex, :e_operador, :cod_unidade)'), recinto)
        for nome, linhas in caso['tabelas'].items():
            tabela = TABELAS[nome][0].__table__
            for linha in linhas:   # uma a uma: o insert em lote usaria as colunas da primeira linha
                conexao.execute(tabela.insert(), _tipos(tabela, linha))
    return engine


# ---------------------------------------------------------------------- relatório do caso

def _rotulo_evento(tabela: str, linha: dict) -> str:
    if tabela == 'apirecintos_embarquedesembarque':
        movimento = _texto(linha['embarqueDesembarque'])
        tipo = {'D': 'DESEMBARQUE', 'E': 'EMBARQUE'}.get(movimento, f'D/E {movimento}')
        return (f'{tipo} escala {linha.get("escala")} viagem {linha.get("viagem")} peso balança '
                f'{linha.get("pesoBrutoBalanca")} manifesto {linha.get("pesoBrutoManifesto")}')
    if tabela == 'apirecintos_acessosveiculo':
        direcao = {'E': 'ENTRADA', 'S': 'SAÍDA'}.get(_texto(linha['direcao']), f'direção {linha["direcao"]}')
        operacao = '' if _texto(linha['operacao']) == 'C' else f' (operação {linha["operacao"]}: não conta)'
        return f'{direcao}{operacao} vazio={linha.get("vazioConteiner")} placa {linha.get("placa")}'
    if tabela == 'apirecintos_inspecoesnaoinvasivas':
        return f'INSPEÇÃO vazio={linha.get("vazio")}'
    return f'PESAGEM {linha.get("pesoBrutoBalanca")} kg tara do conjunto {linha.get("taraConjunto")} placa ' \
           f'{linha.get("placa")}'


class Relatorio:
    """O caso por escrito: Mercante, eventos em ordem e o ciclo gravado."""

    def __init__(self, caso: dict, saida: Saida):
        self.caso, self.escreve = caso, saida
        self.nomes = {_texto(r['cod_siscomex']): r['nome'] for r in caso['recintos']}
        tabelas = caso['tabelas']
        self.manifestos = {_texto(m['numero']): m for m in tabelas.get('manifestosresumo', [])}
        self.escalas = {_texto(e['numeroDaEscala']): e for e in tabelas.get('escalasresumo', [])}

    def recinto(self, codigo) -> str:
        return f'{_texto(codigo)} {self.nomes.get(_texto(codigo), "")}'.strip()

    def escreve_tudo(self):
        meta = self.caso['meta']
        self.escreve(f'CEs pedidos {meta["ces"]}; contêineres {meta["conteineres"]}; eventos desde {meta["desde"]}; '
                     f'extraído em {meta["agora"]}')
        self.mercante()
        self.eventos()
        self.gravado()

    def mercante(self):
        self.escreve.titulo('1. Mercante')
        tabelas = self.caso['tabelas']
        itens = defaultdict(list)
        for item in tabelas.get('itensresumo', []):
            itens[_texto(item['numeroCEmercante'])].append(item)
        for ce in sorted(tabelas.get('conhecimentosresumo', []), key=lambda c: _texto(c['numeroCEmercante'])):
            numero = _texto(ce['numeroCEmercante'])
            self.escreve(f'CE {numero}: tráfego {ce["tipoTrafego"]} BL {ce["tipoBLConhecimento"]} master '
                         f'{ce["numeroCEMaster"]} origem {ce["portoOrigemCarga"]} -> destino {ce["portoDestFinal"]} '
                         f'emissão {ce["dataEmissao"]} manifesto {ce["manifestoCE"]}')
            for item in itens.get(numero, []):
                self.escreve(f'    item {item["numeroSequencialItemCarga"]}: contêiner {item["codigoConteiner"]} iso '
                             f'{item["isoCode"]} peso {item["pesoBruto"]} tara {item["tara"]}')
        for numero, manifesto in sorted(self.manifestos.items()):
            self.escreve(f'Manifesto {numero}: tráfego {manifesto["tipoTrafego"]} carregamento '
                         f'{manifesto["portoCarregamento"]} -> descarregamento {manifesto["portoDescarregamento"]} '
                         f'viagem {manifesto["numeroViagem"]} início {manifesto["dataInicioOperacao"]}')
            for ligacao in tabelas.get('escalamanifestoresumo', []):
                if _texto(ligacao['manifesto']) == numero:
                    self.escreve('    ' + self._escala(_texto(ligacao['escala'])))
        for vazio in tabelas.get('conteinervazioresumo', []):
            self.escreve(f'Vazio: contêiner {vazio["idConteinerVazio"]} no manifesto {vazio["manifesto"]}')

    def _escala(self, numero: str) -> str:
        escala = self.escalas.get(numero)
        if escala is None:
            return f'escala {numero}: não está em escalasresumo'
        return (f'escala {numero}: porto {escala["portoDaEscala"]} atracação prevista '
                f'{escala["dataPrevistaAtracacao"]} efetiva {escala["dataEfetivaPrimeiraAtracacao"]} desatracação '
                f'prevista {escala["dataPrevistaDesatracacao"]} embarcação {escala["embarcacao"]}')

    def eventos(self):
        self.escreve.titulo('2. Eventos da API Recintos (em ordem)')
        por_conteiner = defaultdict(list)
        for tabela in EVENTOS:
            for linha in self.caso['tabelas'].get(tabela, []):
                por_conteiner[_texto(linha['numeroConteiner'])].append((_data(linha['dataHoraOcorrencia']), tabela,
                                                                        linha))
        for conteiner in self.caso['meta']['conteineres']:
            self.escreve(f'Contêiner {conteiner}:')
            for data, tabela, linha in sorted(por_conteiner.get(conteiner, []), key=lambda e: (e[0], e[2]['id'])):
                self.escreve(f'  {_fmt(data)} {self.recinto(linha["codigoRecinto"])}: {_rotulo_evento(tabela, linha)}'
                             f' | CE {linha.get("numeroConhecimento")} tipo {linha.get("tipoConteiner")} '
                             f'(id {linha["id"]}, {linha.get("tipoOperacao")})')
            if not por_conteiner.get(conteiner):
                self.escreve('  nenhum evento no período')

    def gravado(self):
        self.escreve.titulo('3. Ciclo de vida GRAVADO')
        gravado = self.caso['gravado']
        for ciclo in gravado['ciclos']:
            familia = [f'{c["numeroCEmercante"]} ({c["papel"]}{", principal" if c["principal"] else ""})'
                       for c in gravado['ces'] if c['ciclo_de_vida_id'] == ciclo['ID']]
            imagens = sum(1 for e in gravado['escaneamentos']
                          if e['ciclo_de_vida_id'] == ciclo['ID'] and e['imagem_id'])
            self.escreve(f'ciclo {ciclo["ID"]} ({ciclo["versao_heuristica"]}) {ciclo["numeroConteiner"]}: CE '
                         f'{ciclo["numeroCEmercante"]} confiança {ciclo["confianca_ce"]}; desembarque '
                         f'{ciclo["desembarque_id"]} em {_fmt(ciclo["data_desembarque"])} '
                         f'{self.recinto(ciclo["codigoRecintoDesembarque"])}; saída {_fmt(ciclo["data_saida"])}; '
                         f'embarque {_fmt(ciclo["data_embarque"])}; {ciclo["situacao"]}/{ciclo["fluxo"]}: '
                         f'{ciclo["motivo"]}; processado em {_fmt(ciclo["processado_em"])}')
            self.escreve(f'    família: {", ".join(familia) or "-"}; escala {ciclo["escala"]} manifesto '
                         f'{ciclo["numeroManifesto"]}; imagens {imagens}')
        if not gravado['ciclos']:
            self.escreve('nenhum ciclo gravado')


# ---------------------------------------------------------------------- reprocesso

class Reprocesso:
    """Refaz a escolha do CE de cada desembarque com o código atual, explicando cada passo."""

    def __init__(self, engine, caso: dict, saida: Saida):
        self.engine, self.caso, self.escreve = engine, caso, saida
        meta = caso['meta']
        self.conteineres, self.desde, self.agora = meta['conteineres'], _data(meta['desde']), _data(meta['agora'])
        tabelas = caso['tabelas']
        self.info = {_texto(c['numeroCEmercante']): c for c in tabelas.get('conhecimentosresumo', [])}
        self.carregamento = {_texto(m['numero']): m['portoCarregamento'] for m in tabelas.get('manifestosresumo', [])}
        self.nomes = {_texto(r['cod_siscomex']): r['nome'] for r in caso['recintos']}
        self.escala_do_evento = {e['id']: _texto(e.get('escala'))
                                 for e in tabelas.get('apirecintos_embarquedesembarque', [])}
        self.manifestos_da_escala: Dict[str, Set[str]] = defaultdict(set)
        for ligacao in tabelas.get('escalamanifestoresumo', []):
            self.manifestos_da_escala[_texto(ligacao['escala'])].add(_texto(ligacao['manifesto']))

    def explica(self):
        self.escreve.titulo(f'4. Reprocesso com o código atual (agora = {_fmt(self.agora)}; janela da escala '
                            f'{JANELA_ESCALA_DIAS} dias)')
        with self.engine.connect() as conexao:
            carga = CargaCiclo(ConsultaSQL(conexao), self.desde, self.agora)
            self.escopo, operadores, _ = carga.operadores()
            eventos, portuarios = carga.eventos(self.conteineres)
            self.portos = carga.portos_dos_desembarques(portos_dos_recintos(self.caso))
            candidatos = carga.candidatos(self.conteineres)
            conhecidos = carga.conhecidos([e for lista in eventos.values() for e in lista])
        self.operadores = operadores | portuarios
        for conteiner in self.conteineres:
            self._conteiner(conteiner, eventos.get(conteiner, []), candidatos.get(conteiner, []), conhecidos)

    def _conteiner(self, conteiner: str, eventos: List[ldt.Evento], candidatos: list, conhecidos: Set[str]):
        self.escreve(f'Contêiner {conteiner}: {len(eventos)} eventos, {len(candidatos)} CE(s) candidato(s)')
        ciclos = ldt.ciclos_do_conteiner(eventos, self.escopo, self.operadores, self.agora)
        if not ciclos:
            self.escreve('  nenhum ciclo: sem desembarque em operador do escopo no período')
        for ciclo in ciclos:
            self._ciclo(ciclo, candidatos)
        for ciclo, ces in processa_conteiner(eventos, candidatos, self.escopo, self.operadores, self.agora,
                                             conhecidos=conhecidos, portos=self.portos):
            self.escreve(f'  FINAL desembarque {ciclo.desembarque.id}: CE principal {ces.principal} '
                         f'({ces.confianca}); {ciclo.situacao}/{ciclo.fluxo}: {ciclo.motivo}')

    def _ciclo(self, ciclo: ldt.CicloCalculado, candidatos: list):
        desembarque = ciclo.desembarque
        declarados = [ce for ce in (normaliza_ce(d) for d in declarados_do_ciclo(ciclo)) if ce]
        self.escreve(f'  desembarque {desembarque.id} em {_fmt(desembarque.data)} no {desembarque.recinto} '
                     f'{self.nomes.get(desembarque.recinto, "")} (CE declarado: {desembarque.ce_declarado})')
        for ligacao in ciclo.ligacoes():
            evento = ligacao.evento
            self.escreve(f'    ligado: {evento.tipo} {_fmt(evento.data)} {evento.recinto} '
                         f'{self.nomes.get(evento.recinto, "")} CE {evento.ce_declarado} ({ligacao.confianca})')
        self.escreve(f'    CEs declarados no ciclo: {dict(Counter(declarados)) or "nenhum"}')
        escala, porto = self.escala_do_evento.get(desembarque.id), self.portos.get(desembarque.id)
        self.escreve(f'    escala declarada no desembarque: {escala or "nenhuma"} (manifestos dela: '
                     f'{sorted(self.manifestos_da_escala.get(escala, ())) or "-"}); porto do desembarque: '
                     f'{porto or "desconhecido"}')
        for candidato in candidatos:
            self.escreve('    ' + self._candidato(candidato, desembarque.data, set(declarados),
                                                  self.manifestos_da_escala.get(escala, set()), porto))
        ces = resolve_ces(desembarque.data, candidatos, declarados, porto=porto)
        self.escreve(f'    ESCOLHA: principal {ces.principal}, família {ces.ces or "-"}, confiança {ces.confianca} — '
                     f'{"; ".join(ces.motivos)}')

    def _candidato(self, candidato, data: datetime, declarados: Set[str], da_escala: Set[str],
                   porto: Optional[str] = None) -> str:
        info = self.info.get(candidato.ce, {})
        manifesto = _texto(info.get('manifestoCE'))
        distancias = [abs((dia - data.date()).days) for dia in datas_do_candidato(candidato, porto)]
        distancia = min(distancias) if distancias else None
        marcas = [m for m, vale in (('na janela', distancia is not None and distancia <= JANELA_ESCALA_DIAS),
                                    ('DECLARADO', candidato.ce in declarados),
                                    ('manifesto na escala do desembarque', manifesto in da_escala),
                                    (f'carga SAINDO de {porto}', bool(porto) and candidato.origem == porto),
                                    ('sem o contêiner nos itens', not candidato.tem_conteiner)) if vale]
        escalas = [f'{dia.isoformat()} {local}' for dia, local in candidato.escalas] or \
            [d.isoformat() for d in candidato.datas_escala]
        return (f'candidato {candidato.ce}: master {candidato.master} tráfego {candidato.trafego} origem '
                f'{info.get("portoOrigemCarga")} -> destino {info.get("portoDestFinal")}; manifesto {manifesto} '
                f'carregado em {self.carregamento.get(manifesto)}; escalas {escalas} -> {distancia} dia(s) '
                f'{", ".join(marcas)}')


def _com_familia(ciclos: List[dict], ces: List[dict]) -> Dict[int, dict]:
    """{desembarque: linha do ciclo + 'familia' (CEs gravados do ciclo, em ordem)} — agrupa os CEs uma vez (a versão
    que procurava os CEs de cada ciclo na lista inteira levou mais de 1 h com os 200 mil ciclos de 30 dias)."""
    por_ciclo: Dict[int, List[str]] = defaultdict(list)
    for linha in ces:
        por_ciclo[linha['ciclo_de_vida_id']].append(linha['numeroCEmercante'])
    return {ciclo['desembarque_id']: {**ciclo, 'familia': sorted(por_ciclo.get(ciclo['ID'], []))} for ciclo in ciclos}


def _resumo(ciclo: dict) -> dict:
    return {'conteiner': ciclo['numeroConteiner'], 'principal': ciclo['numeroCEmercante'],
            'confianca': ciclo['confianca_ce'], 'situacao': ciclo['situacao'], 'fluxo': ciclo['fluxo'],
            'familia': ciclo['familia']}


def _principais_gravados(gravado: dict) -> Dict[int, dict]:
    return {d: _resumo(c) for d, c in _com_familia(gravado['ciclos'], gravado['ces']).items()}


def portos_dos_recintos(caso: dict) -> Dict[str, str]:
    """O porto de cada recinto guardado na extração (o serviço o recalcula por dia; ver
    `CargaCiclo.portos_dos_recintos`) — {} nos casos extraídos antes de 23/10/2026."""
    return caso['meta'].get('portos_dos_recintos') or {}


def ciclos_do_servico(engine, caso: dict) -> Dict[int, dict]:
    """Roda o serviço (código atual, relógio na hora da extração) no SQLite do caso e devolve os ciclos gravados
    por ele: {desembarque: linha de `ciclodevida_ciclo_de_vida_importacao` + 'familia'}."""
    from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import Servico
    meta = caso['meta']
    agora = _data(meta['agora'])
    Servico(engine, relogio=lambda: agora, portos_dos_recintos=portos_dos_recintos(caso)).processa(
        meta['conteineres'], desde=_data(meta['desde']))
    with engine.connect() as conexao:
        return _com_familia([dict(linha._mapping) for linha in conexao.execute(select(gc.CICLOS))],
                            [dict(linha._mapping) for linha in conexao.execute(select(gc.CICLO_CE))])


def portos_do_caso(engine, caso: dict) -> Dict[int, Optional[str]]:
    """{desembarque: porto} como o serviço calcula (`CargaCiclo.portos_dos_desembarques`)."""
    meta = caso['meta']
    with engine.connect() as conexao:
        carga = CargaCiclo(ConsultaSQL(conexao), _data(meta['desde']), _data(meta['agora']))
        carga.eventos(meta['conteineres'])
        return carga.portos_dos_desembarques(portos_dos_recintos(caso))


def compara_com_o_servico(engine, caso: dict, saida: Saida) -> Dict[int, dict]:
    """Roda o serviço no SQLite do caso e compara o CE principal de cada desembarque com o gravado."""
    novos = {d: _resumo(c) for d, c in ciclos_do_servico(engine, caso).items()}
    gravados = _principais_gravados(caso['gravado'])
    saida.titulo('5. Serviço (código atual) x gravado')
    for desembarque in sorted(set(novos) | set(gravados)):
        novo, antigo = novos.get(desembarque, {}), gravados.get(desembarque, {})
        igual = ('só no reprocesso' if not antigo else 'só no gravado' if not novo
                 else 'igual' if novo.get('principal') == antigo.get('principal') else 'DIFERENTE')
        saida(f'desembarque {desembarque} {novo.get("conteiner") or antigo.get("conteiner")}: {igual} — atual '
              f'{novo.get("principal")} ({novo.get("confianca")}, {novo.get("situacao")}) família '
              f'{novo.get("familia")}; gravado {antigo.get("principal")} ({antigo.get("confianca")}, '
              f'{antigo.get("situacao")}) família {antigo.get("familia")}')
    return novos


def reprocessa(caso: dict, saida: Saida) -> Dict[int, dict]:
    """Relatório do caso, reprocesso explicado e comparação com o gravado; devolve os ciclos do serviço."""
    Relatorio(caso, saida).escreve_tudo()
    engine = banco_do_caso(caso)
    Reprocesso(engine, caso, saida).explica()
    return compara_com_o_servico(engine, caso, saida)


# ---------------------------------------------------------------------- linha de comando

def _argumentos(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--ces', nargs='*', default=[], help='CEs Mercante do caso')
    parser.add_argument('--conteineres', nargs='*', default=[], help='contêineres do caso (além dos itens dos CEs)')
    parser.add_argument('--desde', help=f'eventos desde (AAAA-MM-DD; padrão: {DIAS_PADRAO} dias atrás)')
    parser.add_argument('--reprocessa', help='só reprocessa um caso já extraído (JSON)')
    parser.add_argument('--casos', help='lote: arquivo com um caso por linha ("nome contêiner [contêiner ...]")')
    parser.add_argument('--pasta', default='casos_ciclo', help='pasta dos casos do lote (padrão: casos_ciclo)')
    parser.add_argument('--json', help='grava o caso extraído neste arquivo (padrão: caso_ciclo.json)')
    parser.add_argument('--apelida-placas', action='store_true', help='troca as placas por apelidos estáveis')
    parser.add_argument('--saida', help='grava o relatório neste arquivo (padrão: só na tela)')
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    args = parser.parse_args(argv)
    if not (args.reprocessa or args.casos or args.ces or args.conteineres):
        parser.error('informe --ces e/ou --conteineres, --casos ou --reprocessa')
    return args


def le_casos(linhas) -> List[Tuple[str, List[str]]]:
    """[(nome, contêineres)] das linhas "nome contêiner [contêiner ...]  # comentário" (vazias e só comentário
    ficam de fora)."""
    casos = []
    for linha in linhas:
        partes = linha.split('#', 1)[0].split()
        if len(partes) >= 2:
            casos.append((partes[0], [p.upper() for p in partes[1:]]))
        elif partes:
            raise ValueError(f'caso sem contêiner: {linha.strip()!r}')
    return casos


def _desde(args) -> datetime:
    return datetime.strptime(args.desde, '%Y-%m-%d') if args.desde else datetime.now() - timedelta(days=DIAS_PADRAO)


def _engine_de_extracao(args):  # pragma: no cover - depende do banco real
    from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import uri_do_banco
    from virasana.navii.mercante_por_id import limita_tempo
    engine = create_engine(uri_do_banco(args))
    limita_tempo(engine, LIMITE_SEGUNDOS)
    return engine


def _grava(caso: dict, caminho_json: str, caminho_log: Optional[str] = None):
    with open(caminho_json, 'w', encoding='utf-8') as arquivo:
        json.dump(caso, arquivo, ensure_ascii=False, indent=1)
    if caminho_log:
        with open(caminho_log, 'w', encoding='utf-8') as arquivo:
            reprocessa(caso, Saida(arquivo))


def extrai_lote(args):  # pragma: no cover - depende do banco real
    """Um JSON e um log por caso do arquivo --casos, na --pasta."""
    import os
    with open(args.casos, encoding='utf-8') as arquivo:
        casos = le_casos(arquivo)
    os.makedirs(args.pasta, exist_ok=True)
    with _engine_de_extracao(args).connect() as conexao:
        for nome, conteineres in casos:
            caso = extrai_caso(conexao, [], conteineres, _desde(args), apelidar_placas=args.apelida_placas)
            base = os.path.join(args.pasta, nome)
            _grava(caso, base + '.json', base + '.log')
            print(f'{nome}: {len(caso["gravado"]["ciclos"])} ciclo(s) gravado(s) -> {base}.json')


def _extrai(args) -> dict:  # pragma: no cover - depende do banco real
    with _engine_de_extracao(args).connect() as conexao:
        caso = extrai_caso(conexao, args.ces, args.conteineres, _desde(args), apelidar_placas=args.apelida_placas)
    caminho = args.json or 'caso_ciclo.json'
    _grava(caso, caminho)
    print(f'Caso gravado em {caminho}')
    return caso


def main(argv=None):  # pragma: no cover - depende do banco real
    args = _argumentos(argv)
    if args.casos:
        extrai_lote(args)
        return
    if args.reprocessa:
        with open(args.reprocessa, encoding='utf-8') as arquivo:
            caso = json.load(arquivo)
    else:
        caso = _extrai(args)
    arquivo = open(args.saida, 'w', encoding='utf-8') if args.saida else None
    try:
        reprocessa(caso, Saida(arquivo))
    finally:
        if arquivo:
            arquivo.close()


if __name__ == '__main__':  # pragma: no cover
    main()
