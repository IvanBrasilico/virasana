# coding: utf-8
"""
Indexador da busca por TEXTO do NAVI-I: mantém o índice `navii_ces` do Meilisearch a partir do Mercante (só lê o
MariaDB). Plano em `navii/CONTEXTO.md`, "Busca por texto"; documento e cliente em `navii/texto_navii.py`; instalação
em `navii/MEILISEARCH.md`.

Cada volta (o serviço repete a cada `--intervalo` segundos; seguido enquanto houver fila):
  1. CEs NOVOS pelo ID (`conhecimentosresumo`, chave primária), em páginas — a exportação fica fora do índice;
  2. ESCALAS alteradas (`escalasresumo` e `escalamanifestoresumo` pela `create_date`/`last_modified`, com 5 min de
     sobreposição): os CEs já indexados desses manifestos são refeitos (a data de referência pode ter mudado);
  3. RETENÇÃO, uma vez por dia: apaga os CEs criados há mais de 12 meses (`id < ID da data`).
Depois de o Meilisearch confirmar as escritas, grava as marcas.

Sem marcas, a 1ª volta começa a CARGA INICIAL: cria e configura o índice e põe a marca no 1º ID criado há 3 meses
(`mercante_por_id.id_na_data` — a `create_date` do CE é a da última alteração, nunca serve de janela); as voltas
seguintes, sem pausa, carregam até alcançar o último ID. As marcas ficam no próprio Meilisearch (índice
`navii_texto_estado`): se o índice de CEs sumir, a carga recomeça sozinha. O índice é derivado do MariaDB: perdeu,
reconstrói.

Alterações do próprio CE depois de indexado ficam de fora por enquanto (decisão de 18/10/2026; nos XMLs, o movimento
A muda o texto em 2% a 6% das vezes). CE excluído no Mercante continua no índice até a retenção (a tela ignora o CE
que não acha).

Uso (na raiz do repositório virasana, com SQL_URI e as variáveis NAVII_MEILI_*):
    MEILI_MASTER_KEY='...' python virasana/navii/indexador_texto.py --chaves   # uma vez: as chaves (UID fixo)
    python virasana/navii/indexador_texto.py --situacao
    python virasana/navii/indexador_texto.py --uma-vez --simula      # lê e monta, não escreve nada
    python virasana/navii/indexador_texto.py                          # serviço (supervisor_navii_texto.conf)
"""
import argparse
import json
import logging
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Set, Tuple

import requests
from sqlalchemy import create_engine, func, or_, select
from sqlalchemy.orm import sessionmaker

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.integracao.mercante.mercantealchemy import Conhecimento, Escala, EscalaManifesto  # noqa: E402
from virasana.navii import texto_navii as tn  # noqa: E402
from virasana.navii.busca_navii import Empresa  # noqa: E402  (bhadrasana.models.laudo, como o NAVI-I)
from virasana.navii.mercante_por_id import BLOCO_DATA, id_na_data, limita_tempo  # noqa: E402

logger = logging.getLogger('navii.indexador_texto')

MESES_CARGA = 3                  # carga inicial
MESES_RETENCAO = 12
LOTE = 2000                      # IDs por página
PAGINAS = 10                     # páginas por volta (depois espera o Meilisearch e grava as marcas)
LOTE_IN = 500                    # valores por IN (manifestos, CNPJs)
SOBREPOSICAO = timedelta(minutes=5)   # transação do ETL que grava depois com hora anterior
LIMITE_CONSULTA = 300            # s: max_statement_time de cada conexão no MariaDB
CHAVE_MARCAS = 'marcas'
COLUNAS = [Conhecimento.ID, Conhecimento.numeroCEmercante, Conhecimento.manifestoCE, Conhecimento.numeroCEMaster,
           Conhecimento.numConhecimento, Conhecimento.embarcador, Conhecimento.consignatario, Conhecimento.notify,
           Conhecimento.cnpjNotify, Conhecimento.descricao, Conhecimento.codigoEmpresaNavegacao,
           Conhecimento.portoOrigemCarga, Conhecimento.portoDestFinal, Conhecimento.tipoTrafego]


class ErroIndexador(Exception):
    pass


def _meses(n: int) -> timedelta:
    return timedelta(days=round(n * 30.44))


def _texto(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def _data(texto) -> Optional[datetime]:
    try:
        return datetime.strptime(_texto(texto)[:10], '%Y-%m-%d')
    except ValueError:
        return None


def _momento(valor) -> Optional[datetime]:
    if valor is None or isinstance(valor, datetime):
        return valor
    return datetime.fromisoformat(str(valor)[:19])


def _partes(valores, tamanho: int = LOTE_IN):
    valores = sorted(valores)
    for n in range(0, len(valores), tamanho):
        yield valores[n:n + tamanho]


class Indexador:
    """`fabrica`: cria uma sessão por volta (transação nova: vê os dados novos). `cliente`: ClienteMeili (ou o
    simulado dos testes). `simula`: lê e monta tudo, não escreve nada no Meilisearch (nem as marcas)."""

    def __init__(self, fabrica: Callable, cliente, porto: str = 'BRSSZ', relogio: Callable[[], datetime] = datetime.now,
                 lote: int = LOTE, paginas: int = PAGINAS, meses_carga: int = MESES_CARGA,
                 meses_retencao: int = MESES_RETENCAO, simula: bool = False, bloco_data: int = BLOCO_DATA):
        self.fabrica, self.cliente, self.porto, self.relogio = fabrica, cliente, porto, relogio
        self.lote, self.paginas, self.simula, self.bloco_data = lote, paginas, simula, bloco_data
        self.meses_carga, self.meses_retencao = meses_carga, meses_retencao

    # ---------------------------------------------------------------- marcas
    def marcas(self) -> Optional[dict]:
        return self.cliente.documento(tn.INDICE_ESTADO, CHAVE_MARCAS)

    def _grava_marcas(self, marcas: dict):
        self.cliente.espera(self.cliente.adiciona(tn.INDICE_ESTADO, [marcas], 'chave'))

    def _escreve(self, tarefas: List[dict], tarefa: Callable[[], dict]):
        if not self.simula:
            tarefas.append(tarefa())

    def prepara(self, session) -> dict:
        """As marcas; sem elas (ou sem o índice de CEs), começa a carga inicial. Confere a configuração do índice.
        Na simulação, sem Meilisearch (ainda não instalado), mede a carga inicial."""
        try:
            marcas = self.marcas()
        except (requests.RequestException, tn.ErroMeili):
            if not self.simula:
                raise
            logger.warning('Meilisearch inacessível: a simulação mede a carga inicial')
            return self.inicia(session)
        if marcas and self.cliente.indice(tn.INDICE) is None:
            logger.warning(f'o índice {tn.INDICE} não existe mais: recomeça a carga inicial')
            marcas = None
        if not marcas:
            return self.inicia(session)
        diferentes = tn.configuracao_difere(self.cliente.configuracao(tn.INDICE))
        if diferentes and not self.simula:
            logger.info(f'configuração do índice diferente em {diferentes}: atualiza (o Meilisearch reindexa)')
            self.cliente.espera(self.cliente.configura(tn.INDICE, tn.CONFIGURACAO))
        return marcas

    def inicia(self, session) -> dict:
        agora = self.relogio()
        id_inicial = id_na_data(session, agora - _meses(self.meses_carga), self.bloco_data)
        if id_inicial is None:
            raise ErroIndexador('nenhum CE no Mercante')
        escalas = self._ultima_alteracao_de_escala(session)
        marcas = {'chave': CHAVE_MARCAS, 'id_inicial': id_inicial, 'ultimo_id': id_inicial - 1,
                  'escalas_desde': escalas.isoformat() if escalas else None,
                  'retencao_em': agora.date().isoformat(), 'iniciado_em': agora.isoformat(timespec='seconds')}
        logger.info(f'carga inicial: CEs criados desde {agora - _meses(self.meses_carga):%d/%m/%Y} (ID {id_inicial})'
                    f'{" — SIMULAÇÃO" * self.simula}')
        if not self.simula:
            if self.cliente.indice(tn.INDICE) is None:
                self.cliente.espera(self.cliente.cria_indice(tn.INDICE, tn.CHAVE))
            else:       # índice sem marcas: tira o que ficou antes da janela (o resto é substituído na carga)
                self.cliente.espera(self.cliente.apaga_por_filtro(tn.INDICE, f'id < {id_inicial}'))
            self.cliente.espera(self.cliente.configura(tn.INDICE, tn.CONFIGURACAO))
            self._grava_marcas(marcas)
        return marcas

    @staticmethod
    def _ultima_alteracao_de_escala(session) -> Optional[datetime]:
        """A hora (do banco) da última inclusão/alteração de escala ou de vínculo escala-manifesto."""
        datas = [session.execute(select(func.max(coluna))).scalar()
                 for coluna in (Escala.create_date, Escala.last_modified, EscalaManifesto.create_date,
                                EscalaManifesto.last_modified)]
        datas = [_momento(d) for d in datas if d is not None]
        return max(datas) if datas else None

    # ---------------------------------------------------------------- documentos
    def _escalas(self, session, manifestos: Set[str]) -> Dict[str, list]:
        por_manifesto = defaultdict(list)
        for parte in _partes(manifestos):
            for manifesto, porto, data in session.execute(select(
                    EscalaManifesto.manifesto, Escala.portoDaEscala, Escala.dataPrevistaAtracacao).join(
                    Escala, Escala.numeroDaEscala == EscalaManifesto.escala).where(
                    EscalaManifesto.manifesto.in_(parte))):
                por_manifesto[_texto(manifesto)].append((_texto(porto), _data(data)))
        return por_manifesto

    def _nomes(self, session, cnpjs: Set[str]) -> Dict[str, str]:
        nomes = {}
        for parte in _partes(cnpjs):
            for cnpj, nome in session.execute(select(Empresa.cnpj, Empresa.nome).where(Empresa.cnpj.in_(parte))):
                if _texto(nome):
                    nomes[_texto(cnpj)] = ' '.join(_texto(nome).split())
        return nomes

    def documentos(self, session, ces: List) -> List[dict]:
        escalas = self._escalas(session, {_texto(ce.manifestoCE) for ce in ces if _texto(ce.manifestoCE)})
        nomes = self._nomes(session, {cnpj for ce in ces for cnpj in tn.cnpjs_do_ce(ce)})
        documentos = [tn.documento_do_ce(ce, nomes, tn.referencia(escalas.get(_texto(ce.manifestoCE), []), self.porto))
                      for ce in ces]
        return [d for d in documentos if d]

    def _envia(self, session, ces: List, tarefas: List[dict], estatisticas: Counter, rotulo: str):
        documentos = self.documentos(session, [ce for ce in ces if tn.e_importacao(ce)])
        if documentos:
            self._escreve(tarefas, lambda: self.cliente.adiciona(tn.INDICE, documentos, tn.CHAVE))
        estatisticas[rotulo] += len(documentos)

    # ---------------------------------------------------------------- 1. novos
    def novos(self, session, marcas: dict, tarefas: List[dict], estatisticas: Counter) -> bool:
        """Até `paginas` páginas de IDs depois da marca; devolve se ainda há fila (a última página veio cheia)."""
        ultimo, cheio = marcas['ultimo_id'], False
        for _ in range(self.paginas):
            linhas = session.execute(select(*COLUNAS).where(Conhecimento.ID > ultimo).order_by(
                Conhecimento.ID).limit(self.lote)).all()
            if not linhas:
                cheio = False
                break
            estatisticas['IDs lidos'] += len(linhas)
            self._envia(session, linhas, tarefas, estatisticas, 'CEs novos indexados')
            ultimo, cheio = linhas[-1].ID, len(linhas) == self.lote
            if not cheio:
                break
        marcas['ultimo_id'] = ultimo
        return cheio

    # ---------------------------------------------------------------- 2. escalas
    def escalas(self, session, marcas: dict, tarefas: List[dict], estatisticas: Counter):
        """Refaz os CEs JÁ indexados (ID entre as marcas) dos manifestos com escala ou vínculo novo/alterado."""
        if not marcas.get('escalas_desde'):
            ultima = self._ultima_alteracao_de_escala(session)
            marcas['escalas_desde'] = ultima.isoformat() if ultima else None
            return
        manifestos, vistas = self._manifestos_alterados(session, datetime.fromisoformat(marcas['escalas_desde']),
                                                        estatisticas)
        for parte in _partes(manifestos):
            linhas = session.execute(select(*COLUNAS).where(
                Conhecimento.manifestoCE.in_(parte), Conhecimento.ID >= marcas['id_inicial'],
                Conhecimento.ID <= marcas['ultimo_id'])).all()
            self._envia(session, linhas, tarefas, estatisticas, 'CEs refeitos por escala')
        if vistas:
            marcas['escalas_desde'] = max([datetime.fromisoformat(marcas['escalas_desde'])] + vistas).isoformat()

    @staticmethod
    def _manifestos_alterados(session, marca: datetime, estatisticas: Counter) -> Tuple[Set[str], List[datetime]]:
        """Manifestos com escala ou vínculo escala-manifesto incluído/alterado desde a marca (menos a sobreposição) e
        as horas vistas (do banco), para a marca nova."""
        desde = marca - SOBREPOSICAO
        alteradas = session.execute(select(Escala.numeroDaEscala, Escala.create_date, Escala.last_modified).where(
            or_(Escala.create_date >= desde, Escala.last_modified >= desde))).all()
        vinculos = session.execute(select(EscalaManifesto.manifesto, EscalaManifesto.create_date,
                                          EscalaManifesto.last_modified).where(
            or_(EscalaManifesto.create_date >= desde, EscalaManifesto.last_modified >= desde))).all()
        manifestos = {_texto(m) for m, _, _ in vinculos if _texto(m)}
        for parte in _partes({_texto(e) for e, _, _ in alteradas if _texto(e)}):
            manifestos |= {_texto(m) for (m,) in session.execute(select(EscalaManifesto.manifesto).where(
                EscalaManifesto.escala.in_(parte))) if _texto(m)}
        estatisticas['escalas alteradas'] += len(alteradas)
        estatisticas['manifestos com escala alterada'] += len(manifestos)
        vistas = [_momento(d) for linha in list(alteradas) + list(vinculos) for d in linha[1:] if d is not None]
        return manifestos, vistas

    # ---------------------------------------------------------------- 3. retenção
    def retencao(self, session, marcas: dict, tarefas: List[dict], estatisticas: Counter):
        agora = self.relogio()
        if marcas.get('retencao_em') == agora.date().isoformat():
            return
        limite = id_na_data(session, agora - _meses(self.meses_retencao), self.bloco_data)
        if limite is not None and limite > marcas['id_inicial']:
            self._escreve(tarefas, lambda: self.cliente.apaga_por_filtro(tn.INDICE, f'id < {limite}'))
            estatisticas['retenção: novo ID inicial'] = limite
            marcas['id_inicial'] = limite
        marcas['retencao_em'] = agora.date().isoformat()

    # ---------------------------------------------------------------- voltas
    def uma_volta(self) -> Tuple[Counter, bool]:
        inicio, estatisticas, tarefas = time.perf_counter(), Counter(), []
        with self.fabrica() as session:
            marcas = self.prepara(session)
            cheio = self.novos(session, marcas, tarefas, estatisticas)
            self.escalas(session, marcas, tarefas, estatisticas)
            self.retencao(session, marcas, tarefas, estatisticas)
        for tarefa in tarefas:          # erro aqui: as marcas não andam e a próxima volta refaz (substitui)
            self.cliente.espera(tarefa)
        if not self.simula:
            self._grava_marcas(marcas)
        logger.info(f'volta em {time.perf_counter() - inicio:.1f} s{" (SIMULAÇÃO: nada escrito)" * self.simula}: '
                    f'{dict(estatisticas)}; último ID {marcas["ultimo_id"]}{" (há fila)" * cheio}')
        return estatisticas, cheio

    def roda(self, intervalo: int = 120):  # pragma: no cover - laço infinito
        while True:
            try:
                _, cheio = self.uma_volta()
            except Exception:
                logger.exception('erro na volta; tenta de novo depois do intervalo')
                cheio = False
            if not cheio:
                time.sleep(intervalo)

    def situacao(self) -> dict:
        marcas = self.marcas() or {}
        with self.fabrica() as session:
            maior = session.execute(select(func.max(Conhecimento.ID))).scalar()
        estatisticas = self.cliente.estatisticas(tn.INDICE) or {}
        return {'marcas': marcas, 'último ID do Mercante': maior,
                'IDs por indexar': (maior - marcas['ultimo_id']) if maior and marcas else None,
                'documentos no índice': estatisticas.get('numberOfDocuments'),
                'indexando agora': estatisticas.get('isIndexing')}


def problema_no_ambiente(args, ambiente) -> Optional[str]:
    """O que falta no ambiente para o comando pedido (None se nada)."""
    if args.chaves:
        if not ambiente.get(tn.VARIAVEL_MESTRA):
            return (f'--chaves precisa da chave mestra (a de /etc/meilisearch.env) em {tn.VARIAVEL_MESTRA}, só nesta '
                    f"sessão do terminal: export {tn.VARIAVEL_MESTRA}='...'")
    elif not args.simula and not ambiente.get(tn.VARIAVEL_ESCRITA):
        return (f'{tn.VARIAVEL_ESCRITA} não está definida neste ambiente. Gere as chaves com --chaves (precisa de '
                f'{tn.VARIAVEL_MESTRA}) e defina a variável onde estão SQL_URI e as outras do indexador.')
    return None


def relatorio_de_chaves(chaves: tn.Chaves, url: str) -> str:
    """As linhas `export` (os valores se repetem a cada execução) e o que apagar ou conferir."""
    apaga = f'curl -X DELETE {url}/keys/{{uid}} -H "Authorization: Bearer ${tn.VARIAVEL_MESTRA}"'
    linhas = ['# Chaves do NAVI-I (UID fixo: o mesmo valor a cada execução, enquanto a chave mestra não mudar)']
    linhas += [f"export {variavel}='{valor}'" for variavel, valor in chaves.valores.items()]
    if chaves.criadas:
        linhas.append(f'# criadas agora: {", ".join(chaves.criadas)}')
    for variavel in chaves.divergentes:
        uid = tn.CHAVES[variavel]['uid']
        linhas.append(f'# ATENÇÃO: a chave de {variavel} tem ações ou índices diferentes da definição. Apague e rode '
                      f'--chaves de novo: {apaga.format(uid=uid)}')
    if chaves.avulsas:
        linhas.append('# Chaves "NAVI-I" avulsas (criadas antes, sem UID fixo) — não são usadas; podem ser apagadas:')
        linhas += [f'{apaga.format(uid=c["uid"])}   # {c.get("description")}, criada em {c.get("createdAt")}'
                   for c in chaves.avulsas]
    return '\n'.join(linhas)


def _argumentos(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--porto', default='BRSSZ', help='porto da data de referência (padrão BRSSZ)')
    parser.add_argument('--simula', action='store_true', help='lê e monta os documentos, não escreve nada')
    parser.add_argument('--uma-vez', action='store_true', help='uma volta só (sem laço)')
    parser.add_argument('--situacao', action='store_true', help='mostra as marcas e o tamanho do índice e sai')
    parser.add_argument('--chaves', action='store_true',
                        help=f'cria (se faltam) as chaves de busca e de escrita e mostra os valores; usa a chave '
                             f'mestra em {tn.VARIAVEL_MESTRA}. Não lê o banco')
    parser.add_argument('--intervalo', type=int, default=120, help='segundos entre as voltas sem fila (padrão 120)')
    parser.add_argument('--limite', type=float, default=LIMITE_CONSULTA,
                        help=f'segundos por consulta no MariaDB, 0 = sem limite (padrão {LIMITE_CONSULTA})')
    return parser.parse_args(argv)


def main(argv=None):  # pragma: no cover - depende do banco e do Meilisearch reais
    args = _argumentos(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s', stream=sys.stdout)
    problema = problema_no_ambiente(args, os.environ)
    if problema:
        sys.exit(problema)
    if args.chaves:
        mestre = tn.cliente_do_ambiente(tn.VARIAVEL_MESTRA)
        print(relatorio_de_chaves(tn.garante_chaves(mestre), mestre.url))
        return
    uri = args.uri
    if not uri:
        from ajna_commons.flask.conf import SQL_URI as uri
    engine = create_engine(uri, pool_pre_ping=True)
    limita_tempo(engine, args.limite)
    cliente = tn.cliente_do_ambiente(tn.VARIAVEL_ESCRITA)
    logger.info(f'Indexador da busca por texto — banco {engine.url.render_as_string(hide_password=True)}, '
                f'Meilisearch {cliente.url}{" — SIMULAÇÃO" * args.simula}')
    indexador = Indexador(sessionmaker(bind=engine), cliente, porto=args.porto, simula=args.simula)
    if args.situacao:
        print(json.dumps(indexador.situacao(), ensure_ascii=False, indent=2, default=str))
    elif args.uma_vez:
        indexador.uma_volta()
    else:
        indexador.roda(args.intervalo)


if __name__ == '__main__':  # pragma: no cover
    main()
