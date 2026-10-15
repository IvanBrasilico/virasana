# coding: utf-8
"""
SERVIÇO do Ciclo de Vida v2 (etapa 5 do plano — CICLO_DE_VIDA.md): processamento orientado a eventos.

Substitui o loop de `manager_ciclo_de_vida.py` (que fica intocado, para rollback). Os dois NUNCA devem
rodar juntos: a troca é parar o programa antigo no supervisor e iniciar este (ver
`supervisor_ciclo_de_vida.conf` e a seção "Ativação no Servidor" do CICLO_DE_VIDA.md).

A cada volta:
  1. lê os eventos NOVOS da API Recintos — desembarque/embarque, acessos efetivados, inspeções e
     pesagens — pela marca d'água de cada tabela (último id lido, `ciclodevida_marca_dagua`); os
     contêineres desses eventos são os afetados;
  2. junta os contêineres a REVISITAR: ciclos v2 abertos (prazos: trânsito de 48 h, estada curta,
     abandono em 93 dias), processados há mais de 6 h, e ciclos v2 dos últimos 30 dias sem CE (o CE chega
     depois ao Mercante), há mais de 24 h;
  3. para cada lote de contêineres, numa transação: carrega TODOS os eventos dos últimos dias
     (`carga_ciclo`; recuando até o desembarque dos ciclos ainda abertos do lote, que podem passar disso), refaz
     a linha do tempo e os CEs (`heuristica_ciclo`) e grava (`gravacao_ciclo`) — o resultado depende só dos
     eventos e de "agora", então refazer é seguro;
  4. avança as marcas d'água. Se uma tabela tinha mais eventos que o lote, a próxima volta começa logo;
     senão, espera o intervalo;
  5. etapa do RISCO HISTÓRICO POR CE (`virasana/risco_historico/etapa.py`, 14/10/2026): sinais dos CEs novos,
     parâmetros (no máximo 1x/h) e pontos — em transações próprias, com limite de tempo; qualquer erro dela só
     vai ao log, o ciclo segue. Desligada com `--sem-risco` e em `--simula`.

Escopo: desembarques nos operadores de Santos (`ovr_recintos.e_operador` com `cod_unidade = 0817800`,
inclui o Ecoporto). Na primeira volta (sem marcas), lê os eventos desde `--desde` (padrão: 7 dias).

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    SERVICO=virasana/analytic_scripts/ciclo_de_vida/servico_ciclo_de_vida.py
    virasana-venv/bin/python $SERVICO --homologacao --uma-vez --simula   # mede, não grava
    virasana-venv/bin/python $SERVICO --homologacao --uma-vez            # uma volta, gravando
    virasana-venv/bin/python $SERVICO                                    # contínuo (supervisor)
    virasana-venv/bin/python $SERVICO --reprocessa 5000                  # etapa 6: pendentes antigos
"""
import argparse
import logging
import os
import sys
import time
from collections import Counter
from datetime import datetime, timedelta
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy import and_, create_engine, func, or_, select, text

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida.carga_ciclo import (  # noqa: E402
    CargaCiclo, ConsultaSQL, manifesto_vazio,
)
from virasana.analytic_scripts.ciclo_de_vida.gravacao_ciclo import (  # noqa: E402
    CICLOS, MARCAS, VERSAO, Gravador, ItemGravacao, linha_ciclo,
)
from virasana.analytic_scripts.ciclo_de_vida.heuristica_ciclo import processa_conteiner  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt  # noqa: E402

logger = logging.getLogger('ciclo_de_vida_v2')

# Eventos novos de cada tabela (o SQL é fixo; marca e lote vão como parâmetro)
EVENTOS_NOVOS = {
    'apirecintos_embarquedesembarque':
        'SELECT id, numeroConteiner FROM apirecintos_embarquedesembarque WHERE id > :marca ORDER BY id LIMIT :lote',
    'apirecintos_acessosveiculo':
        'SELECT id, numeroConteiner FROM apirecintos_acessosveiculo '
        "WHERE id > :marca AND operacao = 'C' ORDER BY id LIMIT :lote",
    'apirecintos_inspecoesnaoinvasivas':
        'SELECT id, numeroConteiner FROM apirecintos_inspecoesnaoinvasivas WHERE id > :marca ORDER BY id LIMIT :lote',
    'apirecintos_pesagensveiculo':
        'SELECT id, numeroConteiner FROM apirecintos_pesagensveiculo WHERE id > :marca ORDER BY id LIMIT :lote',
}
# Marca inicial: o id anterior ao primeiro evento desde a data (ou o último id, se não houver evento)
PRIMEIRO_DESDE = {
    'apirecintos_embarquedesembarque':
        'SELECT MIN(id), (SELECT MAX(id) FROM apirecintos_embarquedesembarque) '
        'FROM apirecintos_embarquedesembarque WHERE dataHoraOcorrencia >= :desde',
    'apirecintos_acessosveiculo':
        'SELECT MIN(id), (SELECT MAX(id) FROM apirecintos_acessosveiculo) '
        'FROM apirecintos_acessosveiculo WHERE dataHoraOcorrencia >= :desde',
    'apirecintos_inspecoesnaoinvasivas':
        'SELECT MIN(id), (SELECT MAX(id) FROM apirecintos_inspecoesnaoinvasivas) '
        'FROM apirecintos_inspecoesnaoinvasivas WHERE dataHoraOcorrencia >= :desde',
    'apirecintos_pesagensveiculo':
        'SELECT MIN(id), (SELECT MAX(id) FROM apirecintos_pesagensveiculo) '
        'FROM apirecintos_pesagensveiculo WHERE dataHoraOcorrencia >= :desde',
}


class Servico:
    """Uma instância por processo. `relogio` (padrão `datetime.now`) é o "agora" — os testes o fixam."""

    def __init__(self, engine, mongo_db=None, simula: bool = False, dias_eventos: int = 100,
                 lote_eventos: int = 20000, lote_conteineres: int = 500, revisita_horas: float = 6,
                 revisita_sem_ce_horas: float = 24, revisita_limite: int = 2000, dias_sem_ce: int = 30,
                 dias_maximos_aberto: int = 400, relogio: Callable[[], datetime] = datetime.now,
                 risco: bool = False):
        self.engine, self.mongo_db, self.simula = engine, mongo_db, simula
        self.dias_eventos, self.lote_eventos, self.lote_conteineres = dias_eventos, lote_eventos, lote_conteineres
        self.dias_maximos_aberto = dias_maximos_aberto
        self.revisita_horas, self.revisita_sem_ce_horas = revisita_horas, revisita_sem_ce_horas
        self.revisita_limite, self.dias_sem_ce = revisita_limite, dias_sem_ce
        self.relogio = relogio
        self.etapa_risco = self._carrega_etapa_risco(engine, relogio) if risco and not simula else None

    @staticmethod
    def _carrega_etapa_risco(engine, relogio):
        """Etapa do risco histórico por CE; se nem carregar, fica desligada (o ciclo de vida segue)."""
        try:
            from virasana.risco_historico.etapa import EtapaRisco
            return EtapaRisco(engine, relogio)
        except Exception:  # noqa: o ciclo não depende do risco
            logger.exception('risco histórico: etapa desligada (erro ao carregar)')
            return None

    def _etapa_risco(self):
        if self.etapa_risco is None:
            return
        inicio = time.perf_counter()
        try:
            estatisticas = self.etapa_risco.executa()
            logger.info(f'risco histórico em {time.perf_counter() - inicio:.1f} s: {dict(estatisticas)}')
        except Exception:  # noqa: o ciclo não depende do risco
            logger.exception('risco histórico: erro na etapa (o ciclo de vida segue)')

    # ---------------------------------------------------------------- marcas d'água
    def marcas(self, conexao, desde: datetime) -> Dict[str, int]:
        """Marcas gravadas; a tabela sem marca começa no primeiro evento desde `desde`."""
        gravadas = dict(conexao.execute(select(MARCAS.c.tabela, MARCAS.c.ultimo_id)).all())
        for tabela, sql in PRIMEIRO_DESDE.items():
            if tabela not in gravadas:
                primeiro, ultimo = conexao.execute(text(sql), {'desde': desde}).one()
                gravadas[tabela] = (primeiro - 1) if primeiro is not None else (ultimo or 0)
                logger.info(f'marca inicial de {tabela}: {gravadas[tabela]} (eventos desde {desde:%Y-%m-%d})')
        return gravadas

    def grava_marcas(self, conexao, marcas: Dict[str, int]):
        agora = self.relogio()
        gravadas = {t for t, in conexao.execute(select(MARCAS.c.tabela))}
        for tabela, ultimo_id in marcas.items():
            valores = {'ultimo_id': ultimo_id, 'atualizado_em': agora}
            if tabela in gravadas:
                conexao.execute(MARCAS.update().where(MARCAS.c.tabela == tabela).values(**valores))
            else:
                conexao.execute(MARCAS.insert().values(tabela=tabela, **valores))

    def eventos_novos(self, conexao, marcas: Dict[str, int]) -> Tuple[Set[str], Dict[str, int], bool]:
        """(contêineres afetados, novas marcas, alguma tabela tinha mais que um lote?)."""
        conteineres, novas, cheio = set(), dict(marcas), False
        for tabela, sql in EVENTOS_NOVOS.items():
            linhas = conexao.execute(text(sql), {'marca': marcas[tabela], 'lote': self.lote_eventos}).all()
            if linhas:
                novas[tabela] = max(id_ for id_, _ in linhas)
                conteineres |= {c.strip() for _, c in linhas if c and c.strip()}
            cheio = cheio or len(linhas) >= self.lote_eventos
        return conteineres, novas, cheio

    # ---------------------------------------------------------------- revisita e reprocessamento
    def revisitar(self, conexao) -> Set[str]:
        """Contêineres de ciclos v2 abertos (prazos) processados há mais de `revisita_horas`, e de ciclos
        recentes sem CE (o CE chega depois ao Mercante; o vazio não tem CE) há mais de `revisita_sem_ce_horas`."""
        agora = self.relogio()
        abertos = and_(CICLOS.c.situacao == ldt.ABERTO,
                       CICLOS.c.processado_em < agora - timedelta(hours=self.revisita_horas))
        sem_ce = and_(CICLOS.c.numeroCEmercante.is_(None), or_(CICLOS.c.vazio.is_(None), CICLOS.c.vazio.is_(False)),
                      CICLOS.c.data_desembarque >= agora - timedelta(days=self.dias_sem_ce),
                      CICLOS.c.processado_em < agora - timedelta(hours=self.revisita_sem_ce_horas))
        consulta = (select(CICLOS.c.numeroConteiner, func.min(CICLOS.c.processado_em).label('processado'))
                    .where(CICLOS.c.versao_heuristica == VERSAO, or_(abertos, sem_ce))
                    .group_by(CICLOS.c.numeroConteiner).order_by('processado').limit(self.revisita_limite))
        return {c.strip() for c, _ in conexao.execute(consulta) if c}

    def pendentes_antigos(self, conexao, limite: int) -> Tuple[Set[str], Optional[datetime]]:
        """Contêineres de ciclos do loop antigo ainda pendentes (mais recentes primeiro) e o desembarque mais
        antigo entre eles."""
        consulta = (select(CICLOS.c.numeroConteiner, func.min(CICLOS.c.data_desembarque).label('primeiro'),
                           func.max(CICLOS.c.data_desembarque).label('ultimo'))
                    .where(CICLOS.c.versao_heuristica.is_(None),
                           or_(CICLOS.c.concluido.is_(None), CICLOS.c.concluido.is_(False)),
                           CICLOS.c.numeroConteiner.isnot(None), CICLOS.c.data_desembarque.isnot(None))
                    .group_by(CICLOS.c.numeroConteiner).order_by(func.max(CICLOS.c.data_desembarque).desc())
                    .limit(limite))
        linhas = conexao.execute(consulta).all()
        primeiro = min((_data(p) for _, p, _ in linhas), default=None)
        return {c.strip() for c, _, _ in linhas if c and c.strip()}, primeiro

    # ---------------------------------------------------------------- processamento
    def processa(self, conteineres: Iterable[str], desde: Optional[datetime] = None) -> Counter:
        """Refaz e grava os ciclos dos contêineres, em lotes — uma transação por lote (desfeita na simulação)."""
        conteineres, estatisticas = sorted(set(conteineres)), Counter()
        for i in range(0, len(conteineres), self.lote_conteineres):
            lote = conteineres[i:i + self.lote_conteineres]
            with self.engine.connect() as conexao:
                transacao = conexao.begin()
                try:
                    estatisticas.update(self._processa_lote(conexao, lote, desde))
                except Exception:
                    transacao.rollback()
                    raise
                if self.simula:
                    transacao.rollback()
                else:
                    transacao.commit()
        estatisticas['contêineres'] += len(conteineres)
        return estatisticas

    def _desde_do_lote(self, conexao, conteineres: List[str], desde: Optional[datetime]) -> datetime:
        """Início da carga de eventos do lote: `desde` (ou `dias_eventos` atrás), recuando até o desembarque mais
        antigo dos ciclos v2 ainda ABERTOS desses contêineres — o abandono conta do início da estada atual (ex.:
        a entrada no seco), então um ciclo pode ficar aberto bem mais que `dias_eventos` depois do desembarque;
        sem o desembarque na carga, ele não seria mais refeito (e nunca fecharia). Limite: `dias_maximos_aberto`."""
        agora = self.relogio()
        inicio = desde or agora - timedelta(days=self.dias_eventos)
        mais_antigo = _data(conexao.execute(
            select(func.min(CICLOS.c.data_desembarque))
            .where(CICLOS.c.numeroConteiner.in_(conteineres), CICLOS.c.situacao == ldt.ABERTO,
                   CICLOS.c.versao_heuristica == VERSAO)).scalar())
        if mais_antigo is None:
            return inicio
        return min(inicio, max(mais_antigo, agora - timedelta(days=self.dias_maximos_aberto)))

    def _processa_lote(self, conexao, conteineres: List[str], desde: Optional[datetime]) -> Counter:
        agora = self.relogio()
        carga = CargaCiclo(ConsultaSQL(conexao), self._desde_do_lote(conexao, conteineres, desde), agora)
        escopo, operadores, _ = carga.operadores()
        eventos, portuarios = carga.eventos(conteineres)
        candidatos, vazios = carga.candidatos(conteineres), carga.vazios(conteineres)
        conhecidos = carga.conhecidos([e for lista in eventos.values() for e in lista])
        itens: Dict[int, ItemGravacao] = {}
        for conteiner in conteineres:
            for ciclo, ces in processa_conteiner(eventos.get(conteiner, []), candidatos.get(conteiner, []), escopo,
                                                 operadores | portuarios, agora, conhecidos=conhecidos):
                linha = linha_ciclo(conteiner, ciclo, ces, carga.desembarques.get(ciclo.desembarque.id, {}),
                                    carga.info, manifesto_vazio(vazios.get(conteiner, {}), ciclo.desembarque.data),
                                    carga.tipo_do(conteiner, ciclo.desembarque.id), agora)
                itens[ciclo.desembarque.id] = ItemGravacao(conteiner, ciclo, ces, linha)
        estatisticas = Gravador(conexao, self.mongo_db).grava(list(itens.values()))
        estatisticas.update(Counter(f'situação {i.ciclo.situacao}' for i in itens.values()))
        return estatisticas

    # ---------------------------------------------------------------- voltas
    def uma_volta(self, desde: Optional[datetime] = None) -> Tuple[Counter, bool]:
        """Uma volta (eventos novos + revisita); devolve (estatísticas, havia mais eventos que o lote?)."""
        inicio = time.perf_counter()
        desde = desde or self.relogio() - timedelta(days=7)
        with self.engine.connect() as conexao:
            marcas = self.marcas(conexao, desde)
            novos, novas_marcas, cheio = self.eventos_novos(conexao, marcas)
            revisita = self.revisitar(conexao) - novos
        estatisticas = self.processa(novos | revisita)
        estatisticas.update({'contêineres com evento novo': len(novos), 'contêineres revisitados': len(revisita)})
        if not self.simula:
            with self.engine.begin() as conexao:
                self.grava_marcas(conexao, novas_marcas)
        logger.info(f'volta em {time.perf_counter() - inicio:.1f} s{" (SIMULAÇÃO: nada gravado)" * self.simula}: '
                    f'{dict(estatisticas)}')
        self._etapa_risco()
        return estatisticas, cheio

    def roda(self, intervalo: int = 300, desde: Optional[datetime] = None):  # pragma: no cover - laço infinito
        while True:
            try:
                _, cheio = self.uma_volta(desde)
            except Exception:
                logger.exception('erro na volta; tenta de novo depois do intervalo')
                cheio = False
            if not cheio:
                time.sleep(intervalo)

    def reprocessa(self, limite: int) -> Counter:
        """Etapa 6: reprocessa (v2) os contêineres de ciclos pendentes do loop antigo, os mais recentes primeiro."""
        with self.engine.connect() as conexao:
            conteineres, primeiro = self.pendentes_antigos(conexao, limite)
        if not conteineres:
            logger.info('reprocessamento: nenhum ciclo pendente do loop antigo')
            return Counter()
        desde = min(primeiro, self.relogio() - timedelta(days=self.dias_eventos))   # a carga recua mais 2 dias
        estatisticas = self.processa(conteineres, desde=desde)
        with self.engine.connect() as conexao:
            restantes = conexao.execute(select(func.count()).select_from(CICLOS).where(
                CICLOS.c.versao_heuristica.is_(None),
                or_(CICLOS.c.concluido.is_(None), CICLOS.c.concluido.is_(False)))).scalar()
        estatisticas['pendentes antigos restantes'] = restantes
        logger.info(f'reprocessamento{" (SIMULAÇÃO)" * self.simula}: {dict(estatisticas)}')
        return estatisticas


def _data(valor) -> Optional[datetime]:
    if valor is None or isinstance(valor, datetime):
        return valor
    return datetime.fromisoformat(str(valor)[:19])


def uri_do_banco(args, ambiente=os.environ) -> str:
    """--uri explícita; --homologacao usa SQL_URI_HOM (como o wsgi_staging do bhadrasana2); senão, SQL_URI."""
    if args.uri:
        return args.uri
    if args.homologacao:
        return ambiente['SQL_URI_HOM']
    from ajna_commons.flask.conf import SQL_URI
    return SQL_URI


def _argumentos(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    parser.add_argument('--simula', action='store_true', help='processa e desfaz: não grava nada (nem as marcas)')
    parser.add_argument('--uma-vez', action='store_true', help='uma volta só (sem laço)')
    parser.add_argument('--intervalo', type=int, default=300, help='segundos entre as voltas sem fila (padrão 300)')
    parser.add_argument('--desde', type=lambda v: datetime.strptime(v, '%Y-%m-%d'),
                        help='primeira volta (sem marcas): eventos desde esta data (padrão: 7 dias atrás)')
    parser.add_argument('--reprocessa', type=int, metavar='N',
                        help='etapa 6: reprocessa N contêineres de ciclos pendentes do loop antigo e sai')
    parser.add_argument('--sem-mongo', action='store_true', help='não procura as imagens no Mongo')
    parser.add_argument('--sem-risco', action='store_true',
                        help='desliga a etapa do risco histórico por CE (virasana/risco_historico/etapa.py)')
    return parser.parse_args(argv)


def main(argv=None):  # pragma: no cover - depende do banco real
    args = _argumentos(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s', stream=sys.stdout)
    mongo_db = None
    if not args.sem_mongo:
        from pymongo import MongoClient
        from ajna_commons.flask.conf import DATABASE, MONGODB_URI
        mongo_db = MongoClient(host=MONGODB_URI)[DATABASE]
    engine = create_engine(uri_do_banco(args), pool_pre_ping=True)
    logger.info(f'Ciclo de Vida v2 — banco {engine.url.render_as_string(hide_password=True)}'
                f'{" — SIMULAÇÃO" * args.simula}')
    servico = Servico(engine, mongo_db, simula=args.simula, risco=not args.sem_risco)
    if args.reprocessa:
        servico.reprocessa(args.reprocessa)
    elif args.uma_vez:
        servico.uma_volta(args.desde)
    else:
        servico.roda(args.intervalo, args.desde)


if __name__ == '__main__':  # pragma: no cover
    main()
