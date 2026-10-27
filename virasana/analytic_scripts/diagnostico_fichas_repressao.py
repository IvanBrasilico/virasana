# coding: utf-8
"""
Diagnóstico das FICHAS DE REPRESSÃO NA IMPORTAÇÃO (Fichas, `tipooperacao = 1`) — fase 0 do plano "NAVI-I → Ficha"
(17/10/2026): antes de abrir Fichas pelo card do NAVI-I e de montar a aba de acompanhamento, mede como as Fichas desse
tipo são usadas hoje (roteiro base do manual do Fichas: analista abre e encaminha ao chefe -> chefe programa a RVF e
atribui o auditor -> verificação física informada com texto e fotos -> providências -> encerramento).

  1. catálogo ATUAL dos tipos de evento (id, nome, fase, evento especial, ordem) e o uso de cada um nas Fichas do
     período — os ids variam entre bases: o código deve se guiar pelo evento especial (`EventoEspecial`);
  2. as Fichas do período: por mês, fase, status atual (último evento), setor, CE/CNPJ, responsável x auditor x quem
     abriu, flags; e o total dos outros tipos de Ficha, para comparar;
  3. RVFs: quantas por Ficha (contêineres), com contêiner, de inspeção não invasiva, imagens por RVF (5 ou mais geram
     "Verificação física informada") e quantas Fichas vieram do atalho antigo `/nova_inspecaoni`;
  4. o fluxo pelos eventos (sem os excluídos): sequências mais comuns e o tempo até cada marco (atribuição, auditor,
     inspeção não invasiva, bloqueio de CE, verificação física agendada e INFORMADA — o fim do acompanhamento da aba
     —, interrupção de despacho, encerramento);
  5. as ABERTAS (fase < 3) sem verificação física informada — a população da aba — por setor, idade e status;
  6. o ciclo de vida dos contêineres dessas Fichas: estágio — "entregue/embarcado sem verificação física informada"
     é o alerta mais importante da aba;
  7. textos: tamanho das observações (o campo tem 1.000 caracteres) e os inícios mais comuns (dígitos mascarados);
  8. exemplos de Fichas abertas com a linha do tempo dos eventos.

Somente leitura. CPF nunca sai completo e nomes de pessoas não são impressos (sigilo/LGPD); nos textos livres, os
dígitos saem como '#'. Consultas portáveis (MariaDB em produção, SQLite nos testes).

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    python virasana/analytic_scripts/diagnostico_fichas_repressao.py --saida fichas_repressao.log
    python virasana/analytic_scripts/diagnostico_fichas_repressao.py --meses 36 --exemplos 12
"""
import argparse
import re
import statistics
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Dict, List, Optional

from sqlalchemy import create_engine

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida import estagios_ciclo  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.carga_ciclo import _dt  # noqa: E402
from virasana.analytic_scripts.diagnostico_risco_perdimentos import Diagnostico, mascara  # noqa: E402

TIPO_REPRESSAO_IMPORTACAO = 1
# Cópias de bhadrasana.models.ovr (não importado: carregaria o registry do Fichas)
TIPOS_OPERACAO = ('Mercadoria Abandonada', 'Repressão na importação', 'Repressão na exportação',
                  'Pesquisa e seleção na importação', 'Pesquisa e seleção na exportação', 'Demanda externa',
                  'Registro de operação de outros países', 'Vigilância', 'Registro de operação de outros órgãos')
FASES = ('Iniciada', 'Ativa', 'Suspensa', 'Concluída', 'Arquivada')
ESPECIAIS = {1: 'Responsavel', 2: 'RVF (verificação física informada)', 3: 'TG', 4: 'Autuacao',
             5: 'AuditorResponsavel', 6: 'MudancaSetor', 7: 'InspecaoNaoInvasiva', 8: 'EncerramentoComResultado',
             9: 'EncerramentoSemResultado'}
VERIFICACAO_INFORMADA = 2      # EventoEspecial.RVF: o Fichas gera ao informar a RVF com 5 ou mais imagens
FASE_CONCLUIDA = 3             # fase >= 3: concluída/arquivada (o Fichas bloqueia evento comum)
ATALHO_ANTIGO = re.compile(r'^Inspe..o n.o invasiva .*automaticamente registrada', re.IGNORECASE)
LOTE = 500
FAIXAS_IDADE = ((7, 'até 7 dias'), (30, '8 a 30 dias'), (90, '31 a 90 dias'), (365, '91 a 365 dias'),
                (None, 'mais de 1 ano'))


def sem_acento(texto: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', texto or '') if unicodedata.category(c) != 'Mn').lower()


def mascara_texto(texto: Optional[str], limite: int = 60) -> str:
    """Texto livre para o log: dígitos viram '#' (CPF, CNPJ, telefone...) e corta em `limite`."""
    texto = ' '.join((texto or '').split())
    texto = re.sub(r'\d', '#', texto)
    return texto[:limite] + ('…' if len(texto) > limite else '')


def _texto(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def mascara_cpf(cpf: Optional[str]) -> str:
    cpf = (cpf or '').strip()
    return mascara(cpf) if cpf else '-'


def faixa_idade(dias: float) -> str:
    for limite, rotulo in FAIXAS_IDADE:
        if limite is None or dias <= limite:
            return rotulo
    return FAIXAS_IDADE[-1][1]


def percentis(valores: List[float]) -> str:
    """'mediana X d, p75 Y d, p90 Z d' (dias) ou '-'."""
    if not valores:
        return '-'
    valores = sorted(valores)

    def p(fracao):
        return valores[min(len(valores) - 1, int(round(fracao * (len(valores) - 1))))]
    return f'mediana {statistics.median(valores):.1f} d, p75 {p(.75):.1f} d, p90 {p(.9):.1f} d'


def pct(parte: int, total: int) -> str:
    return f'{parte} ({100 * parte / total:.0f}%)' if total else str(parte)


# Marcos do fluxo: nome -> regra sobre o tipo de evento (id, nome sem acento, especial)
MARCOS = (
    ('atribuição de responsável', lambda t: t.especial == 1),
    ('auditor responsável', lambda t: t.especial == 5),
    ('inspeção não invasiva', lambda t: t.especial == 7),
    ('bloqueio de CE', lambda t: t.nome.startswith('bloqueio de ce')),
    ('verificação física agendada', lambda t: 'verificacao fisica agendada' in t.nome),
    ('VERIFICAÇÃO FÍSICA INFORMADA', lambda t: t.especial == VERIFICACAO_INFORMADA),
    ('interrupção de despacho', lambda t: 'interrupcao de despacho' in t.nome),
    ('encerramento ou auto', lambda t: t.especial in (4, 8, 9)),
)


class DiagnosticoFichas(Diagnostico):
    """Lê uma vez as Fichas do tipo no período (com eventos, RVFs e flags) e escreve as seções."""

    def __init__(self, conexao, saida, tipo: int = TIPO_REPRESSAO_IMPORTACAO, meses: int = 24,
                 agora: Optional[datetime] = None, exemplos: int = 8):
        super().__init__(conexao, saida)
        self.agora = agora or datetime.now()
        self.tipo, self.exemplos = tipo, exemplos
        self.desde = self.agora - timedelta(days=round(meses * 30.44))
        self.params = {'tipo': tipo, 'desde': self.desde}
        self.tipos: Dict[int, SimpleNamespace] = {}
        self.fichas: Dict[int, SimpleNamespace] = {}
        self.eventos: Dict[int, list] = defaultdict(list)
        self.rvfs: Dict[int, list] = defaultdict(list)
        self.flags: Dict[int, list] = defaultdict(list)
        self.setores: Dict[str, str] = {}

    # ---------------------------------------------------------------- carga
    # Universo (escrito por extenso em cada SQL, sem montar texto): Fichas do tipo, abertas desde a data.
    SQL_FICHAS = ('SELECT o.id, o.numero, o.ano, o.fase, o.tipoevento_id, o.setor_id, o.numeroCEmercante, '
                  'o.cnpj_fiscalizado, o.user_name, o.responsavel_cpf, o.cpfauditorresponsavel, '
                  'COALESCE(o.datahora, o.create_date), o.observacoes FROM ovr_ovrs o '
                  'WHERE o.tipooperacao = :tipo AND COALESCE(o.datahora, o.create_date) >= :desde')
    SQL_EVENTOS = ('SELECT e.ovr_id, e.create_date, e.tipoevento_id, e.meramente_informativo, e.excluido, e.motivo, '
                   'e.user_name FROM ovr_eventos e JOIN ovr_ovrs o ON o.id = e.ovr_id '
                   'WHERE o.tipooperacao = :tipo AND COALESCE(o.datahora, o.create_date) >= :desde')
    SQL_RVFS = ('SELECT r.id, r.ovr_id, r.numerolote, r.numeroCEmercante, r.inspecaonaoinvasiva, r.datahora, '
                '(SELECT COUNT(*) FROM ovr_imagensrvf i WHERE i.rvf_id = r.id) '
                'FROM ovr_verificacoesfisicas r JOIN ovr_ovrs o ON o.id = r.ovr_id '
                'WHERE o.tipooperacao = :tipo AND COALESCE(o.datahora, o.create_date) >= :desde')
    SQL_FLAGS = ('SELECT fo.rvf_id, f.nome FROM ovr_flags_ovr fo JOIN ovr_flags f ON f.id = fo.flag_id '
                 'JOIN ovr_ovrs o ON o.id = fo.rvf_id '
                 'WHERE o.tipooperacao = :tipo AND COALESCE(o.datahora, o.create_date) >= :desde')

    def carrega(self):
        self._carrega_tipos()
        self.setores = {_texto(i): _texto(n) for i, n in self.consulta('SELECT id, nome FROM ovr_setores')}
        self._carrega_fichas()
        for ovr_id, data, tipo, informativo, excluido, motivo, usuario in self.consulta(self.SQL_EVENTOS):
            self.eventos[ovr_id].append(SimpleNamespace(data=_dt(data), tipo=tipo, informativo=bool(informativo),
                                                        excluido=bool(excluido), motivo=motivo, usuario=usuario))
        for lista in self.eventos.values():
            lista.sort(key=lambda e: (e.data or datetime.min))
        for id_, ovr_id, lote, ce, ini, data, imagens in self.consulta(self.SQL_RVFS):
            self.rvfs[ovr_id].append(SimpleNamespace(id=id_, lote=_texto(lote).upper(), ce=_texto(ce),
                                                     inspecao=bool(ini), data=_dt(data), imagens=int(imagens or 0)))
        for ovr_id, nome in self.consulta(self.SQL_FLAGS):
            self.flags[ovr_id].append(nome)

    def _carrega_tipos(self):
        for id_, nome, fase, especial, ordem in self.consulta(
                'SELECT id, nome, fase, eventoespecial, ordem FROM ovr_tiposevento'):
            self.tipos[id_] = SimpleNamespace(id=id_, nome_original=_texto(nome), nome=sem_acento(nome),
                                              fase=fase, especial=especial, ordem=ordem)

    def _carrega_fichas(self):
        for linha in self.consulta(self.SQL_FICHAS):
            id_, numero, ano, fase, tipoevento, setor, ce, cnpj, criador, responsavel, auditor, data, obs = linha
            self.fichas[id_] = SimpleNamespace(
                id=id_, numero=numero, ano=ano, fase=fase or 0, tipoevento=tipoevento, setor=_texto(setor),
                ce=_texto(ce), cnpj=_texto(cnpj), criador=_texto(criador), responsavel=_texto(responsavel),
                auditor=_texto(auditor), data=_dt(data), observacoes=obs or '')

    # ---------------------------------------------------------------- auxiliares
    def validos(self, ovr_id) -> list:
        return [e for e in self.eventos.get(ovr_id, ()) if not e.excluido]

    def primeiro(self, ovr_id, regra) -> Optional[SimpleNamespace]:
        return next((e for e in self.validos(ovr_id) if e.tipo in self.tipos and regra(self.tipos[e.tipo])), None)

    def verificacao_informada(self, ovr_id) -> bool:
        return self.primeiro(ovr_id, lambda t: t.especial == VERIFICACAO_INFORMADA) is not None

    def nome_tipo(self, tipo_id) -> str:
        tipo = self.tipos.get(tipo_id)
        return tipo.nome_original if tipo else f'(tipo {tipo_id})'

    def nome_setor(self, setor) -> str:
        return f'{setor} {self.setores.get(setor, "")}'.strip() if setor else '-'

    def abertas_sem_verificacao(self) -> List[SimpleNamespace]:
        return [f for f in self.fichas.values() if f.fase < FASE_CONCLUIDA and not self.verificacao_informada(f.id)]

    def idade_dias(self, ficha) -> float:
        return (self.agora - ficha.data).total_seconds() / 86400 if ficha.data else 0

    def _conta(self, colunas, contador: Counter, total: Optional[int] = None):
        total = total if total is not None else sum(contador.values())
        self.tabela(list(colunas) + ['%'], [(*(k if isinstance(k, tuple) else (k,)), n,
                                             f'{100 * n / total:.0f}%' if total else '-')
                                            for k, n in contador.most_common()])

    # ---------------------------------------------------------------- seções
    def secao_catalogo(self):
        self.titulo('1. CATÁLOGO DOS TIPOS DE EVENTO (atual) e uso nas Fichas do período (sem os excluídos)')
        uso = Counter(e.tipo for ovr_id in self.fichas for e in self.validos(ovr_id))
        fichas = Counter(e.tipo for ovr_id in self.fichas for e in {x.tipo: x for x in self.validos(ovr_id)}.values())
        linhas = [(t.id, t.nome_original, f'{t.fase} {FASES[t.fase] if t.fase is not None and t.fase < 5 else ""}',
                   ESPECIAIS.get(t.especial, ''), t.ordem if t.ordem is not None else '', uso[t.id], fichas[t.id])
                  for t in sorted(self.tipos.values(), key=lambda t: t.id)]
        self.tabela(['id', 'tipo de evento', 'fase', 'evento especial', 'ordem', 'eventos', 'fichas'], linhas)

    def secao_fichas(self):
        total = len(self.fichas)
        self.titulo(f'2. FICHAS DE {TIPOS_OPERACAO[self.tipo].upper()} desde {self.desde:%Y-%m-%d}: {total}')
        self.tabela(['tipo de Ficha (todos, mesmo período)', 'fichas'], [
            (f'{t} {TIPOS_OPERACAO[t] if t is not None and 0 <= t < len(TIPOS_OPERACAO) else ""}', n)
            for t, n in self.consulta('SELECT o.tipooperacao, COUNT(*) FROM ovr_ovrs o WHERE '
                                      'COALESCE(o.datahora, o.create_date) >= :desde GROUP BY o.tipooperacao '
                                      'ORDER BY 2 DESC')])
        if not total:
            return
        self.escreve()
        meses = Counter(f.data.strftime('%Y-%m') if f.data else '-' for f in self.fichas.values())
        self._conta(['mês da Ficha', 'fichas'], meses)
        self.escreve()
        fases = Counter(f'{f.fase} {FASES[f.fase] if f.fase < 5 else ""}' for f in self.fichas.values())
        self._conta(['fase', 'fichas'], fases)
        self.escreve()
        self._conta(['status atual (último evento)', 'fichas'], Counter(self.nome_tipo(f.tipoevento)
                                                                        for f in self.fichas.values()))
        self.escreve()
        self._conta(['setor', 'fichas'], Counter(self.nome_setor(f.setor) for f in self.fichas.values()))
        self.escreve()
        self.secao_pessoas(total)

    def secao_pessoas(self, total):
        fichas = self.fichas.values()
        self.tabela(['cobertura', 'fichas'], [
            ('com CE', pct(sum(bool(f.ce) for f in fichas), total)),
            ('com CNPJ fiscalizado', pct(sum(bool(f.cnpj) for f in fichas), total)),
            ('com responsável', pct(sum(bool(f.responsavel) for f in fichas), total)),
            ('... responsável = quem abriu', pct(sum(bool(f.responsavel) and f.responsavel == f.criador
                                                     for f in fichas), total)),
            ('com auditor responsável', pct(sum(bool(f.auditor) for f in fichas), total)),
            ('pessoas distintas que abriram', len({f.criador for f in fichas if f.criador})),
            ('com flag', pct(sum(bool(self.flags.get(f.id)) for f in fichas), total)),
        ])
        if any(self.flags.values()):
            self.escreve()
            self._conta(['flag', 'fichas'], Counter(nome for f in fichas for nome in set(self.flags.get(f.id, ()))))

    def secao_rvfs(self):
        self.titulo('3. VERIFICAÇÕES FÍSICAS (RVF) das Fichas do período')
        total = len(self.fichas)
        por_ficha = Counter(min(len(self.rvfs.get(f.id, ())), 6) for f in self.fichas.values())
        self.tabela(['RVFs na Ficha (6 = 6 ou mais)', 'fichas'], sorted(por_ficha.items()))
        rvfs = [r for lista in self.rvfs.values() for r in lista]
        imagens = Counter('0' if r.imagens == 0 else '1 a 4' if r.imagens < 5 else '5 ou mais' for r in rvfs)
        self.escreve()
        self.tabela(['RVFs', 'quantidade'], [
            ('total', len(rvfs)),
            ('com contêiner (numerolote)', pct(sum(bool(r.lote) for r in rvfs), len(rvfs))),
            ('inspeção não invasiva', pct(sum(r.inspecao for r in rvfs), len(rvfs))),
            *[(f'imagens: {k}', pct(n, len(rvfs))) for k, n in sorted(imagens.items())],
            ('Fichas do atalho antigo /nova_inspecaoni',
             pct(sum(bool(ATALHO_ANTIGO.search(f.observacoes)) for f in self.fichas.values()), total)),
        ])

    def secao_fluxo(self):
        self.titulo('4. FLUXO PELOS EVENTOS (sem os excluídos): sequências e tempo até os marcos')
        todos = [e for lista in self.eventos.values() for e in lista]
        self.tabela(['eventos', 'quantidade'], [
            ('total', len(todos)), ('excluídos', sum(e.excluido for e in todos)),
            ('meramente informativos', sum(e.informativo for e in todos))])
        sequencias = Counter(' → '.join(self.nome_tipo(e.tipo)[:30] for e in self.validos(f.id)[:5])
                             for f in self.fichas.values())
        self.escreve()
        self.escreve('Sequências mais comuns (até os 5 primeiros eventos):')
        self.tabela(['sequência', 'fichas'], sequencias.most_common(15))
        self.escreve()
        self.escreve('Marcos: quantas Fichas chegaram e em quanto tempo desde a abertura:')
        linhas = []
        for nome, regra in MARCOS:
            dias = []
            for ficha in self.fichas.values():
                evento = self.primeiro(ficha.id, regra)
                if evento and evento.data and ficha.data:
                    dias.append(max((evento.data - ficha.data).total_seconds() / 86400, 0))
            linhas.append((nome, pct(len(dias), len(self.fichas)), percentis(dias)))
        self.tabela(['marco', 'fichas', 'tempo desde a abertura'], linhas)

    def secao_abertas(self):
        abertas = self.abertas_sem_verificacao()
        self.titulo(f'5. ABERTAS (fase < 3) SEM VERIFICAÇÃO FÍSICA INFORMADA — a população da aba: {len(abertas)}')
        if not abertas:
            return
        self._conta(['setor', 'fichas'], Counter(self.nome_setor(f.setor) for f in abertas))
        self.escreve()
        self._conta(['idade da Ficha', 'fichas'], Counter(faixa_idade(self.idade_dias(f)) for f in abertas))
        self.escreve()
        self._conta(['status atual', 'fichas'], Counter(self.nome_tipo(f.tipoevento) for f in abertas))
        self.escreve()
        self.tabela(['nas abertas', 'fichas'], [
            ('com auditor responsável', pct(sum(bool(f.auditor) for f in abertas), len(abertas))),
            ('com bloqueio de CE informado', pct(sum(self.primeiro(f.id, MARCOS[3][1]) is not None for f in abertas),
                                                 len(abertas))),
            ('com RVF de contêiner', pct(sum(any(r.lote for r in self.rvfs.get(f.id, ())) for f in abertas),
                                         len(abertas)))])

    def ciclos(self, conteineres) -> Dict[str, list]:
        """{contêiner: [ciclos]} do ciclo de vida (colunas do estágio), em lotes."""
        resultado = defaultdict(list)
        conteineres = sorted(conteineres)
        for i in range(0, len(conteineres), LOTE):
            lote = conteineres[i:i + LOTE]
            marcadores = ', '.join(f':c{j}' for j in range(len(lote)))
            for linha in self.consulta(
                    'SELECT numeroConteiner, numeroCEmercante, data_desembarque, situacao, concluido, fluxo, '
                    f'data_saida, data_entrada_seco FROM ciclodevida_ciclo_de_vida_importacao '  # nosec B608
                    f'WHERE numeroConteiner IN ({marcadores})', {f'c{j}': c for j, c in enumerate(lote)}):
                cont, ce, desembarque, situacao, concluido, fluxo, saida, entrada = linha
                resultado[(cont or '').strip().upper()].append(SimpleNamespace(
                    ce=(ce or '').strip(), data_desembarque=_dt(desembarque), situacao=situacao,
                    concluido=bool(concluido), fluxo=fluxo, data_saida=_dt(saida), data_entrada_seco=_dt(entrada)))
        return resultado

    @staticmethod
    def ciclo_da_ficha(ficha, rvf, ciclos) -> Optional[SimpleNamespace]:
        """O ciclo do contêiner da RVF: o do CE da Ficha; senão, o desembarque mais perto da Ficha (até 90 dias)."""
        candidatos = ciclos.get(rvf.lote, [])
        do_ce = [c for c in candidatos if c.ce and c.ce in (ficha.ce, rvf.ce)]
        if do_ce:
            return max(do_ce, key=lambda c: c.data_desembarque or datetime.min)
        perto = [c for c in candidatos if c.data_desembarque and ficha.data
                 and abs((c.data_desembarque - ficha.data).days) <= 90]
        return min(perto, key=lambda c: abs(c.data_desembarque - ficha.data), default=None)

    def secao_ciclo(self):
        abertas = self.abertas_sem_verificacao()
        self.titulo('6. CICLO DE VIDA dos contêineres das abertas sem verificação física informada')
        pares = [(f, r) for f in abertas for r in self.rvfs.get(f.id, ()) if r.lote]
        if not pares:
            self.escreve('(nenhuma RVF com contêiner)')
            return
        ciclos = self.ciclos({r.lote for _, r in pares})
        estagios = Counter()
        alerta = Counter()
        for ficha, rvf in pares:
            ciclo = self.ciclo_da_ficha(ficha, rvf, ciclos)
            estagio = estagios_ciclo.estagio_do_ciclo(ciclo) if ciclo else 'sem ciclo de vida localizado'
            estagios[estagio or 'ciclo antigo em aberto'] += 1
            if estagio in (estagios_ciclo.ENTREGUE, estagios_ciclo.EMBARCADO):
                alerta[self.nome_setor(ficha.setor)] += 1
        self._conta(['estágio do contêiner', 'contêineres'], estagios)
        self.escreve()
        self.escreve(f'ALERTA da aba — saiu (entregue/embarcado) sem verificação física informada: '
                     f'{sum(alerta.values())} contêiner(es)')
        if alerta:
            self._conta(['setor', 'contêineres'], alerta)

    def secao_textos(self):
        self.titulo('7. OBSERVAÇÕES das Fichas (o campo tem 1.000 caracteres)')
        tamanhos = Counter('vazia' if not f.observacoes else '1 a 200' if len(f.observacoes) <= 200
                           else '201 a 500' if len(f.observacoes) <= 500 else '501 a 999'
                           if len(f.observacoes) < 1000 else '1.000 (cheio)' for f in self.fichas.values())
        self._conta(['tamanho', 'fichas'], tamanhos)
        self.escreve()
        inicios = Counter(mascara_texto(' '.join(f.observacoes.split()[:4]).lower(), 40)
                          for f in self.fichas.values() if f.observacoes)
        self.tabela(['início das observações (4 palavras, dígitos = #)', 'fichas'], inicios.most_common(10))

    def secao_exemplos(self):
        abertas = sorted(self.abertas_sem_verificacao(), key=lambda f: f.data or datetime.min, reverse=True)
        self.titulo(f'8. EXEMPLOS — {min(self.exemplos, len(abertas))} abertas sem verificação física informada '
                    '(as mais recentes; CPF mascarado, motivo com dígitos = #)')
        for ficha in abertas[:self.exemplos]:
            conteineres = ', '.join(sorted({r.lote for r in self.rvfs.get(ficha.id, ()) if r.lote})) or '-'
            self.escreve()
            self.escreve(f'Ficha {ficha.id} (nº {ficha.numero or "-"}/{ficha.ano or "-"}), aberta em '
                         f'{ficha.data:%d/%m/%Y} — fase {FASES[ficha.fase] if ficha.fase < 5 else ficha.fase}, '
                         f'status "{self.nome_tipo(ficha.tipoevento)}", setor {self.nome_setor(ficha.setor)}')
            self.escreve(f'   CE {ficha.ce or "-"}; contêiner(es) {conteineres}; abriu {mascara_cpf(ficha.criador)}; '
                         f'responsável {mascara_cpf(ficha.responsavel)}; auditor {mascara_cpf(ficha.auditor)}; '
                         f'flags {", ".join(self.flags.get(ficha.id, ())) or "-"}')
            for evento in self.eventos.get(ficha.id, ()):
                marcas = ' [informativo]' * evento.informativo + ' [EXCLUÍDO]' * evento.excluido
                data = f'{evento.data:%d/%m/%Y %H:%M}' if evento.data else '--'
                self.escreve(f'   {data}  {self.nome_tipo(evento.tipo)}{marcas} — {mascara_texto(evento.motivo)}')

    SECOES = ('secao_catalogo', 'secao_fichas', 'secao_rvfs', 'secao_fluxo', 'secao_abertas', 'secao_ciclo',
              'secao_textos', 'secao_exemplos')

    def roda(self):
        inicio = time.perf_counter()
        self.carrega()
        self.escreve(f'   [carga em {time.perf_counter() - inicio:.1f} s: {len(self.fichas)} fichas, '
                     f'{sum(len(v) for v in self.eventos.values())} eventos, '
                     f'{sum(len(v) for v in self.rvfs.values())} RVFs]')
        for nome in self.SECOES:
            inicio = time.perf_counter()
            getattr(self, nome)()
            self.escreve(f'   [seção em {time.perf_counter() - inicio:.1f} s]')
            self.saida.flush()


def main():  # pragma: no cover - depende do banco real
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--tipo', type=int, default=TIPO_REPRESSAO_IMPORTACAO,
                        help='tipooperacao das Fichas (padrão 1 = repressão na importação)')
    parser.add_argument('--meses', type=int, default=24, help='Fichas abertas nos últimos N meses (padrão 24)')
    parser.add_argument('--exemplos', type=int, default=8, help='quantas Fichas de exemplo na seção 8')
    parser.add_argument('--saida', default=datetime.now().strftime('fichas_repressao_%Y%m%d_%H%M%S.log'))
    args = parser.parse_args()
    uri = args.uri
    if not uri:
        from ajna_commons.flask.conf import SQL_URI as uri
    engine = create_engine(uri)
    with engine.connect() as conexao, open(args.saida, 'w', encoding='utf-8') as saida:
        diag = DiagnosticoFichas(conexao, saida, tipo=args.tipo, meses=args.meses, exemplos=args.exemplos)
        diag.escreve(f'Diagnóstico das Fichas ({TIPOS_OPERACAO[args.tipo]}) — {datetime.now():%Y-%m-%d %H:%M:%S}')
        diag.escreve(f'Banco: {engine.url.render_as_string(hide_password=True)}')
        diag.roda()
    print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
