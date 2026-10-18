# coding: utf-8
"""
Diagnóstico para as ESTATÍSTICAS DE USO do NAVI-I (aba gerencial, plano aprovado em 17/10/2026 — `navii/CONTEXTO.md`,
"Estatísticas de uso") — fase 0, antes da coleta:

  1. UNIVERSO: a contagem da tela (`conta_conteineres_navii`, sem filtros) dia a dia, nas duas janelas, com o tempo de
     cada contagem — decide se o universo pode ser calculado sob demanda (por dia, guardado) — e a conferência "soma
     dos dias = contagem do período" (cada contêiner tem UMA data de referência por janela; se a soma passar do
     período, há dupla contagem e o universo por dia não pode ser somado);
  2. FICHAS POR MÊS (repressão na importação): abertas, NAVI-I, com RVF, só com RVF de inspeção não invasiva, com
     verificação física informada, com TG, com resultado, concluídas/arquivadas e encerradas sem resultado;
  3. STATUS: o do dashboard de operações do Fichas x o proposto para a estatística — o dashboard trata qualquer RVF
     como "aguardando saneamento", e a Ficha do NAVI-I já nasce com a RVF da inspeção não invasiva; a estatística usa
     a VERIFICAÇÃO FÍSICA INFORMADA. Resultado = TG ou `ovr_resultados` (resultados por tipo e valor);
  4. MATURAÇÃO: dias da abertura até a verificação informada, o TG, o resultado e o encerramento e, por prazo (30,
     90, 180, 365 dias), quantas Fichas com idade suficiente chegaram a cada marco nesse prazo — para escolher o N da
     comparação por coorte ("com resultado em até N dias da abertura");
  5. POR SETOR: o que cada setor faz com a Ficha (verificação, encerradas sem resultado, coortes) — a base da
     comparação "NAVI-I x demais" não pode misturar processos diferentes (ex.: conferência de DTA canal vermelho).

Medição de 17/10/2026 (produção, sem porto): a soma dos dias passou do período em ~1,7x (efetiva) e ~1,9x (prevista).
Sem filtro de porto, o manifesto entra em CADA dia em que uma das suas escalas (uma por porto em que o navio atraca)
cai — o universo nacional por dia não soma. `--porto` mede o universo do porto (filtro `porto_escala`; o desembarque
efetivo não usa o porto — decisão 9 do NAVI-I). Segunda medição (com o porto): a soma dos dias fica a ~1% do período
(a carga do Mercante trata a atualização de escala como inclusão). A aba soma MESES exatos para trimestre, semestre e
ano: `--meses-universo N` mede a soma dos N últimos meses fechados contra os N meses contados de uma vez.

Somente leitura; não imprime CPF nem nome. Reaproveita a carga do `diagnostico_fichas_repressao.py`. As datas do TG
e do resultado são as do REGISTRO no Fichas (`create_date`).

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    python virasana/analytic_scripts/diagnostico_estatisticas_uso.py --saida estatisticas_uso.log
    python virasana/analytic_scripts/diagnostico_estatisticas_uso.py --dias-universo 30 --porto BRSSZ
    python virasana/analytic_scripts/diagnostico_estatisticas_uso.py --dias-universo 0 --meses-universo 3 --porto BRSSZ
    python virasana/analytic_scripts/diagnostico_estatisticas_uso.py --dias-universo 10 --ate 2026-05-31 --porto BRSSZ
"""
import argparse
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida.carga_ciclo import _dt  # noqa: E402
from virasana.analytic_scripts.diagnostico_fichas_repressao import (  # noqa: E402
    DiagnosticoFichas, FASES, FASE_CONCLUIDA, TIPOS_OPERACAO, TIPO_REPRESSAO_IMPORTACAO, VERIFICACAO_INFORMADA, pct,
    percentis,
)
from virasana.navii import busca_navii  # noqa: E402

# cópia de bhadrasana.models.ovr.TipoResultado (não importado: carregaria o registry do Fichas)
TIPOS_RESULTADO = {1: 'Apreensão', 2: 'Perdimento', 3: 'Crédito', 4: 'Sanção', 5: 'DARF', 6: 'Multa RFB',
                   7: 'Multa de outro órgão'}
JANELAS = ('efetiva', 'prevista')
FLAG_NAVII = 'NAVI-I'
PRAZOS = (30, 90, 180, 365)
MINIMO_SETOR = 20                # setores com menos Fichas vão para uma linha só
MARCOS = ('verificação informada', 'TG', 'resultado (ovr_resultados)', 'TG ou resultado', 'encerramento')
SITUACOES = (('abertas', 'aberta'), ('NAVI-I', 'navii'), ('com RVF', 'rvf'), ('só RVF de INI', 'so_ini'),
             ('verificação informada', 'verificacao'), ('com TG', 'tg'), ('com resultado', 'resultado'),
             ('concluídas/arquivadas', 'encerrada'), ('encerradas sem resultado', 'sem_resultado'))


def status_dashboard(s: SimpleNamespace) -> str:
    """O status de `ovr_dashboard_status` (bhadrasana/models/operacoes_dashboard.py)."""
    if s.fase >= FASE_CONCLUIDA:
        return FASES[s.fase] if s.fase < len(FASES) else f'fase {s.fase}'
    if s.tg:
        return 'Mercadorias já informadas'
    return 'Aguardando saneamento(s)' if s.rvf else 'Selecionado ainda sem aberturas'


def status_proposto(s: SimpleNamespace) -> str:
    """O status para a estatística: pela verificação física informada; resultado = TG ou ovr_resultados."""
    if s.encerrada:
        return 'encerrada sem resultado' if s.sem_resultado else 'encerrada com resultado'
    if s.resultado:
        return 'aberta com resultado'
    if s.tg:
        return 'aberta com TG'
    return 'verificação física informada' if s.verificacao else 'aguardando verificação física'


def _primeira(datas) -> Optional[datetime]:
    return min((d for d in datas if d), default=None)


class DiagnosticoEstatisticas(DiagnosticoFichas):
    """A carga das Fichas do diagnóstico da fase 0 + TG e resultados com data; o universo pela sessão do NAVI-I."""

    SECOES = ('secao_universo', 'secao_mensal', 'secao_status', 'secao_maturacao', 'secao_setores')
    SQL_TGS = ('SELECT t.ovr_id, t.create_date, t.valor FROM ovr_tgovr t JOIN ovr_ovrs o ON o.id = t.ovr_id '
               'WHERE o.tipooperacao = :tipo AND COALESCE(o.datahora, o.create_date) >= :desde')
    SQL_RESULTADOS = ('SELECT r.ovr_id, r.create_date, r.tipo_resultado, r.valor FROM ovr_resultados r '
                      'JOIN ovr_ovrs o ON o.id = r.ovr_id '
                      'WHERE o.tipooperacao = :tipo AND COALESCE(o.datahora, o.create_date) >= :desde')

    def __init__(self, conexao, saida, session=None, dias_universo: int = 14, porto: Optional[str] = None,
                 meses_universo: int = 0, ate: Optional[date] = None, **opcoes):
        super().__init__(conexao, saida, **opcoes)
        self.session, self.dias_universo, self.porto = session, dias_universo, porto
        self.meses_universo, self.ate = meses_universo, ate
        self.tgs: Dict[int, list] = defaultdict(list)
        self.resultados: Dict[int, list] = defaultdict(list)

    def carrega(self):
        super().carrega()
        for ovr_id, data, valor in self.consulta(self.SQL_TGS):
            self.tgs[ovr_id].append(SimpleNamespace(data=_dt(data), valor=float(valor or 0)))
        for ovr_id, data, tipo, valor in self.consulta(self.SQL_RESULTADOS):
            self.resultados[ovr_id].append(SimpleNamespace(data=_dt(data), tipo=tipo, valor=float(valor or 0)))

    # ---------------------------------------------------------------- por Ficha
    def situacao(self, ficha) -> SimpleNamespace:
        rvfs = self.rvfs.get(ficha.id, [])
        verificacao = self.verificacao_informada(ficha.id)
        tg, resultado = bool(self.tgs.get(ficha.id)), bool(self.resultados.get(ficha.id))
        encerrada = ficha.fase >= FASE_CONCLUIDA
        return SimpleNamespace(
            fase=ficha.fase, aberta=True, navii=FLAG_NAVII in self.flags.get(ficha.id, []), rvf=bool(rvfs),
            so_ini=bool(rvfs) and all(r.inspecao for r in rvfs) and not verificacao, verificacao=verificacao,
            tg=tg, resultado=resultado, encerrada=encerrada, sem_resultado=encerrada and not (tg or resultado))

    def marcos(self, ficha) -> Dict[str, Optional[datetime]]:
        """Data de cada marco (None = não chegou). Encerramento: 1º evento válido de um tipo de fase >= 3."""
        verificacao = self.primeiro(ficha.id, lambda t: t.especial == VERIFICACAO_INFORMADA)
        encerramento = self.primeiro(ficha.id, lambda t: (t.fase or 0) >= FASE_CONCLUIDA)
        tg = _primeira(t.data for t in self.tgs.get(ficha.id, ()))
        resultado = _primeira(r.data for r in self.resultados.get(ficha.id, ()))
        return {'verificação informada': verificacao.data if verificacao else None, 'TG': tg,
                'resultado (ovr_resultados)': resultado, 'TG ou resultado': _primeira((tg, resultado)),
                'encerramento': encerramento.data if encerramento else None}

    def dias_ate(self, ficha, data: Optional[datetime]) -> Optional[float]:
        if data is None or ficha.data is None:
            return None
        return max((data - ficha.data).total_seconds() / 86400, 0)

    # ---------------------------------------------------------------- 1. universo
    def secao_universo(self):
        self.titulo('1. UNIVERSO: a contagem da tela, sem filtros, dia a dia (custo; soma dos dias = período)')
        if self.session is None or (self.dias_universo <= 0 and self.meses_universo <= 0):
            self.escreve('(sem sessão do NAVI-I ou sem dias e meses: seção não rodada)')
            return
        self.escreve(f'Porto: {self.porto} (só a atracação prevista; o desembarque efetivo não usa o porto)'
                     if self.porto else 'Sem porto: o país inteiro.')
        fim = self.ate or self.agora.date() - timedelta(days=1)     # `--ate`: dias antigos (cache frio do banco)
        dias = [fim - timedelta(days=n) for n in range(self.dias_universo - 1, -1, -1)]
        for janela in JANELAS:
            if dias:
                self.universo_da_janela(janela, dias)
            if self.meses_universo > 0:
                self.universo_dos_meses(janela)

    def meses_fechados(self) -> List[Tuple[date, date]]:
        """Os `meses_universo` últimos meses fechados (antes do mês de hoje), do mais antigo ao mais novo."""
        meses, fim = [], self.agora.date().replace(day=1) - timedelta(days=1)
        for _ in range(self.meses_universo):
            meses.insert(0, (fim.replace(day=1), fim))
            fim = fim.replace(day=1) - timedelta(days=1)
        return meses

    def universo_dos_meses(self, janela: str):
        """A soma dos meses exatos x os meses contados de uma vez (a aba soma meses para trimestre, semestre e ano)."""
        self.escreve()
        self.escreve(f'Janela "{janela}", meses exatos:')
        meses = self.meses_fechados()
        por_mes = [(inicio, *self.universo(janela, inicio, fim)) for inicio, fim in meses]
        self.tabela(['mês', 'contêineres', 'segundos'],
                    [(f'{inicio:%m/%Y}', total, f'{segundos:.2f}') for inicio, total, segundos in por_mes])
        periodo, segundos = self.universo(janela, meses[0][0], meses[-1][1])
        validos = [total for _, total, _ in por_mes if total is not None]
        if periodo is None or len(validos) < len(meses):
            self.escreve('Sem a comparação: alguma contagem falhou.')
            return
        soma = sum(validos)
        self.escreve(f'Soma dos meses: {soma}; os {len(meses)} meses de uma vez: {periodo} ({segundos:.2f} s) — '
                     f'diferença {soma - periodo} ({100 * (soma - periodo) / periodo if periodo else 0:.2f}%).')

    def universo(self, janela: str, inicio: date, fim: date):
        """(contêineres, segundos) da contagem da tela no período; (None, s) em erro — o diagnóstico segue."""
        comeco = time.perf_counter()
        try:
            filtros = {'data_inicio': inicio.isoformat(), 'data_fim': fim.isoformat()}
            if self.porto:
                filtros['porto_escala'] = [self.porto]
            total = busca_navii.conta_conteineres_navii(self.session, janela, filtros)
        except Exception as err:  # noqa: o log deve seguir mesmo se uma contagem falhar
            self.session.rollback()
            self.escreve(f'!! ERRO na contagem ({janela}, {inicio} a {fim}): {err}')
            total = None
        return total, time.perf_counter() - comeco

    def universo_da_janela(self, janela: str, dias: List[date]):
        self.escreve()
        self.escreve(f'Janela "{janela}":')
        por_dia = [(dia, *self.universo(janela, dia, dia)) for dia in dias]
        self.tabela(['dia', 'contêineres', 'segundos'],
                    [(f'{dia:%d/%m/%Y}', total, f'{segundos:.2f}') for dia, total, segundos in por_dia])
        periodo, segundos_periodo = self.universo(janela, dias[0], dias[-1])
        validos = [total for _, total, _ in por_dia if total is not None]
        tempos = [segundos for *_, segundos in por_dia]
        soma = sum(validos)
        self.escreve(f'Soma dos dias: {soma}; contagem do período de uma vez: {periodo} '
                     f'({segundos_periodo:.2f} s).')
        if periodo is not None and len(validos) == len(dias) and soma != periodo:
            self.escreve(f'!! DUPLA CONTAGEM: a soma dos dias difere do período em {soma - periodo} — o universo por '
                         f'dia não pode ser somado.')
        media = sum(tempos) / len(tempos)
        self.escreve(f'Tempo por dia: médio {media:.2f} s, máximo {max(tempos):.2f} s; estimativa sob demanda: '
                     f'um mês {30 * media:.0f} s, um ano {365 * media / 60:.1f} min (só na 1ª vez: os dias ficam '
                     f'guardados).')

    # ---------------------------------------------------------------- 2. por mês
    def secao_mensal(self):
        self.titulo('2. FICHAS DE REPRESSÃO NA IMPORTAÇÃO POR MÊS DE ABERTURA')
        por_mes: Dict[str, Counter] = defaultdict(Counter)
        for ficha in self.fichas.values():
            s = self.situacao(ficha)
            mes = f'{ficha.data:%Y-%m}' if ficha.data else 'sem data'
            por_mes[mes].update(chave for _, chave in SITUACOES if getattr(s, chave))
        total = Counter()
        for contagem in por_mes.values():
            total.update(contagem)
        linhas = [[mes] + [por_mes[mes][chave] for _, chave in SITUACOES] for mes in sorted(por_mes)]
        linhas.append(['total'] + [total[chave] for _, chave in SITUACOES])
        self.tabela(['mês'] + [nome for nome, _ in SITUACOES], linhas)
        self.escreve('"só RVF de INI": todas as RVFs são de inspeção não invasiva e não há verificação física '
                     'informada (a Ficha aberta pelo NAVI-I nasce assim).')

    # ---------------------------------------------------------------- 3. status e resultados
    def secao_status(self):
        self.titulo('3. STATUS: dashboard de operações do Fichas x proposto para a estatística; resultados')
        pares = Counter((status_dashboard(s), status_proposto(s))
                        for s in (self.situacao(f) for f in self.fichas.values()))
        self._conta(['status do dashboard', 'status proposto', 'fichas'], pares, len(self.fichas))
        self.escreve()
        self.escreve('Resultados (ovr_resultados) por tipo:')
        fichas, registros, valores = Counter(), Counter(), Counter()
        for ovr_id, lista in self.resultados.items():
            for tipo in {r.tipo for r in lista}:
                fichas[tipo] += 1
            for r in lista:
                registros[r.tipo] += 1
                valores[r.tipo] += r.valor
        self.tabela(['tipo', 'fichas', 'registros', 'valor'],
                    [(TIPOS_RESULTADO.get(tipo, f'tipo {tipo}'), fichas[tipo], registros[tipo],
                      f'{valores[tipo]:,.2f}') for tipo in sorted(registros)])
        com_tg = [ovr_id for ovr_id in self.tgs if ovr_id in self.fichas]
        self.escreve(f'Com TG: {pct(len(com_tg), len(self.fichas))}; valor dos TGs: '
                     f'{sum(t.valor for i in com_tg for t in self.tgs[i]):,.2f}')

    # ---------------------------------------------------------------- 4. maturação
    def secao_maturacao(self):
        self.titulo('4. MATURAÇÃO: dias da abertura até cada marco e % que chega em até N dias (coortes)')
        marcos = {ficha.id: self.marcos(ficha) for ficha in self.fichas.values()}
        linhas = []
        for nome in MARCOS:
            dias = {ovr_id: self.dias_ate(self.fichas[ovr_id], datas[nome]) for ovr_id, datas in marcos.items()}
            chegaram = [valor for valor in dias.values() if valor is not None]
            linhas.append([nome, pct(len(chegaram), len(dias)), percentis(chegaram)]
                          + [self.no_prazo(dias, prazo) for prazo in PRAZOS])
        self.tabela(['marco', 'fichas que chegaram', 'tempo desde a abertura']
                    + [f'em até {prazo} d' for prazo in PRAZOS], linhas)
        self.escreve('"em até N d": entre as Fichas abertas há N dias ou mais, quantas chegaram ao marco em até N dias '
                     '— a base da comparação justa entre períodos.')

    # ---------------------------------------------------------------- 5. por setor
    def secao_setores(self):
        self.titulo(f'5. POR SETOR (os com {MINIMO_SETOR} Fichas ou mais): a base da comparação NAVI-I x demais')
        por_setor: Dict[str, list] = defaultdict(list)
        for ficha in self.fichas.values():
            por_setor[ficha.setor].append(ficha)
        grandes = sorted((s for s in por_setor if len(por_setor[s]) >= MINIMO_SETOR), key=lambda s: -len(por_setor[s]))
        self.tabela(['setor', 'fichas', 'verificação informada', 'encerradas sem resultado', 'verificação em até 30 d',
                     'TG ou resultado em até 90 d', 'em até 180 d', 'em até 365 d'],
                    [self.linha_do_setor(setor, por_setor[setor]) for setor in grandes])
        pequenos = [f for s, lista in por_setor.items() if s not in grandes for f in lista]
        self.escreve(f'Setores com menos de {MINIMO_SETOR} Fichas: {len(pequenos)} Fichas.')

    def linha_do_setor(self, setor: str, fichas: list) -> list:
        situacoes = [self.situacao(f) for f in fichas]
        marcos = {f.id: self.marcos(f) for f in fichas}
        verificacao = {i: self.dias_ate(self.fichas[i], m['verificação informada']) for i, m in marcos.items()}
        resultado = {i: self.dias_ate(self.fichas[i], m['TG ou resultado']) for i, m in marcos.items()}
        return [self.nome_setor(setor), len(fichas), pct(sum(s.verificacao for s in situacoes), len(fichas)),
                pct(sum(s.sem_resultado for s in situacoes), len(fichas)), self.no_prazo(verificacao, 30),
                *[self.no_prazo(resultado, prazo) for prazo in (90, 180, 365)]]

    def no_prazo(self, dias: Dict[int, Optional[float]], prazo: int) -> str:
        elegiveis = [ovr_id for ovr_id in dias if self.idade_dias(self.fichas[ovr_id]) >= prazo]
        if not elegiveis:
            return '-'
        chegaram = sum(1 for ovr_id in elegiveis if dias[ovr_id] is not None and dias[ovr_id] <= prazo)
        return f'{chegaram}/{len(elegiveis)} ({100 * chegaram / len(elegiveis):.0f}%)'


def main():  # pragma: no cover - depende do banco real
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--meses', type=int, default=24, help='Fichas abertas nos últimos N meses (padrão 24)')
    parser.add_argument('--dias-universo', type=int, default=14,
                        help='quantos dias (até ontem) contar no universo, por janela (padrão 14; 0 = não contar)')
    parser.add_argument('--porto', help='universo do porto (filtro porto_escala, ex.: BRSSZ); sem: o país inteiro')
    parser.add_argument('--ate', type=lambda v: datetime.strptime(v, '%Y-%m-%d').date(), metavar='AAAA-MM-DD',
                        help='último dia do universo dia a dia (padrão: ontem) — para medir dias antigos')
    parser.add_argument('--meses-universo', type=int, default=0,
                        help='soma dos N últimos meses fechados x os N meses de uma vez (padrão 0 = não medir)')
    parser.add_argument('--saida', default=datetime.now().strftime('estatisticas_uso_%Y%m%d_%H%M%S.log'))
    args = parser.parse_args()
    uri = args.uri
    if not uri:
        from ajna_commons.flask.conf import SQL_URI as uri
    engine = create_engine(uri)
    with engine.connect() as conexao, open(args.saida, 'w', encoding='utf-8') as saida:
        session = Session(bind=conexao)
        diag = DiagnosticoEstatisticas(conexao, saida, session=session, dias_universo=args.dias_universo,
                                       porto=args.porto, meses_universo=args.meses_universo, ate=args.ate,
                                       meses=args.meses)
        diag.escreve(f'Diagnóstico para as estatísticas de uso ({TIPOS_OPERACAO[TIPO_REPRESSAO_IMPORTACAO]}) — '
                     f'{datetime.now():%Y-%m-%d %H:%M:%S}')
        diag.escreve(f'Banco: {engine.url.render_as_string(hide_password=True)}')
        diag.roda()
    print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
