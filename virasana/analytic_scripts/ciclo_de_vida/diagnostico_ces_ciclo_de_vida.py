# coding: utf-8
"""
Diagnóstico dos CEs no Ciclo de Vida: master x filhote (house) e CE declarado nos eventos.

Pergunta do usuário (01/10/2026): o "OUTRO CE" declarado nas inspeções já ligadas (~14%) é bug
do algoritmo antigo ou o par master/filhote? E os eventos da API informam master e house de forma
intercambiada? Se sim, CE "diferente" da mesma família corrobora a mesma operação — informação útil
para a heurística toda. Também mede por que 52% dos ciclos ficam sem CE.

Somente leitura, por AMOSTRA (os ciclos mais recentes do período). Seções:

  1. CE do ciclo: master (tem filhotes), filhote (tem master) ou simples; tipo de BL; tráfego;
  2. inspeções já ligadas com CE declarado: relação com o CE do ciclo (igual, filhote, master, irmão,
     mesmo manifesto, sem relação, inexistente, não é número de CE) e se o contêiner consta nos itens
     do CE declarado;
  3. CE declarado nos eventos do contêiner perto do desembarque (desembarque, acessos, inspeções),
     por tipo de evento — os eventos trocam master e house?
  4. ciclos SEM CE: o CE declarado no desembarque existe no Mercante? o contêiner está nos itens dele,
     dos filhotes ou do master? a escala está a quantos dias do desembarque (o algoritmo usa ±5)?
     prefixo do número declarado.

O SQL é montado só com constantes deste módulo e os valores vão como parâmetro (daí os nosec B608).

Uso (na raiz do repositório virasana):
    python virasana/analytic_scripts/ciclo_de_vida/diagnostico_ces_ciclo_de_vida.py
    python virasana/analytic_scripts/ciclo_de_vida/diagnostico_ces_ciclo_de_vida.py --dias 60 --amostra 20000
"""
import argparse
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, Optional, Set

from sqlalchemy import create_engine

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ciclo_de_vida import (  # noqa: E402
    TEM_CE, Diagnostico, _dt,
)

DIAS_EVENTOS_APOS_DESEMBARQUE = 10
TOLERANCIA_ESCALA = 5  # dias — a mesma do algoritmo atual (TOLERANCIA_PREVISAO_ATRACACAO)
FAIXAS_ESCALA = [(TOLERANCIA_ESCALA + 1, f'até {TOLERANCIA_ESCALA} dias (o algoritmo acharia)'),
                 (16, '6 a 15 dias'), (61, '16 a 60 dias'), (float('inf'), 'mais de 60 dias')]

# Relações entre o CE declarado num evento e o CE do ciclo
IGUAL = 'igual'
FILHOTE = 'declarado é FILHOTE do CE do ciclo'
MASTER = 'declarado é o MASTER do CE do ciclo'
IRMAO = 'irmãos (mesmo master)'
MANIFESTO = 'mesmo manifesto, sem parentesco'
SEM_RELACAO = 'sem relação'
INEXISTENTE = 'declarado não está no Mercante'
NAO_CE = 'não é número de CE (15 dígitos)'
NAO_INFORMADO = 'não informado'


def normaliza_ce(valor) -> Optional[str]:
    """CE com 15 dígitos (sem máscara); qualquer outra coisa -> None."""
    digitos = re.sub(r'\D', '', str(valor or ''))
    return digitos if len(digitos) == 15 else None


def relacao(ce_ciclo: str, declarado, info: Dict[str, dict]) -> str:
    """Relação entre o CE declarado num evento e o CE do ciclo (ver constantes acima).

    info: {ce: {'master': numeroCEMaster, 'manifesto': manifestoCE, ...}} do Mercante.
    """
    if not str(declarado or '').strip():
        return NAO_INFORMADO
    ce = normaliza_ce(declarado)
    if ce is None:
        return NAO_CE
    if ce == ce_ciclo:
        return IGUAL
    if ce not in info:
        return INEXISTENTE
    master_declarado = info[ce].get('master')
    master_ciclo = (info.get(ce_ciclo) or {}).get('master')
    if master_declarado == ce_ciclo:
        return FILHOTE
    if master_ciclo and master_ciclo == ce:
        return MASTER
    if master_declarado and master_declarado == master_ciclo:
        return IRMAO
    if info[ce].get('manifesto') and info[ce].get('manifesto') == (info.get(ce_ciclo) or {}).get('manifesto'):
        return MANIFESTO
    return SEM_RELACAO


def papel(ce: str, info: Dict[str, dict], filhotes: Dict[str, Set[str]]) -> str:
    """Papel do CE na família master/filhote."""
    tem_master = bool((info.get(ce) or {}).get('master'))
    tem_filhotes = bool(filhotes.get(ce))
    if tem_master and tem_filhotes:
        return 'master e filhote'
    if tem_filhotes:
        return 'MASTER (tem filhotes)'
    if tem_master:
        return 'FILHOTE (tem master)'
    return 'simples (sem master/filhote)'


def onde_esta_o_conteiner(conteiner: str, ce: str, info: Dict[str, dict], filhotes: Dict[str, Set[str]],
                          itens: Dict[str, Set[str]]) -> str:
    """Em que CE da família o contêiner aparece nos itens do Mercante."""
    lugares = []
    if conteiner in itens.get(ce, set()):
        lugares.append('no próprio CE')
    if any(conteiner in itens.get(f, set()) for f in filhotes.get(ce, ())):
        lugares.append('nos filhotes')
    master = (info.get(ce) or {}).get('master')
    if master and conteiner in itens.get(master, set()):
        lugares.append('no master')
    return ' + '.join(lugares) or 'em nenhum'


class DiagnosticoCEs(Diagnostico):
    """Reaproveita a infraestrutura (consulta, tabela, lotes) do diagnóstico do ciclo de vida."""

    def __init__(self, conexao, saida, inicio, fim, amostra=20000, amostra_eventos=3000):
        super().__init__(conexao, saida, inicio, fim, amostras=0)
        self.amostra = amostra
        self.amostra_eventos = amostra_eventos
        self.info: Dict[str, dict] = {}   # CEs encontrados no Mercante
        self._consultados: Set[str] = set()
        self._filhotes: Dict[str, Set[str]] = defaultdict(set)
        self._itens: Dict[str, Set[str]] = defaultdict(set)

    # ---------------------------------------------------------------- Mercante (em lote, com cache)
    def carrega_mercante(self, ces: Iterable[str]):
        """Dados do Mercante dos CEs, dos seus masters e filhotes, e os contêineres dos itens."""
        novos = {c for c in ces if c and c not in self._consultados}
        self._consultados |= novos
        for ce, master, tipo_bl, manifesto, trafego in self._por_lotes(
                'SELECT numeroCEmercante, numeroCEMaster, tipoBLConhecimento, manifestoCE, tipoTrafego '
                'FROM conhecimentosresumo WHERE numeroCEmercante IN :ces', 'ces', novos):
            self.info[ce] = {'master': normaliza_ce(master), 'tipo_bl': tipo_bl, 'manifesto': manifesto,
                             'trafego': trafego}
        masters = {self.info[ce]['master'] for ce in novos if ce in self.info and self.info[ce]['master']}
        masters -= self._consultados
        if masters:
            self.carrega_mercante(masters)
        for filhote, master in self._por_lotes(
                'SELECT numeroCEmercante, numeroCEMaster FROM conhecimentosresumo WHERE numeroCEMaster IN :ces',
                'ces', novos):
            self._filhotes[master].add(filhote)
        familia = novos | {f for ce in novos for f in self._filhotes.get(ce, ())} | masters
        for ce, conteiner in self._por_lotes(
                'SELECT numeroCEmercante, codigoConteiner FROM itensresumo WHERE numeroCEmercante IN :ces',
                'ces', familia):
            self._itens[ce].add((conteiner or '').strip())

    # ---------------------------------------------------------------- amostras
    def _ciclos_com_ce(self, limite):
        return self.consulta(
            'SELECT c.ID, c.numeroConteiner, c.numeroCEmercante, c.data_desembarque, c.desembarque_id '
            'FROM ciclodevida_ciclo_de_vida_importacao c WHERE c.data_desembarque >= :inicio '
            'AND c.data_desembarque < :fim AND ' + TEM_CE + ' ORDER BY c.ID DESC LIMIT :limite',  # nosec B608
            {'limite': limite})

    # ---------------------------------------------------------------- 1
    def secao_ce_do_ciclo(self):
        self.titulo(f'1. CE DO CICLO: master, filhote ou simples (amostra de até {self.amostra} ciclos com CE)')
        ciclos = self._ciclos_com_ce(self.amostra)
        self.carrega_mercante({c[2] for c in ciclos})
        papeis, conteineres = Counter(), Counter()
        for _, conteiner, ce, _, _ in ciclos:
            dados = self.info.get(ce)
            if not dados:
                papeis[('CE do ciclo não está no Mercante', '-', '-')] += 1
                continue
            papeis[(papel(ce, self.info, self._filhotes), dados['tipo_bl'] or '-', dados['trafego'] or '-')] += 1
            conteineres[onde_esta_o_conteiner(conteiner, ce, self.info, self._filhotes, self._itens)] += 1
        self.escreve(f'Ciclos na amostra: {len(ciclos)}')
        self.tabela(['papel do CE do ciclo', 'tipoBLConhecimento', 'tráfego', 'ciclos'],
                    [(*chave, n) for chave, n in papeis.most_common()])
        self.escreve()
        self.escreve('Onde o contêiner do ciclo aparece nos itens do Mercante (família do CE do ciclo):')
        self.tabela(['contêiner está', 'ciclos'], conteineres.most_common())

    # ---------------------------------------------------------------- 2
    def secao_inspecoes_ligadas(self):
        self.titulo('2. INSPEÇÕES JÁ LIGADAS: CE declarado x CE do ciclo (mesma amostra)')
        ciclos = {c[0]: c for c in self._ciclos_com_ce(self.amostra)}
        ligacoes = self._por_lotes(
            'SELECT e.ciclo_de_vida_id, i.numeroConhecimento, i.vazio, i.codigoRecinto '
            'FROM ciclodevida_ciclo_de_vida_importacao_escaneamentos e '
            'JOIN apirecintos_inspecoesnaoinvasivas i ON i.id = e.escaneamento_id '
            'WHERE e.ciclo_de_vida_id IN :ids', 'ids', set(ciclos))
        self.carrega_mercante({normaliza_ce(lig[1]) for lig in ligacoes})
        contagem = Counter()
        for ciclo_id, declarado, vazio, _ in ligacoes:
            _, conteiner, ce, _, _ = ciclos[ciclo_id]
            rel = relacao(ce, declarado, self.info)
            consta = '-'
            ce_declarado = normaliza_ce(declarado)
            if ce_declarado and ce_declarado in self.info:
                consta = 'sim' if onde_esta_o_conteiner(conteiner, ce_declarado, self.info, self._filhotes,
                                                        self._itens) != 'em nenhum' else 'NÃO'
            contagem[(rel, consta, 'vazio' if vazio else '-')] += 1
        self.escreve(f'Ligações avaliadas: {len(ligacoes)}')
        self.tabela(['CE declarado na inspeção', 'contêiner consta na família do declarado', 'vazio', 'ligações'],
                    [(*chave, n) for chave, n in contagem.most_common()])

    # ---------------------------------------------------------------- 3
    def secao_eventos_declarados(self):
        self.titulo(f'3. CE DECLARADO NOS EVENTOS do contêiner até {DIAS_EVENTOS_APOS_DESEMBARQUE} dias após o '
                    f'desembarque, por tipo de evento (amostra de {self.amostra_eventos} ciclos com CE)')
        ciclos = self._ciclos_com_ce(self.amostra_eventos)
        eventos = self._eventos_declarados({c[1] for c in ciclos})
        self.carrega_mercante({normaliza_ce(ev[2]) for lista in eventos.values() for ev in lista})
        contagem, familias = Counter(), Counter()
        for _, conteiner, ce, desembarque, _ in ciclos:
            desembarque = _dt(desembarque)
            if desembarque is None:
                continue
            declarados = set()
            for data, tipo, declarado in eventos.get(conteiner, []):
                data = _dt(data)
                if not desembarque <= data <= desembarque + timedelta(days=DIAS_EVENTOS_APOS_DESEMBARQUE):
                    continue
                rel = relacao(ce, declarado, self.info)
                contagem[(tipo, rel)] += 1
                if rel not in (NAO_INFORMADO, NAO_CE):
                    declarados.add(rel)
            familias[self._resumo_familia(declarados)] += 1
        self.tabela(['evento', 'CE declarado x CE do ciclo', 'eventos'],
                    [(*chave, n) for chave, n in sorted(contagem.items())])
        self.escreve()
        self.escreve('Por ciclo: que CEs os eventos declaram ao longo da passagem?')
        self.tabela(['CEs declarados nos eventos do ciclo', 'ciclos'], familias.most_common())

    @staticmethod
    def _resumo_familia(declarados: Set[str]) -> str:
        if not declarados:
            return 'nenhum CE declarado'
        da_familia = {IGUAL, FILHOTE, MASTER, IRMAO}
        if declarados <= {IGUAL}:
            return 'só o CE do ciclo'
        if declarados <= da_familia:
            return 'o CE do ciclo e/ou sua família master/filhote (TROCA master x house)'
        if declarados & da_familia:
            return 'família + CE sem relação/inexistente'
        return 'só CE sem relação/inexistente'

    def _eventos_declarados(self, conteineres) -> Dict[str, list]:
        params = {'inicio_busca': self.periodo['inicio'] - timedelta(days=1),
                  'fim_busca': self.periodo['fim'] + timedelta(days=DIAS_EVENTOS_APOS_DESEMBARQUE)}
        resultado = defaultdict(list)
        for tabela, tipo in (('apirecintos_embarquedesembarque', "CASE WHEN x.embarqueDesembarque = 'E' "
                                                                 "THEN 'embarque' ELSE 'desembarque' END"),
                             ('apirecintos_acessosveiculo', "CASE WHEN x.direcao = 'E' THEN 'acesso entrada' "
                                                            "ELSE 'acesso saída' END"),
                             ('apirecintos_inspecoesnaoinvasivas', "'inspeção'")):
            for conteiner, data, tipo_evento, declarado in self._por_lotes(
                    'SELECT x.numeroConteiner, x.dataHoraOcorrencia, ' + tipo + ', x.numeroConhecimento '  # nosec B608
                    'FROM ' + tabela + ' x WHERE x.numeroConteiner IN :conteineres '
                    'AND x.dataHoraOcorrencia >= :inicio_busca AND x.dataHoraOcorrencia < :fim_busca',
                    'conteineres', conteineres, params):
                resultado[conteiner].append((data, tipo_evento, declarado))
        return resultado

    # ---------------------------------------------------------------- 4
    def secao_ciclos_sem_ce(self):
        self.titulo(f'4. CICLOS SEM CE: o CE declarado no desembarque (amostra de até {self.amostra})')
        ciclos = self.consulta(
            'SELECT c.ID, c.numeroConteiner, c.data_desembarque, d.numeroConhecimento '
            'FROM ciclodevida_ciclo_de_vida_importacao c '
            'LEFT JOIN apirecintos_embarquedesembarque d ON d.id = c.desembarque_id '
            'WHERE c.data_desembarque >= :inicio AND c.data_desembarque < :fim AND NOT ' + TEM_CE +  # nosec B608
            ' ORDER BY c.ID DESC LIMIT :limite', {'limite': self.amostra})
        self.carrega_mercante({normaliza_ce(c[3]) for c in ciclos})
        escalas = self._escalas({(self.info.get(normaliza_ce(c[3])) or {}).get('manifesto') for c in ciclos})
        motivos, prefixos = Counter(), Counter()
        for _, conteiner, desembarque, declarado in ciclos:
            ce = normaliza_ce(declarado)
            prefixos[(ce[:2] if ce else ('não é CE' if str(declarado or '').strip() else 'não informado'),
                      'no Mercante' if ce in self.info else 'fora do Mercante')] += 1
            motivos[self._motivo_sem_ce(conteiner, _dt(desembarque), ce, escalas)] += 1
        self.escreve(f'Ciclos sem CE na amostra: {len(ciclos)}')
        self.tabela(['CE declarado no desembarque', 'contêiner nos itens', 'tráfego', 'escala x desembarque',
                     'ciclos'], [(*chave, n) for chave, n in motivos.most_common()])
        self.escreve()
        self.tabela(['prefixo (2 dígitos) do CE declarado', 'Mercante', 'ciclos'],
                    [(*chave, n) for chave, n in prefixos.most_common(20)])

    def _motivo_sem_ce(self, conteiner, desembarque, ce, escalas) -> tuple:
        if ce is None:
            return ('não informado / não é CE', '-', '-', '-')
        dados = self.info.get(ce)
        if not dados:
            return ('não está no Mercante', '-', '-', '-')
        onde = onde_esta_o_conteiner(conteiner, ce, self.info, self._filhotes, self._itens)
        datas = [d for d in escalas.get(dados['manifesto'], []) if d]
        if not datas or desembarque is None:
            faixa = 'sem escala'
        else:
            dias = min(abs((datetime.strptime(d[:10], '%Y-%m-%d') - desembarque).days) for d in datas)
            faixa = next(r for teto, r in FAIXAS_ESCALA if dias < teto)
        return (f'{papel(ce, self.info, self._filhotes)}', onde, dados['trafego'] or '-', faixa)

    def _escalas(self, manifestos) -> Dict[str, list]:
        resultado = defaultdict(list)
        for manifesto, data in self._por_lotes(
                'SELECT em.manifesto, e.dataPrevistaAtracacao FROM escalamanifestoresumo em '
                'JOIN escalasresumo e ON e.numeroDaEscala = em.escala WHERE em.manifesto IN :manifestos',
                'manifestos', manifestos):
            resultado[manifesto].append(data)
        return resultado


SECOES = ('secao_ce_do_ciclo', 'secao_inspecoes_ligadas', 'secao_eventos_declarados', 'secao_ciclos_sem_ce')


def roda(diag: DiagnosticoCEs) -> list:
    falhas = []
    for nome in SECOES:
        inicio = time.perf_counter()
        try:
            getattr(diag, nome)()
        except Exception as err:  # noqa: uma seção com problema não impede as outras
            diag.escreve(f'!! ERRO na seção {nome}: {type(err).__name__}: {err}')
            falhas.append(nome)
        diag.escreve(f'   [seção em {time.perf_counter() - inicio:.1f} s]')
        diag.saida.flush()
    diag.resumo(5)
    return falhas


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--dias', type=int, default=60, help='período: desembarques dos últimos N dias (padrão 60)')
    parser.add_argument('--amostra', type=int, default=20000, help='ciclos por seção (os mais recentes)')
    parser.add_argument('--amostra-eventos', type=int, default=3000, help='ciclos da seção 3 (eventos)')
    parser.add_argument('--saida', default=datetime.now().strftime('ciclo_de_vida_ces_%Y%m%d_%H%M%S.log'))
    args = parser.parse_args()
    uri = args.uri
    if not uri:
        from ajna_commons.flask.conf import SQL_URI as uri
    fim = datetime.now()
    inicio = fim - timedelta(days=args.dias)
    engine = create_engine(uri)
    with engine.connect() as conexao, open(args.saida, 'w', encoding='utf-8') as saida:
        diag = DiagnosticoCEs(conexao, saida, inicio, fim, args.amostra, args.amostra_eventos)
        diag.escreve(f'Diagnóstico dos CEs do Ciclo de Vida — {datetime.now():%Y-%m-%d %H:%M:%S}; desembarques '
                     f'de {inicio:%Y-%m-%d} a {fim:%Y-%m-%d}')
        diag.escreve(f'Banco: {engine.url.render_as_string(hide_password=True)}')
        falhas = roda(diag)
    print(f'Diagnóstico gravado em {args.saida}' + (f' (seções com erro: {", ".join(falhas)})' if falhas else ''))


if __name__ == '__main__':  # pragma: no cover
    main()
