# coding: utf-8
"""
Núcleo da heurística do Ciclo de Vida da importação: a LINHA DO TEMPO do contêiner.

Função pura (sem banco): recebe os eventos de UM contêiner (desembarques/embarques, acessos de gate
efetivados e inspeções não invasivas), já ordenáveis por data, e devolve um ciclo por desembarque num
operador do escopo, com cada evento ligado e a CONFIANÇA da ligação. Ver CICLO_DE_VIDA.md (plano v2).

Princípio (do usuário): contêiner + intervalo de datas NÃO basta. Um evento só entra no ciclo se a
linha do tempo continuar sem um evento-FRONTEIRA que abra outra operação:
  - novo desembarque do contêiner (corta o segmento);
  - embarque;
  - nova entrada no mesmo recinto sem a saída registrada (prova de saída omitida);
  - entrada com o contêiner VAZIO depois da saída (devolução de vazio — outra operação).
Com a fronteira, a próxima operação nunca é confundida com esta, mesmo com prazos longos.

Estados: no OPERADOR do desembarque -> em TRÂNSITO (saiu) -> em OUTRO OPERADOR (passagem/baldeação;
`ovr_recintos.e_operador`) ou no SECO (TRA, recinto não operador) -> fim.

Confiança (só alta e média concluem o ciclo):
  - alta: evento no recinto esperado, ou CE declarado da família do CE do ciclo;
  - média: recinto diferente (ex.: escaneamento terceirizado), etapa anterior omitida, contêiner
    marcado vazio (sinal fraco: a medição mostrou vazio com o mesmo CE), permanência longa;
  - baixa: CE declarado de OUTRA família.
O CE declarado só corrobora/desconfia: nunca decide sozinho (dado declarado, não 100% confiável).

O resultado depende só dos eventos e de `agora`: recalcular quando chegam eventos novos é seguro
(é a base do processamento orientado a eventos).
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set

DESEMBARQUE, EMBARQUE, ENTRADA, SAIDA, INSPECAO = 'desembarque', 'embarque', 'entrada', 'saida', 'inspecao'
ALTA, MEDIA, BAIXA = 'alta', 'media', 'baixa'
_NIVEL = {BAIXA: 0, MEDIA: 1, ALTA: 2}

OPERADOR, TRA, PASSAGEM, BALDEACAO, INDEFINIDO = 'operador', 'tra', 'passagem', 'baldeacao', 'indefinido'
ABERTO, CONCLUIDO, INCOMPLETO, ABANDONO = 'aberto', 'concluido', 'incompleto', 'abandono'


@dataclass(frozen=True)
class Parametros:
    prazo_transito_horas: float = 48        # saída do operador -> entrada no seco; depois disso, não é TRA
    transito_alerta_horas: float = 6        # trânsito mais longo que isso gera alerta
    tolerancia_gate_horas: float = 2        # inspeção logo antes da entrada (scanner no gate)
    tolerancia_relogio_horas: float = 1     # inspeção um pouco antes do desembarque (relógio)
    permanencia_tipica_dias: float = 10     # saída do operador depois disso: confiança média
    dias_abandono: float = 93               # sem saída: abandono (prazo de 90 dias + margem)


@dataclass(frozen=True)
class Evento:
    tipo: str                               # DESEMBARQUE, EMBARQUE, ENTRADA, SAIDA, INSPECAO
    id: int
    data: datetime
    recinto: str
    vazio: Optional[bool] = None
    ce_declarado: Optional[str] = None


@dataclass
class Ligacao:
    evento: Evento
    confianca: str
    motivos: List[str] = field(default_factory=list)


@dataclass
class CicloCalculado:
    desembarque: Evento
    saida: Optional[Ligacao] = None
    entrada_seco: Optional[Ligacao] = None
    saida_seco: Optional[Ligacao] = None
    embarque: Optional[Ligacao] = None
    escaneamentos: List[Ligacao] = field(default_factory=list)
    fluxo: str = INDEFINIDO
    situacao: str = ABERTO
    motivo: str = ''
    alertas: List[str] = field(default_factory=list)

    @property
    def concluido(self) -> bool:
        return self.situacao != ABERTO

    def ligacoes(self) -> List[Ligacao]:
        etapas = [self.saida, self.entrada_seco, self.saida_seco, self.embarque]
        return [lig for lig in etapas if lig] + self.escaneamentos


def _horas(depois: datetime, antes: datetime) -> float:
    return (depois - antes).total_seconds() / 3600


def _rebaixa(confianca: str, teto: str) -> str:
    return confianca if _NIVEL[confianca] <= _NIVEL[teto] else teto


def sem_retificacoes(eventos: Iterable[Evento]) -> List[Evento]:
    """Uma ocorrência por (tipo, recinto, data/hora): a retificação (id maior) prevalece. Ordenada."""
    unicos = {}
    for evento in sorted(eventos, key=lambda e: e.id):
        unicos[(evento.tipo, evento.recinto, evento.data)] = evento
    return sorted(unicos.values(), key=lambda e: (e.data, e.id))


class _Percurso:
    """Máquina de estados de UM ciclo (eventos do segmento do seu desembarque)."""

    def __init__(self, desembarque: Evento, operadores: Set[str], agora: datetime, familia: Set[str],
                 params: Parametros):
        self.ciclo = CicloCalculado(desembarque)
        self.operadores, self.agora, self.familia, self.params = operadores, agora, familia, params
        self.estado = 'operador'
        self.recinto = desembarque.recinto
        self.desde = desembarque.data            # início do estado atual (para prazos)
        self.etapa_omitida = False
        self.em_espera: List[Evento] = []        # inspeções em trânsito, à espera do próximo evento

    # ------------------------------------------------------------------ apoio
    def _confianca(self, evento: Evento, base: str, motivos: List[str]) -> str:
        """Ajusta a confiança pelo CE declarado (corrobora/desconfia) e pelo vazio (sinal fraco)."""
        declarado = (evento.ce_declarado or '').strip()
        if declarado and self.familia:
            if declarado in self.familia:
                base = ALTA
                motivos.append('CE declarado da família do CE do ciclo')
            else:
                base = BAIXA
                motivos.append('CE declarado de OUTRA família')
        if evento.vazio:
            base = _rebaixa(base, MEDIA)
            motivos.append('contêiner marcado vazio')
        if self.etapa_omitida:
            base = _rebaixa(base, MEDIA)
            motivos.append('etapa anterior não informada')
        return base

    def _liga(self, evento: Evento, base: str, *motivos: str) -> Ligacao:
        lista = list(motivos)
        return Ligacao(evento, self._confianca(evento, base, lista), lista)

    def _fim(self, fluxo: str, situacao: str, motivo: str):
        self._libera_espera()  # inspeções ainda em espera ficam como "em trânsito"
        self.ciclo.fluxo, self.ciclo.situacao, self.ciclo.motivo = fluxo, situacao, motivo
        self.estado = 'fim'

    def _alerta(self, texto: str):
        self.ciclo.alertas.append(texto)

    # ------------------------------------------------------------------ inspeções
    def _inspecao(self, evento: Evento):
        # em trânsito, ou passando por outro operador com a inspeção em OUTRO recinto (ex.: o contêiner
        # "dentro" do Ecoporto e a inspeção no gate do TRA para onde vai): decide o próximo evento
        if self.estado == 'transito' or (self.estado == 'outro_operador' and evento.recinto != self.recinto):
            self.em_espera.append(evento)
            return
        motivos = ['no recinto da etapa'] if evento.recinto == self.recinto else ['em outro recinto (terceirizado?)']
        base = ALTA if evento.recinto == self.recinto else MEDIA
        if evento.data < self.ciclo.desembarque.data:
            base = MEDIA
            motivos.append('antes do desembarque (relógio)')
        self.ciclo.escaneamentos.append(self._liga(evento, base, *motivos))

    def _libera_espera(self, proxima: Optional[Evento] = None, aceita_gate: bool = True):
        """Inspeções feitas em trânsito: junto ao gate da próxima entrada (scanner no gate) ou soltas."""
        for inspecao in self.em_espera:
            no_gate = (proxima is not None and
                       0 <= _horas(proxima.data, inspecao.data) <= self.params.tolerancia_gate_horas)
            if no_gate and not aceita_gate:
                continue  # scanner do gate de uma entrada que NÃO é desta operação
            if no_gate:
                base = ALTA if inspecao.recinto == proxima.recinto else MEDIA
                self.ciclo.escaneamentos.append(self._liga(inspecao, base, 'antes da entrada (scanner no gate)'))
            else:
                self.ciclo.escaneamentos.append(self._liga(inspecao, MEDIA, 'em trânsito'))
        self.em_espera = []

    # ------------------------------------------------------------------ estados
    def _no_operador(self, evento: Evento):
        if evento.tipo == EMBARQUE:
            mesmo = evento.recinto == self.recinto
            self.ciclo.embarque = self._liga(evento, ALTA if mesmo else MEDIA)
            if not mesmo:
                self._alerta('saída do operador não informada')
            if self.ciclo.embarque.confianca == BAIXA:
                self._fim(INDEFINIDO, INCOMPLETO, 'embarque com CE de outra família — pode ser outra operação '
                                                  '(ex.: vazio reembarcado)')
            else:
                self._fim(PASSAGEM if mesmo else BALDEACAO, CONCLUIDO,
                          'embarque no mesmo operador' if mesmo else 'embarque em outro operador')
        elif evento.tipo == SAIDA and evento.recinto == self.recinto:
            dias = _horas(evento.data, self.ciclo.desembarque.data) / 24
            base, motivos = ALTA, []
            if dias > self.params.permanencia_tipica_dias:
                base = MEDIA
                motivos.append(f'saída {dias:.0f} dias após o desembarque')
            self.ciclo.saida = self._liga(evento, base, *motivos)
            self.estado, self.recinto, self.desde = 'transito', None, evento.data
        elif evento.tipo == ENTRADA and evento.recinto == self.recinto:
            self._fim(OPERADOR, INCOMPLETO, 'saída do operador não informada (reentrada no operador)')
        elif evento.tipo == ENTRADA:
            self._alerta('saída do operador não informada')
            self.etapa_omitida = True
            # sem a saída não há como medir o trânsito: a entrada vale pela continuidade (confiança média)
            self.estado, self.desde = 'transito', evento.data
            self._em_transito(evento)
        else:
            self._alerta(f'{evento.tipo} em {evento.recinto} sem entrada — ignorado')

    def _em_transito(self, evento: Evento):
        if evento.tipo == ENTRADA:
            self._entrada_apos_saida(evento)
        elif evento.tipo == EMBARQUE:
            self._libera_espera()
            self.ciclo.embarque = self._liga(evento, MEDIA, 'entrada no operador do embarque não informada')
            self._fim(BALDEACAO, CONCLUIDO, 'embarque em outro operador')
        else:
            self._alerta(f'{evento.tipo} em {evento.recinto} sem entrada — ignorado')

    def _entrada_apos_saida(self, evento: Evento):
        horas = _horas(evento.data, self.desde)
        if evento.recinto in self.operadores:
            self._libera_espera(evento)
            self.estado, self.recinto, self.desde = 'outro_operador', evento.recinto, evento.data
        elif evento.vazio:
            self._libera_espera(evento, aceita_gate=False)
            self._fim(OPERADOR, CONCLUIDO, 'entregue pelo operador (entrada seguinte com o contêiner vazio)')
        elif horas > self.params.prazo_transito_horas:
            self._libera_espera(evento, aceita_gate=False)
            self._alerta(f'entrada em {evento.recinto} {horas:.0f} h depois da saída — fora do prazo de trânsito')
            self._fim(OPERADOR, CONCLUIDO, 'entregue pelo operador')
        else:
            self._libera_espera(evento)
            motivos = [f'trânsito de {horas:.0f} h'] if horas > self.params.transito_alerta_horas else []
            self.ciclo.entrada_seco = self._liga(evento, ALTA, *motivos)
            self.estado, self.recinto, self.desde = 'seco', evento.recinto, evento.data
            self.ciclo.fluxo = TRA

    def _em_outro_operador(self, evento: Evento):
        if evento.tipo == EMBARQUE:
            self.ciclo.embarque = self._liga(evento, ALTA if evento.recinto == self.recinto else MEDIA)
            self._fim(BALDEACAO, CONCLUIDO, 'embarque em outro operador')
        elif evento.tipo == SAIDA and evento.recinto == self.recinto:
            # só passou pelo outro operador (ex.: Ecoporto): segue em trânsito a partir desta saída
            self._alerta(f'passagem pelo operador {self.recinto}')
            self.estado, self.recinto, self.desde = 'transito', None, evento.data
        elif evento.tipo == ENTRADA and evento.recinto == self.recinto:
            self._fim(INDEFINIDO, INCOMPLETO, f'saída do operador {self.recinto} não informada (reentrada)')
        elif evento.tipo == ENTRADA:
            self._alerta(f'saída do operador {self.recinto} não informada')
            self.etapa_omitida = True
            self.estado, self.desde = 'transito', evento.data
            self._em_transito(evento)
        else:
            self._alerta(f'{evento.tipo} em {evento.recinto} sem entrada — ignorado')

    def _no_seco(self, evento: Evento):
        if evento.tipo == SAIDA and evento.recinto == self.recinto:
            motivos = ['saída do seco vazia (desunitização?)'] if evento.vazio else []
            self.ciclo.saida_seco = self._liga(evento, ALTA, *motivos)
            self._fim(TRA, CONCLUIDO, 'saída do recinto seco')
        elif evento.tipo in (ENTRADA, EMBARQUE):
            onde = 'reentrada' if evento.recinto == self.recinto and evento.tipo == ENTRADA else evento.tipo
            self._fim(TRA, INCOMPLETO, f'saída do recinto seco não informada ({onde} em {evento.recinto})')
        else:
            self._alerta(f'{evento.tipo} em {evento.recinto} sem entrada — ignorado')

    # ------------------------------------------------------------------ fechamento por prazo
    def _fecha_por_prazo(self):
        dias = _horas(self.agora, self.desde) / 24
        if self.estado == 'transito':
            self._libera_espera()
            if self.ciclo.saida and self.ciclo.saida.confianca == BAIXA:
                self._fim(OPERADOR, ABERTO, 'saída duvidosa (CE de outra família) — revisar')
            elif _horas(self.agora, self.desde) > self.params.prazo_transito_horas:
                self._fim(OPERADOR, CONCLUIDO, 'entregue pelo operador')
            else:
                self._fim(INDEFINIDO, ABERTO, 'saiu do operador; aguardando possível entrada em recinto seco')
        elif self.estado in ('operador', 'outro_operador', 'seco'):
            if dias > self.params.dias_abandono:
                self._fim(self.ciclo.fluxo, ABANDONO, f'sem saída em {dias:.0f} dias')
            else:
                rotulo = {'operador': 'no operador', 'outro_operador': f'no operador {self.recinto}',
                          'seco': 'no recinto seco'}[self.estado]
                self._fim(self.ciclo.fluxo, ABERTO, rotulo)

    def percorre(self, eventos: List[Evento]) -> CicloCalculado:
        acoes = {'operador': self._no_operador, 'transito': self._em_transito,
                 'outro_operador': self._em_outro_operador, 'seco': self._no_seco}
        for evento in eventos:
            if self.estado == 'fim':
                break
            if evento.tipo == INSPECAO:
                self._inspecao(evento)
            else:
                acoes[self.estado](evento)
        if self.estado != 'fim':
            self._fecha_por_prazo()
        return self.ciclo


def ciclos_do_conteiner(eventos: Iterable[Evento], operadores_escopo: Set[str], operadores: Set[str],
                        agora: datetime, familias: Optional[Dict[int, Set[str]]] = None,
                        params: Parametros = Parametros()) -> List[CicloCalculado]:
    """Um ciclo por desembarque do contêiner num operador do escopo (Santos).

    operadores: todos os recintos `e_operador` (entrada num deles não é TRA);
    familias: {id do desembarque: CEs da família do CE do ciclo} para corroborar o CE declarado.
    """
    eventos = sem_retificacoes(eventos)
    resultado = []
    for i, evento in enumerate(eventos):
        if evento.tipo != DESEMBARQUE or evento.recinto not in operadores_escopo:
            continue
        fim = next((j for j in range(i + 1, len(eventos)) if eventos[j].tipo == DESEMBARQUE), len(eventos))
        tolerancia = timedelta(hours=params.tolerancia_relogio_horas)
        anteriores = [e for e in eventos[:i] if e.tipo == INSPECAO and evento.data - tolerancia <= e.data]
        familia = (familias or {}).get(evento.id, set())
        percurso = _Percurso(evento, operadores, agora, familia, params)
        resultado.append(percurso.percorre(anteriores + eventos[i + 1:fim]))
    return resultado
