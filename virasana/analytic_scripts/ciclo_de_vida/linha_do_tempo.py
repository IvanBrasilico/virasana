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
  - entrada com o contêiner VAZIO depois da saída (devolução de vazio — outra operação);
  - entrada no OPERADOR DO DESEMBARQUE depois de sair dele (o contêiner voltou: outra operação).
Com a fronteira, a próxima operação nunca é confundida com esta, mesmo com prazos longos.

Exceção ao "novo desembarque" e ao "embarque" — REMOÇÃO A BORDO (pedido do usuário, 02/10/2026): desembarque,
embarque e novo desembarque no MESMO operador, o novo desembarque em até `remocao_horas` depois do embarque e
sem acesso de gate no meio (ex.: contêiner tirado do navio para remover outro e devolvido; correção do
operador). O contêiner não saiu da operação: o embarque e o novo desembarque são ignorados, o ciclo do primeiro
desembarque continua, e o novo desembarque fica em `CicloCalculado.remocoes` (a gravação apaga o ciclo que
tenha sido criado para ele).

Estados: no OPERADOR do desembarque -> em TRÂNSITO (saiu) -> em OUTRO OPERADOR (passagem/baldeação;
`ovr_recintos.e_operador`) ou no SECO (TRA, recinto não operador) -> fim.

Omissões e escaneamento fora (decisões do usuário, 02/10/2026):
  - entrada implícita: em trânsito, o recinto informou inspeção ou PESAGEM e depois a saída, mas não a
    entrada — houve uma entrada nele na hora da primeira inspeção/pesagem (confiança média);
  - escaneamento fora antes do TRA: a passagem por outro operador (ex.: Ecoporto) ou uma estada CURTA num
    recinto não operador seguida de outra entrada (ex.: MULTILOG) não é o TRA — a inspeção feita ali fica
    no ciclo, marcada "escaneamento fora"; os acessos dessa passagem não entram no ciclo.

Operador em papel de TRA (usuário, 15/10/2026; medição em `diagnostico_operador_tra.py`): um operador pode armazenar
a carga desembarcada em OUTRO operador (ex.: desembarca na Santos Brasil e fica na Embraport). A duração separa os
dois casos: as passagens para escanear duram até 3 h; as armazenagens, mais de 6 h (mediana de 7 dias):
  - estada no outro operador de mais de `estada_operador_tra_horas` (6 h) que termina em saída (ou em nova entrada,
    com a saída omitida) = ele foi o TRA: entrada e saída do seco são as dele e o escaneamento de lá não é "fora";
    até 6 h, continua passagem (escaneamento terceirizado). Embarque nele continua sendo baldeação;
  - ainda lá há mais de 6 h: TRA provisório, mas só quando o CE do ciclo é de importação emitido em Santos
    (`importacao`) — sem CE, cabotagem ou CE de outro porto, a medição mostrou que costuma ser baldeação esperando o
    navio. Se embarcar depois, o ciclo refeito vira baldeação.
Fronteiras do trânsito (achado da mesma medição): a entrada noutro OPERADOR também passa pelo teste do vazio e pelo
prazo de trânsito (antes, uma devolução de vazio noutro terminal semanas depois virava "baldeação"), e um embarque
em trânsito depois do prazo é outra operação.

Confiança (só alta e média concluem o ciclo):
  - alta: evento no recinto esperado, ou CE declarado da família do CE do ciclo;
  - média: recinto diferente (ex.: escaneamento terceirizado), etapa anterior omitida, contêiner
    marcado vazio (sinal fraco: a medição mostrou vazio com o mesmo CE), permanência longa;
  - baixa: CE declarado de OUTRA família (um número que nem existe no Mercante só não corrobora: média).
O CE declarado só corrobora/desconfia: nunca decide sozinho (dado declarado, não 100% confiável).

O resultado depende só dos eventos e de `agora`: recalcular quando chegam eventos novos é seguro
(é a base do processamento orientado a eventos).
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set

DESEMBARQUE, EMBARQUE, ENTRADA, SAIDA, INSPECAO = 'desembarque', 'embarque', 'entrada', 'saida', 'inspecao'
PESAGEM = 'pesagem'
ALTA, MEDIA, BAIXA = 'alta', 'media', 'baixa'
_NOMES = {INSPECAO: 'inspeção', PESAGEM: 'pesagem'}
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
    passagem_rapida_horas: float = 3        # estada curta num seco seguida de outra entrada: só passou
    passagem_ate_entrada_horas: float = 6   # ... desde que a outra entrada venha até este prazo
    remocao_horas: float = 24               # embarque e novo desembarque no mesmo operador: remoção a bordo
    estada_operador_tra_horas: float = 6    # estada noutro operador mais longa que isso: ele foi o TRA


@dataclass(frozen=True)
class Evento:
    tipo: str                               # DESEMBARQUE, EMBARQUE, ENTRADA, SAIDA, INSPECAO, PESAGEM
    id: int
    data: datetime
    recinto: str
    vazio: Optional[bool] = None
    ce_declarado: Optional[str] = None
    implicita_de: Optional[str] = None      # ENTRADA não informada, deduzida de uma INSPECAO/PESAGEM (mesmo id)


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
    remocoes: List[Evento] = field(default_factory=list)   # desembarques de remoção a bordo absorvidos

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
                 params: Parametros, conhecidos: Optional[Set[str]] = None, importacao: bool = False):
        self.ciclo = CicloCalculado(desembarque)
        self.operadores, self.agora, self.familia, self.params = operadores, agora, familia, params
        self.conhecidos = conhecidos             # CEs que existem no Mercante (None: não verificar)
        self.importacao = importacao             # CE de importação emitido em Santos (TRA provisório no operador)
        self.entrada_outro: Optional[Evento] = None   # entrada no outro operador (vira entrada no seco se for o TRA)
        self.transito_horas = 0.0
        self.operador_tra = False
        self.estado = 'operador'
        self.recinto = desembarque.recinto
        self.desde = desembarque.data            # início do estado atual (para prazos)
        self.etapa_omitida = False
        self.em_espera: List[Evento] = []        # inspeções em trânsito, à espera do próximo evento
        self.indicios: List[Evento] = []         # pesagens em trânsito (indício de entrada não informada)
        self.inicio_estada = 0                   # escaneamentos ligados antes da estada atual

    # ------------------------------------------------------------------ apoio
    def _confianca(self, evento: Evento, base: str, motivos: List[str]) -> str:
        """Ajusta a confiança pelo CE declarado (corrobora/desconfia) e pelo vazio (sinal fraco)."""
        declarado = (evento.ce_declarado or '').strip()
        if declarado and self.familia:
            if declarado in self.familia:
                base = ALTA
                motivos.append('CE declarado da família do CE do ciclo')
            elif self.conhecidos is not None and declarado not in self.conhecidos:
                # número que não existe no Mercante não indica outra operação (sombra de 02/10: a maioria
                # das "outras famílias" era isso) — só não corrobora
                base = _rebaixa(base, MEDIA)
                motivos.append('CE declarado fora do Mercante')
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

    # ------------------------------------------------------------------ inspeções e pesagens
    def _inspecao(self, evento: Evento):
        # em trânsito, ou passando por outro operador com a inspeção em OUTRO recinto (ex.: o contêiner
        # "dentro" do Ecoporto e a inspeção no gate do TRA para onde vai): decide o próximo evento
        fora = self.estado == 'outro_operador' and evento.recinto != self.recinto
        if self.estado in ('transito', 'pos_seco') or fora:
            self.em_espera.append(evento)
            return
        motivos = ['no recinto da etapa'] if evento.recinto == self.recinto else ['em outro recinto (terceirizado?)']
        base = ALTA if evento.recinto == self.recinto else MEDIA
        if evento.data < self.ciclo.desembarque.data:
            base = MEDIA
            motivos.append('antes do desembarque (relógio)')
        self.ciclo.escaneamentos.append(self._liga(evento, base, *motivos))

    def _pesagem(self, evento: Evento):
        """Pesagem só interessa em trânsito: indício de que o contêiner entrou num recinto que não informou
        a entrada (regra do usuário, 02/10/2026)."""
        if self.estado == 'transito':
            self.indicios.append(evento)

    def _libera_espera(self, proxima: Optional[Evento] = None, aceita_gate: bool = True):
        """Inspeções feitas em trânsito: dentro do recinto da próxima entrada (entrada implícita: inspeção na
        hora da entrada ou depois), junto ao gate dela (scanner no gate) ou soltas ("em trânsito")."""
        for inspecao in self.em_espera:
            onde = self._onde(inspecao, proxima)
            if onde != 'transito' and not aceita_gate:
                continue  # é da entrada seguinte, que NÃO é desta operação
            if onde == 'dentro':
                self.ciclo.escaneamentos.append(self._liga(inspecao, ALTA, 'no recinto da etapa (entrada implícita)'))
            elif onde == 'gate':
                base = ALTA if inspecao.recinto == proxima.recinto else MEDIA
                self.ciclo.escaneamentos.append(self._liga(inspecao, base, 'antes da entrada (scanner no gate)'))
            else:
                self.ciclo.escaneamentos.append(self._liga(inspecao, MEDIA, 'em trânsito'))
        self.em_espera, self.indicios = [], []

    def _onde(self, inspecao: Evento, proxima: Optional[Evento]) -> str:
        """'dentro' (no recinto de uma entrada implícita, na hora dela ou depois), 'gate' ou 'transito'."""
        if proxima is None:
            return 'transito'
        horas = _horas(proxima.data, inspecao.data)
        if proxima.implicita_de and horas <= 0 and inspecao.recinto == proxima.recinto:
            return 'dentro'
        return 'gate' if 0 <= horas <= self.params.tolerancia_gate_horas else 'transito'

    def _marca_estada(self, texto: str):
        """Anota os escaneamentos da estada que acabou de se revelar uma passagem (escaneamento fora)."""
        for ligacao in self.ciclo.escaneamentos[self.inicio_estada:]:
            ligacao.motivos.append(texto)

    # ------------------------------------------------------------------ estados
    def _embarque(self, evento: Evento, base: str, fluxo: str, motivo: str, *motivos: str):
        self._libera_espera()
        self.ciclo.embarque = self._liga(evento, base, *motivos)
        if self.ciclo.embarque.confianca == BAIXA:
            self._fim(INDEFINIDO, INCOMPLETO, 'embarque com CE de outra família — pode ser outra operação '
                                              '(ex.: vazio reembarcado)')
        else:
            self._fim(fluxo, CONCLUIDO, motivo)

    def _no_operador(self, evento: Evento):
        if evento.tipo == EMBARQUE:
            mesmo = evento.recinto == self.recinto
            if not mesmo:
                self._alerta('saída do operador não informada')
            self._embarque(evento, ALTA if mesmo else MEDIA, PASSAGEM if mesmo else BALDEACAO,
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
            self._embarque_em_transito(evento)
        elif evento.tipo == SAIDA and self._indicio(evento.recinto):
            self._saida_sem_entrada(evento)
        else:
            self._alerta(f'{evento.tipo} em {evento.recinto} sem entrada — ignorado')

    def _embarque_em_transito(self, evento: Evento):
        horas = _horas(evento.data, self.desde)
        if horas > self.params.prazo_transito_horas:
            # semanas depois da saída: é outra operação (ex.: o contêiner reaproveitado na exportação)
            self._libera_espera(evento, aceita_gate=False)
            self._alerta(f'embarque em {evento.recinto} {horas:.0f} h depois da saída — outra operação')
            self._entrega('entregue pelo operador')
        else:
            self._embarque(evento, MEDIA, BALDEACAO, 'embarque em outro operador',
                           'entrada no operador do embarque não informada')

    def _indicio(self, recinto: str) -> Optional[Evento]:
        """Primeira inspeção/pesagem em trânsito no recinto (indício de entrada não informada)."""
        candidatos = [e for e in self.em_espera + self.indicios if e.recinto == recinto]
        return min(candidatos, key=lambda e: e.data) if candidatos else None

    def _saida_sem_entrada(self, saida: Evento):
        """Regra do usuário (02/10/2026): o recinto informou escaneamento ou pesagem, e a saída, mas não a
        entrada — houve uma entrada nele na hora do primeiro escaneamento/pesagem."""
        indicio = self._indicio(saida.recinto)
        entrada = Evento(ENTRADA, indicio.id, indicio.data, saida.recinto, indicio.vazio, None, indicio.tipo)
        self._alerta(f'entrada em {saida.recinto} não informada — deduzida da {_NOMES[indicio.tipo]}')
        self._entrada_apos_saida(entrada)
        if self.estado == 'seco':
            self._no_seco(saida)
        elif self.estado == 'outro_operador':
            self._em_outro_operador(saida)

    def _entrada_apos_saida(self, evento: Evento):
        horas = _horas(evento.data, self.desde)
        if evento.recinto == self.ciclo.desembarque.recinto:
            # voltou ao operador do desembarque depois de sair: é outra operação (sombra de 01/10: quase
            # sempre a devolução do vazio para exportar) — fronteira
            self._libera_espera(evento, aceita_gate=False)
            self._entrega('entregue pelo operador (depois voltou ao operador do desembarque)')
        elif evento.vazio:
            self._libera_espera(evento, aceita_gate=False)
            self._entrega('entregue pelo operador (entrada seguinte com o contêiner vazio)')
        elif horas > self.params.prazo_transito_horas:
            self._libera_espera(evento, aceita_gate=False)
            self._alerta(f'entrada em {evento.recinto} {horas:.0f} h depois da saída — fora do prazo de trânsito')
            self._entrega('entregue pelo operador')
        elif evento.recinto in self.operadores:
            self.inicio_estada = len(self.ciclo.escaneamentos)
            self._libera_espera(evento)
            self.estado, self.recinto, self.desde = 'outro_operador', evento.recinto, evento.data
            self.entrada_outro, self.transito_horas = evento, horas
        else:
            self._entrada_no_seco(evento, horas)

    def _ligacao_entrada_seco(self, evento: Evento, horas: float, *extras: str) -> Ligacao:
        motivos = ([f'trânsito de {horas:.0f} h'] if horas > self.params.transito_alerta_horas else []) + list(extras)
        base = ALTA
        if evento.implicita_de:
            base = MEDIA
            motivos.append(f'entrada não informada — deduzida da {_NOMES[evento.implicita_de]} no recinto')
        return self._liga(evento, base, *motivos)

    def _entrada_no_seco(self, evento: Evento, horas: float):
        self.inicio_estada = len(self.ciclo.escaneamentos)
        self._libera_espera(evento)
        self.ciclo.entrada_seco = self._ligacao_entrada_seco(evento, horas)
        self.estado, self.recinto, self.desde = 'seco', evento.recinto, evento.data
        self.ciclo.fluxo = TRA

    def _operador_foi_tra(self):
        """A estada no outro operador foi de armazenagem: a entrada nele é a entrada no seco (ver docstring)."""
        papel = f'operador {self.recinto} em papel de TRA'
        self.ciclo.entrada_seco = self._ligacao_entrada_seco(self.entrada_outro, self.transito_horas, papel)
        self.ciclo.fluxo, self.operador_tra = TRA, True
        self._alerta(papel)

    def _em_outro_operador(self, evento: Evento):
        termina = evento.tipo == ENTRADA or (evento.tipo == SAIDA and evento.recinto == self.recinto)
        if evento.tipo == EMBARQUE:
            self._embarque(evento, ALTA if evento.recinto == self.recinto else MEDIA, BALDEACAO,
                           'embarque em outro operador')
        elif termina and _horas(evento.data, self.desde) > self.params.estada_operador_tra_horas:
            self._fim_do_operador_tra(evento)
        elif evento.tipo == SAIDA and evento.recinto == self.recinto:
            # só passou pelo outro operador (ex.: Ecoporto escaneando para o TRA): segue em trânsito
            self._alerta(f'passagem pelo operador {self.recinto}')
            self._marca_estada(f'na passagem pelo operador {self.recinto} (escaneamento fora)')
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

    def _fim_do_operador_tra(self, evento: Evento):
        """Estada longa no outro operador terminando em saída (TRA concluído) ou em nova entrada (saída omitida)."""
        recinto = self.recinto
        self._operador_foi_tra()
        if evento.tipo == SAIDA:
            motivos = ['saída do seco vazia (desunitização?)'] if evento.vazio else []
            self.ciclo.saida_seco = self._liga(evento, ALTA, *motivos)
            self._fim(TRA, CONCLUIDO, f'saída do recinto seco (operador {recinto} em papel de TRA)')
        else:
            onde = 'reentrada' if evento.recinto == recinto else 'entrada'
            self._fim(TRA, INCOMPLETO, f'saída do recinto seco não informada ({onde} em {evento.recinto}; '
                                       f'operador {recinto} em papel de TRA)')

    def _no_seco(self, evento: Evento):
        if evento.tipo == SAIDA and evento.recinto == self.recinto:
            motivos = ['saída do seco vazia (desunitização?)'] if evento.vazio else []
            self.ciclo.saida_seco = self._liga(evento, ALTA, *motivos)
            if _horas(evento.data, self.desde) <= self.params.passagem_rapida_horas:
                # estada curta: pode ser só passagem (escaneamento fora) — decide o próximo evento
                self.estado, self.desde = 'pos_seco', evento.data
            else:
                self._fim(TRA, CONCLUIDO, 'saída do recinto seco')
        elif evento.tipo in (ENTRADA, EMBARQUE):
            onde = 'reentrada' if evento.recinto == self.recinto and evento.tipo == ENTRADA else evento.tipo
            self._fim(TRA, INCOMPLETO, f'saída do recinto seco não informada ({onde} em {evento.recinto})')
        else:
            self._alerta(f'{evento.tipo} em {evento.recinto} sem entrada — ignorado')

    def _apos_seco(self, evento: Evento):
        """Depois de uma estada curta num seco: nova entrada (cheia, em outro recinto, logo em seguida) =
        a estada foi só passagem (ex.: escaneamento terceirizado) e o TRA é o próximo; senão, era o TRA."""
        passagem = (evento.tipo == ENTRADA and not evento.vazio and evento.recinto != self.recinto and
                    evento.recinto != self.ciclo.desembarque.recinto and
                    _horas(evento.data, self.desde) <= self.params.passagem_ate_entrada_horas)
        if not passagem:
            self._conclui_tra()
            return
        self._alerta(f'passagem rápida pelo recinto {self.recinto}')
        self._marca_estada(f'na passagem rápida pelo recinto {self.recinto} (escaneamento fora)')
        self.ciclo.entrada_seco = self.ciclo.saida_seco = None
        self.ciclo.fluxo, self.estado, self.recinto = INDEFINIDO, 'transito', None
        self._entrada_apos_saida(evento)

    def _conclui_tra(self):
        self.em_espera = []  # inspeções depois da saída do seco são de outra etapa
        self._fim(TRA, CONCLUIDO, 'saída do recinto seco')

    # ------------------------------------------------------------------ fechamento
    def _entrega(self, motivo: str):
        """Fecha como entregue pelo operador — menos quando a saída é duvidosa (CE de outra família)."""
        if self.ciclo.saida and self.ciclo.saida.confianca == BAIXA:
            self._fim(OPERADOR, ABERTO, 'saída duvidosa (CE de outra família) — revisar')
        else:
            self._fim(OPERADOR, CONCLUIDO, motivo)

    def _fecha_por_prazo(self):
        horas = _horas(self.agora, self.desde)
        if self.estado == 'outro_operador' and self.importacao and horas > self.params.estada_operador_tra_horas:
            self._operador_foi_tra()   # TRA provisório: ainda lá, com CE de importação de Santos
            self.estado = 'seco'
        if self.estado == 'transito':
            self._libera_espera()
            duvidosa = self.ciclo.saida and self.ciclo.saida.confianca == BAIXA
            if duvidosa or horas > self.params.prazo_transito_horas:
                self._entrega('entregue pelo operador')
            else:
                self._fim(INDEFINIDO, ABERTO, 'saiu do operador; aguardando possível entrada em recinto seco')
        elif self.estado == 'pos_seco':
            if horas > self.params.passagem_ate_entrada_horas:
                self._conclui_tra()
            else:
                self._fim(TRA, ABERTO, 'saiu do recinto seco após estada curta; aguardando possível nova entrada')
        elif horas / 24 > self.params.dias_abandono:
            self._fim(self.ciclo.fluxo, ABANDONO, f'sem saída em {horas / 24:.0f} dias')
        else:
            self._fim(self.ciclo.fluxo, ABERTO, self._rotulo_aberto())

    def _rotulo_aberto(self) -> str:
        if self.estado == 'seco' and self.operador_tra:
            return f'no recinto seco (operador {self.recinto} em papel de TRA)'
        return {'operador': 'no operador', 'outro_operador': f'no operador {self.recinto}',
                'seco': 'no recinto seco'}[self.estado]

    def percorre(self, eventos: List[Evento]) -> CicloCalculado:
        acoes = {'operador': self._no_operador, 'transito': self._em_transito,
                 'outro_operador': self._em_outro_operador, 'seco': self._no_seco, 'pos_seco': self._apos_seco}
        for evento in eventos:
            if self.estado == 'fim':
                break
            if evento.tipo == INSPECAO:
                self._inspecao(evento)
            elif evento.tipo == PESAGEM:
                self._pesagem(evento)
            else:
                acoes[self.estado](evento)
        if self.estado != 'fim':
            self._fecha_por_prazo()
        return self.ciclo


def _remocoes(eventos: List[Evento], params: Parametros) -> Dict[int, Evento]:
    """{id do novo desembarque: embarque} das remoções a bordo (ver docstring do módulo): desembarque -> embarque
    -> novo desembarque, os três no mesmo recinto, sem ENTRADA/SAÍDA de gate desde o primeiro desembarque e o
    novo desembarque em até `remocao_horas` depois do embarque. Encadeia (várias remoções seguidas)."""
    remocoes, ultimo_desembarque, embarque = {}, None, None
    for evento in eventos:
        if evento.tipo == DESEMBARQUE:
            if (embarque is not None and evento.recinto == embarque.recinto
                    and _horas(evento.data, embarque.data) <= params.remocao_horas):
                remocoes[evento.id] = embarque
            ultimo_desembarque, embarque = evento, None
        elif evento.tipo == EMBARQUE:
            mesmo = ultimo_desembarque is not None and evento.recinto == ultimo_desembarque.recinto
            embarque = evento if mesmo else None
        elif evento.tipo in (ENTRADA, SAIDA):
            ultimo_desembarque, embarque = None, None
    return remocoes


def ciclos_do_conteiner(eventos: Iterable[Evento], operadores_escopo: Set[str], operadores: Set[str],
                        agora: datetime, familias: Optional[Dict[int, Set[str]]] = None,
                        params: Parametros = Parametros(),
                        conhecidos: Optional[Set[str]] = None,
                        importacao: Optional[Dict[int, bool]] = None) -> List[CicloCalculado]:
    """Um ciclo por desembarque do contêiner num operador do escopo (Santos).

    operadores: todos os recintos `e_operador` (entrada num deles não é TRA);
    familias: {id do desembarque: CEs da família do CE do ciclo} para corroborar o CE declarado;
    conhecidos: CEs que existem no Mercante — declarado fora dele não indica outra família (None: não
    verificar, todo declarado fora da família desconfia);
    importacao: {id do desembarque: o CE principal é de importação emitido em Santos} — só com ele uma estada longa
    AINDA em andamento noutro operador vira TRA provisório (ver docstring do módulo).
    """
    eventos = sem_retificacoes(eventos)
    remocoes = _remocoes(eventos, params)
    embarques_de_remocao = {e.id for e in remocoes.values()}

    def de_remocao(e: Evento) -> bool:
        return (e.tipo == DESEMBARQUE and e.id in remocoes) or (e.tipo == EMBARQUE and e.id in embarques_de_remocao)

    resultado = []
    for i, evento in enumerate(eventos):
        if evento.tipo != DESEMBARQUE or evento.recinto not in operadores_escopo or evento.id in remocoes:
            continue
        fim = next((j for j in range(i + 1, len(eventos))
                    if eventos[j].tipo == DESEMBARQUE and eventos[j].id not in remocoes), len(eventos))
        tolerancia = timedelta(hours=params.tolerancia_relogio_horas)
        anteriores = [e for e in eventos[:i] if e.tipo == INSPECAO and evento.data - tolerancia <= e.data]
        familia = (familias or {}).get(evento.id, set())
        percurso = _Percurso(evento, operadores, agora, familia, params, conhecidos,
                             (importacao or {}).get(evento.id, False))
        segmento = eventos[i + 1:fim]
        ciclo = percurso.percorre(anteriores + [e for e in segmento if not de_remocao(e)])
        for desembarque in (e for e in segmento if e.tipo == DESEMBARQUE):
            horas = _horas(desembarque.data, remocoes[desembarque.id].data)
            ciclo.remocoes.append(desembarque)
            ciclo.alertas.append(f'remoção a bordo em {desembarque.recinto}: embarque e novo desembarque em '
                                 f'{horas:.0f} h — o ciclo continua')
        resultado.append(ciclo)
    return resultado
