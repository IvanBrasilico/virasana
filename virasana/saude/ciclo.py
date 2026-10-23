"""CICLO DE VIDA e PESAGENS: reaproveita o acompanhamento e a auditoria; acrescenta o que faltava medir.

- serviço: os alertas de `acompanha_ciclo_de_vida.py` (marcas d'água, fila, revisita, imagens por recinto, outro
  gravador) dos desembarques dos últimos DIAS_ACOMPANHAMENTO dias;
- invariantes: `audita_ciclos.Auditoria` nos desembarques das últimas 48 h (violação nova = regressão ou caso real a
  levar ao corpus);
- refeitos: depois de uma troca do núcleo (`--nucleo-desde`), os ciclos dos últimos DIAS_REFAZ dias processados ANTES
  dela — o refaz não terminou (23/10/2026: o de 22/10 perdeu 24 de 41 partes numa queda de rede);
- cobertura: % de ciclos com CE e % dos escaneados com imagem, na semana mais recente que já teve tempo de completar
  (desembarques de 9 a 2 dias atrás) contra a semana anterior; queda de mais de QUEDA_PONTOS pontos = atenção;
- pesagens: % "possível fraude" e % "digitação" da pesagem principal nos últimos 7 dias contra os 21 anteriores; mais
  que SALTO_PESAGEM vezes = atenção (balança ou transmissão com problema).
"""
import io
from datetime import timedelta
from typing import List, Tuple

from sqlalchemy import inspect, text

from virasana.analytic_scripts.ciclo_de_vida import gravacao_ciclo as gc
from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ciclo_de_vida import TEM_CE, TEM_ESCANEAMENTO, TEM_IMAGEM
from virasana.saude.verificacao import ATENCAO, FALHA, NAO_MEDIDO, OK, Resultado, como_data, numero

SERVICO, INVARIANTES, REFEITOS, COBERTURA, PESAGENS = (
    'Ciclo de vida — serviço', 'Ciclo de vida — invariantes', 'Ciclo de vida — refaz', 'Ciclo de vida — cobertura',
    'Pesagens')
DIAS_ACOMPANHAMENTO = 14
HORAS_INVARIANTES = 48
DIAS_REFAZ = 30
QUEDA_PONTOS = 10
SALTO_PESAGEM = 2
MINIMO_PESAGENS = 20             # pesagens principais na semana para comparar
# alertas do acompanhamento que são falha (o serviço parou); os demais são atenção
SINAIS_DE_FALHA = ('serviço parado', "sem marcas d'água")
V2 = f"c.versao_heuristica = '{gc.VERSAO}'"


# ---------------------------------------------------------------------------------------------------- serviço
def alertas_do_acompanhamento(contexto) -> List[str]:
    from virasana.analytic_scripts.ciclo_de_vida.acompanha_ciclo_de_vida import Acompanhamento, roda
    inicio = contexto.agora - timedelta(days=DIAS_ACOMPANHAMENTO)
    with contexto.engine.connect() as conexao:
        return roda(Acompanhamento(conexao, io.StringIO(), inicio, contexto.agora, agora=contexto.agora))


def verifica_servico(contexto) -> Resultado:
    alertas = alertas_do_acompanhamento(contexto)
    if not alertas:
        return Resultado(SERVICO, OK, f'sem alertas no acompanhamento ({DIAS_ACOMPANHAMENTO} dias)')
    falha = any(sinal in alerta for alerta in alertas for sinal in SINAIS_DE_FALHA)
    return Resultado(SERVICO, FALHA if falha else ATENCAO, f'{len(alertas)} alerta(s) do acompanhamento '
                     '(acompanha_ciclo_de_vida.py, seção 7)', alertas)


# ---------------------------------------------------------------------------------------------------- invariantes
def verifica_invariantes(contexto) -> Resultado:
    from virasana.analytic_scripts.ciclo_de_vida.audita_ciclos import Auditoria
    auditoria = Auditoria(contexto.engine, contexto.agora - timedelta(hours=HORAS_INVARIANTES), contexto.agora,
                          progresso=lambda mensagem: None)
    achados = auditoria.executa()
    resumo = (f'{numero(auditoria.total)} ciclos das últimas {HORAS_INVARIANTES} h auditados; '
              f'{len(achados)} com violação')
    detalhes = [f'{a["conteiner"]} desembarque {a["desembarque_id"]} em {a["data_desembarque"]} no {a["recinto"]}: '
                f'{a["violacoes"]}' for a in achados]
    if detalhes:
        detalhes.append('extrair com caso_ciclo.py e levar ao corpus (CICLO_DE_VIDA.md, "Casos reais de regressão")')
    return Resultado(INVARIANTES, ATENCAO if achados else OK, resumo, detalhes)


# ---------------------------------------------------------------------------------------------------- refaz
def verifica_refeitos(contexto) -> Resultado:
    if contexto.nucleo_desde is None:
        return Resultado(REFEITOS, NAO_MEDIDO, 'informe --nucleo-desde AAAA-MM-DD depois de uma troca do núcleo')
    with contexto.engine.connect() as conexao:
        quantos, primeiro, ultimo = conexao.execute(text(
            f'SELECT COUNT(*), MIN(c.data_desembarque), MAX(c.data_desembarque) FROM {gc.CICLOS.name} c '  # nosec B608
            f'WHERE {V2} AND c.data_desembarque >= :inicio AND c.processado_em < :nucleo'),
            {'inicio': contexto.agora - timedelta(days=DIAS_REFAZ), 'nucleo': contexto.nucleo_desde}).first()
    resumo = (f'{numero(quantos)} ciclos dos últimos {DIAS_REFAZ} dias processados antes da troca do núcleo '
              f'({contexto.nucleo_desde:%d/%m/%Y %H:%M})')
    if not quantos:
        return Resultado(REFEITOS, OK, resumo)
    primeiro, ultimo = como_data(primeiro), como_data(ultimo)
    return Resultado(REFEITOS, ATENCAO, resumo, [
        f'desembarques de {primeiro:%d/%m/%Y} a {ultimo:%d/%m/%Y}: refazer no servidor com --refaz-desde '
        f'{primeiro:%Y-%m-%d} --refaz-ate {ultimo + timedelta(days=1):%Y-%m-%d}'])


# ---------------------------------------------------------------------------------------------------- cobertura
def _cobertura(conexao, inicio, fim) -> Tuple[int, int, int, int]:
    """(ciclos v2, com CE, com escaneamento, com imagem) dos desembarques de [inicio, fim)."""
    # SQL montado só com os fragmentos fixos do diagnóstico do ciclo
    linha = conexao.execute(text(
        f'SELECT COUNT(*), SUM(CASE WHEN {TEM_CE} THEN 1 ELSE 0 END), '  # nosec B608
        f'SUM(CASE WHEN {TEM_ESCANEAMENTO} THEN 1 ELSE 0 END), SUM(CASE WHEN {TEM_IMAGEM} THEN 1 ELSE 0 END) '
        f'FROM {gc.CICLOS.name} c WHERE {V2} AND c.data_desembarque >= :inicio AND c.data_desembarque < :fim'),
        {'inicio': inicio, 'fim': fim}).first()
    return tuple(int(valor or 0) for valor in linha)


def _pct(parte: int, total: int):
    return 100.0 * parte / total if total else None


def verifica_cobertura(contexto) -> Resultado:
    fim = contexto.agora - timedelta(days=2)    # os 2 últimos dias ainda recebem CE e imagem
    with contexto.engine.connect() as conexao:
        recente = _cobertura(conexao, fim - timedelta(days=7), fim)
        anterior = _cobertura(conexao, fim - timedelta(days=14), fim - timedelta(days=7))
    medidas = (('com CE', _pct(recente[1], recente[0]), _pct(anterior[1], anterior[0])),
               ('escaneados com imagem', _pct(recente[3], recente[2]), _pct(anterior[3], anterior[2])))
    resumo = '; '.join(f'{nome} {_texto_pct(atual)} (sem. ant. {_texto_pct(antes)})' for nome, atual, antes in medidas)
    detalhes = [f'{nome}: {_texto_pct(atual)} na semana mais recente, {_texto_pct(antes)} na anterior — queda de '
                f'{antes - atual:.0f} pontos'.replace('.', ',') for nome, atual, antes in medidas
                if atual is not None and antes is not None and antes - atual > QUEDA_PONTOS]
    return Resultado(COBERTURA, ATENCAO if detalhes else OK,
                     f'{numero(recente[0])} ciclos (desembarques de 9 a 2 dias atrás): {resumo}', detalhes)


def _texto_pct(valor) -> str:
    return '—' if valor is None else f'{valor:.0f}%'


# ---------------------------------------------------------------------------------------------------- pesagens
def verifica_pesagens(contexto) -> Resultado:
    if not inspect(contexto.engine).has_table(gc.PESAGENS.name):
        return Resultado(PESAGENS, NAO_MEDIDO, f'tabela {gc.PESAGENS.name} ausente')
    recente = contexto.agora - timedelta(days=7)
    with contexto.engine.connect() as conexao:
        linhas = conexao.execute(text(
            'SELECT diagnostico, SUM(CASE WHEN data >= :recente THEN 1 ELSE 0 END), '  # nosec B608
            f'SUM(CASE WHEN data < :recente THEN 1 ELSE 0 END) FROM {gc.PESAGENS.name} '
            'WHERE principal = 1 AND data >= :inicio AND data < :agora GROUP BY diagnostico'),
            {'recente': recente, 'inicio': recente - timedelta(days=21), 'agora': contexto.agora}).all()
    semana = {d: int(r or 0) for d, r, _ in linhas}
    antes = {d: int(a or 0) for d, _, a in linhas}
    total, total_antes = sum(semana.values()), sum(antes.values())
    detalhes = []
    for diagnostico, rotulo in (('divergencia', 'possível fraude'), ('digitacao', 'digitação provável')):
        atual, anterior = _pct(semana.get(diagnostico, 0), total), _pct(antes.get(diagnostico, 0), total_antes)
        if total >= MINIMO_PESAGENS and atual and anterior and atual > SALTO_PESAGEM * anterior:
            detalhes.append(f'{rotulo}: {atual:.1f}% nos últimos 7 dias, {anterior:.1f}% nos 21 anteriores — balança '
                            'ou transmissão com problema?'.replace('.', ','))
    resumo = (f'{numero(total)} pesagens principais em 7 dias; possível fraude '
              f'{_texto_pct(_pct(semana.get("divergencia", 0), total))}, digitação '
              f'{_texto_pct(_pct(semana.get("digitacao", 0), total))}')
    return Resultado(PESAGENS, ATENCAO if detalhes else OK, resumo, detalhes)
