"""APLICAÇÕES que dependem de serviços próprios: risco histórico, busca por texto, NAVI-I (tempo de resposta) e as
Fichas de FMA (carga abandonada) criadas pelo `fma_update.py` do Fichas.

- risco histórico: a etapa roda a cada volta do serviço do ciclo — marca da extração atualizada há mais de
  RISCO_PARADO_HORAS, IDs do Mercante ainda não lidos ou CEs na fila da pontuação acima dos limites = atenção;
- busca por texto: o Meilisearch responde, o índice acompanha o Mercante (IDs por indexar acima de BUSCA_ATRASO_IDS =
  atenção) — precisa da chave do indexador no ambiente (`NAVII_MEILI_CHAVE_ESCRITA`), senão sai não medido;
- NAVI-I: tempo no servidor da 1ª página das pesquisas das últimas 24 h (a coleta de uso grava contagem, consulta e
  cards): mediana, p95 e % de lentas (>= SEGUNDOS_LENTA, o limite da tela);
- Fichas de FMA: a mais nova criada há mais de FMA_DIAS dias = atenção (o cron diário do Fichas parou?).
"""
from datetime import timedelta
from statistics import median
from typing import List

from sqlalchemy import inspect, select, text

from virasana.saude.verificacao import (
    ATENCAO, FALHA, NAO_MEDIDO, OK, Resultado, ha_quanto, horas_desde, numero,
)

RISCO, BUSCA, NAVII, FMA = ('Risco histórico', 'Busca por texto (Meilisearch)', 'NAVI-I (tempo de resposta)',
                            'Fichas de FMA (abandono)')
RISCO_PARADO_HORAS = 6
RISCO_IDS_NAO_LIDOS = 100000
RISCO_FILA_PONTUACAO = 50000
BUSCA_ATRASO_IDS = 20000
NAVII_MINIMO = 20                # pesquisas nas 24 h para julgar o tempo
NAVII_P95_SEGUNDOS = 10
NAVII_LENTAS = 0.05
FMA_DIAS = 3


# ---------------------------------------------------------------------------------------------------- risco
def verifica_risco(contexto) -> Resultado:
    from virasana.risco_historico.extracao import Extracao
    from virasana.risco_historico.pontuacao import Pontuacao
    from virasana.risco_historico.tabelas import CES, MARCAS
    if not inspect(contexto.engine).has_table(CES.name):
        return Resultado(RISCO, NAO_MEDIDO, f'tabela {CES.name} ausente')
    with contexto.engine.connect() as conexao:
        atualizada = conexao.execute(select(MARCAS.c.atualizado_em).order_by(MARCAS.c.atualizado_em.desc())
                                     .limit(1)).scalar()
        extracao = Extracao(conexao, relogio=lambda: contexto.agora).situacao()
        pontuacao = Pontuacao(conexao, relogio=lambda: contexto.agora).situacao()
    horas = horas_desde(contexto.agora, atualizada)
    nao_lidos, fila = extracao['IDs do Mercante ainda não lidos'], pontuacao['CEs na fila da pontuação'] or 0
    detalhes = []
    if horas is None or horas > RISCO_PARADO_HORAS:
        detalhes.append(f'extração parada: marca atualizada {ha_quanto(horas)} — a etapa roda no serviço do ciclo '
                        '(sem --sem-risco?)')
    if nao_lidos > RISCO_IDS_NAO_LIDOS:
        detalhes.append(f'{numero(nao_lidos)} IDs do Mercante ainda não lidos pela extração')
    if fila > RISCO_FILA_PONTUACAO:
        detalhes.append(f'{numero(fila)} CEs na fila da pontuação')
    faixas = ', '.join(f'{faixa} {numero(n)}' for faixa, n in sorted(pontuacao['CEs por faixa'].items()))
    return Resultado(RISCO, ATENCAO if detalhes else OK,
                     f'extração {ha_quanto(horas)}; {numero(nao_lidos)} IDs por ler; fila {numero(fila)}; {faixas}',
                     detalhes)


# ---------------------------------------------------------------------------------------------------- busca por texto
def verifica_busca(contexto) -> Resultado:
    from virasana.navii import texto_navii as tn
    from virasana.navii.indexador_texto import CHAVE_MARCAS
    if contexto.meili is None:
        return Resultado(BUSCA, NAO_MEDIDO, f'sem a chave do indexador ({tn.VARIAVEL_ESCRITA}) no ambiente')
    estado = (contexto.meili.saude() or {}).get('status')
    if estado != 'available':
        return Resultado(BUSCA, FALHA, f'Meilisearch não está disponível (status {estado!r})')
    marcas = contexto.meili.documento(tn.INDICE_ESTADO, CHAVE_MARCAS) or {}
    estatisticas = contexto.meili.estatisticas(tn.INDICE) or {}
    with contexto.engine.connect() as conexao:
        maior = conexao.execute(text('SELECT MAX(ID) FROM conhecimentosresumo')).scalar() or 0
    por_indexar = int(maior) - int(marcas.get('ultimo_id') or 0) if marcas else None
    resumo = (f'{numero(estatisticas.get("numberOfDocuments") or 0)} CEs no índice; '
              f'{"sem marcas" if por_indexar is None else numero(por_indexar) + " IDs do Mercante por indexar"}')
    if por_indexar is None or por_indexar > BUSCA_ATRASO_IDS:
        return Resultado(BUSCA, ATENCAO, resumo, ['o indexador (programa navii_texto do supervisor) está atrasado ou '
                                                  'parado: CEs novos não aparecem na busca por texto'])
    return Resultado(BUSCA, OK, resumo)


# ---------------------------------------------------------------------------------------------------- NAVI-I
def verifica_navii(contexto) -> Resultado:
    from virasana.navii.coleta_uso import COLECAO
    from virasana.navii.uso_navii import SEGUNDOS_LENTA
    if contexto.mongo is None:
        return Resultado(NAVII, NAO_MEDIDO, 'sem conexão com o Mongo do virasana')
    tempos = _tempos_das_pesquisas(contexto.mongo[COLECAO], contexto.agora - timedelta(days=1))
    if not tempos:
        return Resultado(NAVII, OK, 'nenhuma pesquisa nas últimas 24 h')
    tempos.sort()
    p95 = tempos[min(len(tempos) - 1, int(0.95 * len(tempos)))]
    lentas = sum(1 for t in tempos if t >= SEGUNDOS_LENTA)
    resumo = (f'{numero(len(tempos))} pesquisas em 24 h; 1ª página no servidor: mediana {median(tempos):.1f} s, '
              f'p95 {p95:.1f} s; {lentas} lenta(s) (>= {SEGUNDOS_LENTA} s)').replace('.', ',')
    ruim = len(tempos) >= NAVII_MINIMO and (p95 > NAVII_P95_SEGUNDOS or lentas > NAVII_LENTAS * len(tempos))
    if not ruim:
        return Resultado(NAVII, OK, resumo)
    return Resultado(NAVII, ATENCAO, resumo,
                     ['ver as lentas no log (grep NAVII_LENTA) e medir com navii/diagnostico_sql.py --analyze'])


def _tempos_das_pesquisas(colecao, desde) -> List[float]:
    """Segundos no servidor da 1ª página de cada pesquisa (contagem ou busca + consulta + cards)."""
    tempos = []
    for documento in colecao.find({'inicio': {'$gte': desde}}, {'tempos': 1}):
        partes = [valor for valor in (documento.get('tempos') or {}).values() if isinstance(valor, (int, float))]
        if partes:
            tempos.append(float(sum(partes)))
    return tempos


# ---------------------------------------------------------------------------------------------------- Fichas de FMA
def verifica_fma(contexto) -> Resultado:
    if not inspect(contexto.engine).has_table('ovr_ovrs'):
        return Resultado(FMA, NAO_MEDIDO, 'tabela ovr_ovrs ausente')
    with contexto.engine.connect() as conexao:
        mais_nova = conexao.execute(text('SELECT MAX(create_date) FROM ovr_ovrs WHERE tipooperacao = 0')).scalar()
    horas = horas_desde(contexto.agora, mais_nova)
    resumo = f'Ficha de FMA mais nova criada {ha_quanto(horas)}'
    if horas is None or horas > 24 * FMA_DIAS:
        return Resultado(FMA, ATENCAO, resumo, [f'nenhuma Ficha de FMA nova em {FMA_DIAS} dias: conferir o cron do '
                                                'fma_update.py (bhadrasana2) e a API da Janela Única Portuária'])
    return Resultado(FMA, OK, resumo)
