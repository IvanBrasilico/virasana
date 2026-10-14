# coding: utf-8
"""
Lote 2a do risco histórico por CE: extrai e grava em `risco_historico_ce` os
SINAIS de cada CE (não exportação), a partir de uma marca inicial e depois
incrementalmente (ver CONTEXTO.md). Três marcas d'água (`risco_historico_marca`):

  - CEs novos: ID do conhecimentosresumo (cresce a cada inclusão);
  - CEs alterados (retificação): `create_date` do conhecimentosresumo, que é a
    data da ÚLTIMA ALTERAÇÃO (ON UPDATE current_timestamp no banco) e tem índice.
    Só re-extrai se mudou desde a leitura gravada (`ce_alterado_em`) — sem
    depender do relógio desta máquina;
  - NCMs que chegam depois do CE: ID do ncmitemresumo; só re-extrai o CE se a
    NCM é mais nova que a maior usada nos sinais dele (`ncm_id`).

Marca inicial: o menor ID dos CEs emitidos (dataEmissao, com índice) nos últimos
N dias (decisão do usuário: 1 mês), ou um ID informado; recusa se a faixa de IDs
for grande demais (dataEmissao errada num CE antigo puxaria a marca para trás).

Re-extrair é seguro: os sinais dependem só do Mercante. Quem chama controla a
transação (a linha de comando faz uma por volta; --simula desfaz).
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Tuple

from sqlalchemy import bindparam, func, select, text

from virasana.analytic_scripts.dados_risco_perdimentos import _por_lotes
from virasana.risco_historico import sinais as sinais_mod
from virasana.risco_historico.tabelas import CES, MARCAS

MARCA_INICIAL, MARCA_CE, MARCA_ALTERACAO, MARCA_NCM = 'ce_id_inicial', 'ce_id', 'ce_alterado_em', 'ncm_id'
TRAFEGO_EXPORTACAO = '7'
LIMITE_CES_POR_DIA = 30000          # ~13 mil CEs novos por dia no F0 (com exportação)
SOBREPOSICAO = timedelta(minutes=5)  # transação do ETL que grava depois com hora anterior
PAGINAS_ALTERADOS = 5                # páginas (create_date, ID) por volta
COLUNAS_SINAIS = ('ce_id', 'dataEmissao', 'sinais', 'ce_alterado_em', 'ncm_id', 'sinais_em')

CAMPOS_CE = ('ID, numeroCEmercante, manifestoCE, embarcador, consignatario, portoOrigemCarga, portoDestFinal, '
             'tipoTrafego, dataEmissao, create_date')
# SQL: só a lista fixa de campos acima entra no texto
SQL_NOVOS = text(f'SELECT {CAMPOS_CE} FROM conhecimentosresumo '  # nosec B608
                 'WHERE ID > :marca ORDER BY ID LIMIT :lote')
SQL_ALTERADOS = text(f'SELECT {CAMPOS_CE} FROM conhecimentosresumo '  # nosec B608
                     'WHERE create_date >= :desde AND NOT (create_date = :desde AND ID <= :id) '
                     'AND ID BETWEEN :inicial AND :ultimo ORDER BY create_date, ID LIMIT :lote')
SQL_CES_POR_NUMERO = text(f'SELECT {CAMPOS_CE} FROM conhecimentosresumo '  # nosec B608
                          'WHERE numeroCEmercante IN :ces').bindparams(bindparam('ces', expanding=True))
SQL_NCMS_NOVAS = text('SELECT ID, numeroCEMercante FROM ncmitemresumo WHERE ID > :marca ORDER BY ID LIMIT :lote')
SQL_PRIMEIRO_EMITIDO = text('SELECT MIN(ID) FROM conhecimentosresumo '
                            'WHERE dataEmissao >= :desde AND dataEmissao <= :ate')
SQL_MAIOR_CE = text('SELECT MAX(ID), MAX(create_date) FROM conhecimentosresumo')
SQL_MAIOR_NCM = text('SELECT MAX(ID) FROM ncmitemresumo')
SQL_GRAVADOS = select(CES.c.numeroCEmercante, CES.c.ce_alterado_em, CES.c.ncm_id).where(
    CES.c.numeroCEmercante.in_(bindparam('ces', expanding=True)))


class ErroInicio(ValueError):
    pass


def _data(valor) -> Optional[datetime]:
    if valor is None or isinstance(valor, datetime):
        return valor
    return datetime.fromisoformat(str(valor)[:19])


def _numero(linha: dict) -> str:
    return (linha['numeroCEmercante'] or '').strip()


def _e_exportacao(linha: dict) -> bool:
    return (linha['tipoTrafego'] or '').strip() == TRAFEGO_EXPORTACAO


class Extracao:
    """Uma conexão (com a transação de quem chama). `relogio` é o "agora" — os testes o fixam."""

    def __init__(self, conexao, relogio: Callable[[], datetime] = datetime.now, lote_ces: int = 10000,
                 lote_alterados: int = 20000, lote_ncms: int = 50000):
        self.conexao, self.relogio = conexao, relogio
        self.lote_ces, self.lote_alterados, self.lote_ncms = lote_ces, lote_alterados, lote_ncms

    # ---------------------------------------------------------------- marcas d'água
    def marcas(self) -> Dict[str, Tuple[Optional[int], Optional[datetime]]]:
        return {nome: (ultimo_id, _data(ultima_data)) for nome, ultimo_id, ultima_data in self.conexao.execute(
            select(MARCAS.c.nome, MARCAS.c.ultimo_id, MARCAS.c.ultima_data))}

    def grava_marca(self, nome: str, ultimo_id: Optional[int] = None, ultima_data: Optional[datetime] = None):
        valores = {'ultimo_id': ultimo_id, 'ultima_data': ultima_data, 'atualizado_em': self.relogio()}
        if self.conexao.execute(MARCAS.update().where(MARCAS.c.nome == nome).values(**valores)).rowcount == 0:
            self.conexao.execute(MARCAS.insert().values(nome=nome, **valores))

    def _primeiro_emitido(self, desde_dias: int) -> int:
        agora = self.relogio()
        desde_id = self.conexao.execute(SQL_PRIMEIRO_EMITIDO, {
            'desde': (agora - timedelta(days=desde_dias)).strftime('%Y-%m-%d'),
            'ate': agora.strftime('%Y-%m-%d')}).scalar()
        if desde_id is None:
            raise ErroInicio(f'nenhum CE emitido nos últimos {desde_dias} dias')
        return int(desde_id)

    def inicia(self, desde_dias: int = 30, desde_id: Optional[int] = None, reinicia: bool = False) -> Optional[dict]:
        """Grava as marcas iniciais, se ainda não existem; devolve o que fez (None se já havia marcas).
        `reinicia` apaga as marcas antes (os sinais já gravados ficam e são refeitos se lidos de novo). Um
        `desde_id` com marcas já gravadas é erro — não é ignorado em silêncio."""
        marcas = self.marcas()
        if reinicia:
            self.conexao.execute(MARCAS.delete())
            marcas = {}
        if MARCA_CE in marcas:
            if desde_id is not None:
                raise ErroInicio(f'as marcas já existem (início no ID {marcas[MARCA_INICIAL][0]}): o --desde-id '
                                 'não é aplicado. Para recomeçar, use --reinicia.')
            return None
        desde_id = self._primeiro_emitido(desde_dias) if desde_id is None else int(desde_id)
        maior_id, maior_data = self.conexao.execute(SQL_MAIOR_CE).one()
        if desde_id > int(maior_id or 0):
            raise ErroInicio(f'o ID inicial {desde_id} é maior que o último ID do Mercante ({maior_id})')
        a_carregar = int(maior_id) - desde_id + 1
        limite = max(desde_dias, 1) * LIMITE_CES_POR_DIA
        if a_carregar > limite:
            raise ErroInicio(f'{a_carregar} IDs a carregar a partir do ID {desde_id}: mais que {limite} '
                             f'({desde_dias} dias x {LIMITE_CES_POR_DIA}). '
                             'Confira a dataEmissao ou informe --desde-id.')
        self.grava_marca(MARCA_INICIAL, ultimo_id=int(desde_id))
        self.grava_marca(MARCA_CE, ultimo_id=int(desde_id) - 1)
        self.grava_marca(MARCA_ALTERACAO, ultima_data=_data(maior_data))
        self.grava_marca(MARCA_NCM, ultimo_id=int(self.conexao.execute(SQL_MAIOR_NCM).scalar() or 0))
        return {'marca inicial (ID)': int(desde_id), 'IDs a carregar': a_carregar}

    # ---------------------------------------------------------------- gravação
    def _gravados(self, numeros) -> Dict[str, dict]:
        return {_numero(g): g for g in _por_lotes(self.conexao, SQL_GRAVADOS, 'ces', set(numeros))}

    def _extrai_e_grava(self, linhas: List[dict], estatisticas: Counter, chave: str):
        ces = [linha for linha in linhas if not _e_exportacao(linha)]
        estatisticas['exportação ignorada'] += len(linhas) - len(ces)
        if not ces:
            return
        extraidos = sinais_mod.sinais_dos_ces(self.conexao, ces)
        agora, gravados = self.relogio(), self._gravados(_numero(c) for c in ces)
        novos, existentes = [], []
        for ce in ces:
            sinais, ncm_id = extraidos[ce['numeroCEmercante']]
            valores = {'ce_id': ce['ID'], 'dataEmissao': (ce['dataEmissao'] or '')[:10] or None,
                       'sinais': sinais_mod.para_json(sinais), 'ce_alterado_em': _data(ce['create_date']),
                       'ncm_id': ncm_id, 'sinais_em': agora}
            if _numero(ce) in gravados:
                existentes.append({'b_numero': _numero(ce), **{f'b_{k}': v for k, v in valores.items()}})
            else:
                novos.append({'numeroCEmercante': _numero(ce), 'criado_em': agora, **valores})
        if novos:
            self.conexao.execute(CES.insert(), novos)
        if existentes:
            self.conexao.execute(CES.update().where(CES.c.numeroCEmercante == bindparam('b_numero')).values(
                **{coluna: bindparam(f'b_{coluna}') for coluna in COLUNAS_SINAIS}), existentes)
        estatisticas[chave] += len(ces)

    # ---------------------------------------------------------------- voltas
    def _ncms_novas(self, marcas, estatisticas: Counter) -> bool:
        linhas = self.conexao.execute(SQL_NCMS_NOVAS, {'marca': marcas[MARCA_NCM][0] or 0,
                                                       'lote': self.lote_ncms}).all()
        if not linhas:
            return False
        maior_por_ce: Dict[str, int] = defaultdict(int)
        for id_, numero in linhas:
            numero = (numero or '').strip()
            maior_por_ce[numero] = max(maior_por_ce[numero], int(id_))
        gravados = self._gravados(maior_por_ce)
        atrasados = [ce for ce, maior in maior_por_ce.items() if ce in gravados and gravados[ce]['ncm_id'] < maior]
        if atrasados:
            ces = [dict(linha._mapping) for linha in _linhas_por_numero(self.conexao, atrasados)]
            self._extrai_e_grava(ces, estatisticas, 'CEs re-extraídos por NCM nova')
        self.grava_marca(MARCA_NCM, ultimo_id=max(int(id_) for id_, _ in linhas))
        estatisticas['NCMs novas lidas'] += len(linhas)
        return len(linhas) >= self.lote_ncms

    def _reextrai_se_mudou(self, linhas: List[dict], estatisticas: Counter):
        linhas = [linha for linha in linhas if not _e_exportacao(linha)]   # nunca estão na tabela
        gravados = self._gravados(_numero(linha) for linha in linhas)
        mudaram = [linha for linha in linhas if _numero(linha) not in gravados
                   or _data(linha['create_date']) > (_data(gravados[_numero(linha)]['ce_alterado_em']) or datetime.min)]
        self._extrai_e_grava(mudaram, estatisticas, 'CEs alterados re-extraídos')

    def _alterados(self, marcas, estatisticas: Counter) -> bool:
        """CEs já lidos (faixa de IDs) com create_date desde a marca, em páginas (create_date, ID). A volta
        que termina com fila guarda a chave exata (continua dali); a que esvazia guarda só a data, e a próxima
        recomeça um pouco antes (sobreposição) — muitos CEs no mesmo instante não prendem a marca."""
        marca_id, marca = marcas[MARCA_ALTERACAO]
        chave = (marca, marca_id) if marca_id else (marca - SOBREPOSICAO, 0)
        lidos, cheio = 0, False
        for _ in range(PAGINAS_ALTERADOS):
            linhas = [dict(linha._mapping) for linha in self.conexao.execute(SQL_ALTERADOS, {
                'desde': chave[0], 'id': chave[1], 'inicial': marcas[MARCA_INICIAL][0],
                'ultimo': marcas[MARCA_CE][0], 'lote': self.lote_alterados})]
            if linhas:
                self._reextrai_se_mudou(linhas, estatisticas)
                lidos += len(linhas)
                chave = (_data(linhas[-1]['create_date']), int(linhas[-1]['ID']))
            cheio = len(linhas) >= self.lote_alterados
            if not cheio:
                break
        nova = (chave[1] if cheio else 0, max(chave[0], marca)) if lidos else (0, marca)
        if nova != (marca_id or 0, marca):
            self.grava_marca(MARCA_ALTERACAO, ultimo_id=nova[0], ultima_data=nova[1])
        estatisticas['CEs alterados lidos'] += lidos
        return cheio

    def _novos(self, marcas, estatisticas: Counter) -> bool:
        linhas = [dict(linha._mapping) for linha in self.conexao.execute(
            SQL_NOVOS, {'marca': marcas[MARCA_CE][0], 'lote': self.lote_ces})]
        if not linhas:
            return False
        self._extrai_e_grava(linhas, estatisticas, 'CEs novos gravados')
        self.grava_marca(MARCA_CE, ultimo_id=max(int(linha['ID']) for linha in linhas))
        estatisticas['CEs novos lidos'] += len(linhas)
        return len(linhas) >= self.lote_ces

    def uma_volta(self) -> Tuple[Counter, bool]:
        """(estatísticas, ainda há fila?). Exige as marcas (`inicia`)."""
        marcas = self.marcas()
        if MARCA_CE not in marcas:
            raise ErroInicio('sem marcas: rode a carga inicial (inicia)')
        estatisticas: Counter = Counter()
        cheio = self._ncms_novas(marcas, estatisticas)
        cheio = self._alterados(marcas, estatisticas) or cheio
        cheio = self._novos(marcas, estatisticas) or cheio
        return estatisticas, cheio

    def situacao(self) -> dict:
        marcas = self.marcas()
        maior_id = self.conexao.execute(text('SELECT MAX(ID) FROM conhecimentosresumo')).scalar() or 0
        gravados = self.conexao.execute(select(func.count()).select_from(CES)).scalar()
        return {'CEs na tabela': gravados or 0,
                'IDs do Mercante ainda não lidos': max(int(maior_id) - int(marcas.get(MARCA_CE, (0,))[0] or 0), 0)}


def _linhas_por_numero(conexao, numeros: List[str]):
    for lote_inicio in range(0, len(numeros), 500):
        yield from conexao.execute(SQL_CES_POR_NUMERO, {'ces': numeros[lote_inicio:lote_inicio + 500]})
