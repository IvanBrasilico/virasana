# coding: utf-8
"""
Lote 1 do risco histórico por CE: os PARÂMETROS (peso de cada valor), em versões
guardadas (ver CONTEXTO.md):

  1. fichas positivas (fichas_positivas) e os sinais delas
     (dados_risco_perdimentos.extrai) -> calculo.historico (K, k, fichas, importadores);
  2. população: os sinais gravados em risco_historico_ce dos CEs que entraram nos
     últimos `janela_dias` (90) — sem reler o Mercante (decisão depois do F0);
  3. pesos (calculo.tabela_de_pesos) -> nova versão em risco_historico_versao e
     risco_historico_parametro (todos os valores do histórico, com o motivo
     quando o peso é zero).

Nova versão só quando as fichas positivas mudam (assinatura dos ids e pesos) ou
a última passou de `validade_dias` (30): entram ~10 fichas por mês.
"""
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import select

from virasana.analytic_scripts import dados_risco_perdimentos as drp
from virasana.risco_historico import calculo, fichas_positivas
from virasana.risco_historico.sinais import CITADO, conta, de_json
from virasana.risco_historico.tabelas import CES, PARAMETROS, VERSOES

JANELA_DIAS = 90
VALIDADE_DIAS = 30
FICHAS_NA_TELA = 10
VEZES_MAXIMO = 1e11   # cabe em DECIMAL(14, 2)


@dataclass
class Calculo:
    fichas: List[dict]
    empresas: Dict[str, str]
    hist: calculo.Historico
    total_n: int
    pesos: Dict[str, Dict[str, calculo.Peso]]
    assinatura: str
    desde: datetime


def assinatura(fichas: Iterable[dict]) -> str:
    texto = ';'.join(f'{f["id"]}:{f["peso"]:g}' for f in sorted(fichas, key=lambda f: f['id']))
    return hashlib.sha256(texto.encode()).hexdigest()


def populacao(conexao, desde: datetime) -> Tuple[int, dict]:
    """(N, contagens) dos CEs gravados em risco_historico_ce desde `desde`."""
    total = 0

    def lidos():
        nonlocal total
        for (texto,) in conexao.execute(select(CES.c.sinais).where(CES.c.criado_em >= desde)):
            total += 1
            yield de_json(texto)

    contagens = conta(lidos())
    return total, contagens


def ultima_versao(conexao) -> Optional[dict]:
    linha = conexao.execute(select(VERSOES).order_by(VERSOES.c.ID.desc()).limit(1)).first()
    return dict(linha._mapping) if linha else None


def motivo_nova_versao(conexao, agora: datetime, fichas: List[dict],
                       validade_dias: int = VALIDADE_DIAS) -> Optional[str]:
    ultima = ultima_versao(conexao)
    if ultima is None:
        return 'primeira versão'
    if ultima['assinatura'] != assinatura(fichas):
        return 'fichas positivas mudaram'
    if agora - ultima['criado_em'] >= timedelta(days=validade_dias):
        return 'versão vencida'
    return None


def calcula(conexao, agora: datetime, janela_dias: int = JANELA_DIAS, fichas: Optional[List[dict]] = None) -> Calculo:
    fichas = fichas_positivas.busca(conexao) if fichas is None else fichas
    extracao = drp.extrai(conexao, fichas_positivas.RESULTADOS_POSITIVOS, fichas=fichas)
    hist = calculo.historico(extracao['fichas'])
    desde = agora - timedelta(days=janela_dias)
    total_n, contagens = populacao(conexao, desde)
    return Calculo(fichas, extracao['empresas'], hist, total_n, calculo.tabela_de_pesos(hist, contagens, total_n),
                   assinatura(fichas), desde)


def _descricao(dimensao: str, valor: str, empresas: Dict[str, str]) -> str:
    if dimensao in ('importador', CITADO):
        return empresas.get(valor, '')
    if dimensao == 'embarcador':
        return ''
    return drp.descreve(dimensao, valor, {})


def grava(conexao, calc: Calculo, agora: datetime, motivo: str) -> int:
    versao_id = conexao.execute(VERSOES.insert().values(
        criado_em=agora, motivo=motivo[:60], assinatura=calc.assinatura, fichas=len(calc.fichas),
        total_k=round(calc.hist.total, 2), total_n=calc.total_n, populacao_desde=calc.desde,
        constantes=json.dumps(calculo.constantes(), sort_keys=True))).inserted_primary_key[0]
    linhas = [{'versao_id': versao_id, 'dimensao': dimensao, 'valor': valor, 'descricao':
               (_descricao(dimensao, valor, calc.empresas) or '')[:200] or None,
               'fichas': p.fichas, 'importadores': p.importadores, 'k': round(p.k, 2), 'n': p.n,
               'vezes': round(min(p.vezes, VEZES_MAXIMO), 2), 'peso': round(p.peso, 3), 'motivo': p.motivo or None,
               'fichas_ids': ','.join(str(i) for i in sorted(set(calc.hist.fichas[dimensao][valor]), reverse=True))}
              for dimensao, valores in calc.pesos.items() for valor, p in valores.items()]
    if linhas:
        conexao.execute(PARAMETROS.insert(), linhas)
    return versao_id


def atualiza(conexao, agora: datetime, forca: bool = False, janela_dias: int = JANELA_DIAS,
             validade_dias: int = VALIDADE_DIAS) -> Tuple[Optional[int], str]:
    """(id da nova versão ou None, motivo/situação)."""
    fichas = fichas_positivas.busca(conexao)
    motivo = 'forçada' if forca else motivo_nova_versao(conexao, agora, fichas, validade_dias)
    if not motivo:
        return None, 'parâmetros em dia'
    calc = calcula(conexao, agora, janela_dias, fichas)
    if not calc.total_n:
        return None, 'população vazia: rode antes a extração dos sinais (lotes.py --sinais)'
    return grava(conexao, calc, agora, motivo), motivo


def pesos_da_versao(conexao, versao_id: int) -> Dict[str, Dict[str, calculo.Peso]]:
    """Os pesos gravados de uma versão, como `calculo.tabela_de_pesos` (para pontuar e explicar)."""
    versao = conexao.execute(select(VERSOES).where(VERSOES.c.ID == versao_id)).one()
    pesos: Dict[str, Dict[str, calculo.Peso]] = {dimensao: {} for dimensao in calculo.REGRAS}
    for p in conexao.execute(select(PARAMETROS).where(PARAMETROS.c.versao_id == versao_id)):
        pesos[p.dimensao][p.valor] = calculo.Peso(p.fichas, p.importadores, float(p.k), float(versao.total_k), p.n,
                                                  versao.total_n, float(p.vezes), float(p.peso), p.motivo or '')
    return pesos


def para_tela(conexao, todos: bool = False) -> Optional[dict]:
    """Última versão e seus parâmetros por grupo (só os que contam, ou todos)."""
    versao = ultima_versao(conexao)
    if versao is None:
        return None
    consulta = select(PARAMETROS).where(PARAMETROS.c.versao_id == versao['ID'])
    if not todos:
        consulta = consulta.where(PARAMETROS.c.peso > 0)
    linhas = []
    for linha in conexao.execute(consulta.order_by(PARAMETROS.c.peso.desc(), PARAMETROS.c.fichas.desc())):
        item = dict(linha._mapping)
        item['rotulo'] = calculo.ROTULOS[item['dimensao']]
        item['pct'] = 100 * item['n'] / versao['total_n'] if versao['total_n'] else 0
        item['fichas_lista'] = [int(i) for i in item['fichas_ids'].split(',') if i][:FICHAS_NA_TELA]
        linhas.append(item)
    grupos = [{'chave': grupo, 'rotulo': calculo.ROTULOS[grupo], 'identidade': grupo in calculo.IDENTIDADE,
               'regras': {d: calculo.REGRAS[d] for d in dimensoes},
               'linhas': [linha for linha in linhas if linha['dimensao'] in dimensoes]}
              for grupo, dimensoes in calculo.GRUPOS.items()]
    return {'versao': {**versao, 'constantes': json.loads(versao['constantes'])}, 'grupos': grupos,
            'todos': todos}
