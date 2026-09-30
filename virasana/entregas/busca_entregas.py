# coding: utf-8
"""
Entregas de contêineres importados por um CNPJ.

Dado um CNPJ (ou a raiz de 8 dígitos) e um período, lista as SAÍDAS de
contêineres carregados pelos gates dos recintos (Acesso Veículo da API
Recintos) que correspondem a CEs de importação em que ele é consignatário
ou notify:
CNPJ, mercadoria, contêiner, data de saída, recinto, placa (e semirreboque,
transportadora, motorista).

Como se liga a saída ao CE:
  1. CEs de importação (tipoTrafego 5) com o CNPJ no consignatário OU no
     notify (a parte a ser notificada — muitas vezes o importador de fato
     quando o consignatário é "à ordem" ou um agente; coluna "Papel"), cujo
     manifesto começou a operar entre (início - `dias_antes_do_manifesto`) e
     o fim do período — a carga chega antes de sair do recinto.
  2. Contêineres desses CEs (Item com código de contêiner).
  3. Saídas (direção S, operação C = acesso efetivado, não agendamento) desses
     contêineres, carregados (não vazios), dentro do período.
  4. Cada saída fica com o CE mais recente (pela data do manifesto) anterior a
     ela — o mesmo contêiner pode ter vindo em CEs diferentes. Se o gate
     informou o número do conhecimento e ele é um dos CEs, a ligação é
     "confirmada pelo CE"; senão, "contêiner e data".

Retificações do mesmo evento (mesmo contêiner, recinto e data/hora) aparecem
uma vez só. Um contêiner pode ter mais de uma saída legítima (terminal e,
depois, porto seco): todas são listadas.

Somente leitura; SQL portável (MariaDB e SQLite nos testes).
"""
import io
import re
from datetime import date, datetime, time, timedelta
from typing import Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy import bindparam, text

from virasana.navii import referencias

TRAFEGO_IMPORTACAO = '5'
DIAS_ANTES_DO_MANIFESTO = 90
MAXIMO_DIAS_PERIODO = 366
TAMANHO_LOTE = 500

SQL_CONTEINERES_DOS_CES = text('''
SELECT c.numeroCEmercante AS ce, c.consignatario AS consignatario, c.cnpjNotify AS notify,
       c.descricao AS mercadoria, m.dataInicioOperacaoDate AS data_manifesto, i.codigoConteiner AS conteiner
FROM conhecimentosresumo c
JOIN manifestosresumo m ON m.numero = c.manifestoCE
JOIN itensresumo i ON i.numeroCEmercante = c.numeroCEmercante
WHERE (c.consignatario LIKE :cnpj_prefixo OR c.cnpjNotify LIKE :cnpj_prefixo)
  AND c.tipoTrafego = :trafego
  AND m.dataInicioOperacaoDate BETWEEN :inicio_manifesto AND :fim_manifesto
  AND i.codigoConteiner IS NOT NULL AND i.codigoConteiner <> ''
''')

SQL_SAIDAS = text('''
SELECT id, numeroConteiner, dataHoraOcorrencia, codigoRecinto, placa, placaSemirreboque,
       cnpjTransportador, cpfMotorista, nomeMotorista, numeroConhecimento
FROM apirecintos_acessosveiculo
WHERE numeroConteiner IN :conteineres
  AND direcao = 'S' AND operacao = 'C'
  AND (vazioConteiner IS NULL OR vazioConteiner = 0)
  AND dataHoraOcorrencia BETWEEN :inicio AND :fim
ORDER BY dataHoraOcorrencia
''').bindparams(bindparam('conteineres', expanding=True))

SQL_NCMS = text('''
SELECT numeroCEMercante AS ce, codigoConteiner AS conteiner, identificacaoNCM AS ncm
FROM ncmitemresumo WHERE numeroCEMercante IN :ces
''').bindparams(bindparam('ces', expanding=True))

SQL_RECINTOS = text('SELECT cod_siscomex, nome FROM ovr_recintos WHERE cod_siscomex IN :codigos'
                    ).bindparams(bindparam('codigos', expanding=True))

SQL_EMPRESA = text('SELECT nome FROM laudo_empresas WHERE cnpj = :cnpj')

COLUNAS = [
    ('cnpj', 'CNPJ'), ('papel', 'Papel do CNPJ'), ('empresa', 'Empresa'), ('ce', 'CE Mercante'),
    ('mercadoria', 'Mercadoria'),
    ('ncms', 'NCMs do contêiner'), ('conteiner', 'Contêiner'), ('data_saida', 'Data de saída'),
    ('recinto', 'Recinto'), ('nome_recinto', 'Nome do recinto'), ('placa', 'Placa'),
    ('placa_semirreboque', 'Semirreboque'), ('cnpj_transportador', 'CNPJ transportador'),
    ('cpf_motorista', 'CPF motorista'), ('nome_motorista', 'Motorista'), ('ligacao', 'Ligação saída-CE'),
]


class ParametrosInvalidos(ValueError):
    pass


def valida_parametros(cnpj: Optional[str], inicio: Optional[date], fim: Optional[date]) -> str:
    """Devolve o CNPJ só com dígitos; erro legível se algo estiver inválido."""
    digitos = re.sub(r'\D', '', cnpj or '')
    if len(digitos) not in (8, 14):
        raise ParametrosInvalidos('Informe o CNPJ completo (14 dígitos) ou a raiz (8 dígitos).')
    if not inicio or not fim:
        raise ParametrosInvalidos('Informe o período (início e fim).')
    if fim < inicio:
        raise ParametrosInvalidos('A data final é anterior à inicial.')
    if (fim - inicio).days > MAXIMO_DIAS_PERIODO:
        raise ParametrosInvalidos(f'Período máximo: {MAXIMO_DIAS_PERIODO} dias.')
    return digitos


def _lotes(valores: List, tamanho: int = TAMANHO_LOTE) -> Iterable[List]:
    for i in range(0, len(valores), tamanho):
        yield valores[i:i + tamanho]


def _como_datetime(valor) -> Optional[datetime]:
    if valor is None or valor == '':
        return None
    return pd.Timestamp(valor).to_pydatetime()


def _papel(linha, cnpj: str) -> dict:
    """Em que papel o CNPJ pesquisado aparece no CE (e qual CNPJ exibir)."""
    consignatario = (linha['consignatario'] or '').strip()
    notify = re.sub(r'\D', '', linha['notify'] or '')
    papeis = []
    if consignatario.startswith(cnpj):
        papeis.append('consignatário')
    if notify.startswith(cnpj):
        papeis.append('notify')
    return {'cnpj': consignatario if papeis and papeis[0] == 'consignatário' else notify,
            'papel': ' e '.join(papeis)}


def _escolhe_ce(saida: dict, candidatos: List[dict]) -> Optional[dict]:
    """CE da saída: o informado pelo gate, se for um dos candidatos; senão o de
    manifesto mais recente que não seja posterior à saída."""
    data_saida = _como_datetime(saida['dataHoraOcorrencia'])
    anteriores = [c for c in candidatos
                  if c['data_manifesto'] is None or _como_datetime(c['data_manifesto']) <= data_saida]
    if not anteriores:
        return None
    ce_gate = (saida.get('numeroConhecimento') or '').strip()
    for candidato in anteriores:
        if ce_gate and candidato['ce'] == ce_gate:
            return {**candidato, 'ligacao': 'confirmada pelo CE'}
    mais_recente = max(anteriores, key=lambda c: _como_datetime(c['data_manifesto']) or datetime.min)
    return {**mais_recente, 'ligacao': 'contêiner e data'}


def busca_entregas(conexao, cnpj: str, inicio: date, fim: date,
                   dias_antes_do_manifesto: int = DIAS_ANTES_DO_MANIFESTO) -> dict:
    """Saídas de contêineres carregados dos CEs de importação do CNPJ no período.

    `conexao`: Session ou Connection SQLAlchemy. Returns:
        {'cnpj', 'empresa', 'linhas': [dict por saída, chaves de COLUNAS],
         'qtd_ces', 'qtd_conteineres', 'conteineres_sem_saida': [...]}
    """
    cnpj = valida_parametros(cnpj, inicio, fim)
    parametros_ces = {
        'cnpj_prefixo': cnpj + '%', 'trafego': TRAFEGO_IMPORTACAO,
        'inicio_manifesto': inicio - timedelta(days=dias_antes_do_manifesto), 'fim_manifesto': fim,
    }
    ces_por_conteiner: Dict[str, List[dict]] = {}
    for linha in conexao.execute(SQL_CONTEINERES_DOS_CES, parametros_ces).mappings():
        conteiner = linha['conteiner'].strip().upper()
        ces_por_conteiner.setdefault(conteiner, []).append(
            {**dict(linha), 'conteiner': conteiner, **_papel(linha, cnpj)})

    saidas = {}  # (contêiner, recinto, data/hora) -> saída (retificação sobrescreve)
    periodo = {'inicio': datetime.combine(inicio, time.min), 'fim': datetime.combine(fim, time.max)}
    for lote in _lotes(sorted(ces_por_conteiner)):
        for saida in conexao.execute(SQL_SAIDAS, {**periodo, 'conteineres': lote}).mappings():
            chave = (saida['numeroConteiner'], saida['codigoRecinto'], str(saida['dataHoraOcorrencia']))
            if chave not in saidas or saida['id'] > saidas[chave]['id']:
                saidas[chave] = dict(saida)

    linhas, conteineres_com_saida = [], set()
    for saida in sorted(saidas.values(), key=lambda s: str(s['dataHoraOcorrencia'])):
        ce = _escolhe_ce(saida, ces_por_conteiner.get(saida['numeroConteiner'], []))
        if ce is None:
            continue
        conteineres_com_saida.add(saida['numeroConteiner'])
        linhas.append({
            'cnpj': ce['cnpj'], 'papel': ce['papel'], 'ce': ce['ce'], 'mercadoria': ce['mercadoria'],
            'conteiner': ce['conteiner'],
            'data_saida': _como_datetime(saida['dataHoraOcorrencia']), 'recinto': saida['codigoRecinto'],
            'placa': saida['placa'], 'placa_semirreboque': saida['placaSemirreboque'],
            'cnpj_transportador': saida['cnpjTransportador'], 'cpf_motorista': saida['cpfMotorista'],
            'nome_motorista': saida['nomeMotorista'], 'ligacao': ce['ligacao'],
        })

    _enriquece(conexao, linhas)
    empresa = None
    if len(cnpj) == 14:
        empresa = conexao.execute(SQL_EMPRESA, {'cnpj': cnpj}).scalar()
    for linha in linhas:
        linha['empresa'] = empresa if linha['cnpj'] == cnpj else None
    return {
        'cnpj': cnpj, 'empresa': empresa, 'linhas': linhas,
        'qtd_ces': len({c['ce'] for lista in ces_por_conteiner.values() for c in lista}),
        'qtd_conteineres': len(ces_por_conteiner),
        'conteineres_sem_saida': sorted(set(ces_por_conteiner) - conteineres_com_saida),
    }


def _enriquece(conexao, linhas: List[dict]):
    """NCMs do contêiner (com a posição) e nome do recinto, em lote."""
    ces = sorted({linha['ce'] for linha in linhas})
    ncms: Dict[tuple, List[str]] = {}
    for lote in _lotes(ces):
        for ncm in conexao.execute(SQL_NCMS, {'ces': lote}).mappings():
            chave = ((ncm['ce'] or '').strip(), (ncm['conteiner'] or '').strip().upper())
            codigo = re.sub(r'\D', '', ncm['ncm'] or '')
            if codigo and codigo not in ncms.setdefault(chave, []):
                ncms[chave].append(codigo)
    codigos = sorted({linha['recinto'] for linha in linhas if linha['recinto']})
    nomes_recintos = {}
    for lote in _lotes(codigos):
        nomes_recintos.update({r['cod_siscomex']: r['nome'] for r in
                               conexao.execute(SQL_RECINTOS, {'codigos': lote}).mappings()})
    for linha in linhas:
        codigos_ncm = ncms.get((linha['ce'], linha['conteiner']), [])
        linha['ncms'] = '; '.join(
            f"{c} {referencias.descricao_posicao_ncm(c) or ''}".strip() for c in codigos_ncm)
        linha['nome_recinto'] = nomes_recintos.get(linha['recinto'])


def planilha_xlsx(resultado: dict) -> bytes:
    """Planilha Excel com as entregas (aba "Entregas") e os contêineres sem
    saída localizada (aba "Sem saída")."""
    entregas = pd.DataFrame(resultado['linhas'], columns=[c for c, _ in COLUNAS])
    entregas = entregas.rename(columns=dict(COLUNAS))
    sem_saida = pd.DataFrame({'Contêiner': resultado['conteineres_sem_saida']})
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as escritor:
        entregas.to_excel(escritor, sheet_name='Entregas', index=False)
        sem_saida.to_excel(escritor, sheet_name='Sem saída', index=False)
    return buffer.getvalue()
