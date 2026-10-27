# coding: utf-8
"""
Risco histórico das Fichas: de cada Ficha com resultado (padrão: Apreensão e
Perdimento) extrai os SINAIS que poderiam virar filtros de risco para novos
CEs — importador, embarcador, rota, porto de baldeação, NCM declarada e
encontrada, marcas, CNPJs/CPFs citados... — e agrega por dimensão: em
quantas fichas cada valor aparece, quantos perdimentos/apreensões, valor e
datas. Só extração e visualização por enquanto; no futuro estes "filtros
automáticos" devem alimentar o loop do ciclo de vida (grau de risco por CE).

Fontes (somente leitura) e decisões tomadas a partir do diagnóstico de
produção (diagnostico_risco_perdimentos.py, 01/10/2026):
  - Ficha (ovr_ovrs): CE, CNPJ fiscalizado (é o consignatário em ~86% dos
    casos), observações. RVF: CE, lote/contêiner, descrições, apreensões,
    infrações, marcas. TG: NCM ENCONTRADA nos itens (84% das fichas).
    Flags da ficha.
  - Mercante: consignatário, embarcador, portos de origem/destino, porto de
    carregamento do manifesto (baldeação) e NCM DECLARADA do CE.
  - Ignorados: CPFs do `motivo` dos eventos (são de auditores — atribuição de
    responsável etc.), tipo de mercadoria do TG (sempre "Alimentos"), marca
    do item do TG (sempre vazia), notify (vazio nesses CEs).
  - Exportação (tipoTrafego 7): por padrão as fichas cujos CEs são todos de
    exportação ficam de fora — os filtros são para a importação.

Dimensões muito frequentes por natureza (rota, porto, NCM) servem de
contexto; os candidatos mais fortes a filtro são os de identidade
(importador, embarcador, CNPJ/CPF citado, marca) e as combinações
(país de origem + NCM).

Uso (na raiz do repositório virasana):
    python virasana/analytic_scripts/dados_risco_perdimentos.py
    python virasana/analytic_scripts/dados_risco_perdimentos.py --tipos 2 --inicio 2023-01-01 --saida risco.xlsx
"""
import argparse
import io
import re
import sys
import unicodedata
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy import bindparam, create_engine, text

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.dados_transporte_apreensoes import (  # noqa: E402
    cpf_valido, extrai_conteineres, extrai_cpfs, normaliza_conteiner,
)
from virasana.analytic_scripts.diagnostico_risco_perdimentos import (  # noqa: E402
    TIPOS_PADRAO, TIPOS_RESULTADO, extrai_cnpjs,
)
from virasana.navii import referencias  # noqa: E402

TRAFEGO_EXPORTACAO = '7'
TAMANHO_LOTE = 500
PERDIMENTO, APREENSAO = 2, 1

# (chave, rótulo, é identidade?) — ordem de exibição
DIMENSOES = [
    ('importador', 'Importador (raiz do CNPJ / CPF)', True),
    ('embarcador', 'Embarcador', True),
    ('cnpj_citado', 'CNPJ citado nos textos', True),
    ('cpf_citado', 'CPF citado nos textos', True),
    ('marca', 'Marca encontrada (RVF)', True),
    ('pais_ncm', 'País de origem + posição NCM declarada', False),
    ('ncm_declarado', 'Posição NCM declarada (CE)', False),
    ('ncm_encontrado', 'Posição NCM encontrada (TG)', False),
    ('rota', 'Rota (origem > destino)', False),
    ('porto_origem', 'Porto de origem', False),
    ('pais_origem', 'País de origem', False),
    ('porto_baldeacao', 'Porto de baldeação (manifesto)', False),
    ('conteiner', 'Contêiner', False),
    ('infracao', 'Infração', False),
    ('apreensao', 'Tipo de apreensão', False),
    ('flag', 'Flag da ficha', False),
]
ROTULOS = {chave: rotulo for chave, rotulo, _ in DIMENSOES}

SUFIXOS_EMPRESA = {'CO', 'LTD', 'LIMITED', 'INC', 'LLC', 'CORP', 'CORPORATION', 'COMPANY', 'SA', 'LTDA', 'GMBH',
                   'SRL', 'SPA', 'BV', 'PTE', 'PVT', 'AG', 'NV', 'EIRELI', 'ME', 'EPP'}


def _sql(consulta: str, *listas: str):
    texto = text(consulta)
    return texto.bindparams(*[bindparam(nome, expanding=True) for nome in listas]) if listas else texto


SQL_FICHAS = """
SELECT o.id, o.numero, o.datahora, o.numeroCEmercante, o.cnpj_fiscalizado, o.observacoes
FROM ovr_ovrs o
WHERE EXISTS (SELECT 1 FROM ovr_resultados r WHERE r.ovr_id = o.id AND r.tipo_resultado IN :tipos)
  {filtro_data}
ORDER BY o.datahora
"""
SQL_RESULTADOS = _sql('SELECT ovr_id, tipo_resultado, valor FROM ovr_resultados '
                      'WHERE ovr_id IN :ids AND tipo_resultado IN :tipos', 'ids', 'tipos')
SQL_RVFS = _sql('SELECT ovr_id, numeroCEmercante, numerolote, descricao, descricao_interna '
                'FROM ovr_verificacoesfisicas WHERE ovr_id IN :ids', 'ids')
SQL_APREENSOES = _sql('SELECT v.ovr_id, t.descricao FROM ovr_verificacoesfisicas v '
                      'JOIN ovr_apreensoes_rvf a ON a.rvf_id = v.id JOIN ovr_tiposapreensao t ON t.id = a.tipo_id '
                      'WHERE v.ovr_id IN :ids', 'ids')
SQL_INFRACOES = _sql('SELECT v.ovr_id, i.nome FROM ovr_verificacoesfisicas v '
                     'JOIN ovr_infracoesencontradas ie ON ie.rvf_id = v.id '
                     'JOIN ovr_infracoes i ON i.id = ie.infracao_id '
                     'WHERE v.ovr_id IN :ids', 'ids')
SQL_MARCAS = _sql('SELECT v.ovr_id, m.nome FROM ovr_verificacoesfisicas v '
                  'JOIN ovr_marcasencontradas me ON me.rvf_id = v.id JOIN ovr_marcas m ON m.id = me.marca_id '
                  'WHERE v.ovr_id IN :ids', 'ids')
SQL_FLAGS = _sql('SELECT fo.rvf_id AS ovr_id, f.nome FROM ovr_flags_ovr fo JOIN ovr_flags f ON f.id = fo.flag_id '
                 'WHERE fo.rvf_id IN :ids', 'ids')  # a coluna se chama rvf_id, mas é a ficha
SQL_TG = _sql('SELECT t.ovr_id, i.ncm, t.descricao, t.observacoes FROM ovr_tgovr t '
              'LEFT JOIN ovr_itenstg i ON i.tg_id = t.id WHERE t.ovr_id IN :ids', 'ids')
SQL_CES = _sql('SELECT numeroCEmercante, manifestoCE, embarcador, consignatario, portoOrigemCarga, portoDestFinal, '
               'tipoTrafego FROM conhecimentosresumo WHERE numeroCEmercante IN :ces', 'ces')
SQL_MANIFESTOS = _sql('SELECT numero, portoCarregamento FROM manifestosresumo WHERE numero IN :numeros', 'numeros')
SQL_NCM_CE = _sql('SELECT numeroCEMercante, identificacaoNCM FROM ncmitemresumo WHERE numeroCEMercante IN :ces', 'ces')
SQL_EMPRESAS = _sql('SELECT cnpj, nome FROM laudo_empresas WHERE cnpj IN :cnpjs', 'cnpjs')


# ------------------------------------------------------------ normalização

def normaliza_embarcador(nome: Optional[str]) -> Optional[str]:
    """'Ningbo Toys Co., Ltd.' -> 'NINGBO TOYS': maiúsculas, sem acento/pontuação
    e sem os sufixos societários do fim (agrupa grafias da mesma empresa)."""
    texto = unicodedata.normalize('NFKD', nome or '').encode('ascii', 'ignore').decode().upper()
    texto = re.sub(r'\bS\s*[./]\s*A\b\.?', ' SA ', texto)  # "S.A." / "S/A" viram o sufixo SA
    palavras = re.sub(r'[^A-Z0-9]+', ' ', texto).split()
    while palavras and palavras[-1] in SUFIXOS_EMPRESA:
        palavras.pop()
    return ' '.join(palavras) or None


def documento(valor: Optional[str]) -> Optional[str]:
    """Importador a partir de CNPJ/CPF: raiz de 8 dígitos do CNPJ, ou 'CPF ' + CPF
    válido. Nome estrangeiro e lixo -> None."""
    digitos = re.sub(r'\D', '', valor or '')
    if len(digitos) == 14:
        return digitos[:8]
    if len(digitos) == 8:
        return digitos
    if len(digitos) == 11 and cpf_valido(digitos):
        return 'CPF ' + digitos
    return None


def posicao_ncm(ncm: Optional[str]) -> Optional[str]:
    digitos = re.sub(r'\D', '', ncm or '')
    return digitos[:4] if len(digitos) >= 4 else None


def _ce(valor) -> Optional[str]:
    digitos = re.sub(r'\D', '', str(valor or ''))
    return digitos if len(digitos) == 15 else None


def _lotes(valores: List, tamanho: int = TAMANHO_LOTE) -> Iterable[List]:
    valores = sorted(valores)
    for i in range(0, len(valores), tamanho):
        yield valores[i:i + tamanho]


def _por_lotes(conexao, consulta, nome: str, valores, **outros) -> List[dict]:
    linhas = []
    for lote in _lotes(list(valores)):
        linhas += [dict(linha._mapping) for linha in conexao.execute(consulta, {nome: lote, **outros})]
    return linhas


# ------------------------------------------------------------ extração

def busca_fichas(conexao, tipos=TIPOS_PADRAO, inicio: Optional[datetime] = None,
                 fim: Optional[datetime] = None) -> List[dict]:
    filtro_data, params = '', {'tipos': list(tipos)}
    if inicio:
        filtro_data += ' AND o.datahora >= :inicio'
        params['inicio'] = inicio
    if fim:
        filtro_data += ' AND o.datahora < :fim'
        params['fim'] = fim
    # SQL: só os filtros fixos acima entram no texto
    consulta = _sql(SQL_FICHAS.format(filtro_data=filtro_data), 'tipos')  # nosec B608
    return [dict(linha._mapping) for linha in conexao.execute(consulta, params)]


def _sinal(ficha: dict, dimensao: str, valor):
    if valor:
        ficha['sinais'][dimensao].add(valor)


def _texto(valor) -> str:
    return (valor or '').strip()


def _sinais_do_fichas(conexao, fichas: Dict[int, dict], tipos) -> Dict[int, list]:
    """Resultados e sinais tirados do próprio Fichas (ficha, RVF, TG, categorias).
    Devolve os textos livres de cada ficha, para as citações."""
    ids = list(fichas)
    textos = {i: [fichas[i].get('observacoes') or ''] for i in ids}
    for r in _por_lotes(conexao, SQL_RESULTADOS, 'ids', ids, tipos=list(tipos)):
        fichas[r['ovr_id']]['resultados'].append(r['tipo_resultado'])
        fichas[r['ovr_id']]['valor'] += float(r['valor'] or 0)
    for ficha in fichas.values():
        ficha['ces'].add(_ce(ficha.get('numeroCEmercante')))
        _sinal(ficha, 'importador', documento(ficha.get('cnpj_fiscalizado')))
    for rvf in _por_lotes(conexao, SQL_RVFS, 'ids', ids):
        fichas[rvf['ovr_id']]['ces'].add(_ce(rvf['numeroCEmercante']))
        _sinal(fichas[rvf['ovr_id']], 'conteiner', normaliza_conteiner(rvf['numerolote']))
        textos[rvf['ovr_id']] += [rvf['descricao'] or '', rvf['descricao_interna'] or '']
    for consulta, dimensao, campo in ((SQL_APREENSOES, 'apreensao', 'descricao'), (SQL_INFRACOES, 'infracao', 'nome'),
                                      (SQL_MARCAS, 'marca', 'nome'), (SQL_FLAGS, 'flag', 'nome')):
        for linha in _por_lotes(conexao, consulta, 'ids', ids):
            valor = _texto(linha[campo])
            _sinal(fichas[linha['ovr_id']], dimensao, valor.upper() if dimensao == 'marca' else valor)
    for tg in _por_lotes(conexao, SQL_TG, 'ids', ids):
        _sinal(fichas[tg['ovr_id']], 'ncm_encontrado', posicao_ncm(tg['ncm']))
        textos[tg['ovr_id']] += [tg['descricao'] or '', tg['observacoes'] or '']
    for ficha in fichas.values():
        ficha['ces'].discard(None)
    return textos


def _sinais_dos_textos(fichas: Dict[int, dict], textos: Dict[int, list]):
    """CNPJs, CPFs e contêineres citados (o motivo dos eventos fica de fora: traz CPF de auditores)."""
    for ficha_id, partes in textos.items():
        texto = '\n'.join(dict.fromkeys(partes))
        for dimensao, extrator in (('cnpj_citado', extrai_cnpjs), ('cpf_citado', extrai_cpfs),
                                   ('conteiner', extrai_conteineres)):
            for valor in extrator(texto):
                _sinal(fichas[ficha_id], dimensao, valor)


def _dados_mercante(conexao, todos_ces: set):
    """CEs, porto de carregamento dos manifestos e posições NCM declaradas, em lote."""
    ces = {c['numeroCEmercante']: c for c in _por_lotes(conexao, SQL_CES, 'ces', todos_ces)}
    manifestos = {m['numero']: m['portoCarregamento'] for m in _por_lotes(
        conexao, SQL_MANIFESTOS, 'numeros', {c['manifestoCE'] for c in ces.values() if c['manifestoCE']})}
    ncms_ce: Dict[str, set] = {}
    for n in _por_lotes(conexao, SQL_NCM_CE, 'ces', set(ces)):
        posicao = posicao_ncm(n['identificacaoNCM'])
        if posicao:
            ncms_ce.setdefault(n['numeroCEMercante'], set()).add(posicao)
    return ces, manifestos, ncms_ce


def _sinais_do_ce(ficha: dict, ce: dict, manifestos: dict, ncms_ce: dict):
    """Importador, embarcador, rota, portos, baldeação e NCM declarada de um CE da ficha."""
    _sinal(ficha, 'importador', documento(ce['consignatario']))
    _sinal(ficha, 'embarcador', normaliza_embarcador(ce['embarcador']))
    origem, destino = _texto(ce['portoOrigemCarga']).upper(), _texto(ce['portoDestFinal']).upper()
    _sinal(ficha, 'porto_origem', origem)
    _sinal(ficha, 'pais_origem', origem[:2])
    if origem and destino:
        _sinal(ficha, 'rota', f'{origem} > {destino}')
    carregamento = _texto(manifestos.get(ce['manifestoCE'])).upper()
    if carregamento and carregamento != origem:
        _sinal(ficha, 'porto_baldeacao', carregamento)
    for posicao in ncms_ce.get(ce['numeroCEmercante'], ()):
        _sinal(ficha, 'ncm_declarado', posicao)
        if origem:
            _sinal(ficha, 'pais_ncm', f'{origem[:2]} {posicao}')


def _e_exportacao(ce: dict) -> bool:
    return ce['tipoTrafego'] == TRAFEGO_EXPORTACAO


def _aplica_mercante(fichas: Dict[int, dict], dados_mercante, incluir_exportacao: bool) -> int:
    """Sinais do Mercante em cada ficha; tira as fichas só de exportação (se
    não for para incluir). Devolve quantas foram descartadas."""
    ces, manifestos, ncms_ce = dados_mercante
    descartadas = 0
    for ficha_id in list(fichas):
        ficha = fichas[ficha_id]
        conhecidos = [ces[ce] for ce in ficha['ces'] if ce in ces]
        ficha['exportacao'] = bool(conhecidos) and all(_e_exportacao(c) for c in conhecidos)
        if ficha['exportacao'] and not incluir_exportacao:
            del fichas[ficha_id]
            descartadas += 1
            continue
        for ce in conhecidos:
            if incluir_exportacao or not _e_exportacao(ce):
                _sinais_do_ce(ficha, ce, manifestos, ncms_ce)
    return descartadas


def _empresas(conexao, fichas: Dict[int, dict], ces: dict) -> Dict[str, str]:
    """Nome das empresas (laudo_empresas) por CNPJ e por raiz, para exibição."""
    cnpjs = {re.sub(r'\D', '', c['consignatario'] or '') for c in ces.values()}
    cnpjs |= {re.sub(r'\D', '', f.get('cnpj_fiscalizado') or '') for f in fichas.values()}
    cnpjs |= {c for f in fichas.values() for c in f['sinais']['cnpj_citado']}
    empresas = {}
    for e in _por_lotes(conexao, SQL_EMPRESAS, 'cnpjs', {c for c in cnpjs if len(c) == 14}):
        empresas.setdefault(e['cnpj'], e['nome'])
        empresas.setdefault(e['cnpj'][:8], e['nome'])
    return empresas


def extrai(conexao, tipos=TIPOS_PADRAO, inicio=None, fim=None, incluir_exportacao=False,
           fichas: Optional[Iterable[dict]] = None) -> dict:
    """Fichas do universo com seus sinais. `fichas` (opcional) substitui a busca
    por tipo de resultado — ex.: as fichas positivas do risco por CE
    (`risco_historico.fichas_positivas`); `tipos` continua dizendo quais
    resultados entram em `resultados`/`valor`. Returns:
        {'fichas': [ {id, numero, datahora, resultados, perdimento, apreensao, valor,
                      ces, exportacao, sinais: {dimensao: set(valores)}} ],
         'descartadas_exportacao': int, 'ces_no_mercante': int, 'ces': int,
         'empresas': {raiz: nome}}
    """
    if fichas is None:
        fichas = busca_fichas(conexao, tipos, inicio, fim)
    fichas = {f['id']: {**f, 'sinais': {chave: set() for chave, _, _ in DIMENSOES}, 'ces': set(),
                        'resultados': [], 'valor': 0.0} for f in fichas}
    if not fichas:
        return {'fichas': [], 'descartadas_exportacao': 0, 'ces_no_mercante': 0, 'ces': 0, 'empresas': {}}
    _sinais_dos_textos(fichas, _sinais_do_fichas(conexao, fichas, tipos))
    todos_ces = {ce for ficha in fichas.values() for ce in ficha['ces']}
    dados_mercante = _dados_mercante(conexao, todos_ces)
    descartadas = _aplica_mercante(fichas, dados_mercante, incluir_exportacao)
    for ficha in fichas.values():
        ficha['perdimento'] = PERDIMENTO in ficha['resultados']
        ficha['apreensao'] = APREENSAO in ficha['resultados']
    return {'fichas': list(fichas.values()), 'descartadas_exportacao': descartadas,
            'ces': len(todos_ces), 'ces_no_mercante': len(dados_mercante[0]),
            'empresas': _empresas(conexao, fichas, dados_mercante[0])}


# ------------------------------------------------------------ agregação

def descreve(dimensao: str, valor: str, empresas: dict) -> str:
    """Nome legível do valor (empresa, porto, país, posição NCM), quando conhecido."""
    if dimensao in ('importador', 'cnpj_citado'):
        return empresas.get(valor) or empresas.get(valor[:8]) or ''
    if dimensao in ('porto_origem', 'porto_baldeacao'):
        return referencias.descreve_porto(valor) or ''
    if dimensao == 'pais_origem':
        return referencias.nome_pais(valor) or ''
    if dimensao == 'rota':
        origem, _, destino = valor.partition(' > ')
        return f'{referencias.descreve_porto(origem) or origem} > {referencias.descreve_porto(destino) or destino}'
    if dimensao in ('ncm_declarado', 'ncm_encontrado'):
        return referencias.descricao_posicao_ncm(valor) or ''
    if dimensao == 'pais_ncm':
        pais, _, posicao = valor.partition(' ')
        return f'{referencias.nome_pais(pais) or pais} — {referencias.descricao_posicao_ncm(posicao) or posicao}'
    return ''


def indicadores(extracao: dict) -> Dict[str, List[dict]]:
    """Por dimensão, uma linha por valor, da mais para a menos frequente:
    fichas, perdimentos, apreensões, valor, primeira/última data, ids das fichas."""
    agregado: Dict[str, Dict[str, dict]] = {chave: {} for chave, _, _ in DIMENSOES}
    for ficha in extracao['fichas']:
        for dimensao, valores in ficha['sinais'].items():
            for valor in valores:
                item = agregado[dimensao].setdefault(valor, {'fichas': [], 'perdimentos': 0, 'apreensoes': 0,
                                                             'valor': 0.0, 'datas': []})
                item['fichas'].append(ficha['id'])
                item['perdimentos'] += ficha['perdimento']
                item['apreensoes'] += ficha['apreensao']
                item['valor'] += ficha['valor']
                if ficha.get('datahora'):
                    item['datas'].append(pd.Timestamp(ficha['datahora']).to_pydatetime())
    resultado = {}
    for dimensao, valores in agregado.items():
        linhas = []
        for valor, item in valores.items():
            linhas.append({
                'valor': valor, 'descricao': descreve(dimensao, valor, extracao['empresas']),
                'qtd_fichas': len(item['fichas']), 'perdimentos': item['perdimentos'],
                'apreensoes': item['apreensoes'], 'valor_total': round(item['valor'], 2),
                'primeira': min(item['datas']) if item['datas'] else None,
                'ultima': max(item['datas']) if item['datas'] else None,
                'fichas': sorted(item['fichas'], reverse=True),
            })
        resultado[dimensao] = sorted(linhas, key=lambda lin: (-lin['qtd_fichas'], -lin['valor_total'], lin['valor']))
    return resultado


def resumo(extracao: dict) -> dict:
    fichas = extracao['fichas']
    return {
        'fichas': len(fichas),
        'perdimentos': sum(f['perdimento'] for f in fichas),
        'apreensoes': sum(f['apreensao'] for f in fichas),
        'valor': round(sum(f['valor'] for f in fichas), 2),
        'com_ce': sum(1 for f in fichas if f['ces']),
        'ces': extracao['ces'], 'ces_no_mercante': extracao['ces_no_mercante'],
        'descartadas_exportacao': extracao['descartadas_exportacao'],
    }


def linhas_fichas(extracao: dict) -> List[dict]:
    """Uma linha por ficha, com os sinais em texto (aba "Fichas" da planilha)."""
    linhas = []
    for ficha in extracao['fichas']:
        linha = {'ficha_id': ficha['id'], 'ficha_numero': ficha.get('numero'), 'data': ficha.get('datahora'),
                 'resultados': ', '.join(TIPOS_RESULTADO.get(t, str(t)) for t in sorted(set(ficha['resultados']))),
                 'valor': round(ficha['valor'], 2), 'ces': ', '.join(sorted(ficha['ces']))}
        for dimensao, _, _ in DIMENSOES:
            linha[dimensao] = '; '.join(sorted(ficha['sinais'][dimensao]))
        linhas.append(linha)
    return linhas


def planilha_xlsx(extracao: dict, parametros: Optional[dict] = None) -> bytes:
    """Aba "Resumo", uma aba por dimensão (todos os valores) e a aba "Fichas"."""
    tabelas = indicadores(extracao)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as escritor:
        quadro = [(chave, valor) for chave, valor in (parametros or {}).items()]
        quadro += [(chave, valor) for chave, valor in resumo(extracao).items()]
        pd.DataFrame(quadro, columns=['Item', 'Valor']).to_excel(escritor, sheet_name='Resumo', index=False)
        for dimensao, rotulo, _ in DIMENSOES:
            df = pd.DataFrame([{**lin, 'fichas': ', '.join(map(str, lin['fichas']))} for lin in tabelas[dimensao]],
                              columns=['valor', 'descricao', 'qtd_fichas', 'perdimentos', 'apreensoes',
                                       'valor_total', 'primeira', 'ultima', 'fichas'])
            df.to_excel(escritor, sheet_name=dimensao[:31], index=False)
        pd.DataFrame(linhas_fichas(extracao)).to_excel(escritor, sheet_name='Fichas', index=False)
    return buffer.getvalue()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--tipos', default=','.join(map(str, TIPOS_PADRAO)),
                        help='tipos de resultado (padrão 1,2 = Apreensão e Perdimento)')
    parser.add_argument('--inicio', help="data inicial da ficha ('YYYY-MM-DD')")
    parser.add_argument('--fim', help="data final da ficha, exclusiva ('YYYY-MM-DD')")
    parser.add_argument('--incluir-exportacao', action='store_true',
                        help='não descarta as fichas cujos CEs são todos de exportação')
    parser.add_argument('--saida', default=datetime.now().strftime('risco_perdimentos_%Y%m%d_%H%M.xlsx'))
    args = parser.parse_args()
    uri = args.uri
    if not uri:
        from ajna_commons.flask.conf import SQL_URI as uri
    tipos = tuple(int(t) for t in args.tipos.split(','))
    inicio = datetime.strptime(args.inicio, '%Y-%m-%d') if args.inicio else None
    fim = datetime.strptime(args.fim, '%Y-%m-%d') if args.fim else None
    with create_engine(uri).connect() as conexao:
        extracao = extrai(conexao, tipos, inicio, fim, args.incluir_exportacao)
    with open(args.saida, 'wb') as arquivo:
        arquivo.write(planilha_xlsx(extracao, {'tipos': args.tipos, 'inicio': args.inicio, 'fim': args.fim}))
    print(resumo(extracao))
    tabelas = indicadores(extracao)
    for dimensao, rotulo, _ in DIMENSOES:
        repetidos = sum(1 for lin in tabelas[dimensao] if lin['qtd_fichas'] >= 2)
        print(f'{rotulo}: {len(tabelas[dimensao])} valores, {repetidos} em 2+ fichas')
    print(f'Planilha gravada em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
