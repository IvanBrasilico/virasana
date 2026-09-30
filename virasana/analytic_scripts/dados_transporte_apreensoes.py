"""Recupera dados de transporte de apreensões

Para cada verificação física (RVF) do Fichas com apreensão de drogas, procura
os eventos de Acesso Veículo (API Recintos, gates de entrada/saída) do mesmo
transporte e extrai placa, semirreboque, motorista e transportadora — dados
para a análise de risco.

Formas de ligação (coluna `forma_ligacao` do CSV), da mais para a menos forte:
  - conteiner_rvf: contêiner informado na RVF (campo numerolote);
  - ce_mercante: CE Mercante da RVF/OVR contra o conhecimento do acesso;
  - conteiner_texto: contêiner citado nas observações da OVR ou na descrição
    da RVF (só números com dígito verificador ISO 6346 válido);
  - placa_texto: placa (padrão antigo ou Mercosul) citada nesses textos,
    contra a placa do cavalo ou do semirreboque;
  - cpf_texto: CPF (dígitos verificadores válidos) citado nesses textos,
    contra o CPF do motorista.
Todas limitadas a uma janela de datas em torno da RVF (padrão: 30 dias antes
e 30 depois). Ligações por placa e CPF encontram OUTRAS viagens do mesmo
veículo/motorista no período — úteis para a análise, mas mais fracas; por
isso a forma de ligação vai em cada linha. Um acesso achado por mais de uma
forma sai numa linha só, com as formas separadas por "+".

Não se liga por CNPJ citado no texto: quase sempre é o do importador/
exportador, e casar com o CNPJ da transportadora traria milhares de acessos.

Saída: CSV (separador ";", UTF-8 com BOM para abrir no Excel), uma linha por
(RVF, acesso encontrado). Apreensões da mesma RVF são agregadas (tipos, peso
total, descrições). RVFs sem nenhum acesso encontrado também saem, com
`forma_ligacao` = "nenhuma", para medir a cobertura.

Somente leitura. Uso (a partir da raiz do repo virasana):

    python virasana/analytic_scripts/dados_transporte_apreensoes.py
    python virasana/analytic_scripts/dados_transporte_apreensoes.py --inicio 2025-01-01 --saida apreensoes.csv
    python virasana/analytic_scripts/dados_transporte_apreensoes.py --dias-antes 15 --dias-depois 15
"""
import argparse
import re
import sys
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

SQL_APREENSOES = '''
SELECT ficha.id AS ovr_id, ficha.numero AS ovr_numero, ficha.datahora AS ovr_datahora,
       ficha.numeroCEmercante AS ovr_ce, ficha.observacoes AS ovr_observacoes,
       rvf.id AS rvf_id, rvf.datahora AS rvf_datahora, rvf.numeroCEmercante AS rvf_ce,
       rvf.numerolote AS rvf_numerolote, rvf.descricao AS rvf_descricao,
       rvf.descricao_interna AS rvf_descricao_interna,
       a.peso AS apreensao_peso, a.descricao AS apreensao_descricao, t.descricao AS apreensao_tipo
FROM ovr_ovrs ficha
INNER JOIN ovr_verificacoesfisicas rvf ON ficha.id = rvf.ovr_id
INNER JOIN ovr_apreensoes_rvf a ON a.rvf_id = rvf.id
LEFT JOIN ovr_tiposapreensao t ON t.id = a.tipo_id
WHERE ficha.datahora >= :inicio AND ficha.datahora < :fim
ORDER BY ficha.datahora, rvf.id
'''

COLUNAS_ACESSO = ('id', 'codigoRecinto', 'dataHoraOcorrencia', 'operacao', 'direcao', 'tipoOperacao',
                  'placa', 'placaSemirreboque', 'numeroConteiner', 'numeroConhecimento',
                  'cnpjTransportador', 'cpfMotorista', 'nomeMotorista')

# Coluna do acesso consultada por forma de ligação. Os nomes vêm desta
# constante (nunca do usuário), então montar o SQL com eles é seguro.
CAMPOS_POR_FORMA = {
    'conteiner_rvf': ('numeroConteiner',),
    'ce_mercante': ('numeroConhecimento',),
    'conteiner_texto': ('numeroConteiner',),
    'placa_texto': ('placa', 'placaSemirreboque'),
    'cpf_texto': ('cpfMotorista',),
}

COLUNAS_CSV = [
    'ovr_id', 'ovr_numero', 'ovr_datahora', 'rvf_id', 'rvf_datahora', 'numeroCEmercante', 'rvf_numerolote',
    'apreensao_tipos', 'apreensao_peso_total', 'apreensao_descricoes',
    'forma_ligacao', 'valor_ligacao',
    'acesso_id', 'acesso_recinto', 'acesso_datahora', 'dias_da_rvf', 'acesso_operacao', 'acesso_direcao',
    'acesso_tipoOperacao', 'placa', 'placaSemirreboque', 'numeroConteiner', 'numeroConhecimento',
    'cnpjTransportador', 'cpfMotorista', 'nomeMotorista',
]

RE_CONTEINER = re.compile(r'\b([A-Z]{3}[UJZ])\s?-?\s?(\d{6})\s?-?\s?(\d)\b')
RE_PLACA = re.compile(r'\b([A-Z]{3})\s?-?\s?(\d[A-Z0-9]\d{2})\b(?![.,]\d)')
# siglas que, seguidas de número, parecem placa ("NCM 8471", "RVF 1234")
NAO_SAO_PLACAS = {'NCM', 'CPF', 'RVF', 'OVR', 'DUE', 'DTA', 'LCL', 'FCL', 'TEU', 'KGS', 'ANO', 'LOT', 'REF', 'NRO',
                  'NUM', 'SEI', 'IPL', 'BOE', 'TCO', 'ART', 'LEI', 'CEP', 'TEL', 'FAX'}
RE_CPF = re.compile(r'(?<![\d./-])(\d{3})\.?(\d{3})\.?(\d{3})-?(\d{2})(?![\d./-])')


# ------------------------------------------------------------ identificadores

def digito_conteiner(prefixo_e_numero: str) -> Optional[int]:
    """Dígito verificador ISO 6346 dos 10 primeiros caracteres (ex.: 'MSKU123456')."""
    if not re.fullmatch(r'[A-Z]{4}\d{6}', prefixo_e_numero or ''):
        return None
    valores, valor = {}, 10
    for letra in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
        if valor % 11 == 0:  # a norma pula os múltiplos de 11
            valor += 1
        valores[letra] = valor
        valor += 1
    soma = sum((valores[c] if c.isalpha() else int(c)) * 2 ** i for i, c in enumerate(prefixo_e_numero))
    return soma % 11 % 10


def conteiner_valido(numero: Optional[str]) -> bool:
    numero = numero or ''
    return bool(re.fullmatch(r'[A-Z]{4}\d{7}', numero)) and digito_conteiner(numero[:10]) == int(numero[10])


def normaliza_conteiner(valor: Optional[str]) -> Optional[str]:
    """'mscu 123456-7' -> 'MSCU1234567'; None se não tiver formato de contêiner."""
    limpo = re.sub(r'[^A-Z0-9]', '', (valor or '').upper())
    return limpo if re.fullmatch(r'[A-Z]{4}\d{7}', limpo) else None


def cpf_valido(cpf: str) -> bool:
    if not re.fullmatch(r'\d{11}', cpf or '') or cpf == cpf[0] * 11:
        return False
    for tamanho in (9, 10):
        soma = sum(int(cpf[i]) * (tamanho + 1 - i) for i in range(tamanho))
        if (soma * 10 % 11) % 10 != int(cpf[tamanho]):
            return False
    return True


def extrai_conteineres(texto: Optional[str]) -> List[str]:
    """Contêineres citados no texto, só com dígito verificador válido."""
    achados = (''.join(m) for m in RE_CONTEINER.findall((texto or '').upper()))
    return list(dict.fromkeys(c for c in achados if conteiner_valido(c)))


def extrai_placas(texto: Optional[str]) -> List[str]:
    """Placas no padrão antigo (ABC1234) ou Mercosul (ABC1D23) citadas no texto."""
    return list(dict.fromkeys(''.join(m) for m in RE_PLACA.findall((texto or '').upper())
                              if m[0] not in NAO_SAO_PLACAS))


def extrai_cpfs(texto: Optional[str]) -> List[str]:
    """CPFs (com ou sem máscara) citados no texto, só com dígitos verificadores válidos."""
    achados = (''.join(m) for m in RE_CPF.findall(texto or ''))
    return list(dict.fromkeys(c for c in achados if cpf_valido(c)))


# ------------------------------------------------------------ consultas

def busca_apreensoes(conexao, inicio: datetime, fim: datetime) -> pd.DataFrame:
    """Uma linha por RVF com apreensão, com as apreensões agregadas."""
    df = pd.read_sql(text(SQL_APREENSOES), conexao, params={'inicio': inicio, 'fim': fim})
    if df.empty:
        return df
    df['apreensao_peso'] = pd.to_numeric(df['apreensao_peso'], errors='coerce')
    chaves = [c for c in df.columns if not c.startswith('apreensao_')]
    agregado = df.groupby('rvf_id', sort=False).agg(
        apreensao_tipos=('apreensao_tipo', lambda s: '; '.join(dict.fromkeys(x for x in s if x))),
        apreensao_peso_total=('apreensao_peso', 'sum'),
        apreensao_descricoes=('apreensao_descricao', lambda s: '; '.join(dict.fromkeys(x for x in s if x))),
    ).reset_index()
    return df[chaves].drop_duplicates('rvf_id').merge(agregado, on='rvf_id')


def ligacoes(registro: dict) -> List[Tuple[str, str]]:
    """(forma de ligação, valor) a pesquisar para uma RVF, sem repetir valores."""
    textos = ' \n '.join(str(registro.get(c) or '') for c in
                         ('ovr_observacoes', 'rvf_descricao', 'rvf_descricao_interna'))
    resultado = []
    conteiner_rvf = normaliza_conteiner(registro.get('rvf_numerolote'))
    if conteiner_rvf:
        resultado.append(('conteiner_rvf', conteiner_rvf))
    for ce in dict.fromkeys(re.sub(r'\D', '', str(registro.get(c) or '')) for c in ('rvf_ce', 'ovr_ce')):
        if len(ce) == 15:
            resultado.append(('ce_mercante', ce))
    resultado += [('conteiner_texto', c) for c in extrai_conteineres(textos) if c != conteiner_rvf]
    resultado += [('placa_texto', p) for p in extrai_placas(textos)]
    resultado += [('cpf_texto', c) for c in extrai_cpfs(textos)]
    return resultado


def busca_acessos(conexao, campo: str, valor: str, inicio: datetime, fim: datetime) -> List[dict]:
    if campo not in {c for campos in CAMPOS_POR_FORMA.values() for c in campos}:
        raise ValueError(f'campo não permitido: {campo}')
    # campo validado acima contra CAMPOS_POR_FORMA; o valor vai como parâmetro
    sql = (f'SELECT {", ".join(COLUNAS_ACESSO)} FROM apirecintos_acessosveiculo '  # nosec B608
           f'WHERE {campo} = :valor AND dataHoraOcorrencia BETWEEN :inicio AND :fim '
           f'ORDER BY dataHoraOcorrencia')
    parametros = {'valor': valor, 'inicio': inicio, 'fim': fim}
    return [dict(linha._mapping) for linha in conexao.execute(text(sql), parametros)]


def _como_datetime(valor):
    if valor is None or pd.isna(valor):
        return None
    return pd.Timestamp(valor).to_pydatetime()


def relaciona(conexao, apreensoes: pd.DataFrame, dias_antes: int = 30, dias_depois: int = 30) -> List[dict]:
    """Linhas do CSV: cada RVF com cada acesso encontrado (ou uma linha "nenhuma")."""
    linhas = []
    for registro in apreensoes.to_dict('records'):
        referencia = _como_datetime(registro.get('rvf_datahora')) or _como_datetime(registro.get('ovr_datahora'))
        base = {
            'ovr_id': registro['ovr_id'], 'ovr_numero': registro.get('ovr_numero'),
            'ovr_datahora': registro.get('ovr_datahora'), 'rvf_id': registro['rvf_id'],
            'rvf_datahora': registro.get('rvf_datahora'),
            'numeroCEmercante': registro.get('rvf_ce') or registro.get('ovr_ce'),
            'rvf_numerolote': registro.get('rvf_numerolote'),
            'apreensao_tipos': registro.get('apreensao_tipos'),
            'apreensao_peso_total': registro.get('apreensao_peso_total'),
            'apreensao_descricoes': registro.get('apreensao_descricoes'),
        }
        achados: Dict[object, dict] = {}  # id do acesso -> linha (junta formas de ligação)
        if referencia is not None:
            inicio, fim = referencia - timedelta(days=dias_antes), referencia + timedelta(days=dias_depois)
            for forma, valor in ligacoes(registro):
                for campo in CAMPOS_POR_FORMA[forma]:
                    for acesso in busca_acessos(conexao, campo, valor, inicio, fim):
                        linha = achados.get(acesso['id'])
                        if linha is None:
                            data_acesso = _como_datetime(acesso['dataHoraOcorrencia'])
                            linha = achados[acesso['id']] = {
                                **base, 'forma_ligacao': forma, 'valor_ligacao': valor,
                                'acesso_id': acesso['id'], 'acesso_recinto': acesso['codigoRecinto'],
                                'acesso_datahora': acesso['dataHoraOcorrencia'],
                                'dias_da_rvf': round((data_acesso - referencia).total_seconds() / 86400, 1),
                                'acesso_operacao': acesso['operacao'], 'acesso_direcao': acesso['direcao'],
                                'acesso_tipoOperacao': acesso['tipoOperacao'],
                                **{c: acesso[c] for c in ('placa', 'placaSemirreboque', 'numeroConteiner',
                                                          'numeroConhecimento', 'cnpjTransportador',
                                                          'cpfMotorista', 'nomeMotorista')},
                            }
                        elif forma not in linha['forma_ligacao'].split('+'):
                            linha['forma_ligacao'] += '+' + forma
                            linha['valor_ligacao'] += '+' + valor
        if achados:
            linhas += sorted(achados.values(), key=lambda linha: str(linha['acesso_datahora']))
        else:
            linhas.append({**base, 'forma_ligacao': 'nenhuma' if referencia else 'sem data de referência'})
    return linhas


def resumo(linhas: Iterable[dict]) -> str:
    df = pd.DataFrame(list(linhas), columns=COLUNAS_CSV)
    if df.empty:
        return 'Nenhuma RVF com apreensão no período.'
    rvfs = df['rvf_id'].nunique()
    com_acesso = df.loc[df['acesso_id'].notna(), 'rvf_id'].nunique()
    formas = df.loc[df['acesso_id'].notna(), 'forma_ligacao'].str.split('+').explode().value_counts()
    return (f'{rvfs} RVFs com apreensão; {com_acesso} com ao menos um acesso encontrado '
            f'({df["acesso_id"].notna().sum()} acessos).\nAcessos por forma de ligação:\n{formas.to_string()}')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--inicio', default='2000-01-01', help="data inicial da OVR ('YYYY-MM-DD')")
    parser.add_argument('--fim', default=None, help="data final da OVR, exclusiva ('YYYY-MM-DD'; padrão: amanhã)")
    parser.add_argument('--dias-antes', type=int, default=30, help='janela de busca antes da RVF (dias)')
    parser.add_argument('--dias-depois', type=int, default=30, help='janela de busca depois da RVF (dias)')
    parser.add_argument('--saida', default=datetime.now().strftime('dados_transporte_apreensoes_%Y%m%d_%H%M.csv'))
    args = parser.parse_args()

    uri = args.uri
    if not uri:
        from ajna_commons.flask.conf import SQL_URI as uri
    inicio = datetime.strptime(args.inicio, '%Y-%m-%d')
    fim = datetime.strptime(args.fim, '%Y-%m-%d') if args.fim else datetime.now() + timedelta(days=1)

    with create_engine(uri).connect() as conexao:
        apreensoes = busca_apreensoes(conexao, inicio, fim)
        print(f'{len(apreensoes)} RVFs com apreensão entre {inicio:%Y-%m-%d} e {fim:%Y-%m-%d}; buscando acessos...')
        linhas = relaciona(conexao, apreensoes, args.dias_antes, args.dias_depois)
    pd.DataFrame(linhas, columns=COLUNAS_CSV).to_csv(args.saida, sep=';', index=False, encoding='utf-8-sig')
    print(resumo(linhas))
    print(f'CSV gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
