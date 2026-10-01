# coding: utf-8
"""
Blueprint da tela "Transporte das apreensões".

  GET /apreensoes_transporte/          -> tela (formulário + resumo + tabela)
  GET /apreensoes_transporte/planilha  -> mesma consulta, em planilha Excel

A lógica é a do script `analytic_scripts/dados_transporte_apreensoes.py`
(RVFs com apreensão no Fichas -> acessos de veículo da API Recintos,
agrupados por motorista + veículo + transportadora); aqui só se valida a
entrada, chama e formata. Rotas sem redirect da barra final e URLs só por
`url_for` (a app roda sob /virasana atrás de proxy — ver CLAUDE.md).
"""
import io
from datetime import date, datetime, time, timedelta

import pandas as pd
from flask import Blueprint, current_app, render_template, request, send_file
from flask_login import login_required

from virasana.analytic_scripts.dados_transporte_apreensoes import (
    COLUNAS_CSV, ORDEM_FORMAS, busca_apreensoes, relaciona,
)

bp_apreensoes = Blueprint('apreensoes', __name__, url_prefix='/apreensoes_transporte')

LIMITE_LINHAS_TELA = 500
MAXIMO_DIAS_PERIODO = 366
MAXIMO_DIAS_JANELA = 90
PERIODO_PADRAO_DIAS = 90

# Cabeçalhos legíveis da planilha (mesma ordem de COLUNAS_CSV)
CABECALHOS = {
    'ovr_id': 'Ficha (id)', 'ovr_numero': 'Ficha (número)', 'ovr_datahora': 'Data da ficha',
    'rvf_id': 'RVF (id)', 'rvf_datahora': 'Data da RVF', 'numeroCEmercante': 'CE Mercante',
    'rvf_numerolote': 'Lote/contêiner da RVF', 'apreensao_tipos': 'Tipos de apreensão',
    'apreensao_peso_total': 'Peso apreendido', 'apreensao_descricoes': 'Descrição da apreensão',
    'forma_ligacao': 'Forma de ligação', 'valor_ligacao': 'Valor ligado',
    'cpfMotorista': 'CPF motorista', 'nomeMotorista': 'Motorista', 'placa': 'Placa',
    'placaSemirreboque': 'Semirreboque', 'cnpjTransportador': 'CNPJ transportador',
    'qtd_ocorrencias': 'Ocorrências', 'movimentos': 'Movimentos', 'primeira_datahora': 'Primeira',
    'ultima_datahora': 'Última', 'dias_da_rvf_mais_proximo': 'Dias da RVF (mais próximo)',
    'recintos': 'Recintos', 'conteineres': 'Contêineres', 'conhecimentos': 'CEs',
}


class ParametrosInvalidos(ValueError):
    pass


def _data(nome: str, padrao: date) -> date:
    valor = request.args.get(nome)
    if not valor:
        return padrao
    try:
        return datetime.strptime(valor, '%Y-%m-%d').date()
    except ValueError:
        raise ParametrosInvalidos(f'Data inválida: {valor}')


def _inteiro(nome: str, padrao: int) -> int:
    valor = request.args.get(nome)
    if valor in (None, ''):
        return padrao
    try:
        numero = int(valor)
    except ValueError:
        raise ParametrosInvalidos(f'Número inválido: {valor}')
    if not 0 <= numero <= MAXIMO_DIAS_JANELA:
        raise ParametrosInvalidos(f'A janela vai de 0 a {MAXIMO_DIAS_JANELA} dias.')
    return numero


def parametros() -> dict:
    """Parâmetros da tela, validados (com os padrões quando não vierem)."""
    hoje = date.today()
    p = {
        'inicio': _data('inicio', hoje - timedelta(days=PERIODO_PADRAO_DIAS)),
        'fim': _data('fim', hoje),
        'dias_antes': _inteiro('dias_antes', 30),
        'dias_depois': _inteiro('dias_depois', 30),
        'outras_viagens': request.args.get('outras_viagens') in ('1', 'on', 'true'),
    }
    if p['fim'] < p['inicio']:
        raise ParametrosInvalidos('A data final é anterior à inicial.')
    if (p['fim'] - p['inicio']).days > MAXIMO_DIAS_PERIODO:
        raise ParametrosInvalidos(f'Período máximo: {MAXIMO_DIAS_PERIODO} dias (cada RVF faz várias consultas).')
    return p


def consulta(conexao, p: dict) -> list:
    """Linhas (COLUNAS_CSV) das RVFs com apreensão cujas fichas são do período."""
    inicio = datetime.combine(p['inicio'], time.min)
    fim = datetime.combine(p['fim'] + timedelta(days=1), time.min)  # fim inclusivo
    apreensoes = busca_apreensoes(conexao, inicio, fim)
    if apreensoes.empty:
        return []
    return relaciona(conexao, apreensoes, p['dias_antes'], p['dias_depois'], p['outras_viagens'])


def resumo(linhas: list) -> dict:
    """Números do topo da tela e a contagem por forma de ligação."""
    achados = [linha for linha in linhas if linha.get('qtd_ocorrencias')]
    por_forma = {}
    for linha in achados:
        forma = por_forma.setdefault(linha['forma_ligacao'], {'conjuntos': 0, 'ocorrencias': 0})
        forma['conjuntos'] += 1
        forma['ocorrencias'] += linha['qtd_ocorrencias']
    return {
        'rvfs': len({linha['rvf_id'] for linha in linhas}),
        'rvfs_com_acesso': len({linha['rvf_id'] for linha in achados}),
        'conjuntos': len(achados),
        'ocorrencias': sum(linha['qtd_ocorrencias'] for linha in achados),
        'por_forma': sorted(por_forma.items(),
                            key=lambda item: ORDEM_FORMAS.index(item[0].split('+')[0])),
    }


def planilha_xlsx(linhas: list, p: dict) -> bytes:
    dados = pd.DataFrame(linhas, columns=COLUNAS_CSV).rename(columns=CABECALHOS)
    r = resumo(linhas)
    quadro = pd.DataFrame(
        [('Período das fichas', f"{p['inicio']:%d/%m/%Y} a {p['fim']:%d/%m/%Y}"),
         ('Janela de acessos (dias antes / depois da RVF)', f"{p['dias_antes']} / {p['dias_depois']}"),
         ('Outras viagens incluídas', 'sim' if p['outras_viagens'] else 'não'),
         ('RVFs com apreensão', r['rvfs']), ('RVFs com algum acesso', r['rvfs_com_acesso']),
         ('Conjuntos motorista/veículo/transportadora', r['conjuntos']), ('Ocorrências', r['ocorrencias'])]
        + [(f'Forma: {forma}', f"{v['conjuntos']} conjuntos, {v['ocorrencias']} ocorrências")
           for forma, v in r['por_forma']],
        columns=['Item', 'Valor'])
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as escritor:
        dados.to_excel(escritor, sheet_name='Conjuntos', index=False)
        quadro.to_excel(escritor, sheet_name='Resumo', index=False)
    return buffer.getvalue()


def _conexao():
    return current_app.config['db_session'].connection()


@bp_apreensoes.route('/', strict_slashes=False)  # sem redirect da barra final (proxy; ver CLAUDE.md)
@login_required
def index():
    contexto = {'linhas': None, 'resumo': None, 'erro': None, 'limite': LIMITE_LINHAS_TELA, 'p': None}
    try:
        p = parametros()
        contexto['p'] = p
        if request.args.get('pesquisar'):
            contexto['linhas'] = consulta(_conexao(), p)
            contexto['resumo'] = resumo(contexto['linhas'])
    except ParametrosInvalidos as err:
        contexto['erro'] = str(err)
    if contexto['p'] is None:  # parâmetros inválidos: reexibe o que veio
        contexto['p'] = {'inicio': request.args.get('inicio', ''), 'fim': request.args.get('fim', ''),
                         'dias_antes': request.args.get('dias_antes', ''),
                         'dias_depois': request.args.get('dias_depois', ''),
                         'outras_viagens': bool(request.args.get('outras_viagens'))}
    return render_template('apreensoes_transporte.html', **contexto)


@bp_apreensoes.route('/planilha')
@login_required
def planilha():
    try:
        p = parametros()
    except ParametrosInvalidos as err:
        return str(err), 400
    nome = f"transporte_apreensoes_{p['inicio']:%Y%m%d}_{p['fim']:%Y%m%d}.xlsx"
    return send_file(
        io.BytesIO(planilha_xlsx(consulta(_conexao(), p), p)), as_attachment=True, download_name=nome,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )
