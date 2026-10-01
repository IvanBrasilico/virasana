# coding: utf-8
"""
Gera as tabelas de referência locais do NAVI-I (JSON, código -> nome) a
partir de fontes públicas. É um "BD" provisório: o sistema não tem hoje boas
tabelas de posições NCM, portos, países e navios.

Fontes:
  - NCM (capítulos e posições, em português): API pública do Portal Único
    Siscomex — tabela oficial vigente.
  - Portos: UN/LOCODE (UNECE), pelo espelho `datasets/un-locode` no GitHub.
    Só entram as localidades com função de PORTO. O Mercante usa vários
    códigos que no LOCODE são outra coisa (CNSHA é o aeroporto de Xangai;
    BRITA é Itabuna/BA; CNZZZ é uma estação de trem) — nesses casos é melhor
    mostrar só o código do que um nome errado. Os casos conhecidos ficam em
    `portos_ajustes.json` (mantido à mão; este script não o altera).
  - Países (português): API de países do IBGE, completada pela Wikidata para
    territórios que o IBGE não lista (Hong Kong, Taiwan, Porto Rico...).
  - Navios (número IMO -> nome): Wikidata. NÃO é fonte oficial nem completa
    (cobriu ~74% dos IMOs de uma amostra de escalas) e o nome pode estar
    desatualizado se o navio foi rebatizado. As fontes oficiais (IMO GISIS,
    Equasis) exigem login e não permitem extração em lote. Embarcações de
    navegação interior vêm no Mercante com número de inscrição nacional (10
    dígitos), sem fonte pública de nomes.

Filtro de uso (o que mantém as tabelas pequenas e relevantes):
  As fontes públicas não dizem, de forma confiável, quais navios operam
  contêineres nem quais portos estão nas rotas do Brasil (na Wikidata, 60% dos
  navios têm tipo genérico "ship"; o UN/LOCODE não marca porto de contêiner).
  Quem sabe isso é a própria base do Mercante. Com `--banco`, o script consulta
  (somente leitura) as escalas a partir de `--desde` e grava em `usados.json`:
    - navios: embarcações de escalas cujos manifestos têm ao menos um
      contêiner declarado (porta-contêineres, Ro-Ro/ConRo que levam contêiner);
    - portos: portos dessas escalas, mais os portos de carregamento/
      descarregamento dos manifestos e de origem/destino dos CEs do período.
  Havendo `usados.json`, portos.json e navios.json saem só com esses códigos
  (`--sem-filtro-de-uso` desliga). O `usados.json` fica versionado: regerar
  sem acesso ao banco mantém o filtro.

Uso (a partir da raiz do repo virasana):

    python virasana/navii/referencias/atualiza_referencias.py            # baixa e gera
    python virasana/navii/referencias/atualiza_referencias.py --banco    # idem, atualizando antes o usados.json
    python virasana/navii/referencias/atualiza_referencias.py --brutos D # gera a partir de arquivos já baixados em D
    python virasana/navii/referencias/atualiza_referencias.py --guardar-brutos D

O download tenta `urllib` e, se falhar (rede com inspeção de TLS), usa
`curl --ssl-no-revoke -L` (no Windows a checagem de revogação do certificado
costuma estar indisponível na rede interna; a cadeia continua sendo validada).
"""
import argparse
import csv
import gzip
import io
import json
import os
import re
import shutil
# subprocess: só o curl de download, com argumentos fixos (ver baixa())
import subprocess  # nosec B404
import tempfile
import urllib.parse
import urllib.request
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Optional

AQUI = os.path.dirname(os.path.abspath(__file__))
USER_AGENT = 'navii-referencias/0.1 (uso interno)'

FONTES = OrderedDict([
    ('ncm.json', {
        'arquivo_bruto': 'ncm.json',
        'url': 'https://portalunico.siscomex.gov.br/classif/api/publico/nomenclatura/download/json',
        'descricao': 'NCM vigente — Portal Único Siscomex',
    }),
    ('portos.json', {
        'arquivo_bruto': 'locode.csv',
        'url': 'https://raw.githubusercontent.com/datasets/un-locode/main/data/code-list.csv',
        'descricao': 'UN/LOCODE (UNECE), espelho datasets/un-locode',
    }),
    ('paises.json', {
        'arquivo_bruto': 'paises.json',
        'url': 'https://servicodados.ibge.gov.br/api/v1/paises',
        'descricao': 'Países em português — IBGE (completado pela Wikidata)',
    }),
    ('paises_wikidata', {
        'arquivo_bruto': 'paises_wikidata.json',
        'url': 'https://query.wikidata.org/sparql?format=json&query=' + urllib.parse.quote(
            'SELECT ?codigo ?nome WHERE { ?s wdt:P297 ?codigo . ?s rdfs:label ?nome . '
            'FILTER(LANG(?nome) = "pt-br" || LANG(?nome) = "pt") }'),
        'descricao': 'Países/territórios ISO 3166-1 em português — Wikidata',
    }),
    ('navios.json', {
        'arquivo_bruto': 'navios.json',
        'url': 'https://query.wikidata.org/sparql?format=json&query=' + urllib.parse.quote(
            'SELECT ?imo ?nome WHERE { ?s wdt:P458 ?imo . ?s rdfs:label ?nome . FILTER(LANG(?nome) = "en") }'),
        'descricao': 'Navios por número IMO — Wikidata (cobertura parcial, não oficial)',
    }),
])


def baixa(url: str, destino: str):
    try:
        # pede sem compressão (le_texto ainda descomprime, se o servidor ignorar o pedido)
        requisicao = urllib.request.Request(url, headers={'User-Agent': USER_AGENT, 'Accept-Encoding': 'identity'})
        with urllib.request.urlopen(requisicao, timeout=180) as resposta, open(destino, 'wb') as saida:  # nosec B310
            shutil.copyfileobj(resposta, saida)
        return
    except Exception as err:  # noqa
        print(f'  urllib falhou ({err}); tentando curl...')
    if not shutil.which('curl'):
        raise RuntimeError('Sem acesso por urllib e curl não encontrado.')
    subprocess.run(  # nosec B603 B607
        ['curl', '-sS', '-m', '300', '--ssl-no-revoke', '-L', '-A', USER_AGENT, '-o', destino, url],
        check=True,
    )


def le_texto(caminho: str) -> str:
    """Lê um arquivo baixado tolerando compressão e codificação: alguns
    servidores (ex.: IBGE) respondem em gzip mesmo sem o cliente pedir, e o
    urllib grava os bytes comprimidos. Tenta UTF-8 (com ou sem BOM), depois
    cp1252 e, por fim, latin-1 (que nunca falha)."""
    with open(caminho, 'rb') as arquivo:
        dados = arquivo.read()
    if dados[:2] == b'\x1f\x8b':
        dados = gzip.decompress(dados)
    for codificacao in ('utf-8-sig', 'cp1252'):
        try:
            return dados.decode(codificacao)
        except UnicodeDecodeError:
            continue
    return dados.decode('latin-1')


def le_json(caminho):
    return json.loads(le_texto(caminho))


def gera_ncm(bruto: dict) -> dict:
    """{'capitulos': {'84': ...}, 'posicoes': {'8471': ...}} — só códigos vigentes.

    Nem toda posição tem linha própria de 4 dígitos na tabela (ex.: 9503 só
    aparece como "9503.00"): vale a descrição do código mais curto da posição.
    """
    capitulos, posicoes, tamanho = {}, {}, {}
    for item in bruto['Nomenclaturas']:
        if item.get('Data_Fim') != '31/12/9999':
            continue
        codigo = item['Codigo'].replace('.', '')
        # a tabela traz marcação HTML (<i>T-shirts</i>); a tela exibe texto puro
        descricao = re.sub(r'<[^>]+>', '', item['Descricao']).strip().rstrip('.:').strip()
        if len(codigo) == 2:
            capitulos[codigo] = descricao
        elif len(codigo) >= 4 and len(codigo) < tamanho.get(codigo[:4], 99):
            posicoes[codigo[:4]] = descricao
            tamanho[codigo[:4]] = len(codigo)
    return {'capitulos': capitulos, 'posicoes': posicoes}


def gera_portos(linhas) -> dict:
    """{'BRSSZ': 'Santos'} — só localidades com função de porto ('1' em Function)."""
    portos = {}
    for linha in linhas:
        if linha['Location'] and linha['Name'] and '1' in (linha['Function'] or ''):
            portos.setdefault(linha['Country'] + linha['Location'], linha['Name'].strip())
    return portos


def gera_paises(ibge: list, wikidata: dict) -> dict:
    """{'BR': 'Brasil'} — IBGE tem precedência; a Wikidata completa o que falta."""
    paises = {}
    for linha in wikidata['results']['bindings']:
        codigo, nome = linha['codigo']['value'].strip().upper(), linha['nome']['value'].strip()
        # prefere o rótulo pt-br ao pt quando vierem os dois
        if codigo not in paises or linha['nome'].get('xml:lang') == 'pt-br':
            paises[codigo] = nome[:1].upper() + nome[1:]
    for pais in ibge:
        paises[pais['id']['ISO-3166-1-ALPHA-2']] = pais['nome']['abreviado']
    return paises


def gera_navios(wikidata: dict) -> dict:
    """{'9519119': 'BRASILIA HIGHWAY'} — só IMOs de 7 dígitos com nome de verdade."""
    nomes = {}
    for linha in wikidata['results']['bindings']:
        imo, nome = linha['imo']['value'].strip(), linha['nome']['value'].strip()
        nome = re.sub(r'\s*\((ship|schip|navio)[^)]*\)\s*$', '', nome, flags=re.I)
        if not re.fullmatch(r'\d{7}', imo) or re.fullmatch(r'(IMO )?\d+', nome) or not nome:
            continue
        nomes.setdefault(imo, set()).add(nome.upper())
    # navio com mais de um rótulo: o menor nome (os demais costumam trazer sufixos)
    return {imo: sorted(candidatos, key=lambda n: (len(n), n))[0] for imo, candidatos in nomes.items()}


# Consultas do filtro de uso. A subquery escalar com LIMIT 1 é verificada por
# manifesto e entra em itensresumo pelo CE — nunca pelo código do contêiner
# (há milhões de itens com contêiner vazio; ver CONTEXTO.md).
SQL_ESCALAS_COM_CONTEINER = """
SELECT DISTINCT e.embarcacao, e.portoDaEscala
FROM escalasresumo e
JOIN escalamanifestoresumo em ON em.escala = e.numeroDaEscala
WHERE e.dataPrevistaAtracacao >= :desde
  AND (SELECT 1 FROM conhecimentosresumo c
       JOIN itensresumo i ON i.numeroCEmercante = c.numeroCEmercante
       WHERE c.manifestoCE = em.manifesto AND i.codigoConteiner <> ''
       LIMIT 1) IS NOT NULL
"""
SQL_PORTOS_MANIFESTOS = """
SELECT DISTINCT portoCarregamento, portoDescarregamento
FROM manifestosresumo WHERE dataInicioOperacaoDate >= :desde
"""
SQL_PORTOS_CES = """
SELECT DISTINCT portoOrigemCarga, portoDestFinal
FROM conhecimentosresumo WHERE create_date >= :desde
"""


def codigos_usados(conexao, desde: str) -> dict:
    """Navios e portos efetivamente usados na base do Mercante desde `desde`
    ('YYYY-MM-DD'). `conexao` é uma conexão SQLAlchemy. Somente leitura."""
    from sqlalchemy import text

    navios, portos = set(), set()
    for embarcacao, porto in conexao.execute(text(SQL_ESCALAS_COM_CONTEINER), {'desde': desde}):
        navios.add((embarcacao or '').strip())
        portos.add((porto or '').strip().upper())
    for sql in (SQL_PORTOS_MANIFESTOS, SQL_PORTOS_CES):
        for linha in conexao.execute(text(sql), {'desde': desde}):
            portos.update((codigo or '').strip().upper() for codigo in linha)
    return {
        'desde': desde,
        'gerado_em': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'navios': sorted(navios - {''}),
        'portos': sorted(portos - {''}),
    }


def filtra_usados(tabela: dict, usados) -> dict:
    """Mantém só os códigos usados; `usados=None` (sem usados.json) não filtra."""
    if usados is None:
        return tabela
    usados = set(usados)
    return {codigo: nome for codigo, nome in tabela.items() if codigo in usados}


def le_existente(nome: str) -> Optional[dict]:
    """JSON já gerado nesta pasta (ou None se não existir / estiver ilegível)."""
    try:
        return le_json(os.path.join(AQUI, nome))
    except Exception:  # noqa
        return None


def grava(nome: str, dados: dict):
    caminho = os.path.join(AQUI, nome)
    with io.open(caminho, 'w', encoding='utf-8', newline='\n') as arquivo:
        json.dump(dados, arquivo, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    print(f'  {nome}: {os.path.getsize(caminho) / 1024:.0f} KB')


def _argumentos():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--brutos', help='diretório com os arquivos já baixados (não acessa a internet)')
    parser.add_argument('--guardar-brutos', help='diretório onde guardar os arquivos baixados')
    parser.add_argument('--banco', action='store_true',
                        help='consulta a base do Mercante (somente leitura) e atualiza o usados.json')
    parser.add_argument('--uri', help='URI SQLAlchemy do banco para --banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--desde', default=(datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'),
                        help="início do período considerado por --banco ('YYYY-MM-DD'; padrão: 12 meses atrás)")
    parser.add_argument('--sem-filtro-de-uso', action='store_true',
                        help='ignora o usados.json e gera as tabelas completas')
    return parser.parse_args()


def _atualiza_usados(args):
    """--banco: grava o usados.json com os navios/portos usados na base desde --desde."""
    import sys
    from sqlalchemy import create_engine
    uri = args.uri
    if not uri:
        sys.path.append('../ajna_docs/commons')
        from ajna_commons.flask.conf import SQL_URI as uri
    print(f'Consultando navios e portos usados desde {args.desde} (pode levar alguns minutos)...')
    with create_engine(uri).connect() as conexao:
        grava('usados.json', codigos_usados(conexao, args.desde))


def _baixa_fontes(args) -> dict:
    """Caminho do arquivo bruto de cada fonte; baixa se não vier --brutos.
    Uma fonte fora do ar não impede as demais."""
    diretorio = args.brutos or args.guardar_brutos or tempfile.mkdtemp(prefix='navii_ref_')
    os.makedirs(diretorio, exist_ok=True)
    caminhos = {}
    for chave, fonte in FONTES.items():
        caminhos[chave] = os.path.join(diretorio, fonte['arquivo_bruto'])
        if args.brutos:
            continue
        print(f"Baixando {fonte['descricao']}...")
        try:
            baixa(fonte['url'], caminhos[chave])
        except Exception as err:  # noqa: uma fonte fora do ar não impede as demais
            print(f'  [AVISO] download falhou ({err}); a tabela que depende dela será mantida como está.')
    return caminhos


class _GeradorTolerante:
    """Roda cada gerador; em erro ou resultado vazio, avisa e mantém o arquivo anterior."""

    def __init__(self):
        self.falhas = []

    def gera(self, nome: str, gerador):
        try:
            tabela = gerador()
            if not tabela:
                raise ValueError('resultado vazio')
            return tabela
        except Exception as err:  # noqa: segue com as demais tabelas
            self.falhas.append(nome)
            print(f'  [AVISO] {nome} não foi gerado ({type(err).__name__}: {err}); mantido o arquivo anterior.')
            return None


def _gera_tabelas(caminhos: dict, gerador: _GeradorTolerante) -> dict:
    bruto_ncm = gerador.gera('ncm.json (leitura)', lambda: le_json(caminhos['ncm.json']))
    return {
        'bruto_ncm': bruto_ncm,
        'ncm': gerador.gera('ncm.json', lambda: gera_ncm(bruto_ncm)) if bruto_ncm else None,
        'portos': gerador.gera('portos.json', lambda: gera_portos(
            csv.DictReader(io.StringIO(le_texto(caminhos['portos.json']))))),
        'paises': gerador.gera('paises.json', lambda: gera_paises(le_json(caminhos['paises.json']),
                                                                  le_json(caminhos['paises_wikidata']))),
        'navios': gerador.gera('navios.json', lambda: gera_navios(le_json(caminhos['navios.json']))),
    }


def _grava_filtrada(nome: str, todos: dict, usados: Optional[dict], quantidades: dict):
    """Grava portos/navios filtrados pelo uso real na base (usados.json)."""
    tabela = filtra_usados(todos, usados and usados[nome])
    grava(f'{nome}.json', tabela)
    quantidades.update({nome: len(tabela), f'{nome}_na_fonte': len(todos)})
    if not usados:
        return
    print(f"  filtro de uso (desde {usados['desde']}): {len(tabela)} de "
          f'{len(usados[nome])} {nome} usados têm nome.')
    if nome == 'portos':
        sem_nome = sorted(set(usados['portos']) - set(tabela))
        print(f'  Portos usados sem nome ({len(sem_nome)}), candidatos a portos_ajustes.json: '
              + ', '.join(sem_nome[:60]))


def _grava_fontes(quantidades: dict, vigencia, falhas: list, usados: Optional[dict]):
    grava('fontes.json', {
        'gerado_em': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'ncm_vigencia': vigencia,
        'quantidades': quantidades,
        'nao_atualizados_nesta_execucao': falhas,
        'filtro_de_uso': ({'desde': usados['desde'], 'gerado_em': usados['gerado_em'],
                           'portos_usados': len(usados['portos']), 'navios_usados': len(usados['navios'])}
                          if usados else None),
        'fontes': {chave: {'descricao': f['descricao'], 'url': f['url']} for chave, f in FONTES.items()},
    })


def main():
    args = _argumentos()
    if args.banco:
        _atualiza_usados(args)
    caminho_usados = os.path.join(AQUI, 'usados.json')
    usados = le_json(caminho_usados) if os.path.exists(caminho_usados) and not args.sem_filtro_de_uso else None
    caminhos = _baixa_fontes(args)

    print('Gerando JSONs em', AQUI)
    fontes_json = le_existente('fontes.json') or {}
    quantidades = dict(fontes_json.get('quantidades') or {})
    vigencia = fontes_json.get('ncm_vigencia')
    gerador = _GeradorTolerante()
    tabelas = _gera_tabelas(caminhos, gerador)
    if tabelas['ncm']:
        grava('ncm.json', tabelas['ncm'])
        quantidades.update(ncm_capitulos=len(tabelas['ncm']['capitulos']),
                           ncm_posicoes=len(tabelas['ncm']['posicoes']))
        bruto_ncm = tabelas['bruto_ncm']
        vigencia = f"{bruto_ncm.get('Data_Ultima_Atualizacao_NCM')} — {bruto_ncm.get('Ato')}"
    if tabelas['paises']:
        grava('paises.json', tabelas['paises'])
        quantidades['paises'] = len(tabelas['paises'])
    for nome in ('portos', 'navios'):
        if tabelas[nome]:
            _grava_filtrada(nome, tabelas[nome], usados, quantidades)

    _grava_fontes(quantidades, vigencia, gerador.falhas, usados)
    if gerador.falhas:
        print(f'Concluído com avisos: não atualizados {", ".join(gerador.falhas)} (mantidas as versões anteriores).')
    else:
        print('Concluído.')


if __name__ == '__main__':  # pragma: no cover
    main()
