# coding: utf-8
"""
PORTO de cada operador e o CE declarado no desembarque — diagnóstico (21/10/2026), só leitura.

Motivação: caso CAAU5545018 (CICLO_DE_VIDA.md). O desembarque em Santos ficou com um CE de cabotagem que SAI de
Santos (origem e carregamento BRSSZ), embora o próprio desembarque declarasse o CE de importação — cujo manifesto
não tinha escala ligada no Mercante. O ciclo de vida não sabe o porto do recinto (assume Santos nas escalas) e só
usa o CE declarado para desempatar.

Perguntas:
  1. Dá para saber o porto do recinto com o que já existe? Fontes comparadas por recinto: `ovr_recintos.cod_carga`
     (o código do terminal no Mercante, ex.: BRSSZ016 — os 5 primeiros caracteres são o porto), o porto da escala
     declarada nos embarques/desembarques (escalasresumo), o porto de descarregamento (desembarque) ou de
     carregamento (embarque) do manifesto do CE declarado e o terminal desse manifesto; mais a unidade. Sai uma
     proposta de porto por recinto e as divergências.
  2. Quanto vale o CE declarado no desembarque? Existe no Mercante, tem o contêiner nos itens, é de SAÍDA (origem
     = porto do recinto), é de exportação ou cabotagem, o manifesto é da escala declarada; manifesto sem escala no
     Mercante (e se o navio e o início da operação conferem com a escala declarada); e o que o ciclo de vida gravou:
     o principal é o declarado?
  3. Tamanho do defeito do caso: ciclos gravados cujo CE principal é de SAÍDA do porto do desembarque, com o CE
     declarado no desembarque ao lado (--csv com todos).
Consultas pelo período dos eventos (índice de dataHoraOcorrencia) e, no Mercante, só por chave (CE, manifesto,
escala, id do desembarque) em lotes — nada de varrer as tabelas do Mercante.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    D=virasana/analytic_scripts/ciclo_de_vida/diagnostico_porto_desembarque.py
    virasana-venv/bin/python $D --dias 7 --saida porto_desembarque.log --csv porto_desembarque.csv
"""
import argparse
import csv
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida.carga_ciclo import ConsultaSQL  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.caso_ciclo import Saida  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.ces_do_ciclo import normaliza_ce  # noqa: E402

DIAS_PADRAO = 7
MINIMO_EVENTOS = 20        # eventos com a fonte para confiar na moda do porto
CONCORDANCIA = 0.95        # fração da moda para propor o porto
JANELA_INICIO_DIAS = 3     # início da operação do manifesto perto do desembarque
AMOSTRA = 30
PORTO = re.compile(r'[A-Z]{2}[A-Z0-9]{3}')
TRAFEGO_EXPORTACAO, TRAFEGO_CABOTAGEM = '7', '3'

SQL_RECINTOS = ('SELECT cod_siscomex, nome, e_operador, cod_unidade, cod_carga FROM ovr_recintos '
                "WHERE cod_siscomex IS NOT NULL AND cod_siscomex <> ''")
SQL_EVENTOS = ('SELECT id, numeroConteiner, dataHoraOcorrencia, codigoRecinto, embarqueDesembarque, '
               'numeroConhecimento, escala FROM apirecintos_embarquedesembarque '
               'WHERE dataHoraOcorrencia >= :desde AND dataHoraOcorrencia < :ate')
SQL_ESCALAS = ('SELECT numeroDaEscala, portoDaEscala, embarcacao, dataPrevistaAtracacao FROM escalasresumo '
               'WHERE numeroDaEscala IN :escalas')
SQL_CICLOS = ('SELECT desembarque_id, numeroCEmercante, confianca_ce FROM ciclodevida_ciclo_de_vida_importacao '
              'WHERE desembarque_id IN :ids')
SQL_CES = ('SELECT numeroCEmercante, tipoTrafego, portoOrigemCarga, portoDestFinal, manifestoCE '
           'FROM conhecimentosresumo WHERE numeroCEmercante IN :ces')
SQL_ITENS = 'SELECT numeroCEmercante, codigoConteiner FROM itensresumo WHERE numeroCEmercante IN :ces'
SQL_MANIFESTOS = ('SELECT numero, portoCarregamento, portoDescarregamento, codigoTerminalCarregamento, '
                  'codigoTerminalDescarregamento, numeroImoDPC, dataInicioOperacao FROM manifestosresumo '
                  'WHERE numero IN :manifestos')
SQL_LIGACOES = 'SELECT manifesto, escala FROM escalamanifestoresumo WHERE manifesto IN :manifestos'

COLUNAS_CSV = ('desembarque_id', 'data', 'recinto', 'porto', 'conteiner', 'principal', 'confianca',
               'principal_trafego', 'principal_origem', 'principal_destino', 'declarado', 'declarado_trafego',
               'declarado_origem', 'declarado_destino', 'declarado_tem_o_conteiner', 'manifesto_declarado_sem_escala',
               'escala_declarada')


def _t(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def porto_do_codigo(codigo) -> Optional[str]:
    """Porto (UN/LOCODE) no começo de um código de terminal do Mercante (BRSSZ016 -> BRSSZ)."""
    texto = _t(codigo).upper()[:5]
    return texto if PORTO.fullmatch(texto) else None


def _data(valor) -> Optional[datetime]:
    if isinstance(valor, datetime) or valor is None:
        return valor
    try:
        return datetime.strptime(_t(valor)[:10], '%Y-%m-%d')
    except ValueError:
        return None


@dataclass
class Evento:
    id: int
    conteiner: str
    data: datetime
    recinto: str
    tipo: str            # 'D' desembarque, 'E' embarque (como o recinto informou)
    ce: Optional[str]
    escala: str


class Dados:
    """Os embarques/desembarques do período e o Mercante e o ciclo de vida ligados a eles (lotes por chave)."""

    def __init__(self, diag, desde: datetime, ate: datetime):
        self.diag, self.desde, self.ate = diag, desde, ate

    def carrega(self) -> 'Dados':
        self.recintos = {_t(c): {'nome': n, 'operador': bool(o), 'unidade': _t(u), 'cod_carga': _t(cc)}
                         for c, n, o, u, cc in self.diag.consulta(SQL_RECINTOS)}
        self.eventos = [Evento(id_, _t(cont), _data(data), _t(rec), _t(tipo).upper(), normaliza_ce(ce), _t(esc))
                        for id_, cont, data, rec, tipo, ce, esc in self.diag.consulta(
                            SQL_EVENTOS, {'desde': self.desde, 'ate': self.ate})]
        self.escalas = {_t(n): {'porto': _t(p), 'navio': _t(e), 'prevista': _t(d)} for n, p, e, d in
                        self.diag._por_lotes(SQL_ESCALAS, 'escalas', {e.escala for e in self.eventos})}
        self.ciclos = {id_: (normaliza_ce(ce), _t(conf)) for id_, ce, conf in self.diag._por_lotes(
            SQL_CICLOS, 'ids', [e.id for e in self.eventos if e.tipo == 'D'])}
        ces = {e.ce for e in self.eventos if e.ce} | {ce for ce, _ in self.ciclos.values() if ce}
        self.ces = {normaliza_ce(ce): {'trafego': _t(t), 'origem': _t(o).upper(), 'destino': _t(d).upper(),
                                       'manifesto': _t(m)}
                    for ce, t, o, d, m in self.diag._por_lotes(SQL_CES, 'ces', ces)}
        self.itens = {(normaliza_ce(ce), _t(cont)) for ce, cont in self.diag._por_lotes(SQL_ITENS, 'ces', self.ces)}
        manifestos = {info['manifesto'] for info in self.ces.values()} - {''}
        self.manifestos = {_t(n): {'carregamento': _t(pc).upper(), 'descarregamento': _t(pd).upper(),
                                   'terminal_carregamento': _t(tc), 'terminal_descarregamento': _t(td),
                                   'navio': _t(imo), 'inicio': _data(ini)}
                           for n, pc, pd, tc, td, imo, ini in self.diag._por_lotes(
                               SQL_MANIFESTOS, 'manifestos', manifestos)}
        self.ligacoes: Dict[str, Set[str]] = defaultdict(set)
        for manifesto, escala in self.diag._por_lotes(SQL_LIGACOES, 'manifestos', manifestos):
            self.ligacoes[_t(manifesto)].add(_t(escala))
        return self

    def manifesto_do(self, ce: Optional[str]) -> Optional[dict]:
        info = self.ces.get(ce) if ce else None
        return self.manifestos.get(info['manifesto']) if info else None


# ---------------------------------------------------------------------- 1. porto por recinto

def evidencias_de_porto(dados: Dados) -> Dict[str, dict]:
    """Por recinto: os portos das escalas declaradas e dos manifestos dos CEs declarados, e os terminais."""
    evidencias = defaultdict(lambda: {'escala': Counter(), 'manifesto': Counter(), 'terminal': Counter(),
                                      'D': 0, 'E': 0})
    for evento in dados.eventos:
        evid = evidencias[evento.recinto]
        evid[evento.tipo] = evid.get(evento.tipo, 0) + 1
        escala = dados.escalas.get(evento.escala)
        if escala and escala['porto']:
            evid['escala'][escala['porto']] += 1
        manifesto = dados.manifesto_do(evento.ce)
        if manifesto is None:
            continue
        lado = 'descarregamento' if evento.tipo == 'D' else 'carregamento'
        if manifesto[lado]:
            evid['manifesto'][manifesto[lado]] += 1
        if manifesto[f'terminal_{lado}']:
            evid['terminal'][manifesto[f'terminal_{lado}']] += 1
    return evidencias


def moda(contagem: Counter) -> Tuple[Optional[str], float, int]:
    total = sum(contagem.values())
    if not total:
        return None, 0.0, 0
    valor, quantidade = contagem.most_common(1)[0]
    return valor, quantidade / total, total


def proposta(recinto: dict, evid: dict, minimo: int = MINIMO_EVENTOS) -> Tuple[Optional[str], str]:
    """(porto, de onde veio): a moda das escalas ou dos manifestos (com eventos e concordância suficientes),
    conferida com o porto do `cod_carga`; sem eventos suficientes, o do `cod_carga`."""
    pelo_cadastro = porto_do_codigo(recinto.get('cod_carga'))
    for fonte in ('escala', 'manifesto'):
        porto, fracao, total = moda(evid[fonte])
        if porto and total >= minimo and fracao >= CONCORDANCIA:
            if pelo_cadastro and pelo_cadastro != porto:
                return pelo_cadastro, f'cod_carga, DIVERGE da {fonte} ({porto}, {fracao:.0%} de {total})'
            return porto, f'{fonte} ({fracao:.0%} de {total})' + (', = cod_carga' if pelo_cadastro else '')
    if pelo_cadastro:
        return pelo_cadastro, 'cod_carga (sem eventos suficientes para conferir)'
    return None, 'indefinido'


def _texto_moda(contagem: Counter) -> str:
    valor, fracao, total = moda(contagem)
    return f'{valor} {fracao:.0%} de {total}' if valor else '-'


def secao_portos(dados: Dados, saida: Saida, minimo: int) -> Dict[str, Optional[str]]:
    saida.titulo('1. Porto de cada recinto (com embarques/desembarques no período ou operador)')
    evidencias = evidencias_de_porto(dados)
    portos = {}
    for codigo in sorted(set(evidencias) | {c for c, r in dados.recintos.items() if r['operador']}):
        recinto = dados.recintos.get(codigo, {'nome': '(fora do ovr_recintos)', 'operador': False, 'unidade': '',
                                              'cod_carga': ''})
        evid = evidencias[codigo]
        portos[codigo], fonte = proposta(recinto, evid, minimo)
        saida(f'{codigo} {recinto["nome"]} (operador: {"sim" if recinto["operador"] else "não"}; unidade '
              f'{recinto["unidade"] or "-"}; cod_carga {recinto["cod_carga"] or "-"}): D {evid.get("D", 0)} / E '
              f'{evid.get("E", 0)}')
        saida(f'    escala declarada: {_texto_moda(evid["escala"])}; manifesto do CE: '
              f'{_texto_moda(evid["manifesto"])}; terminal do manifesto: {_texto_moda(evid["terminal"])}')
        saida(f'    PORTO PROPOSTO: {portos[codigo] or "-"} — {fonte}')
    return portos


# ---------------------------------------------------------------------- 2. CE declarado no desembarque

def _marcas_do_declarado(evento: Evento, dados: Dados, porto: Optional[str]) -> List[str]:
    info = dados.ces.get(evento.ce)
    if info is None:
        return []
    marcas = ['CE no Mercante']
    if (evento.ce, evento.conteiner) in dados.itens:
        marcas.append('contêiner nos itens do CE')
    if porto and info['origem'] == porto:
        marcas.append('CE de SAÍDA do porto (origem = porto do recinto)')
    if info['trafego'] in (TRAFEGO_EXPORTACAO, TRAFEGO_CABOTAGEM):
        marcas.append('CE de exportação' if info['trafego'] == TRAFEGO_EXPORTACAO else 'CE de cabotagem')
    return marcas + _marcas_do_manifesto(evento, dados)


def _marcas_do_manifesto(evento: Evento, dados: Dados) -> List[str]:
    manifesto = dados.manifesto_do(evento.ce)
    if manifesto is None:
        return ['manifesto fora do Mercante']
    escalas = dados.ligacoes.get(dados.ces[evento.ce]['manifesto'], set())
    if evento.escala and evento.escala in escalas:
        return ['manifesto da escala declarada']
    if escalas:
        return ['manifesto com escala, mas não a declarada']
    marcas = ['manifesto SEM escala no Mercante']
    escala = dados.escalas.get(evento.escala)
    if escala and manifesto['navio'] and manifesto['navio'] == escala['navio']:
        marcas.append('sem escala: navio do manifesto = navio da escala declarada')
    if manifesto['inicio'] and abs((manifesto['inicio'].date() - evento.data.date()).days) <= JANELA_INICIO_DIAS:
        marcas.append(f'sem escala: início da operação do manifesto a até {JANELA_INICIO_DIAS} dias')
    return marcas


def _de_saida(dados: Dados, ce: Optional[str], porto: Optional[str]) -> bool:
    info = dados.ces.get(ce) if ce else None
    return bool(info and porto and info['origem'] == porto)


def _marcas_do_ciclo(evento: Evento, dados: Dados, porto: Optional[str]) -> List[str]:
    if evento.id not in dados.ciclos:
        return []
    principal, _ = dados.ciclos[evento.id]
    marcas = ['ciclo gravado']
    if evento.ce:
        marcas.append('principal = declarado' if principal == evento.ce else 'principal ≠ declarado')
    if _de_saida(dados, principal, porto):
        marcas.append('principal de SAÍDA do porto')
        if evento.ce and evento.ce != principal and not _de_saida(dados, evento.ce, porto):
            marcas.append('principal de SAÍDA e o declarado não (o caso CAAU)')
    return marcas


def marcas_do_desembarque(evento: Evento, dados: Dados, porto: Optional[str]) -> List[str]:
    marcas = ['desembarques', 'com CE declarado' if evento.ce else 'sem CE declarado']
    if evento.ce:
        marcas += _marcas_do_declarado(evento, dados, porto)
    return marcas + _marcas_do_ciclo(evento, dados, porto)


def marcas_do_embarque(evento: Evento, dados: Dados, porto: Optional[str]) -> List[str]:
    marcas = ['embarques', 'com CE declarado' if evento.ce else 'sem CE declarado']
    if evento.ce and evento.ce in dados.ces:
        marcas.append('CE no Mercante')
        if _de_saida(dados, evento.ce, porto):
            marcas.append('CE de SAÍDA do porto (origem = porto do recinto)')
    return marcas


def linha_do_defeito(evento: Evento, dados: Dados, porto: Optional[str]) -> dict:
    principal, confianca = dados.ciclos[evento.id]
    p, d = dados.ces.get(principal, {}), dados.ces.get(evento.ce, {}) if evento.ce else {}
    manifesto = d.get('manifesto')
    return {'desembarque_id': evento.id, 'data': f'{evento.data:%Y-%m-%d %H:%M}', 'recinto': evento.recinto,
            'porto': porto, 'conteiner': evento.conteiner, 'principal': principal, 'confianca': confianca,
            'principal_trafego': p.get('trafego'), 'principal_origem': p.get('origem'),
            'principal_destino': p.get('destino'), 'declarado': evento.ce, 'declarado_trafego': d.get('trafego'),
            'declarado_origem': d.get('origem'), 'declarado_destino': d.get('destino'),
            'declarado_tem_o_conteiner': (evento.ce, evento.conteiner) in dados.itens if evento.ce else None,
            'manifesto_declarado_sem_escala': (not dados.ligacoes.get(manifesto)) if manifesto else None,
            'escala_declarada': evento.escala}


def secao_declarados(dados: Dados, saida: Saida, portos: Dict[str, Optional[str]]) -> Tuple[dict, List[dict]]:
    """Contagens por recinto operador (e no total) e as linhas do defeito (principal de saída)."""
    contagens: Dict[str, Counter] = defaultdict(Counter)
    defeitos = []
    for evento in dados.eventos:
        if evento.tipo not in ('D', 'E') or not dados.recintos.get(evento.recinto, {}).get('operador'):
            continue
        porto = portos.get(evento.recinto)
        marcas = (marcas_do_desembarque if evento.tipo == 'D' else marcas_do_embarque)(evento, dados, porto)
        for chave in (evento.recinto, 'TOTAL'):
            contagens[chave].update(f'{evento.tipo} {m}' for m in marcas)
        if 'principal de SAÍDA do porto' in marcas:
            defeitos.append(linha_do_defeito(evento, dados, porto))
    saida.titulo('2. CE declarado nos embarques/desembarques dos operadores (D = desembarque, E = embarque)')
    for chave in sorted(contagens, key=lambda c: (c != 'TOTAL', c)):
        nome = 'TOTAL' if chave == 'TOTAL' else f'{chave} {dados.recintos[chave]["nome"]} (porto {portos.get(chave)})'
        saida(nome)
        base = {tipo: contagens[chave][f'{tipo} {"desembarques" if tipo == "D" else "embarques"}'] for tipo in 'DE'}
        for marca, quantidade in sorted(contagens[chave].items()):
            total = base[marca[0]]
            saida(f'    {marca}: {quantidade}' + (f' ({100 * quantidade / total:.1f}%)' if total else ''))
    return contagens, defeitos


def secao_defeitos(defeitos: List[dict], saida: Saida, caminho_csv: Optional[str]):
    saida.titulo(f'3. Ciclos gravados com o CE principal de SAÍDA do porto do desembarque: {len(defeitos)}')
    por_confianca = Counter((d['confianca'], d['principal_trafego']) for d in defeitos)
    for (confianca, trafego), quantidade in sorted(por_confianca.items()):
        saida(f'    confiança {confianca}, tráfego do principal {trafego}: {quantidade}')
    for d in defeitos[:AMOSTRA]:
        saida(f'  {d["data"]} {d["conteiner"]} no {d["recinto"]}: principal {d["principal"]} ({d["confianca"]}; '
              f'{d["principal_trafego"]} {d["principal_origem"]}->{d["principal_destino"]}) x declarado '
              f'{d["declarado"]} ({d["declarado_trafego"]} {d["declarado_origem"]}->{d["declarado_destino"]}; '
              f'contêiner nos itens {d["declarado_tem_o_conteiner"]}; manifesto sem escala '
              f'{d["manifesto_declarado_sem_escala"]})')
    if caminho_csv:
        with open(caminho_csv, 'w', encoding='utf-8', newline='') as arquivo:
            escritor = csv.DictWriter(arquivo, fieldnames=COLUNAS_CSV, delimiter=';')
            escritor.writeheader()
            escritor.writerows(defeitos)
        saida(f'  todos em {caminho_csv}')


def diagnostica(diag, saida: Saida, desde: datetime, ate: datetime, minimo: int = MINIMO_EVENTOS,
                caminho_csv: Optional[str] = None) -> dict:
    """As três seções; devolve os portos propostos, as contagens e os defeitos (para os testes)."""
    dados = Dados(diag, desde, ate).carrega()
    saida(f'Embarques/desembarques de {desde:%Y-%m-%d %H:%M} a {ate:%Y-%m-%d %H:%M}: {len(dados.eventos)}; CEs '
          f'declarados no Mercante: {len(dados.ces)}; manifestos: {len(dados.manifestos)}; ciclos gravados: '
          f'{len(dados.ciclos)}')
    portos = secao_portos(dados, saida, minimo)
    contagens, defeitos = secao_declarados(dados, saida, portos)
    secao_defeitos(defeitos, saida, caminho_csv)
    return {'portos': portos, 'contagens': contagens, 'defeitos': defeitos}


def _argumentos(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dias', type=int, default=DIAS_PADRAO, help='dias de eventos até --ate (padrão: 7)')
    parser.add_argument('--ate', help='fim do período (AAAA-MM-DD, exclusivo; padrão: agora)')
    parser.add_argument('--minimo', type=int, default=MINIMO_EVENTOS, help='eventos para propor o porto pela moda')
    parser.add_argument('--saida', help='grava o relatório neste arquivo (padrão: só na tela)')
    parser.add_argument('--csv', help='grava os ciclos com o principal de saída neste CSV (;)')
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    return parser.parse_args(argv)


def main(argv=None):  # pragma: no cover - depende do banco real
    from sqlalchemy import create_engine
    from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import uri_do_banco
    from virasana.navii.mercante_por_id import limita_tempo
    args = _argumentos(argv)
    ate = datetime.strptime(args.ate, '%Y-%m-%d') if args.ate else datetime.now()
    engine = create_engine(uri_do_banco(args))
    limita_tempo(engine, 300)
    arquivo = open(args.saida, 'w', encoding='utf-8') if args.saida else None
    try:
        with engine.connect() as conexao:
            diagnostica(ConsultaSQL(conexao), Saida(arquivo), ate - timedelta(days=args.dias), ate, args.minimo,
                        args.csv)
    finally:
        if arquivo:
            arquivo.close()


if __name__ == '__main__':  # pragma: no cover
    main()
