# coding: utf-8
"""
DIAGNÓSTICO das IMAGENS que faltam nos escaneamentos do Ciclo de Vida v2 (acompanhamento de 02/10/2026: BTP
76% e Embraport 84% com imagem; os demais recintos ~100%). Somente leitura (SQL e Mongo).

  1. Por DIA e recinto: escaneamentos e quantos sem imagem — falta concentrada em dias = escâner/envio parado;
  2. Atributos da INSPEÇÃO (API Recintos) com x sem imagem: número sem OCR, contingência, retificação,
     contêiner vazio;
  3. AMOSTRA no Mongo (só `metadata.numeroinformado` e `metadata.dataescaneamento`, regra do usuário), cada
     escaneamento sem imagem classificado:
       - existe na defasagem esperada (±15 min)   -> a imagem chegou DEPOIS ao Mongo e o ciclo não foi refeito;
       - existe só com outra defasagem             -> a tabela `imagens_ciclo.DEFASAGEM_HORAS` não vale sempre;
       - imagem do contêiner só a mais de 48 h     -> outra passagem pelo escâner;
       - número parecido no horário (até 2 dif.)   -> número lido/digitado diferente (OCR);
       - outras imagens no horário, nenhuma parecida -> a imagem deste contêiner não veio;
       - nenhuma imagem no horário                 -> escâner/envio parado;
     o atraso de transmissão da inspeção (dataHoraTransmissao - dataHoraOcorrencia) entra na tabela;
  4. EXEMPLOS de cada categoria, para conferir à mão.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    DIAG=virasana/analytic_scripts/ciclo_de_vida/diagnostico_imagens_ciclo.py
    virasana-venv/bin/python $DIAG --saida imagens.log
    virasana-venv/bin/python $DIAG --recintos 8931359 8931404 8931356 --dias 14 --amostra 200 --saida imagens.log
"""
import argparse
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import List, Optional, Sequence, Tuple

from sqlalchemy import create_engine

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ciclo_de_vida import Diagnostico, _dt, _horas  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.imagens_ciclo import (  # noqa: E402
    JANELA_MINUTOS, defasagens_do, imagens_do_conteiner,
)
from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import uri_do_banco  # noqa: E402

RECINTOS_PADRAO = ('8931359', '8931404', '8931356')   # BTP, Embraport e Santos Brasil (controle, ~99%)
JANELA_CONTEINER_HORAS = 48     # imagem do mesmo contêiner perto da inspeção
JANELA_LONGE_DIAS = 30          # ... ou longe
DIFERENCAS_PARECIDO = 2         # número com até 2 diferenças = parecido (OCR/digitação)
LIMITE_IMAGENS_NO_HORARIO = 500
EXEMPLOS = 5

EXISTE_ESPERADA = '1) existe na defasagem esperada (chegou depois?)'
OUTRA_DEFASAGEM = '2) existe só com outra defasagem'
SO_LONGE = '3) imagem do contêiner só a mais de 48 h'
PARECIDO = '4) número parecido no horário (OCR?)'
SEM_ESTE = '5) outras imagens no horário, nenhuma parecida'
NENHUMA = '6) nenhuma imagem no horário'

ESCANEAMENTOS_V2 = ('FROM ciclodevida_ciclo_de_vida_importacao_escaneamentos e '
                    'JOIN ciclodevida_ciclo_de_vida_importacao c ON c.ID = e.ciclo_de_vida_id '
                    'LEFT JOIN apirecintos_inspecoesnaoinvasivas i ON i.id = e.escaneamento_id '
                    "WHERE c.versao_heuristica = 'v2' AND e.data_escaneamento >= :inicio "
                    'AND e.data_escaneamento < :fim AND e.codigoRecinto IN :recintos')


def distancia(a: str, b: str) -> int:
    """Distância de edição (Levenshtein) entre dois números de contêiner."""
    anterior = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        atual = [i]
        for j, cb in enumerate(b, 1):
            atual.append(min(anterior[j] + 1, atual[j - 1] + 1, anterior[j - 1] + (ca != cb)))
        anterior = atual
    return anterior[-1]


def _residuo(data_imagem: datetime, alvos: Sequence[datetime]) -> timedelta:
    return min(abs(data_imagem - alvo) for alvo in alvos)


def classifica(db, conteiner: str, data: datetime, recinto: Optional[str]) -> Tuple[str, str]:
    """(categoria, detalhe) de um escaneamento sem imagem — ver docstring do módulo."""
    alvos = [data + timedelta(hours=h) for h in defasagens_do(recinto)]
    janela = timedelta(minutes=JANELA_MINUTOS)
    perto = imagens_do_conteiner(db, conteiner, data - timedelta(hours=JANELA_CONTEINER_HORAS),
                                 data + timedelta(hours=JANELA_CONTEINER_HORAS))
    if perto:
        data_imagem, imagem_id = min(perto, key=lambda im: _residuo(im[0], alvos))
        if _residuo(data_imagem, alvos) <= janela:
            return EXISTE_ESPERADA, f'imagem {imagem_id} em {data_imagem:%d/%m %H:%M}'
        horas = _horas(data_imagem, data)
        return OUTRA_DEFASAGEM, f'{horas:+.1f} h (imagem {imagem_id} em {data_imagem:%d/%m %H:%M})'
    longe = db['fs.files'].count_documents({'metadata.numeroinformado': conteiner, 'metadata.dataescaneamento': {
        '$gte': data - timedelta(days=JANELA_LONGE_DIAS), '$lte': data + timedelta(days=JANELA_LONGE_DIAS)}})
    if longe:
        return SO_LONGE, f'{longe} imagem(ns) em ±{JANELA_LONGE_DIAS} dias'
    numeros = set()
    for alvo in alvos:
        for documento in db['fs.files'].find(
                {'metadata.dataescaneamento': {'$gte': alvo - janela, '$lte': alvo + janela}},
                {'metadata.numeroinformado': 1}).limit(LIMITE_IMAGENS_NO_HORARIO):
            numeros.add(((documento.get('metadata') or {}).get('numeroinformado') or '').strip().upper())
    numeros.discard('')
    parecidos = sorted(n for n in numeros if distancia(n, conteiner) <= DIFERENCAS_PARECIDO)
    if parecidos:
        return PARECIDO, ', '.join(parecidos[:5])
    if numeros:
        return SEM_ESTE, f'{len(numeros)} outro(s) contêiner(es) no horário'
    return NENHUMA, ''


class DiagnosticoImagens(Diagnostico):
    """Reaproveita a saída e as consultas do diagnóstico; período = escaneamentos de `inicio` a `fim`."""

    def __init__(self, conexao, saida, inicio: datetime, fim: datetime, recintos: Sequence[str], amostra: int,
                 mongo_db=None, agora: Optional[datetime] = None):
        super().__init__(conexao, saida, inicio, fim, agora=agora)
        self.recintos, self.amostra, self.mongo_db = list(recintos), amostra, mongo_db

    def _consulta(self, sql, params=None):
        return self.consulta(sql, {**(params or {}), 'recintos': self.recintos}, listas=('recintos',))

    def secao_por_dia(self):
        self.titulo('1. ESCANEAMENTOS v2 POR DIA E RECINTO — com e sem imagem')
        linhas = self._consulta(
            'SELECT DATE(e.data_escaneamento) AS dia, e.codigoRecinto, COUNT(*), '  # nosec B608
            'SUM(CASE WHEN e.imagem_id IS NULL THEN 1 ELSE 0 END) ' + ESCANEAMENTOS_V2 +
            ' GROUP BY dia, e.codigoRecinto ORDER BY dia, e.codigoRecinto')
        por_dia = defaultdict(dict)
        for dia, recinto, total, sem in linhas:
            por_dia[str(dia)[:10]][recinto] = (int(total or 0), int(sem or 0))
        colunas = ['dia'] + [f'{r} (sem / total)' for r in self.recintos]
        self.tabela(colunas, [[dia] + [self._celula(valores.get(r)) for r in self.recintos]
                              for dia, valores in sorted(por_dia.items())])

    @staticmethod
    def _celula(valores) -> str:
        if not valores:
            return '-'
        total, sem = valores
        return f'{sem} / {total} ({100 * sem / total:.0f}%)' if total else '-'

    def secao_atributos(self):
        self.titulo('2. ATRIBUTOS DA INSPEÇÃO (API Recintos) — com x sem imagem')
        self.tabela(['recinto', 'imagem', 'escaneamentos', 'número sem OCR', 'contingência', 'retificação', 'vazio',
                     'inspeção não encontrada'], self._consulta(
            "SELECT e.codigoRecinto, CASE WHEN e.imagem_id IS NULL THEN 'sem' ELSE 'com' END AS imagem, "  # nosec
            'COUNT(*), SUM(CASE WHEN i.ocrNumero = 0 THEN 1 ELSE 0 END), '
            'SUM(CASE WHEN i.contingencia = 1 THEN 1 ELSE 0 END), '
            "SUM(CASE WHEN i.tipoOperacao = 'R' THEN 1 ELSE 0 END), SUM(CASE WHEN i.vazio = 1 THEN 1 ELSE 0 END), "
            'SUM(CASE WHEN i.id IS NULL THEN 1 ELSE 0 END) ' + ESCANEAMENTOS_V2 +
            ' GROUP BY e.codigoRecinto, imagem ORDER BY e.codigoRecinto, imagem'))

    def amostra_sem_imagem(self) -> List[tuple]:
        """[(escaneamento, data, recinto, contêiner, situação do ciclo, ocr, contingência, transmissão)]."""
        linhas = []
        for recinto in self.recintos:
            linhas += self.consulta(
                'SELECT e.escaneamento_id, e.data_escaneamento, e.codigoRecinto, c.numeroConteiner, c.situacao, '
                'i.ocrNumero, i.contingencia, i.dataHoraTransmissao ' + ESCANEAMENTOS_V2.replace(  # nosec B608
                    'IN :recintos', '= :recinto') +
                ' AND e.imagem_id IS NULL ORDER BY e.data_escaneamento DESC LIMIT :amostra',
                {'recinto': recinto, 'amostra': self.amostra})
        return linhas

    def secao_mongo(self):
        self.titulo(f'3. AMOSTRA NO MONGO — até {self.amostra} escaneamentos sem imagem por recinto')
        if self.mongo_db is None:
            self.escreve('(desligado: --sem-mongo)')
            return {}
        contagem, exemplos = Counter(), defaultdict(list)
        atraso = defaultdict(list)
        for (escaneamento, data, recinto, conteiner, situacao, ocr, contingencia,
             transmissao) in self.amostra_sem_imagem():
            data = _dt(data)
            categoria, detalhe = classifica(self.mongo_db, (conteiner or '').strip(), data, recinto)
            contagem[(recinto, categoria)] += 1
            horas = _horas(transmissao, data)
            if horas is not None:
                atraso[(recinto, categoria)].append(horas)
            if len(exemplos[(recinto, categoria)]) < EXEMPLOS:
                exemplos[(recinto, categoria)].append(
                    f'{conteiner} inspeção {escaneamento} em {data:%d/%m %H:%M} (ciclo {situacao}; '
                    f'OCR {"sim" if ocr else "não" if ocr is not None else "?"}'
                    f'{"; contingência" if contingencia else ""}) — {detalhe}')
        self.escreve('Defasagem esperada = imagens_ciclo.DEFASAGEM_HORAS do recinto; "atraso" = transmissão da '
                     'inspeção à API - ocorrência (mediana, em horas).')
        self.tabela(['recinto', 'categoria', 'escaneamentos', 'atraso de transmissão (mediana)'], [
            (recinto, categoria, n, _mediana(atraso[(recinto, categoria)]))
            for (recinto, categoria), n in sorted(contagem.items())])
        return exemplos

    def secao_exemplos(self, exemplos):
        self.titulo(f'4. EXEMPLOS (até {EXEMPLOS} por recinto e categoria)')
        for (recinto, categoria), linhas in sorted(exemplos.items()):
            self.escreve()
            self.escreve(f'--- {recinto} — {categoria}')
            for linha in linhas:
                self.escreve(f'   {linha}')


def _mediana(valores: List[float]) -> str:
    if not valores:
        return '-'
    ordenados = sorted(valores)
    return f'{ordenados[len(ordenados) // 2]:.1f} h'


def roda(diag: DiagnosticoImagens):
    diag.secao_por_dia()
    diag.secao_atributos()
    diag.secao_exemplos(diag.secao_mongo())
    diag.resumo(5)


def main():  # pragma: no cover - depende do banco real
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    parser.add_argument('--recintos', nargs='+', default=list(RECINTOS_PADRAO), help='códigos dos recintos')
    parser.add_argument('--dias', type=int, default=14, help='escaneamentos dos últimos N dias (padrão 14)')
    parser.add_argument('--ate-dias', type=int, default=3,
                        help='... até N dias atrás: a imagem já teve tempo de chegar (padrão 3)')
    parser.add_argument('--amostra', type=int, default=200, help='escaneamentos sem imagem por recinto no Mongo')
    parser.add_argument('--sem-mongo', action='store_true', help='só as seções do banco SQL')
    parser.add_argument('--saida', help='grava o relatório neste arquivo (padrão: só na tela)')
    args = parser.parse_args()
    mongo_db = None
    if not args.sem_mongo:
        from pymongo import MongoClient
        from ajna_commons.flask.conf import DATABASE, MONGODB_URI
        mongo_db = MongoClient(host=MONGODB_URI)[DATABASE]
    engine = create_engine(uri_do_banco(args))
    agora = datetime.now()
    inicio, fim = agora - timedelta(days=args.dias), agora - timedelta(days=args.ate_dias)
    saida = open(args.saida, 'w', encoding='utf-8') if args.saida else sys.stdout
    try:
        with engine.connect() as conexao:
            diag = DiagnosticoImagens(conexao, saida, inicio, fim, args.recintos, args.amostra, mongo_db, agora)
            diag.escreve(f'Diagnóstico das imagens do Ciclo de Vida v2 — {agora:%Y-%m-%d %H:%M}; escaneamentos de '
                         f'{inicio:%Y-%m-%d} a {fim:%Y-%m-%d}; recintos {", ".join(args.recintos)}')
            roda(diag)
    finally:
        if args.saida:
            saida.close()
    if args.saida:
        print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
