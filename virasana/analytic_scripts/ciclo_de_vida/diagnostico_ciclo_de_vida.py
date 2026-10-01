# coding: utf-8
"""
Diagnóstico do Ciclo de Vida da importação: estatísticas e amostras das tabelas
envolvidas, para planejar as mudanças do algoritmo (ver CICLO_DE_VIDA.md).

Somente leitura. Não traz placa, CPF nem nome de motorista. O SQL é montado só com
constantes deste módulo (nomes de tabela e condições fixas) e os valores vão sempre
como parâmetro — por isso as linhas `# nosec B608`. Seções:

  1. estrutura e volume das tabelas (MariaDB);
  2. estado dos ciclos: pendentes por idade e por estágio, duplicidades, `saida_id` sem `data_saida`;
  3. ciclos por mês de desembarque (período): CE, saída, seco, escaneamento, imagem;
  4. tipo de tráfego do CE x situação (importação, passagem, cabotagem...);
  5. passagem/baldeação: pendentes com EMBARQUE posterior do contêiner (mesmo/outro operador);
  6. saídas: pendentes cuja saída veio depois da janela de 72 h; saídas que são agendamento;
     permanência no operador; perfil dos acessos dos operadores (operação, direção, vazio);
  7. trânsito: concluídos "pelo operador" com entrada em outro recinto depois das 6 h; entradas
     no seco com o contêiner vazio;
  8. escaneamentos: inspeções com CE/vazio por recinto; ciclos sem escaneamento que têm
     inspeção com o próprio CE (recinto e defasagem); escaneamentos já ligados com outro CE;
  9. risco de outra operação: inspeções do mesmo contêiner perto do ciclo (outro CE, vazio, sem CE);
 10. amostras: linha do tempo dos eventos do contêiner para ciclos de cada situação-problema;
 11. Mongo (opcional, --mongo): campos das imagens (CE, recinto, vazio) e defasagem de fuso.

Uso (na raiz do repositório virasana):
    python virasana/analytic_scripts/ciclo_de_vida/diagnostico_ciclo_de_vida.py
    python virasana/analytic_scripts/ciclo_de_vida/diagnostico_ciclo_de_vida.py --dias 60 --amostras 8 --mongo
"""
import argparse
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

from sqlalchemy import bindparam, create_engine, text

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

OPERADORES = ('8931356', '8931359', '8931404')  # os mesmos de manager_ciclo_de_vida
HORAS_PERMANENCIA_MOLHADO = 72
HORAS_TRANSIT_TIME = 6
DIAS_ABANDONO = 93
TAMANHO_LOTE = 500

TABELAS = ['ciclodevida_ciclo_de_vida_importacao', 'ciclodevida_ciclo_de_vida_importacao_escaneamentos',
           'apirecintos_embarquedesembarque', 'apirecintos_acessosveiculo', 'apirecintos_inspecoesnaoinvasivas']

CICLO_EM_PERIODO = 'c.data_desembarque >= :inicio AND c.data_desembarque < :fim'
TEM_ESCANEAMENTO = ('EXISTS (SELECT 1 FROM ciclodevida_ciclo_de_vida_importacao_escaneamentos e '
                    'WHERE e.ciclo_de_vida_id = c.ID)')
TEM_IMAGEM = ('EXISTS (SELECT 1 FROM ciclodevida_ciclo_de_vida_importacao_escaneamentos e '
              'WHERE e.ciclo_de_vida_id = c.ID AND e.imagem_id IS NOT NULL)')
TEM_CE = "(c.numeroCEmercante IS NOT NULL AND c.numeroCEmercante <> '')"
PENDENTE = '(c.concluido IS NULL OR c.concluido = 0)'


def _faixa(horas: Optional[float], limites) -> str:
    """Rótulo da faixa de tempo (em horas) — limites: [(teto_em_horas, rótulo), ...]."""
    if horas is None:
        return 'sem data'
    for teto, rotulo in limites:
        if horas < teto:
            return rotulo
    return limites[-1][1]


FAIXAS_PERMANENCIA = [(24, '< 1 dia'), (72, '1 a 3 dias'), (24 * 7, '3 a 7 dias'), (24 * 30, '7 a 30 dias'),
                      (24 * 93, '30 a 93 dias'), (float('inf'), '> 93 dias')]
FAIXAS_DEFASAGEM = [(-1, 'antes do desembarque'), (24, '0 a 24 h'), (72, '1 a 3 dias'), (24 * 30, '3 a 30 dias'),
                    (float('inf'), '> 30 dias')]


def _dt(valor) -> Optional[datetime]:
    """Data/hora de uma linha de consulta: o MariaDB devolve datetime; o SQLite, texto."""
    if valor is None or isinstance(valor, datetime):
        return valor
    return datetime.fromisoformat(str(valor)[:19])


def _horas(depois, antes) -> Optional[float]:
    depois, antes = _dt(depois), _dt(antes)
    if depois is None or antes is None:
        return None
    return (depois - antes).total_seconds() / 3600


class Diagnostico:
    def __init__(self, conexao, saida, inicio: datetime, fim: datetime, agora: Optional[datetime] = None,
                 amostras: int = 5, limite_risco: int = 2000):
        self.conexao = conexao
        self.saida = saida
        self.mysql = conexao.dialect.name == 'mysql'
        self.periodo = {'inicio': inicio, 'fim': fim}
        self.agora = agora or datetime.now()
        self.amostras = amostras
        self.limite_risco = limite_risco
        self.tempos = []

    # ---------------------------------------------------------------- saída
    def escreve(self, texto=''):
        self.saida.write(texto + '\n')

    def titulo(self, texto):
        self.escreve()
        self.escreve('=' * 100)
        self.escreve(texto)
        self.escreve('=' * 100)

    def tabela(self, colunas, linhas):
        linhas = [['' if v is None else str(v) for v in linha] for linha in linhas]
        larguras = [max([len(c)] + [len(linha[i]) for linha in linhas]) for i, c in enumerate(colunas)]
        self.escreve(' | '.join(c.ljust(larguras[i]) for i, c in enumerate(colunas)))
        self.escreve('-+-'.join('-' * largura for largura in larguras))
        for linha in linhas:
            self.escreve(' | '.join(v.ljust(larguras[i]) for i, v in enumerate(linha)))

    def consulta(self, sql, params=None, listas=()) -> list:
        """Linhas da consulta (tuplas); em erro, registra e devolve [] (o diagnóstico segue)."""
        comando = text(sql)
        if listas:
            comando = comando.bindparams(*[bindparam(nome, expanding=True) for nome in listas])
        inicio = time.perf_counter()
        try:
            linhas = [tuple(linha) for linha in self.conexao.execute(comando, {**self.periodo, **(params or {})})]
        except Exception as err:  # noqa: o log deve seguir mesmo se um item falhar
            self.escreve(f'!! ERRO [{" ".join(sql.split())[:90]}...]: {err}')
            return []
        segundos = time.perf_counter() - inicio
        self.tempos.append((' '.join(sql.split())[:70], segundos))
        if segundos > 5:
            self.escreve(f'   (consulta levou {segundos:.1f} s)')
        return linhas

    def numero(self, sql, params=None) -> int:
        linhas = self.consulta(sql, params)
        return int(linhas[0][0] or 0) if linhas else 0

    def _por_lotes(self, sql, nome, valores, params=None) -> list:
        linhas = []
        valores = sorted(v for v in set(valores) if v)
        for i in range(0, len(valores), TAMANHO_LOTE):
            linhas += self.consulta(sql, {**(params or {}), nome: valores[i:i + TAMANHO_LOTE]}, listas=(nome,))
        return linhas

    # ---------------------------------------------------------------- 1
    def secao_estrutura(self):
        self.titulo('1. ESTRUTURA E VOLUME DAS TABELAS')
        if not self.mysql:
            self.escreve('(SHOW CREATE TABLE só no MariaDB/MySQL)')
            return
        self.tabela(['tabela', 'linhas (estimativa)', 'dados_mb', 'indices_mb'], self.consulta(
            'SELECT TABLE_NAME, TABLE_ROWS, ROUND(DATA_LENGTH/1048576), ROUND(INDEX_LENGTH/1048576) '
            'FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN :tabelas '
            'ORDER BY TABLE_ROWS DESC', {'tabelas': TABELAS}, listas=('tabelas',)))
        for tabela in TABELAS:
            for linha in self.consulta('SHOW CREATE TABLE ' + tabela):  # nosec B608
                self.escreve()
                self.escreve(linha[1])

    # ---------------------------------------------------------------- 2
    def secao_estado(self):
        self.titulo('2. ESTADO DOS CICLOS (todos, sem filtro de período)')
        total = self.numero('SELECT COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao c')
        pendentes = self.numero('SELECT COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao c WHERE '  # nosec B608
                                '' + PENDENTE)
        self.escreve(f'Ciclos: {total}; pendentes (não concluídos): {pendentes}')
        self.escreve()
        self.escreve('Pendentes por idade (desde o desembarque):')
        limites = {'d3': self.agora - timedelta(days=3), 'd7': self.agora - timedelta(days=7),
                   'd30': self.agora - timedelta(days=30), 'd93': self.agora - timedelta(days=DIAS_ABANDONO)}
        self.tabela(['idade', 'pendentes'], self.consulta(
            "SELECT CASE WHEN c.data_desembarque IS NULL THEN 'sem data' "  # nosec B608
            "WHEN c.data_desembarque >= :d3 THEN '1) < 3 dias' WHEN c.data_desembarque >= :d7 THEN '2) 3 a 7 dias' "
            "WHEN c.data_desembarque >= :d30 THEN '3) 7 a 30 dias' WHEN c.data_desembarque >= :d93 "
            "THEN '4) 30 a 93 dias' ELSE '5) > 93 dias' END AS idade, COUNT(*) "
            'FROM ciclodevida_ciclo_de_vida_importacao c WHERE ' + PENDENTE + ' GROUP BY idade ORDER BY idade',
            limites))
        self.escreve()
        self.escreve('Pendentes por estágio (1 = tem):')
        self.tabela(['CE', 'saída', 'saída SEM data', 'entrada seco', 'saída seco', 'escaneamento', 'ciclos'],
                    self.consulta(
                        'SELECT CASE WHEN ' + TEM_CE + ' THEN 1 ELSE 0 END AS ce, '  # nosec B608
                        'CASE WHEN c.saida_id IS NULL THEN 0 ELSE 1 END AS saida, '
                        'CASE WHEN c.saida_id IS NOT NULL AND c.data_saida IS NULL THEN 1 ELSE 0 END AS sem_data, '
                        'CASE WHEN c.entrada_seco_id IS NULL THEN 0 ELSE 1 END AS entrada, '
                        'CASE WHEN c.saida_seco_id IS NULL THEN 0 ELSE 1 END AS saida_seco, '
                        'CASE WHEN ' + TEM_ESCANEAMENTO + ' THEN 1 ELSE 0 END AS esc, COUNT(*) '
                        'FROM ciclodevida_ciclo_de_vida_importacao c WHERE ' + PENDENTE +
                        ' GROUP BY ce, saida, sem_data, entrada, saida_seco, esc ORDER BY 7 DESC'))
        self.escreve()
        self.tabela(['inconsistência', 'ciclos'], [
            ('saida_id preenchido e data_saida vazia (todos)', self.numero(
                'SELECT COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao c '
                'WHERE c.saida_id IS NOT NULL AND c.data_saida IS NULL')),
            ('sem desembarque_id', self.numero(
                'SELECT COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao c WHERE c.desembarque_id IS NULL')),
            ('(CE, contêiner) repetidos em mais de um ciclo — pares', self.numero(
                'SELECT COUNT(*) FROM (SELECT c.numeroCEmercante, c.numeroConteiner '  # nosec B608
                'FROM ciclodevida_ciclo_de_vida_importacao c WHERE ' + TEM_CE +
                ' GROUP BY c.numeroCEmercante, c.numeroConteiner HAVING COUNT(*) > 1) q')),
            ('escaneamento ligado a mais de um ciclo', self.numero(
                'SELECT COUNT(*) FROM (SELECT escaneamento_id FROM '
                'ciclodevida_ciclo_de_vida_importacao_escaneamentos GROUP BY escaneamento_id '
                'HAVING COUNT(DISTINCT ciclo_de_vida_id) > 1) q')),
        ])

    # ---------------------------------------------------------------- 3
    def secao_mensal(self):
        self.titulo('3. CICLOS POR MÊS DE DESEMBARQUE (período)')
        self.tabela(['mês', 'ciclos', 'concluídos', 'com CE', 'com saída', 'entrada seco', 'saída seco',
                     'escaneamento', 'imagem'], self.consulta(
            'SELECT SUBSTR(CAST(c.data_desembarque AS CHAR), 1, 7) AS mes, COUNT(*), '  # nosec B608
            'SUM(CASE WHEN c.concluido = 1 THEN 1 ELSE 0 END), SUM(CASE WHEN ' + TEM_CE + ' THEN 1 ELSE 0 END), '
            'SUM(CASE WHEN c.saida_id IS NOT NULL THEN 1 ELSE 0 END), '
            'SUM(CASE WHEN c.entrada_seco_id IS NOT NULL THEN 1 ELSE 0 END), '
            'SUM(CASE WHEN c.saida_seco_id IS NOT NULL THEN 1 ELSE 0 END), '
            'SUM(CASE WHEN ' + TEM_ESCANEAMENTO + ' THEN 1 ELSE 0 END), '
            'SUM(CASE WHEN ' + TEM_IMAGEM + ' THEN 1 ELSE 0 END) '
            'FROM ciclodevida_ciclo_de_vida_importacao c WHERE ' + CICLO_EM_PERIODO + ' GROUP BY mes ORDER BY mes'))

    # ---------------------------------------------------------------- 4
    def secao_trafego(self):
        self.titulo('4. TIPO DE TRÁFEGO DO CE x SITUAÇÃO (período; 5 importação, 9 passagem, 3 cabotagem, '
                    '7 exportação)')
        self.tabela(['tipoTrafego', 'ciclos', 'concluídos', 'pendentes', 'com escaneamento'], self.consulta(
            'SELECT cr.tipoTrafego, COUNT(*), SUM(CASE WHEN c.concluido = 1 THEN 1 ELSE 0 END), '  # nosec B608
            'SUM(CASE WHEN ' + PENDENTE + ' THEN 1 ELSE 0 END), SUM(CASE WHEN ' + TEM_ESCANEAMENTO +
            ' THEN 1 ELSE 0 END) FROM ciclodevida_ciclo_de_vida_importacao c '
            'JOIN conhecimentosresumo cr ON cr.numeroCEmercante = c.numeroCEmercante '
            'WHERE ' + CICLO_EM_PERIODO + ' GROUP BY cr.tipoTrafego ORDER BY 2 DESC'))
        sem_ce = self.numero('SELECT COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao c WHERE ' +  # nosec B608
                             CICLO_EM_PERIODO + ' AND NOT ' + TEM_CE)
        self.escreve(f'Ciclos do período sem CE ligado: {sem_ce}')

    # ---------------------------------------------------------------- 5
    def secao_passagem(self):
        self.titulo('5. PASSAGEM / BALDEAÇÃO — pendentes com EMBARQUE posterior do contêiner (período)')
        embarque = ('EXISTS (SELECT 1 FROM apirecintos_embarquedesembarque ed WHERE ed.numeroConteiner = '
                    "c.numeroConteiner AND ed.embarqueDesembarque = 'E' AND ed.dataHoraOcorrencia > c.data_desembarque "
                    'AND ed.dataHoraOcorrencia < :fim_busca AND {recinto})')
        params = {'fim_busca': self.periodo['fim'] + timedelta(days=DIAS_ABANDONO)}
        linhas = []
        for rotulo, recinto in (('embarque no MESMO operador', 'ed.codigoRecinto = c.codigoRecintoDesembarque'),
                                ('embarque em OUTRO operador (baldeação)',
                                 'ed.codigoRecinto <> c.codigoRecintoDesembarque')):
            linhas.append((rotulo, self.numero(
                'SELECT COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao c WHERE ' + CICLO_EM_PERIODO +  # nosec B608
                ' AND ' + PENDENTE + ' AND ' + embarque.format(recinto=recinto), params)))
        self.tabela(['pendentes com...', 'ciclos'], linhas)

    # ---------------------------------------------------------------- 6
    def secao_saidas(self):
        self.titulo('6. SAÍDAS DO OPERADOR')
        saida_tardia = ('EXISTS (SELECT 1 FROM apirecintos_acessosveiculo a WHERE a.numeroConteiner = '
                        "c.numeroConteiner AND a.codigoRecinto = c.codigoRecintoDesembarque AND a.direcao = 'S' "
                        "AND a.operacao = 'C' AND a.dataHoraOcorrencia > c.data_desembarque "
                        'AND a.dataHoraOcorrencia < :fim_busca)')
        self.tabela(['pendentes sem saída (período)', 'ciclos'], [
            ('total', self.numero('SELECT COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao c WHERE ' +  # nosec B608
                                  CICLO_EM_PERIODO + ' AND ' + PENDENTE + ' AND c.saida_id IS NULL')),
            ('com saída (acesso efetivado) existente até 93 dias — perdida pela janela de 72 h', self.numero(
                'SELECT COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao c WHERE '  # nosec B608
                + CICLO_EM_PERIODO + ' AND ' + PENDENTE + ' AND c.saida_id IS NULL AND ' + saida_tardia,
                {'fim_busca': self.periodo['fim'] + timedelta(days=DIAS_ABANDONO)})),
        ])
        self.escreve()
        self.escreve('Saída gravada no ciclo é um AGENDAMENTO (operacao G) e não um acesso:')
        self.tabela(['operacao da saída', 'ciclos'], self.consulta(
            'SELECT a.operacao, COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao c '  # nosec B608
            'JOIN apirecintos_acessosveiculo a ON a.id = c.saida_id WHERE ' + CICLO_EM_PERIODO +
            ' GROUP BY a.operacao'))
        self.escreve()
        self.escreve('Permanência no operador (desembarque -> saída), ciclos do período com saída:')
        contagem = Counter(_faixa(_horas(saida, desembarque), FAIXAS_PERMANENCIA) for desembarque, saida in
                           self.consulta('SELECT c.data_desembarque, c.data_saida FROM '  # nosec B608
                                         'ciclodevida_ciclo_de_vida_importacao c WHERE ' + CICLO_EM_PERIODO +
                                         ' AND c.data_saida IS NOT NULL'))
        self.tabela(['permanência', 'ciclos'], [(f, contagem[f]) for _, f in FAIXAS_PERMANENCIA if contagem[f]])
        self.escreve()
        self.escreve('Acessos de veículo nos operadores (período): operação x direção x tipoOperacao x vazio')
        self.tabela(['operacao', 'direcao', 'tipoOperacao', 'vazio', 'acessos', 'com contêiner'], self.consulta(
            'SELECT a.operacao, a.direcao, a.tipoOperacao, a.vazioConteiner, COUNT(*), '
            "SUM(CASE WHEN a.numeroConteiner IS NOT NULL AND a.numeroConteiner <> '' THEN 1 ELSE 0 END) "
            'FROM apirecintos_acessosveiculo a WHERE a.codigoRecinto IN :operadores '
            'AND a.dataHoraOcorrencia >= :inicio AND a.dataHoraOcorrencia < :fim '
            'GROUP BY a.operacao, a.direcao, a.tipoOperacao, a.vazioConteiner ORDER BY 5 DESC',
            {'operadores': list(OPERADORES)}, listas=('operadores',)))

    # ---------------------------------------------------------------- 7
    def secao_transito(self):
        self.titulo('7. TRÂNSITO OPERADOR -> SECO (período)')
        self.escreve('Concluídos "pelo operador" (sem entrada no seco) que tiveram ENTRADA em outro recinto depois '
                     'da saída, até 72 h — perdidos pela regra das 6 h? (por vazio informado na entrada)')
        linhas = self.consulta(
            'SELECT c.data_saida, a.dataHoraOcorrencia, a.vazioConteiner FROM '  # nosec B608
            'ciclodevida_ciclo_de_vida_importacao c '
            'JOIN apirecintos_acessosveiculo a ON a.numeroConteiner = c.numeroConteiner '
            "AND a.codigoRecinto <> c.codigoRecintoDesembarque AND a.direcao = 'E' AND a.operacao = 'C' "
            'AND a.dataHoraOcorrencia > c.data_saida AND a.dataHoraOcorrencia < :fim_busca '
            'WHERE ' + CICLO_EM_PERIODO + ' AND c.concluido = 1 AND c.entrada_seco_id IS NULL '
            'AND c.data_saida IS NOT NULL', {'fim_busca': self.periodo['fim'] + timedelta(days=5)})
        contagem = Counter()
        for saida, entrada, vazio in linhas:
            horas = _horas(entrada, saida)
            if HORAS_TRANSIT_TIME <= horas <= HORAS_PERMANENCIA_MOLHADO:
                contagem[('6 a 24 h' if horas < 24 else '24 a 72 h', 'vazio' if vazio else 'cheio/não informado')] += 1
        self.tabela(['entrada depois da saída', 'contêiner', 'entradas'],
                    [(f, v, n) for (f, v), n in sorted(contagem.items())])
        self.escreve()
        self.escreve('Entradas no seco gravadas no ciclo com o contêiner informado VAZIO (provável outra operação):')
        self.tabela(['vazioConteiner da entrada', 'ciclos'], self.consulta(
            'SELECT a.vazioConteiner, COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao c '  # nosec B608
            'JOIN apirecintos_acessosveiculo a ON a.id = c.entrada_seco_id WHERE ' + CICLO_EM_PERIODO +
            ' GROUP BY a.vazioConteiner'))

    # ---------------------------------------------------------------- 8
    def secao_escaneamentos(self):
        self.titulo('8. ESCANEAMENTOS')
        self.escreve('Inspeções não invasivas do período por recinto (CE informado e vazio):')
        self.tabela(['recinto', 'inspeções', 'com CE (15 díg.)', 'vazio = 1', 'sem contêiner'], self.consulta(
            'SELECT i.codigoRecinto, COUNT(*), SUM(CASE WHEN LENGTH(i.numeroConhecimento) = 15 THEN 1 ELSE 0 END), '
            'SUM(CASE WHEN i.vazio = 1 THEN 1 ELSE 0 END), '
            "SUM(CASE WHEN i.numeroConteiner IS NULL OR i.numeroConteiner = '' THEN 1 ELSE 0 END) "
            'FROM apirecintos_inspecoesnaoinvasivas i WHERE i.dataHoraOcorrencia >= :inicio '
            'AND i.dataHoraOcorrencia < :fim GROUP BY i.codigoRecinto ORDER BY 2 DESC')[:25])
        self._sem_escaneamento_recuperaveis_por_ce()
        self._ligados_conferencia()

    def _sem_escaneamento_recuperaveis_por_ce(self):
        self.escreve()
        self.escreve('Ciclos do período SEM escaneamento que têm inspeção com o PRÓPRIO CE e contêiner:')
        linhas = self.consulta(
            'SELECT c.ID, c.codigoRecintoDesembarque, c.codigoRecintoArmazenagem, c.data_desembarque, '  # nosec B608
            'i.codigoRecinto, i.dataHoraOcorrencia, i.vazio FROM ciclodevida_ciclo_de_vida_importacao c '
            'JOIN apirecintos_inspecoesnaoinvasivas i ON i.numeroConhecimento = c.numeroCEmercante '
            'AND i.numeroConteiner = c.numeroConteiner WHERE ' + CICLO_EM_PERIODO + ' AND ' + TEM_CE +
            ' AND NOT ' + TEM_ESCANEAMENTO)
        sem_esc = self.numero('SELECT COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao c WHERE ' +  # nosec B608
                              CICLO_EM_PERIODO + ' AND NOT ' + TEM_ESCANEAMENTO)
        recintos, defasagens = Counter(), Counter()
        for _, rec_desemb, rec_armaz, desembarque, rec_insp, data_insp, vazio in linhas:
            onde = ('operador do desembarque' if rec_insp == rec_desemb else
                    'recinto seco do ciclo' if rec_insp == rec_armaz else 'OUTRO recinto (terceirizado/erro)')
            recintos[(onde, 'vazio' if vazio else '-')] += 1
            defasagens[_faixa(_horas(data_insp, desembarque), FAIXAS_DEFASAGEM)] += 1
        self.escreve(f'Sem escaneamento no período: {sem_esc}; recuperáveis pelo CE: '
                     f'{len({linha[0] for linha in linhas})} ciclos ({len(linhas)} inspeções)')
        self.tabela(['onde foi a inspeção', 'vazio', 'inspeções'], [(o, v, n) for (o, v), n in recintos.most_common()])
        self.tabela(['inspeção x desembarque', 'inspeções'],
                    [(f, defasagens[f]) for _, f in FAIXAS_DEFASAGEM if defasagens[f]])

    def _ligados_conferencia(self):
        self.escreve()
        self.escreve('Escaneamentos JÁ ligados (período): o CE da inspeção confere com o do ciclo? e o recinto?')
        self.tabela(['CE da inspeção', 'recinto da inspeção', 'vazio', 'ligações'], self.consulta(
            "SELECT CASE WHEN i.numeroConhecimento IS NULL OR i.numeroConhecimento = '' "  # nosec B608
            "THEN 'não informado' "
            "WHEN i.numeroConhecimento = c.numeroCEmercante THEN 'igual' ELSE 'OUTRO CE' END AS ce, "
            "CASE WHEN i.codigoRecinto = c.codigoRecintoDesembarque THEN 'operador' "
            "WHEN i.codigoRecinto = c.codigoRecintoArmazenagem THEN 'seco' ELSE 'outro' END AS recinto, "
            'i.vazio, COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao c '
            'JOIN ciclodevida_ciclo_de_vida_importacao_escaneamentos e ON e.ciclo_de_vida_id = c.ID '
            'JOIN apirecintos_inspecoesnaoinvasivas i ON i.id = e.escaneamento_id WHERE ' + CICLO_EM_PERIODO +
            ' GROUP BY ce, recinto, i.vazio ORDER BY 4 DESC'))

    # ---------------------------------------------------------------- 9
    def secao_risco(self):
        self.titulo(f'9. RISCO DE OUTRA OPERAÇÃO — inspeções do mesmo contêiner perto do ciclo '
                    f'(até {self.limite_risco} ciclos do período com CE)')
        ciclos = self.consulta(
            'SELECT c.ID, c.numeroConteiner, c.numeroCEmercante, c.data_desembarque FROM '  # nosec B608
            'ciclodevida_ciclo_de_vida_importacao c WHERE ' + CICLO_EM_PERIODO + ' AND ' + TEM_CE +
            ' ORDER BY c.ID DESC')[:self.limite_risco]
        inspecoes = self._inspecoes_por_conteiner({c[1] for c in ciclos})
        categorias = Counter()
        for _, conteiner, ce, desembarque in ciclos:
            for data, _, ce_insp, vazio in inspecoes.get(conteiner, []):
                horas = _horas(data, desembarque)
                if horas is None or not -24 <= horas <= 24 * DIAS_ABANDONO:
                    continue
                tipo = ('com o CE do ciclo' if ce_insp == ce else 'sem CE' if not ce_insp else 'com OUTRO CE')
                categorias[(tipo, 'vazio' if vazio else '-', _faixa(horas, FAIXAS_DEFASAGEM))] += 1
        self.tabela(['inspeção', 'vazio', 'quando (x desembarque)', 'inspeções'],
                    [(*chave, n) for chave, n in sorted(categorias.items())])
        self.escreve('Leitura: "com OUTRO CE" e "vazio" perto do ciclo mostram o risco de ligar a operação errada '
                     'quando se busca só por contêiner + data.')

    def _inspecoes_por_conteiner(self, conteineres) -> Dict[str, list]:
        resultado = defaultdict(list)
        for data, recinto, conteiner, ce, vazio in self._por_lotes(
                'SELECT i.dataHoraOcorrencia, i.codigoRecinto, i.numeroConteiner, i.numeroConhecimento, i.vazio '
                'FROM apirecintos_inspecoesnaoinvasivas i WHERE i.numeroConteiner IN :conteineres '
                'AND i.dataHoraOcorrencia >= :inicio_busca AND i.dataHoraOcorrencia < :fim_busca',
                'conteineres', conteineres,
                {'inicio_busca': self.periodo['inicio'] - timedelta(days=2),
                 'fim_busca': self.periodo['fim'] + timedelta(days=DIAS_ABANDONO)}):
            resultado[conteiner].append((data, recinto, ce, vazio))
        return resultado

    # ---------------------------------------------------------------- 10
    CATEGORIAS_AMOSTRA = [
        ('pendente sem saída há mais de 7 dias',
         PENDENTE + ' AND c.saida_id IS NULL AND c.data_desembarque < :d7'),
        ('saida_id sem data_saida', 'c.saida_id IS NOT NULL AND c.data_saida IS NULL'),
        ('pendente com embarque posterior (passagem?)',
         PENDENTE + ' AND EXISTS (SELECT 1 FROM apirecintos_embarquedesembarque ed WHERE '  # nosec B608
                    'ed.numeroConteiner = '
         "c.numeroConteiner AND ed.embarqueDesembarque = 'E' AND ed.dataHoraOcorrencia > c.data_desembarque)"),
        ('sem escaneamento, mas há inspeção com o próprio CE',
         'NOT ' + TEM_ESCANEAMENTO + ' AND EXISTS (SELECT 1 FROM apirecintos_inspecoesnaoinvasivas i '  # nosec B608
         'WHERE i.numeroConhecimento = c.numeroCEmercante AND i.numeroConteiner = c.numeroConteiner)'),
        ('concluído pelo operador sem escaneamento',
         'c.concluido = 1 AND c.entrada_seco_id IS NULL AND NOT ' + TEM_ESCANEAMENTO),
        ('TRA concluído sem escaneamento',
         'c.concluido = 1 AND c.saida_seco_id IS NOT NULL AND NOT ' + TEM_ESCANEAMENTO),
    ]

    def secao_amostras(self):
        self.titulo(f'10. AMOSTRAS — linha do tempo do contêiner ({self.amostras} por situação, período)')
        if not self.amostras:
            self.escreve('(desligado: --amostras 0)')
            return
        for rotulo, condicao in self.CATEGORIAS_AMOSTRA:
            linhas = self.consulta(
                'SELECT c.ID FROM ciclodevida_ciclo_de_vida_importacao c WHERE '  # nosec B608
                + CICLO_EM_PERIODO + ' AND ' + condicao + ' ORDER BY c.ID DESC',
                {'d7': self.agora - timedelta(days=7)})
            ids = [linha[0] for linha in linhas[:self.amostras]]
            self.escreve()
            self.escreve(f'--- {rotulo}: {len(ids)} amostra(s)')
            for ciclo_id in ids:
                self._linha_do_tempo(ciclo_id)

    def _linha_do_tempo(self, ciclo_id):
        ciclo = self.consulta(
            'SELECT c.ID, c.numeroConteiner, c.numeroCEmercante, c.codigoRecintoDesembarque, c.data_desembarque, '
            'c.saida_id, c.data_saida, c.codigoRecintoArmazenagem, c.entrada_seco_id, c.saida_seco_id, c.concluido, '
            'cr.tipoTrafego FROM ciclodevida_ciclo_de_vida_importacao c LEFT JOIN conhecimentosresumo cr '
            'ON cr.numeroCEmercante = c.numeroCEmercante WHERE c.ID = :id', {'id': ciclo_id})
        if not ciclo:
            return
        (_, conteiner, ce, rec_desemb, desembarque, saida_id, data_saida, rec_armaz, entrada_id, saida_seco_id,
         concluido, trafego) = ciclo[0]
        ligados = {linha[0] for linha in self.consulta(
            'SELECT escaneamento_id FROM ciclodevida_ciclo_de_vida_importacao_escaneamentos '
            'WHERE ciclo_de_vida_id = :id', {'id': ciclo_id})}
        self.escreve()
        self.escreve(f'Ciclo {ciclo_id}: contêiner {conteiner}, CE {ce or "-"} (tráfego {trafego or "-"}), '
                     f'desembarque {desembarque} em {rec_desemb}, saída {data_saida or "-"}, seco {rec_armaz or "-"}, '
                     f'concluído {bool(concluido)}')
        eventos = self._eventos_do_conteiner(conteiner, desembarque)
        marcas = {('acesso', saida_id): 'SAÍDA do ciclo', ('acesso', entrada_id): 'ENTRADA SECO do ciclo',
                  ('acesso', saida_seco_id): 'SAÍDA SECO do ciclo'}
        self.tabela(['data/hora', 'evento', 'recinto', 'operação', 'tipoOp', 'vazio', 'CE informado', 'no ciclo'],
                    [(data, tipo, recinto, operacao, tipo_op, vazio, ce_ev,
                      'LIGADO' if tipo == 'inspeção' and ev_id in ligados else marcas.get(('acesso', ev_id), '')
                      if tipo.startswith('acesso') else '')
                     for data, tipo, recinto, operacao, tipo_op, vazio, ce_ev, ev_id in eventos])

    def _eventos_do_conteiner(self, conteiner, desembarque) -> list:
        desembarque = _dt(desembarque)
        if not conteiner or not desembarque:
            return []
        params = {'conteiner': conteiner, 'de': desembarque - timedelta(days=2),
                  'ate': desembarque + timedelta(days=DIAS_ABANDONO + 2)}
        filtro = 'WHERE x.numeroConteiner = :conteiner AND x.dataHoraOcorrencia >= :de AND x.dataHoraOcorrencia < :ate'
        eventos = [(d, 'embarque' if ed == 'E' else 'desembarque', r, '', t, '', ce, i) for d, ed, r, t, ce, i in
                   self.consulta('SELECT x.dataHoraOcorrencia, x.embarqueDesembarque, x.codigoRecinto, '  # nosec B608
                                 'x.tipoOperacao, '
                                 'x.numeroConhecimento, x.id FROM apirecintos_embarquedesembarque x ' + filtro, params)]
        eventos += [(d, 'acesso ' + ('entrada' if di == 'E' else 'saída'), r, o, t, v, ce, i)
                    for d, di, r, o, t, v, ce, i in self.consulta(
                        'SELECT x.dataHoraOcorrencia, x.direcao, x.codigoRecinto, x.operacao, '  # nosec B608
                        'x.tipoOperacao, '
                        'x.vazioConteiner, x.numeroConhecimento, x.id FROM apirecintos_acessosveiculo x ' + filtro,
                        params)]
        eventos += [(d, 'inspeção', r, '', t, v, ce, i) for d, r, t, v, ce, i in self.consulta(
            'SELECT x.dataHoraOcorrencia, x.codigoRecinto, x.tipoOperacao, x.vazio, '  # nosec B608
            'x.numeroConhecimento, x.id '
            'FROM apirecintos_inspecoesnaoinvasivas x ' + filtro, params)]
        return sorted(eventos, key=lambda ev: _dt(ev[0]))

    # ---------------------------------------------------------------- 11
    def secao_mongo(self, db, limite=300):
        self.titulo('11. MONGO — campos das imagens e defasagem de horário')
        if db is None:
            self.escreve('(desligado: use --mongo)')
            return
        arquivos = db['fs.files']
        filtro = {'metadata.dataescaneamento': {'$gte': self.periodo['inicio'], '$lt': self.periodo['fim']}}
        total = arquivos.count_documents(filtro)
        self.tabela(['campo', 'imagens', '%'], [
            (campo, n, f'{100 * n / total:.1f}%' if total else '-') for campo, n in [
                ('(todas do período)', total)] + [
                (campo, arquivos.count_documents({**filtro, campo: {'$exists': True, '$nin': [None, '']}}))
                for campo in ('metadata.numeroinformado', 'metadata.recinto', 'metadata.carga.conhecimento',
                              'metadata.carga.vazio', 'metadata.predictions.vazio')]])
        self.escreve()
        self.escreve('Estrutura de metadata (chaves, 1 amostra):')
        amostra = arquivos.find_one(filtro, {'metadata': 1}) or {}
        self.escreve(', '.join(sorted(_chaves(amostra.get('metadata') or {}))))
        self._defasagem_mongo(arquivos, limite)

    def _defasagem_mongo(self, arquivos, limite):
        self.escreve()
        self.escreve(f'Defasagem imagem (Mongo) - inspeção (API) nas imagens já ligadas (até {limite}):')
        ligacoes = self.consulta(
            'SELECT e.imagem_id, i.dataHoraOcorrencia FROM ciclodevida_ciclo_de_vida_importacao c '  # nosec B608
            'JOIN ciclodevida_ciclo_de_vida_importacao_escaneamentos e ON e.ciclo_de_vida_id = c.ID '
            'JOIN apirecintos_inspecoesnaoinvasivas i ON i.id = e.escaneamento_id WHERE ' + CICLO_EM_PERIODO +
            ' AND e.imagem_id IS NOT NULL')[:limite]
        from bson import ObjectId
        contagem = Counter()
        for imagem_id, data_inspecao in ligacoes:
            documento = arquivos.find_one({'_id': ObjectId(str(imagem_id).strip())}, {'metadata.dataescaneamento': 1})
            data_imagem = ((documento or {}).get('metadata') or {}).get('dataescaneamento')
            horas = _horas(data_imagem.replace(tzinfo=None) if data_imagem else None, data_inspecao)
            contagem['sem data' if horas is None else f'{round(horas):+d} h'] += 1
        self.tabela(['imagem - inspeção', 'imagens'], sorted(contagem.items(), key=lambda kv: kv[0]))

    def resumo(self, numero=12):
        self.titulo(f'{numero}. CONSULTAS MAIS LENTAS')
        self.tabela(['consulta', 's'], [(q, f'{s:.1f}') for q, s in sorted(self.tempos, key=lambda t: -t[1])[:10]])


def _chaves(documento: dict, prefixo='', profundidade=3) -> Iterable[str]:
    for chave, valor in documento.items():
        nome = f'{prefixo}{chave}'
        yield nome
        if isinstance(valor, dict) and profundidade > 1:
            yield from _chaves(valor, nome + '.', profundidade - 1)


SECOES = ('secao_estrutura', 'secao_estado', 'secao_mensal', 'secao_trafego', 'secao_passagem', 'secao_saidas',
          'secao_transito', 'secao_escaneamentos', 'secao_risco', 'secao_amostras')


def roda(diag: Diagnostico, mongo_db=None) -> List[str]:
    """Roda todas as seções; devolve as que falharam (o log segue mesmo assim)."""
    falhas = []
    for nome in SECOES + ('secao_mongo',):
        inicio = time.perf_counter()
        try:
            getattr(diag, nome)(mongo_db) if nome == 'secao_mongo' else getattr(diag, nome)()
        except Exception as err:  # noqa: uma seção com problema não impede as outras
            diag.escreve(f'!! ERRO na seção {nome}: {type(err).__name__}: {err}')
            falhas.append(nome)
        diag.escreve(f'   [seção em {time.perf_counter() - inicio:.1f} s]')
        diag.saida.flush()
    diag.resumo()
    return falhas


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--dias', type=int, default=120, help='período: desembarques dos últimos N dias (padrão 120)')
    parser.add_argument('--amostras', type=int, default=5, help='ciclos de amostra por situação (0 = nenhum)')
    parser.add_argument('--limite-risco', type=int, default=2000, help='ciclos avaliados na seção 9')
    parser.add_argument('--mongo', action='store_true', help='inclui a seção do Mongo (MONGODB_URI do ajna_commons)')
    parser.add_argument('--saida', default=datetime.now().strftime('ciclo_de_vida_diagnostico_%Y%m%d_%H%M%S.log'))
    args = parser.parse_args()
    uri = args.uri
    if not uri:
        from ajna_commons.flask.conf import SQL_URI as uri
    mongo_db = None
    if args.mongo:
        from pymongo import MongoClient
        from ajna_commons.flask.conf import DATABASE, MONGODB_URI
        mongo_db = MongoClient(host=MONGODB_URI)[DATABASE]
    fim = datetime.now()
    inicio = fim - timedelta(days=args.dias)
    engine = create_engine(uri)
    with engine.connect() as conexao, open(args.saida, 'w', encoding='utf-8') as saida:
        diag = Diagnostico(conexao, saida, inicio, fim, amostras=args.amostras, limite_risco=args.limite_risco)
        diag.escreve(f'Diagnóstico do Ciclo de Vida — {datetime.now():%Y-%m-%d %H:%M:%S}; desembarques de '
                     f'{inicio:%Y-%m-%d} a {fim:%Y-%m-%d}')
        diag.escreve(f'Banco: {engine.url.render_as_string(hide_password=True)}')
        falhas = roda(diag, mongo_db)
    print(f'Diagnóstico gravado em {args.saida}' + (f' (seções com erro: {", ".join(falhas)})' if falhas else ''))


if __name__ == '__main__':  # pragma: no cover
    main()
