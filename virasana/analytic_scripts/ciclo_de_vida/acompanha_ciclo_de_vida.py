# coding: utf-8
"""
ACOMPANHAMENTO do Ciclo de Vida v2 em produção (serviço `servico_ciclo_de_vida.py`): relatório curto, para
rodar a qualquer momento e comparar com o anterior. Somente leitura; consultas por índice (data de
desembarque, `processado_em`, `situacao`, chave primária das tabelas da API) — leva segundos.

  1. SERVIÇO: marcas d'água e eventos ainda não lidos de cada tabela da API (fila), última gravação, ciclos
     gravados na última hora/dia e sinais de OUTRO gravador (o loop antigo ainda rodando);
  2. COBERTURA por semana de desembarque: ciclos v2 x antigos, abertos, CE, vazio, entrada no seco
     (implícita), escaneamento, imagem, potencial OOG;
  3. SITUAÇÃO x FLUXO dos ciclos v2 e os motivos mais comuns dos que não concluíram; abertos por idade;
  4. ESCANEAMENTOS por recinto (imagem, escaneamento fora, confiança) e imagens que faltam por idade;
  5. CE: confiança, ciclos sem CE por idade (o Mercante atrasa) e papéis da família;
  6. PENDENTES DO LOOP ANTIGO (o que falta reprocessar com `--reprocessa N`);
  7. ALERTAS: o que merece atenção nesta rodada.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    ACOMPANHA=virasana/analytic_scripts/ciclo_de_vida/acompanha_ciclo_de_vida.py
    virasana-venv/bin/python $ACOMPANHA                    # produção, desembarques dos últimos 28 dias
    virasana-venv/bin/python $ACOMPANHA --homologacao --dias 14 --saida acompanhamento.log
"""
import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy import create_engine

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ciclo_de_vida import (  # noqa: E402
    CICLO_EM_PERIODO, PENDENTE, TEM_CE, TEM_ESCANEAMENTO, TEM_IMAGEM, Diagnostico, _dt, _horas,
)
from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import EVENTOS_NOVOS, uri_do_banco  # noqa: E402

CICLO = 'ciclodevida_ciclo_de_vida_importacao'
V2 = "c.versao_heuristica = 'v2'"
MARCA_PARADA_MINUTOS = 30        # o serviço grava as marcas a cada volta (5 min sem fila)
FILTRO = {'apirecintos_acessosveiculo': " AND operacao = 'C'"}   # o que o serviço lê (EVENTOS_NOVOS)
FILA_ALERTA = 20000              # eventos não lidos: um lote inteiro do serviço (lote_eventos)
FILA_TETO = 1000000              # a contagem para aqui (é só para dar a ordem de grandeza)
REVISITA_ATRASADA_HORAS = 12     # abertos são revisitados a cada 6 h
IMAGEM_MINIMA = 0.5              # recinto com menos imagens que isso: conferir a defasagem (imagens_ciclo)
ESCANEAMENTOS_MINIMOS = 20       # ... desde que tenha escaneamentos suficientes para medir


def _pct(parte, total) -> str:
    return f'{parte} ({100 * parte / total:.0f}%)' if total else str(parte)


def _segunda(dia) -> str:
    """'2026-10-02' -> '2026-09-28' (segunda-feira da semana)."""
    data = datetime.fromisoformat(str(dia)[:10])
    return (data - timedelta(days=data.weekday())).strftime('%Y-%m-%d')


class Acompanhamento(Diagnostico):
    """Reaproveita a saída e as consultas do diagnóstico; período = desembarques de `inicio` a `fim`."""

    def __init__(self, conexao, saida, inicio: datetime, fim: datetime, agora: Optional[datetime] = None):
        super().__init__(conexao, saida, inicio, fim, agora=agora)
        self.alertas: List[str] = []

    # ---------------------------------------------------------------- 1
    def secao_servico(self):
        self.titulo("1. SERVIÇO — marcas d'água, gravações e outro gravador")
        marcas = {t: (u, a) for t, u, a in self.consulta(
            'SELECT tabela, ultimo_id, atualizado_em FROM ciclodevida_marca_dagua')}
        if not marcas:
            self.alertas.append("sem marcas d'água: o serviço ainda não completou nenhuma volta gravando")
        self.tabela(['tabela', 'marca (último id lido)', 'maior id', 'eventos não lidos', 'data do evento da marca',
                     'marca gravada há'], [self._marca(tabela, *marcas.get(tabela, (None, None)))
                                           for tabela in EVENTOS_NOVOS])
        self.escreve()
        ultima = self.consulta(f'SELECT MAX(c.processado_em) FROM {CICLO} c')  # nosec B608
        ultima = _dt(ultima[0][0]) if ultima else None
        hora, dia = self.agora - timedelta(hours=1), self.agora - timedelta(days=1)
        self.tabela(['gravações v2', 'valor'], [
            ('última gravação', ultima),
            ('ciclos gravados na última hora', self.numero(
                f'SELECT COUNT(*) FROM {CICLO} c WHERE c.processado_em >= :hora', {'hora': hora})),  # nosec B608
            ('ciclos gravados nas últimas 24 h', self.numero(
                f'SELECT COUNT(*) FROM {CICLO} c WHERE c.processado_em >= :dia', {'dia': dia})),  # nosec B608
        ])
        self._outro_gravador()

    def _marca(self, tabela, ultimo_id, atualizado) -> tuple:
        """Linha da tabela da API: marca, maior id, eventos não lidos (fila), data do evento da marca e há quanto
        tempo a marca foi gravada (o serviço a grava a cada volta)."""
        maior = self.numero(f'SELECT MAX(id) FROM {tabela}')  # nosec B608 - nome fixo de EVENTOS_NOVOS
        fila = None if ultimo_id is None else self.numero(
            f'SELECT COUNT(*) FROM (SELECT id FROM {tabela} WHERE id > :marca{FILTRO.get(tabela, "")} '  # nosec B608
            'LIMIT :teto) q', {'marca': ultimo_id, 'teto': FILA_TETO})
        parada = _horas(self.agora, atualizado)
        if parada is not None and parada * 60 > MARCA_PARADA_MINUTOS:
            self.alertas.append(f'marca de {tabela} sem atualização há {parada * 60:.0f} min: serviço parado '
                                'ou volta demorada (ver error.log/access.log)')
        if fila is not None and fila >= FILA_ALERTA:
            self.alertas.append(f'{tabela}: {fila}{"+" * (fila == FILA_TETO)} eventos ainda não lidos (fila)')
        return (tabela.replace('apirecintos_', ''), ultimo_id, maior, f'{fila}+' if fila == FILA_TETO else fila,
                self._data_do_evento(tabela, ultimo_id), None if parada is None else f'{parada * 60:.0f} min')

    def _data_do_evento(self, tabela, id_):
        if id_ is None:
            return None
        linhas = self.consulta(f'SELECT dataHoraOcorrencia FROM {tabela} WHERE id <= :id '  # nosec B608
                               'ORDER BY id DESC LIMIT 1', {'id': id_})
        return _dt(linhas[0][0]) if linhas else None

    def _outro_gravador(self):
        """O serviço grava `last_modified` = `processado_em` e confiança em todo escaneamento: diferença é sinal
        de outro gravador nas mesmas tabelas (o loop antigo não pode rodar junto)."""
        alterados = self.numero(f'SELECT COUNT(*) FROM {CICLO} c WHERE {CICLO_EM_PERIODO} AND {V2} '  # nosec B608
                                'AND c.last_modified > c.processado_em')
        sem_confianca = self.numero(
            'SELECT COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao_escaneamentos e '  # nosec B608
            f'JOIN {CICLO} c ON c.ID = e.ciclo_de_vida_id WHERE {CICLO_EM_PERIODO} AND {V2} AND e.confianca IS NULL')
        self.escreve()
        self.tabela(['sinal de outro gravador (período)', 'ciclos/escaneamentos'], [
            ('ciclos v2 alterados depois do serviço', alterados),
            ('escaneamentos sem confiança em ciclos v2', sem_confianca)])
        if alterados or sem_confianca:
            self.alertas.append(f'{alterados} ciclos v2 alterados depois do serviço e {sem_confianca} escaneamentos '
                                'sem confiança: o loop antigo (manager_ciclo_de_vida) ainda está rodando?')

    # ---------------------------------------------------------------- 2
    def secao_cobertura(self):
        self.titulo('2. COBERTURA POR SEMANA DE DESEMBARQUE (segunda-feira da semana)')
        aberto = f"(c.situacao = 'aberto' OR (c.versao_heuristica IS NULL AND {PENDENTE}))"
        colunas = [V2, aberto, TEM_CE, 'c.vazio = 1', 'c.data_entrada_seco IS NOT NULL',
                   'c.entrada_seco_implicita_de IS NOT NULL', TEM_ESCANEAMENTO, TEM_IMAGEM, 'c.oog = 1']
        somas = ', '.join(f'SUM(CASE WHEN {coluna} THEN 1 ELSE 0 END)' for coluna in colunas)
        semanas = defaultdict(lambda: [0] * (len(colunas) + 1))
        for dia, *valores in self.consulta(f'SELECT DATE(c.data_desembarque) AS dia, COUNT(*), {somas} '  # nosec B608
                                           f'FROM {CICLO} c WHERE {CICLO_EM_PERIODO} GROUP BY dia'):
            semana = semanas[_segunda(dia)]
            for i, valor in enumerate(valores):
                semana[i] += int(valor or 0)
        linhas = []
        for semana, (total, v2, abertos, ce, vazios, seco, implicita, esc, img, oog) in sorted(semanas.items()):
            linhas.append((semana, total, _pct(v2, total), total - v2, _pct(abertos, total),
                           _pct(ce, total - vazios), vazios, seco, implicita, _pct(esc, total), _pct(img, esc), oog))
        self.tabela(['semana', 'ciclos', 'v2', 'antigos', 'abertos', 'com CE (fora vazios)', 'vazios',
                     'entrada seco', '... implícita', 'com escaneamento', 'com imagem', 'OOG'], linhas)

    # ---------------------------------------------------------------- 3
    def secao_situacao(self):
        self.titulo('3. SITUAÇÃO x FLUXO (ciclos v2 do período) e motivos de quem não concluiu')
        self.tabela(['situação', 'fluxo', 'ciclos'], self.consulta(
            f'SELECT c.situacao, c.fluxo, COUNT(*) FROM {CICLO} c WHERE {CICLO_EM_PERIODO} AND {V2} '  # nosec B608
            'GROUP BY c.situacao, c.fluxo ORDER BY 3 DESC'))
        self.escreve()
        self.tabela(['situação', 'motivo', 'ciclos'], self.consulta(
            f'SELECT c.situacao, c.motivo, COUNT(*) FROM {CICLO} c WHERE {CICLO_EM_PERIODO} AND {V2} '  # nosec B608
            "AND c.situacao <> 'concluido' GROUP BY c.situacao, c.motivo ORDER BY 3 DESC LIMIT 15"))
        self.escreve()
        limites = {'d2': self.agora - timedelta(days=2), 'd7': self.agora - timedelta(days=7),
                   'd30': self.agora - timedelta(days=30), 'd93': self.agora - timedelta(days=93),
                   'revisita': self.agora - timedelta(hours=REVISITA_ATRASADA_HORAS)}
        linhas = self.consulta(
            "SELECT CASE WHEN c.data_desembarque >= :d2 THEN '1) < 2 dias' "  # nosec B608
            "WHEN c.data_desembarque >= :d7 THEN '2) 2 a 7 dias' WHEN c.data_desembarque >= :d30 "
            "THEN '3) 7 a 30 dias' WHEN c.data_desembarque >= :d93 THEN '4) 30 a 93 dias' ELSE '5) > 93 dias' END "
            'AS idade, COUNT(*), SUM(CASE WHEN c.processado_em < :revisita THEN 1 ELSE 0 END) '
            f"FROM {CICLO} c WHERE c.situacao = 'aberto' AND {V2} GROUP BY idade ORDER BY idade", limites)
        self.escreve('Ciclos v2 abertos (todos) por idade do desembarque:')
        self.tabela(['idade', 'abertos', f'sem revisita há > {REVISITA_ATRASADA_HORAS} h'], linhas)
        velhos = sum(int(n or 0) for idade, n, _ in linhas if idade.startswith('5'))
        atrasados = sum(int(n or 0) for _, _, n in linhas)
        if velhos:
            self.alertas.append(f'{velhos} ciclos v2 abertos com desembarque há mais de 93 dias (deviam ser abandono)')
        if atrasados:
            self.alertas.append(f'{atrasados} ciclos v2 abertos sem revisita há mais de {REVISITA_ATRASADA_HORAS} h '
                                '(fila de revisita maior que 2.000 por volta?)')

    # ---------------------------------------------------------------- 4
    def secao_escaneamentos(self):
        self.titulo('4. ESCANEAMENTOS DOS CICLOS v2 (período) — por recinto que informou')
        nomes = dict(self.consulta('SELECT cod_siscomex, MIN(nome) FROM ovr_recintos GROUP BY cod_siscomex'))
        de_ciclos_v2 = (f'FROM ciclodevida_ciclo_de_vida_importacao_escaneamentos e JOIN {CICLO} c '
                        f'ON c.ID = e.ciclo_de_vida_id WHERE {CICLO_EM_PERIODO} AND {V2}')
        linhas = []
        for recinto, total, imagens, fora, alta, media in self.consulta(
                'SELECT e.codigoRecinto, COUNT(*), SUM(CASE WHEN e.imagem_id IS NOT NULL THEN 1 ELSE 0 END), '
                "SUM(CASE WHEN e.fora = 1 THEN 1 ELSE 0 END), SUM(CASE WHEN e.confianca = 'alta' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN e.confianca = 'media' THEN 1 ELSE 0 END) " + de_ciclos_v2 +  # nosec B608
                ' GROUP BY e.codigoRecinto ORDER BY 2 DESC'):
            total, imagens = int(total or 0), int(imagens or 0)
            linhas.append((recinto, (nomes.get(recinto) or '')[:30], total, _pct(imagens, total), fora, alta, media))
            if total >= ESCANEAMENTOS_MINIMOS and imagens < IMAGEM_MINIMA * total:
                self.alertas.append(f'recinto {recinto}: só {_pct(imagens, total)} dos escaneamentos com imagem — '
                                    'conferir a defasagem de horário (imagens_ciclo.DEFASAGEM_HORAS)')
        self.tabela(['recinto', 'nome', 'escaneamentos', 'com imagem', 'fora', 'conf. alta', 'conf. média'], linhas)
        self.escreve()
        limites = {'h6': self.agora - timedelta(hours=6), 'd1': self.agora - timedelta(days=1),
                   'd3': self.agora - timedelta(days=3)}
        self.escreve('Escaneamentos SEM imagem por idade do escaneamento (imagem que chega depois ao Mongo só é '
                     'procurada de novo quando o contêiner é reprocessado):')
        self.tabela(['idade', 'sem imagem', 'em ciclos já encerrados'], self.consulta(
            "SELECT CASE WHEN e.data_escaneamento >= :h6 THEN '1) < 6 h' WHEN e.data_escaneamento >= :d1 "
            "THEN '2) 6 a 24 h' WHEN e.data_escaneamento >= :d3 THEN '3) 1 a 3 dias' ELSE '4) > 3 dias' END "
            "AS idade, COUNT(*), SUM(CASE WHEN c.situacao <> 'aberto' THEN 1 ELSE 0 END) " +  # nosec B608
            de_ciclos_v2 + ' AND e.imagem_id IS NULL GROUP BY idade ORDER BY idade', limites))

    # ---------------------------------------------------------------- 5
    def secao_ce(self):
        self.titulo('5. CE DOS CICLOS v2 (período)')
        self.tabela(['confiança do CE', 'ciclos'], self.consulta(
            "SELECT CASE WHEN c.vazio = 1 THEN '(vazio: manifesto)' "  # nosec B608
            "WHEN c.numeroCEmercante IS NULL THEN '(sem CE)' ELSE c.confianca_ce END AS conf, COUNT(*) "
            f'FROM {CICLO} c WHERE {CICLO_EM_PERIODO} AND {V2} GROUP BY conf ORDER BY 2 DESC'))
        self.escreve()
        limites = {'d2': self.agora - timedelta(days=2), 'd7': self.agora - timedelta(days=7)}
        self.tabela(['sem CE (não vazio), desembarque há', 'ciclos'], self.consulta(
            "SELECT CASE WHEN c.data_desembarque >= :d2 THEN '1) < 2 dias (Mercante pode atrasar)' "  # nosec B608
            "WHEN c.data_desembarque >= :d7 THEN '2) 2 a 7 dias' ELSE '3) > 7 dias' END AS idade, COUNT(*) "
            f'FROM {CICLO} c WHERE {CICLO_EM_PERIODO} AND {V2} AND c.numeroCEmercante IS NULL '
            'AND (c.vazio IS NULL OR c.vazio = 0) GROUP BY idade ORDER BY idade', limites))
        self.escreve()
        self.tabela(['papel na família', 'CEs', 'principal'], self.consulta(
            'SELECT cc.papel, COUNT(*), SUM(CASE WHEN cc.principal = 1 THEN 1 ELSE 0 END) '  # nosec B608
            f'FROM ciclodevida_ciclo_ce cc JOIN {CICLO} c ON c.ID = cc.ciclo_de_vida_id '
            f'WHERE {CICLO_EM_PERIODO} AND {V2} GROUP BY cc.papel ORDER BY 2 DESC'))

    # ---------------------------------------------------------------- 6
    def secao_pendentes_antigos(self):
        self.titulo('6. PENDENTES DO LOOP ANTIGO (todos) — reprocessar com --reprocessa N')
        linhas = self.consulta(
            'SELECT SUBSTR(CAST(c.data_desembarque AS CHAR), 1, 7) AS mes, COUNT(*) '  # nosec B608
            f'FROM {CICLO} c WHERE c.versao_heuristica IS NULL AND {PENDENTE} GROUP BY mes ORDER BY mes DESC')
        self.tabela(['mês do desembarque', 'pendentes'], linhas)
        self.escreve(f'Total: {sum(int(n or 0) for _, n in linhas)}')

    # ---------------------------------------------------------------- 7
    def secao_alertas(self):
        self.titulo('7. ALERTAS')
        for alerta in self.alertas or ['nenhum']:
            self.escreve(f'- {alerta}')


SECOES = ('secao_servico', 'secao_cobertura', 'secao_situacao', 'secao_escaneamentos', 'secao_ce',
          'secao_pendentes_antigos')


def roda(acompanhamento: Acompanhamento) -> List[str]:
    """Roda as seções (uma com erro não impede as outras) e termina nos alertas; devolve os alertas."""
    for nome in SECOES:
        try:
            getattr(acompanhamento, nome)()
        except Exception as err:  # noqa: o relatório deve seguir mesmo se uma seção falhar
            acompanhamento.escreve(f'!! ERRO na seção {nome}: {type(err).__name__}: {err}')
            acompanhamento.alertas.append(f'seção {nome} com erro: {err}')
    acompanhamento.secao_alertas()
    return acompanhamento.alertas


def main():  # pragma: no cover - depende do banco real
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    parser.add_argument('--dias', type=int, default=28, help='período: desembarques dos últimos N dias (padrão 28)')
    parser.add_argument('--saida', help='grava o relatório neste arquivo (padrão: só na tela)')
    args = parser.parse_args()
    engine = create_engine(uri_do_banco(args))
    fim = datetime.now()
    inicio = fim - timedelta(days=args.dias)
    saida = open(args.saida, 'w', encoding='utf-8') if args.saida else sys.stdout
    try:
        with engine.connect() as conexao:
            acompanhamento = Acompanhamento(conexao, saida, inicio, fim)
            acompanhamento.escreve(f'Acompanhamento do Ciclo de Vida v2 — {fim:%Y-%m-%d %H:%M}; desembarques de '
                                   f'{inicio:%Y-%m-%d} a {fim:%Y-%m-%d}; banco '
                                   f'{engine.url.render_as_string(hide_password=True)}')
            alertas = roda(acompanhamento)
    finally:
        if args.saida:
            saida.close()
    if args.saida:
        print(f'Relatório em {args.saida}; {len(alertas)} alerta(s)')


if __name__ == '__main__':  # pragma: no cover
    main()
