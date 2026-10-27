# coding: utf-8
"""
Diagnóstico das Fichas com resultado — base para o "risco histórico"
(dados_risco_perdimentos).

Antes de extrair filtros de risco do histórico do Fichas (embarcadores,
importadores, NCMs, rotas... onde já houve perdimento/apreensão), mede o que
os dados realmente oferecem:

  1. estrutura e volume das tabelas envolvidas (SHOW CREATE TABLE no MariaDB);
  2. resultados por tipo e ano (quantidade, fichas, valor);
  3. cobertura das fichas com resultado: CE, CNPJ fiscalizado, RVF, contêiner,
     TG/itens com NCM e marca, apreensões, eventos, processos, flags;
  4. casamento com o Mercante (CE encontrado, tráfego, contêineres, embarcador,
     consignatário/notify, baldeação, NCM) e se o CNPJ fiscalizado é o
     consignatário;
  5. o que os TEXTOS livres citam (CPF, CNPJ, contêiner, placa, NCM) — só
     contagens e exemplos MASCARADOS;
  6. distribuições não sensíveis (tipos de evento/apreensão/infração/
     mercadoria, flags, capítulos NCM, países e portos de origem) e
     concentração (quantos importadores/embarcadores/NCMs/rotas aparecem em
     2 ou mais fichas — o que viraria "filtro automático").

Somente leitura. CPF/CNPJ nunca saem completos no log (sigilo fiscal/LGPD).
Consultas portáveis (MariaDB em produção, SQLite nos testes).

Uso (na raiz do repositório virasana):
    python virasana/analytic_scripts/diagnostico_risco_perdimentos.py
    python virasana/analytic_scripts/diagnostico_risco_perdimentos.py --tipos 2 --inicio 2022-01-01
"""
import argparse
import re
import sys
import time
from collections import Counter
from datetime import datetime

from sqlalchemy import create_engine, text

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.dados_transporte_apreensoes import (  # noqa: E402
    cpf_valido, extrai_conteineres, extrai_cpfs, extrai_placas,
)

# bhadrasana.models.ovr.TipoResultado (não importado: carregaria o registry do Fichas)
TIPOS_RESULTADO = {1: 'Apreensao', 2: 'Perdimento', 3: 'Credito', 4: 'Sancao', 5: 'DARF',
                   6: 'MultaRFB', 7: 'MultaOutroOrgao'}
TIPOS_PADRAO = (1, 2)

TABELAS = [
    'ovr_ovrs', 'ovr_resultados', 'ovr_verificacoesfisicas', 'ovr_apreensoes_rvf', 'ovr_tiposapreensao',
    'ovr_infracoesencontradas', 'ovr_infracoes', 'ovr_marcasencontradas', 'ovr_marcas', 'ovr_tgovr',
    'ovr_itenstg', 'ovr_tiposmercadoria', 'ovr_eventos', 'ovr_tiposevento', 'ovr_processos',
    'ovr_flags_ovr', 'ovr_flags',
]

RE_CNPJ = re.compile(r'(?<![\d./-])(\d{2})\.?(\d{3})\.?(\d{3})/?(\d{4})-?(\d{2})(?![\d./-])')
RE_NCM = re.compile(r'\bNCM\s*:?\s*(\d{4}(?:\.?\d{2}){0,2})', re.IGNORECASE)


def cnpj_valido(cnpj: str) -> bool:
    if not re.fullmatch(r'\d{14}', cnpj or '') or cnpj == cnpj[0] * 14:
        return False
    for tamanho in (12, 13):
        pesos = list(range(tamanho - 7, 1, -1)) + list(range(9, 1, -1))
        soma = sum(int(d) * p for d, p in zip(cnpj[:tamanho], pesos))
        if (0 if soma % 11 < 2 else 11 - soma % 11) != int(cnpj[tamanho]):
            return False
    return True


def extrai_cnpjs(texto) -> list:
    """CNPJs (com ou sem máscara) citados no texto, só com dígitos verificadores válidos."""
    achados = (''.join(m) for m in RE_CNPJ.findall(texto or ''))
    return list(dict.fromkeys(c for c in achados if cnpj_valido(c)))


def extrai_ncms(texto) -> list:
    """NCMs citados como 'NCM 8471.30.12' (só os dígitos)."""
    return list(dict.fromkeys(re.sub(r'\D', '', m) for m in RE_NCM.findall(texto or '')))


def mascara(valor: str) -> str:
    """'52998224725' -> '529.***.***-25'; CNPJ mostra só a raiz parcial."""
    if len(valor) == 11:
        return f'{valor[:3]}.***.***-{valor[-2:]}'
    if len(valor) == 14:
        return f'{valor[:2]}.{valor[2:5]}.***/****-{valor[-2:]}'
    return valor[:3] + '*' * max(len(valor) - 5, 0) + valor[-2:]


def _lista_sql(valores) -> str:
    return ', '.join(str(int(v)) for v in valores)  # só inteiros: seguro montar no SQL


class Diagnostico:
    def __init__(self, conexao, saida, tipos=TIPOS_PADRAO, inicio=None, fim=None):
        self.conexao = conexao
        self.saida = saida
        self.mysql = conexao.dialect.name == 'mysql'
        # Universo: fichas com resultado dos tipos escolhidos, no período (data da ficha)
        filtro_data = ''
        self.params = {}
        if inicio:
            filtro_data += ' AND o.datahora >= :inicio'
            self.params['inicio'] = inicio
        if fim:
            filtro_data += ' AND o.datahora < :fim'
            self.params['fim'] = fim
        # só inteiros entram no SQL; as datas vão como parâmetro
        self.fichas = (f'(SELECT DISTINCT r.ovr_id AS id FROM ovr_resultados r '  # nosec
                       f'JOIN ovr_ovrs o ON o.id = r.ovr_id '
                       f'WHERE r.tipo_resultado IN ({_lista_sql(tipos)}){filtro_data})')
        self.tipos = tipos

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

    def consulta(self, sql, params=None):
        """Linhas da consulta; em erro, registra e devolve [] (o diagnóstico segue)."""
        inicio = time.perf_counter()
        try:
            linhas = [tuple(linha) for linha in self.conexao.execute(text(sql), {**self.params, **(params or {})})]
        except Exception as err:  # noqa: o log deve seguir mesmo se um item falhar
            self.escreve(f'!! ERRO [{" ".join(sql.split())[:90]}...]: {err}')
            return []
        segundos = time.perf_counter() - inicio
        if segundos > 5:
            self.escreve(f'   (consulta levou {segundos:.1f} s)')
        return linhas

    def numero(self, sql) -> int:
        linhas = self.consulta(sql)
        return int(linhas[0][0] or 0) if linhas else 0

    # ---------------------------------------------------------------- seções
    def secao_estrutura(self):
        self.titulo('1. ESTRUTURA E VOLUME DAS TABELAS')
        if not self.mysql:
            self.escreve('(SHOW CREATE TABLE só no MariaDB/MySQL)')
            return
        lista = ', '.join(f"'{t}'" for t in TABELAS)
        self.tabela(['tabela', 'linhas (estimativa)', 'dados_mb', 'indices_mb'], self.consulta(
            f'SELECT TABLE_NAME, TABLE_ROWS, ROUND(DATA_LENGTH/1048576), ROUND(INDEX_LENGTH/1048576) '  # nosec
            f'FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ({lista}) '
            f'ORDER BY TABLE_ROWS DESC'))
        for tabela in TABELAS:
            # nomes de tabela vêm da constante TABELAS
            for linha in self.consulta(f'SHOW CREATE TABLE {tabela}'):  # nosec B608
                self.escreve()
                self.escreve(linha[1])

    def secao_resultados(self):
        self.titulo('2. RESULTADOS POR TIPO E ANO (todas as fichas, sem filtro de tipo/período)')
        linhas = self.consulta(
            'SELECT r.tipo_resultado, SUBSTR(CAST(o.datahora AS CHAR), 1, 4) AS ano_ficha, COUNT(*) AS resultados, '
            'COUNT(DISTINCT r.ovr_id) AS fichas, SUM(r.valor) AS valor '
            'FROM ovr_resultados r JOIN ovr_ovrs o ON o.id = r.ovr_id GROUP BY r.tipo_resultado, ano_ficha '
            'ORDER BY ano_ficha, r.tipo_resultado')
        self.tabela(['tipo', 'ano', 'resultados', 'fichas', 'valor'],
                    [(f'{t} {TIPOS_RESULTADO.get(t, "?")}', ano, n, f, round(v or 0, 2)) for t, ano, n, f, v in linhas])
        self.escreve()
        self.escreve(f'Universo das próximas seções: fichas com resultado dos tipos '
                     f'{[TIPOS_RESULTADO.get(t, t) for t in self.tipos]}'
                     f'{" no período informado" if self.params else ""}.')

    def _cobertura(self, itens, total):
        linhas = []
        for descricao, sql in itens:
            n = self.numero(sql)
            linhas.append((descricao, n, f'{100 * n / total:.1f}%' if total else '-'))
        self.tabela(['item', 'fichas', '%'], linhas)

    def secao_cobertura(self):
        self.titulo('3. COBERTURA DAS FICHAS COM RESULTADO')
        f = self.fichas
        total = self.numero(f'SELECT COUNT(*) FROM {f} fr')  # nosec
        self.escreve(f'Fichas no universo: {total}')
        self.escreve()

        def existe(tabela, condicao='1=1', chave='ovr_id'):
            return (f'SELECT COUNT(*) FROM {f} fr WHERE EXISTS (SELECT 1 FROM {tabela} x '  # nosec
                    f'WHERE x.{chave} = fr.id AND {condicao})')

        def na_ficha(condicao):
            return f'SELECT COUNT(*) FROM {f} fr JOIN ovr_ovrs o ON o.id = fr.id WHERE {condicao}'  # nosec

        rvf = 'ovr_verificacoesfisicas'
        self._cobertura([
            ('CE na ficha (15 dígitos)', na_ficha('LENGTH(TRIM(o.numeroCEmercante)) = 15')),
            ('CNPJ fiscalizado (8+ dígitos)', na_ficha('LENGTH(TRIM(o.cnpj_fiscalizado)) >= 8')),
            ('observações preenchidas', na_ficha('LENGTH(TRIM(o.observacoes)) > 0')),
            ('com RVF', existe(rvf)),
            ('RVF com CE', existe(rvf, 'LENGTH(TRIM(x.numeroCEmercante)) = 15')),
            ('RVF com lote/contêiner preenchido', existe(rvf, 'LENGTH(TRIM(x.numerolote)) > 0')),
            ('RVF com descrição', existe(rvf, 'LENGTH(TRIM(x.descricao)) > 0')),
            ('RVF com apreensão', f'SELECT COUNT(*) FROM {f} fr WHERE EXISTS (SELECT 1 FROM {rvf} v '  # nosec
                                  f'JOIN ovr_apreensoes_rvf a ON a.rvf_id = v.id WHERE v.ovr_id = fr.id)'),
            ('RVF com infração', f'SELECT COUNT(*) FROM {f} fr WHERE EXISTS (SELECT 1 FROM {rvf} v '  # nosec
                                 f'JOIN ovr_infracoesencontradas i ON i.rvf_id = v.id WHERE v.ovr_id = fr.id)'),
            ('RVF com marca', f'SELECT COUNT(*) FROM {f} fr WHERE EXISTS (SELECT 1 FROM {rvf} v '  # nosec
                              f'JOIN ovr_marcasencontradas m ON m.rvf_id = v.id WHERE v.ovr_id = fr.id)'),
            ('com TG', existe('ovr_tgovr')),
            ('TG com item com NCM', f'SELECT COUNT(*) FROM {f} fr WHERE EXISTS (SELECT 1 FROM ovr_tgovr t '  # nosec
                                    f'JOIN ovr_itenstg i ON i.tg_id = t.id WHERE t.ovr_id = fr.id '
                                    f'AND LENGTH(TRIM(i.ncm)) >= 4)'),
            ('TG com item com marca', f'SELECT COUNT(*) FROM {f} fr WHERE EXISTS (SELECT 1 FROM ovr_tgovr t '  # nosec
                                      f'JOIN ovr_itenstg i ON i.tg_id = t.id WHERE t.ovr_id = fr.id '
                                      f'AND i.marca_id IS NOT NULL)'),
            ('com evento com motivo', existe('ovr_eventos', 'LENGTH(TRIM(x.motivo)) > 0')),
            ('com processo', existe('ovr_processos')),
            ('com flag', existe('ovr_flags_ovr', chave='rvf_id')),  # a coluna se chama rvf_id, mas é a ficha
        ], total)

    def secao_mercante(self):
        self.titulo('4. CASAMENTO COM O MERCANTE (fichas com CE na ficha ou na RVF)')
        ces = (f'(SELECT DISTINCT fr.id AS ficha, TRIM(o.numeroCEmercante) AS ce FROM {self.fichas} fr '  # nosec
               f'JOIN ovr_ovrs o ON o.id = fr.id WHERE LENGTH(TRIM(o.numeroCEmercante)) = 15 '
               f'UNION SELECT DISTINCT fr.id, TRIM(v.numeroCEmercante) FROM {self.fichas} fr '
               f'JOIN ovr_verificacoesfisicas v ON v.ovr_id = fr.id WHERE LENGTH(TRIM(v.numeroCEmercante)) = 15)')
        base = f'FROM {ces} fc JOIN conhecimentosresumo c ON c.numeroCEmercante = fc.ce'  # nosec
        total_ces = self.numero(f'SELECT COUNT(DISTINCT ce) FROM {ces} fc')  # nosec
        fichas_ce = self.numero(f'SELECT COUNT(DISTINCT ficha) FROM {ces} fc')  # nosec
        self.escreve(f'Fichas com CE: {fichas_ce}; CEs distintos: {total_ces}')
        self.escreve()
        itens = [
            ('CE encontrado em conhecimentosresumo', f'SELECT COUNT(DISTINCT fc.ce) {base}'),
            ('  com contêiner em itensresumo',
             f'SELECT COUNT(DISTINCT fc.ce) {base} WHERE EXISTS (SELECT 1 FROM '  # nosec B608
             f'itensresumo i WHERE i.numeroCEmercante = fc.ce '
             f"AND i.codigoConteiner <> '')"),
            ('  com NCM em ncmitemresumo',
             f'SELECT COUNT(DISTINCT fc.ce) {base} WHERE EXISTS (SELECT 1 FROM '  # nosec B608
             f'ncmitemresumo n WHERE n.numeroCEMercante = fc.ce)'),
            ('  embarcador preenchido', f'SELECT COUNT(DISTINCT fc.ce) {base} WHERE LENGTH(TRIM(c.embarcador)) > 0'),
            ('  consignatário com 14 dígitos', f'SELECT COUNT(DISTINCT fc.ce) {base} '
                                               f'WHERE LENGTH(TRIM(c.consignatario)) = 14'),
            ('  notify preenchido', f'SELECT COUNT(DISTINCT fc.ce) {base} WHERE LENGTH(TRIM(c.cnpjNotify)) > 0'),
            ('  baldeação (manifesto carregado fora da origem)',
             f'SELECT COUNT(DISTINCT fc.ce) {base} JOIN manifestosresumo m ON m.numero = c.manifestoCE '
             f'WHERE m.portoCarregamento <> c.portoOrigemCarga'),
        ]
        contagens = [(descricao, self.numero(sql)) for descricao, sql in itens]
        self.tabela(['item', 'CEs', '%'],
                    [(d, n, f'{100 * n / total_ces:.1f}%' if total_ces else '-') for d, n in contagens])
        self.escreve()
        self.escreve('CNPJ fiscalizado x partes do CE (raiz de 8 dígitos):')
        papeis = self.consulta(
            f'SELECT SUM(CASE WHEN SUBSTR(TRIM(o.cnpj_fiscalizado), 1, 8) = '  # nosec
            f'SUBSTR(TRIM(c.consignatario), 1, 8) THEN 1 ELSE 0 END), '
            f'SUM(CASE WHEN SUBSTR(TRIM(o.cnpj_fiscalizado), 1, 8) = '
            f'SUBSTR(TRIM(c.cnpjNotify), 1, 8) THEN 1 ELSE 0 END), COUNT(*) '
            f'{base} JOIN ovr_ovrs o ON o.id = fc.ficha WHERE LENGTH(TRIM(o.cnpj_fiscalizado)) >= 8')
        if papeis:
            consignatario, notify, total = (int(v or 0) for v in papeis[0])
            self.tabela(['fiscalizado é...', 'fichas x CE', 'de'], [
                ('o consignatário', consignatario, total), ('o notify', notify, total)])
        self.escreve()
        self.escreve('Tráfego dos CEs:')
        self.tabela(['tipoTrafego', 'CEs'], self.consulta(
            f'SELECT c.tipoTrafego, COUNT(DISTINCT fc.ce) {base} GROUP BY c.tipoTrafego ORDER BY 2 DESC'))
        self.ces = ces
        self.base_ce = base

    def secao_textos(self):
        self.titulo('5. O QUE OS TEXTOS LIVRES CITAM (exemplos mascarados)')
        f = self.fichas
        textos = {}
        fontes = [
            ('observações da ficha',
             f'SELECT fr.id, o.observacoes FROM {f} fr JOIN ovr_ovrs o ON o.id = fr.id'),  # nosec B608
            ('descrição da RVF',
             f'SELECT v.ovr_id, v.descricao FROM {f} fr '  # nosec B608
             f'JOIN ovr_verificacoesfisicas v ON v.ovr_id = fr.id'),
            ('descrição interna da RVF',
             f'SELECT v.ovr_id, v.descricao_interna FROM {f} fr '  # nosec B608
             f'JOIN ovr_verificacoesfisicas v ON v.ovr_id = fr.id'),
            ('motivo dos eventos',
             f'SELECT e.ovr_id, e.motivo FROM {f} fr JOIN ovr_eventos e ON e.ovr_id = fr.id'),  # nosec B608
            ('TG (descrição/observações)',
             f"SELECT t.ovr_id, COALESCE(t.descricao, '') || ' ' || "  # nosec B608
             f"COALESCE(t.observacoes, '') FROM {f} fr JOIN ovr_tgovr t ON t.ovr_id = fr.id"),
        ]
        if self.mysql:  # MySQL não tem || para concatenar
            fontes[-1] = ('TG (descrição/observações)',
                          f"SELECT t.ovr_id, CONCAT_WS(' ', t.descricao, t.observacoes) "  # nosec B608
                          f'FROM {f} fr JOIN ovr_tgovr t ON t.ovr_id = fr.id')
        extratores = {'CPF': extrai_cpfs, 'CNPJ': extrai_cnpjs, 'contêiner': extrai_conteineres,
                      'placa': extrai_placas, 'NCM citado': extrai_ncms}
        por_fonte = []
        fichas_por_tipo = {tipo: set() for tipo in extratores}
        exemplos = {tipo: [] for tipo in extratores}
        for nome_fonte, sql in fontes:
            contagem = Counter()
            for ficha, texto in self.consulta(sql):
                textos.setdefault(ficha, []).append(texto or '')
                for tipo, extrai in extratores.items():
                    achados = extrai(texto or '')
                    if achados:
                        contagem[tipo] += 1
                        fichas_por_tipo[tipo].add(ficha)
                        if len(exemplos[tipo]) < 3:
                            exemplos[tipo].append(achados[0])
            por_fonte.append([nome_fonte] + [contagem[t] for t in extratores])
        self.tabela(['fonte (registros com citação)'] + list(extratores), por_fonte)
        self.escreve()
        self.tabela(['citação', 'fichas', 'exemplos'],
                    [(tipo, len(fichas_por_tipo[tipo]),
                      ', '.join(mascara(e) if tipo in ('CPF', 'CNPJ') else e for e in exemplos[tipo]))
                     for tipo in extratores])
        cpfs_falsos = sum(1 for t in textos.values() for c in re.findall(r'\b\d{11}\b', ' '.join(t))
                          if not cpf_valido(c))
        self.escreve()
        self.escreve(f'Sequências de 11 dígitos SEM dígito verificador de CPF válido (descartadas): {cpfs_falsos}')

    def _top(self, titulo, sql, colunas=('valor', 'fichas'), limite=15):
        self.escreve()
        self.escreve(titulo)
        self.tabela(list(colunas), self.consulta(sql)[:limite])

    def secao_distribuicoes(self):
        self.titulo('6. DISTRIBUIÇÕES (não sensíveis) E CONCENTRAÇÃO')
        f = self.fichas
        self._top('Tipos de evento mais frequentes nas fichas:',
                  f'SELECT t.nome, COUNT(DISTINCT e.ovr_id) FROM {f} fr '  # nosec
                  f'JOIN ovr_eventos e ON e.ovr_id = fr.id JOIN ovr_tiposevento t ON t.id = e.tipoevento_id '
                  f'GROUP BY t.nome ORDER BY 2 DESC')
        self._top('Tipos de apreensão:',
                  f'SELECT t.descricao, COUNT(DISTINCT v.ovr_id) FROM {f} fr JOIN ovr_verificacoesfisicas v '  # nosec
                  f'ON v.ovr_id = fr.id JOIN ovr_apreensoes_rvf a ON a.rvf_id = v.id '
                  f'JOIN ovr_tiposapreensao t ON t.id = a.tipo_id GROUP BY t.descricao ORDER BY 2 DESC')
        self._top('Infrações:',
                  f'SELECT i.nome, COUNT(DISTINCT v.ovr_id) FROM {f} fr JOIN ovr_verificacoesfisicas v '  # nosec
                  f'ON v.ovr_id = fr.id JOIN ovr_infracoesencontradas ie ON ie.rvf_id = v.id '
                  f'JOIN ovr_infracoes i ON i.id = ie.infracao_id GROUP BY i.nome ORDER BY 2 DESC')
        self._top('Tipos de mercadoria do TG:',
                  f'SELECT m.nome, COUNT(DISTINCT t.ovr_id) FROM {f} fr JOIN ovr_tgovr t ON t.ovr_id = fr.id '  # nosec
                  f'JOIN ovr_tiposmercadoria m ON m.id = t.tipomercadoria_id GROUP BY m.nome ORDER BY 2 DESC')
        self._top('Flags:',
                  f'SELECT fl.nome, COUNT(DISTINCT fo.rvf_id) FROM {f} fr JOIN ovr_flags_ovr fo '  # nosec
                  f'ON fo.rvf_id = fr.id JOIN ovr_flags fl ON fl.id = fo.flag_id GROUP BY fl.nome ORDER BY 2 DESC')
        self._top('Capítulos NCM dos itens do TG:',
                  f'SELECT SUBSTR(TRIM(i.ncm), 1, 2), COUNT(DISTINCT t.ovr_id) FROM {f} fr JOIN ovr_tgovr t '  # nosec
                  f'ON t.ovr_id = fr.id JOIN ovr_itenstg i ON i.tg_id = t.id WHERE LENGTH(TRIM(i.ncm)) >= 2 '
                  f'GROUP BY SUBSTR(TRIM(i.ncm), 1, 2) ORDER BY 2 DESC')
        if hasattr(self, 'ces'):
            base = self.base_ce
            self._top('Países de origem da carga (CE):',
                      f'SELECT SUBSTR(c.portoOrigemCarga, 1, 2), COUNT(DISTINCT fc.ficha) {base} '
                      f'GROUP BY SUBSTR(c.portoOrigemCarga, 1, 2) ORDER BY 2 DESC')
            self._top('Portos de origem da carga (CE):',
                      f'SELECT c.portoOrigemCarga, COUNT(DISTINCT fc.ficha) {base} '
                      f'GROUP BY c.portoOrigemCarga ORDER BY 2 DESC')
            self._top('Capítulos NCM declarados no CE:',
                      f'SELECT SUBSTR(n.identificacaoNCM, 1, 2), COUNT(DISTINCT fc.ficha) {base} '
                      f'JOIN ncmitemresumo n ON n.numeroCEMercante = fc.ce '
                      f'GROUP BY SUBSTR(n.identificacaoNCM, 1, 2) ORDER BY 2 DESC')
            self.escreve()
            self.escreve('Concentração — quantos valores distintos e quantos aparecem em 2+ fichas '
                         '(candidatos a filtro automático):')
            dimensoes = [
                ('importador (raiz CNPJ do consignatário)', 'SUBSTR(TRIM(c.consignatario), 1, 8)',
                 'LENGTH(TRIM(c.consignatario)) = 14', ''),
                ('embarcador (nome)', 'UPPER(TRIM(c.embarcador))', 'LENGTH(TRIM(c.embarcador)) > 0', ''),
                ('notify (raiz)', 'SUBSTR(TRIM(c.cnpjNotify), 1, 8)', 'LENGTH(TRIM(c.cnpjNotify)) >= 8', ''),
                ('rota (origem>destino)', "c.portoOrigemCarga || '>' || c.portoDestFinal",
                 'c.portoOrigemCarga IS NOT NULL', ''),
                ('posição NCM (CE)', 'SUBSTR(n.identificacaoNCM, 1, 4)', 'LENGTH(n.identificacaoNCM) >= 4',
                 'JOIN ncmitemresumo n ON n.numeroCEMercante = fc.ce'),
            ]
            linhas = []
            for nome, expressao, condicao, juncao in dimensoes:
                if self.mysql:
                    expressao = expressao.replace("c.portoOrigemCarga || '>' || c.portoDestFinal",
                                                  "CONCAT(c.portoOrigemCarga, '>', c.portoDestFinal)")
                contagem = self.consulta(
                    f'SELECT COUNT(*), SUM(CASE WHEN q.fichas >= 2 THEN 1 ELSE 0 END), MAX(q.fichas) FROM '  # nosec
                    f'(SELECT {expressao} AS valor, COUNT(DISTINCT fc.ficha) AS fichas {base} {juncao} '
                    f'WHERE {condicao} GROUP BY {expressao}) q')
                if contagem:
                    distintos, repetidos, maximo = contagem[0]
                    linhas.append((nome, distintos, int(repetidos or 0), maximo))
            self.tabela(['dimensão', 'distintos', 'em 2+ fichas', 'máx. fichas'], linhas)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--tipos', default=','.join(map(str, TIPOS_PADRAO)),
                        help='tipos de resultado do universo, separados por vírgula (padrão 1,2 = '
                             'Apreensão e Perdimento; 1..7, ver TIPOS_RESULTADO)')
    parser.add_argument('--inicio', help="data inicial da ficha ('YYYY-MM-DD')")
    parser.add_argument('--fim', help="data final da ficha, exclusiva ('YYYY-MM-DD')")
    parser.add_argument('--saida', default=datetime.now().strftime('risco_perdimentos_diagnostico_%Y%m%d_%H%M%S.log'))
    args = parser.parse_args()
    uri = args.uri
    if not uri:
        from ajna_commons.flask.conf import SQL_URI as uri
    tipos = tuple(int(t) for t in args.tipos.split(','))
    inicio = datetime.strptime(args.inicio, '%Y-%m-%d') if args.inicio else None
    fim = datetime.strptime(args.fim, '%Y-%m-%d') if args.fim else None
    engine = create_engine(uri)
    with engine.connect() as conexao, open(args.saida, 'w', encoding='utf-8') as saida:
        diag = Diagnostico(conexao, saida, tipos, inicio, fim)
        diag.escreve(f'Diagnóstico do risco histórico (Fichas com resultado) — {datetime.now():%Y-%m-%d %H:%M:%S}')
        diag.escreve(f'Banco: {engine.url.render_as_string(hide_password=True)}')
        for secao in (diag.secao_estrutura, diag.secao_resultados, diag.secao_cobertura, diag.secao_mercante,
                      diag.secao_textos, diag.secao_distribuicoes):
            inicio_secao = time.perf_counter()
            secao()
            diag.escreve(f'   [seção em {time.perf_counter() - inicio_secao:.1f} s]')
            saida.flush()
    print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
