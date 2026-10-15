# coding: utf-8
"""Testes do DIAGNÓSTICO do operador em papel de TRA (diagnostico_operador_tra.py): percurso e classes (função
pura) e o relatório inteiro sobre o cenário do serviço do ciclo de vida, com três contêineres novos:

  COTR  desembarca na BTP, sai, entra no ECOPORTO, é escaneado e fica 5 dias  -> "final (TRA?)"
  CPAS  desembarca na BTP, sai, passa 40 min no ECOPORTO (escaneia) e vai ao EADI -> "passagem" (EADI = TRA)
  CABR  desembarca no ECOPORTO, sai e entra na BTP, onde continua          -> "em aberto"
"""
import io
import re
import unittest

from sqlalchemy import text

from virasana.analytic_scripts.ciclo_de_vida import diagnostico_operador_tra as dot
from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
from virasana.analytic_scripts.ciclo_de_vida import servico_ciclo_de_vida as sv
from virasana.tests.test_servico_ciclo_de_vida import (
    AGORA, BTP, DESDE, EADI, ECO, Relogio, _evento, _insere, d, monta_banco,
)

from bhadrasana.models.apirecintos import AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva  # noqa: I100

OPERADORES = {BTP, ECO}


def ev(tipo, id_, data, recinto, vazio=None):
    return ldt.Evento(tipo, id_, data, recinto, vazio)


def caminho(*eventos, agora=AGORA):
    """Percurso do primeiro evento (o desembarque) e a classe de cada estada depois dele."""
    ordenados = sorted(eventos, key=lambda e: (e.data, e.id))
    resultado = dot.percurso('C1', ordenados[1:], ordenados[0], OPERADORES)
    return resultado, [dot.classe(resultado, i, agora) for i in range(1, len(resultado.estadas))]


DESEMB = ev(ldt.DESEMBARQUE, 1, d(2), BTP)
SAI_BTP = ev(ldt.SAIDA, 2, d(3), BTP)


class TestPercurso(unittest.TestCase):

    def test_passagem_pelo_operador_e_tra(self):
        resultado, classes = caminho(DESEMB, SAI_BTP, ev(ldt.ENTRADA, 3, d(3, 9), ECO),
                                     ev(ldt.INSPECAO, 4, d(3, 9, 20), ECO), ev(ldt.SAIDA, 5, d(3, 9, 40), ECO),
                                     ev(ldt.ENTRADA, 6, d(3, 11), EADI), ev(ldt.SAIDA, 7, d(6), EADI))
        self.assertEqual(classes, [dot.PASSAGEM, dot.FINAL])
        self.assertEqual([e.recinto for e in resultado.estadas], [BTP, ECO, EADI])
        self.assertEqual([len(e.inspecoes) for e in resultado.estadas], [0, 1, 0])
        self.assertEqual(resultado.estadas[1].faixa(), '30 min–3 h')
        self.assertEqual(resultado.corte, dot.CORTE_HOJE)
        self.assertEqual(dot.padrao(resultado), 'OP → OP2(30 min–3 h, insp) → SECO(1–3 dias)')

    def test_operador_como_tra_e_scanner_no_gate(self):
        resultado, classes = caminho(DESEMB, SAI_BTP, ev(ldt.INSPECAO, 3, d(3, 9), ECO),
                                     ev(ldt.ENTRADA, 4, d(3, 10), ECO), ev(ldt.SAIDA, 5, d(9), ECO))
        self.assertEqual(classes, [dot.FINAL])
        self.assertEqual(len(resultado.estadas[1].inspecoes), 1)   # 1 h antes da entrada: gate
        self.assertEqual(resultado.estadas[1].faixa(), '3–10 dias')

    def test_estada_curta_final_saiu_agora_e_em_aberto(self):
        self.assertEqual(caminho(DESEMB, SAI_BTP, ev(ldt.ENTRADA, 3, d(3, 9), ECO), ev(ldt.SAIDA, 4, d(3, 10), ECO))[1],
                         [dot.FINAL_CURTA])
        self.assertEqual(caminho(DESEMB, SAI_BTP, ev(ldt.ENTRADA, 3, d(3, 9), ECO), ev(ldt.SAIDA, 4, d(9), ECO),
                                 agora=d(10))[1], [dot.SAIU_AGORA])
        self.assertEqual(caminho(DESEMB, SAI_BTP, ev(ldt.ENTRADA, 3, d(3, 9), ECO))[1], [dot.EM_ABERTO])

    def test_longa_e_segue_baldeacao_e_saida_omitida(self):
        self.assertEqual(caminho(DESEMB, SAI_BTP, ev(ldt.ENTRADA, 3, d(3, 9), ECO), ev(ldt.SAIDA, 4, d(4), ECO),
                                 ev(ldt.ENTRADA, 5, d(4, 10), EADI))[1], [dot.LONGA_E_SEGUE, dot.EM_ABERTO])
        self.assertEqual(caminho(DESEMB, SAI_BTP, ev(ldt.ENTRADA, 3, d(3, 9), ECO),
                                 ev(ldt.EMBARQUE, 4, d(5), ECO))[1], [dot.BALDEACAO])
        self.assertEqual(caminho(DESEMB, SAI_BTP, ev(ldt.ENTRADA, 3, d(3, 9), ECO),
                                 ev(ldt.ENTRADA, 4, d(4), EADI))[1], [dot.SAIDA_OMITIDA, dot.EM_ABERTO])

    def test_entrada_nao_informada(self):
        resultado, classes = caminho(DESEMB, SAI_BTP, ev(ldt.INSPECAO, 3, d(3, 9), ECO), ev(ldt.SAIDA, 4, d(3, 11), ECO))
        self.assertEqual(classes, [dot.SEM_ENTRADA])
        self.assertEqual((resultado.estadas[1].faixa(), len(resultado.estadas[1].inspecoes)),
                         (dot.SEM_ENTRADA_INFORMADA, 1))

    def test_fronteiras(self):
        for evento, corte in ((ev(ldt.ENTRADA, 5, d(5), EADI, vazio=True), 'entrada com o contêiner vazio'),
                              (ev(ldt.ENTRADA, 5, d(5), BTP), 'voltou ao operador do desembarque'),
                              (ev(ldt.DESEMBARQUE, 5, d(5), BTP), 'novo desembarque')):
            with self.subTest(corte=corte):
                resultado, classes = caminho(DESEMB, SAI_BTP, ev(ldt.ENTRADA, 3, d(3, 9), ECO), evento)
                self.assertEqual((resultado.corte, classes), (corte, [dot.CORTADA]))
        resultado, classes = caminho(DESEMB, SAI_BTP, ev(ldt.ENTRADA, 3, d(9), EADI))
        self.assertEqual((resultado.corte, classes), ('entrada mais de 48 h depois da saída', []))

    def test_faixa(self):
        self.assertEqual([dot.faixa(h) for h in (0.2, 2, 5, 12, 48, 100, 500)],
                         ['≤ 30 min', '30 min–3 h', '3–6 h', '6–24 h', '1–3 dias', '3–10 dias', '> 10 dias'])


def acrescenta_cenario(engine):
    desembarque, acesso = dict(embarqueDesembarque='D'), dict(operacao='C', vazioConteiner=False)
    with engine.begin() as c:
        _insere(c, EmbarqueDesembarque, [_evento(501, 'COTR', d(3), BTP, **desembarque),
                                         _evento(502, 'CPAS', d(5), BTP, **desembarque),
                                         _evento(503, 'CABR', d(7), ECO, **desembarque)])
        _insere(c, AcessoVeiculo, [
            _evento(601, 'COTR', d(4), BTP, direcao='S', **acesso),
            _evento(602, 'COTR', d(4, 10), ECO, direcao='E', **acesso),
            _evento(603, 'COTR', d(9), ECO, direcao='S', **acesso),
            _evento(604, 'CPAS', d(6), BTP, direcao='S', **acesso),
            _evento(605, 'CPAS', d(6, 9), ECO, direcao='E', **acesso),
            _evento(606, 'CPAS', d(6, 9, 40), ECO, direcao='S', **acesso),
            _evento(607, 'CPAS', d(6, 11), EADI, direcao='E', **acesso),
            _evento(608, 'CPAS', d(9), EADI, direcao='S', **acesso),
            _evento(609, 'CABR', d(8), ECO, direcao='S', **acesso),
            _evento(610, 'CABR', d(8, 10), BTP, direcao='E', **acesso)])
        _insere(c, InspecaoNaoInvasiva, [_evento(701, 'COTR', d(4, 11), ECO, vazio=False),
                                         _evento(702, 'CPAS', d(6, 9, 20), ECO, vazio=False)])


def linha(texto: str, *celulas) -> bool:
    """Alguma linha de tabela com estas células, nesta ordem (espaços de alinhamento livres)."""
    padrao = r'\|'.join(r'\s*' + re.escape(str(c)) + r'\s*' for c in celulas)
    return re.search(r'(^|\n)' + padrao + r'(\||\n|$)', texto) is not None


class TestRelatorio(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine, cls.mongo = monta_banco()
        acrescenta_cenario(cls.engine)
        sv.Servico(cls.engine, cls.mongo, relogio=Relogio(AGORA)).uma_volta(DESDE)
        saida = io.StringIO()
        with cls.engine.connect() as conexao:
            cls.diag = dot.DiagnosticoOperadorTra(conexao, saida, d(1), d(10), AGORA, amostras=2)
            dot.roda(cls.diag)
        cls.texto = saida.getvalue()

    def test_sem_erro_e_volume(self):
        self.assertNotIn('ERRO', self.texto)
        self.assertTrue(linha(self.texto, '8931359 BTP', 3, 2, '67%'), self.texto)
        self.assertTrue(linha(self.texto, '8931318 ECOPORTO', 2, 1, '50%'))
        self.assertIn('Contêineres candidatos: 3; analisados: 3', self.texto)
        self.assertEqual(len(self.diag.percursos), 3)

    def test_classes_e_pares(self):
        self.assertTrue(linha(self.texto, 'passagem', 1, '100%', '0.7 h', 0, '-', '-'))
        self.assertTrue(linha(self.texto, 'final (TRA?)', 1, '100%', '4.9 d', 1, '0%', '2.9 d'))
        self.assertTrue(linha(self.texto, 'em aberto', 1, '0%', '-', 0, '-', '-'))
        self.assertTrue(linha(self.texto, '8931359 BTP → 8931318 ECOPORTO', 2, '100%', 1, 1, 0, 0, 0, 0, 0))

    def test_como_foi_gravado_e_completude(self):
        # o ECOPORTO como TRA: gravado como entregue pela BTP, com o escaneamento de lá marcado "fora"
        self.assertTrue(linha(self.texto, 'final (TRA?)', 'operador/concluido', 1, 0, 1, 0, 0), self.texto)
        self.assertTrue(linha(self.texto, 'passagem', 'tra/concluido', 1, 0, 1, 0, 0))
        self.assertTrue(linha(self.texto, 'em aberto', 'indefinido/aberto', 1, 0, 0, 0, 1))
        self.assertTrue(linha(self.texto, '8931318 ECOPORTO', 'final (TRA?)', 1, 1, 1, 0, 0, 0))
        self.assertTrue(linha(self.texto, '8931359 BTP', 'em aberto', 1, 0, 0, 0, 0, 1))

    def test_referencia_padroes_dta_e_exemplos(self):
        self.assertTrue(linha(self.texto, '1–3 dias', 1, '100%'))      # o EADI do CPAS
        self.assertTrue(linha(self.texto, 'OP → OP2(3–10 dias, insp)', 1))
        self.assertIn('(tabela navii_dtas não existe neste banco)', self.texto)
        self.assertIn('--- final (TRA?) em 8931318 ECOPORTO (3–10 dias)', self.texto)
        self.assertIn('COTR — desembarque 03/03 08:00 em 8931359 BTP; corte: eventos até hoje; gravado: '
                      'operador/concluido — entregue pelo operador', self.texto)
        self.assertIn('04/03 11:00  inspecao    8931318 ECOPORTO', self.texto)


class TestDtas(unittest.TestCase):
    """Com a tabela navii_dtas: CE do ciclo do COTR com uma DTA."""

    def test_dta_por_classe(self):
        from virasana.navii import dta_navii
        engine, mongo = monta_banco()
        acrescenta_cenario(engine)
        sv.Servico(engine, mongo, relogio=Relogio(AGORA)).uma_volta(DESDE)
        dta_navii.DTAS.create(engine)
        with engine.begin() as c:
            c.execute(text("UPDATE ciclodevida_ciclo_de_vida_importacao SET numeroCEmercante = '152600000000077' "
                           'WHERE desembarque_id = 501'))
            c.execute(dta_navii.DTAS.insert().values(nr_ce='152600000000077', codigo_declaracao_transito='2600000001',
                                                     descricao_tipo_declaracao_transito='DTA - PASSAGEM'))
        saida = io.StringIO()
        with engine.connect() as conexao:
            diag = dot.DiagnosticoOperadorTra(conexao, saida, d(1), d(10), AGORA)
            diag.monta_percursos(['COTR'])
            diag.secao_dtas()
        self.assertTrue(linha(saida.getvalue(), 'final (TRA?)', 1, 1, '100%', 'DTA - PASSAGEM (1)'), saida.getvalue())


if __name__ == '__main__':
    unittest.main()
