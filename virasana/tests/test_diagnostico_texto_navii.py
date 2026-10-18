"""Testes do diagnóstico da busca por texto (analytic_scripts/diagnostico_texto_navii.py) — F0.

Agora = 17/10/2026 12h; 12 meses de CEs por mês; campos e amostra dos últimos 3 meses (desde 18/07/2026).
  ID 1  01/09/2025 (fora dos 12 meses)
  ID 10 10/05/2026 importação (só na contagem por mês)
  ID 20 01/08 importação; embarcador CNPJ com nome; consignatário estrangeiro; notify com e-mail, telefone e endereço;
        M1 (BRRIG 05/08 e BRSSZ 10/08: a referência é a de Santos, 9 dias depois); 2 contêineres
  ID 21 15/08 importação; consignatário CNPJ SEM nome; M2 (só BRRIG 20/08: a primeira escala, 5 dias); sem contêiner
  ID 22 01/09 EXPORTAÇÃO (fora do índice)
  ID 23 01/10 importação; consignatário CNPJ com nome; notify só endereço; M3 sem escala (sem referência); 1 contêiner
  ID 24 10/10 importação; embarcador CNPJ com nome; notify com telefone; M1 (criado 61 dias DEPOIS da previsão);
        7 contêineres
"""
import io
import unittest
from datetime import datetime

from virasana.analytic_scripts import diagnostico_texto_navii as dtn
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Escala, EscalaManifesto, Item  # noqa: I100
from virasana.navii.busca_navii import Empresa
from virasana.tests.test_navii_busca_sqlite import cria_sessao

COM_NOME, SEM_NOME = '11222333000181', '99888777000166'


def monta(session):
    session.add_all([Empresa(cnpj=COM_NOME, nome='IMPORTADORA ALFA LTDA'),
                     Empresa(cnpj=SEM_NOME, nome='  ')])            # cadastrado, mas sem nome: não conta
    ces = (
        (1, datetime(2025, 9, 1), '5', 'M1', '', '', ''),
        (10, datetime(2026, 5, 10), '5', 'M1', 'X', 'Y', ''),
        (20, datetime(2026, 8, 1), '5', 'M1', COM_NOME, 'FOREIGN NAME LTD',
         'JOHN DOE, john.doe@acme.com, TEL +1 555 1234 5678, 12 MAIN ST'),
        (21, datetime(2026, 8, 15), '5', 'M2', 'ACME CO', SEM_NOME, ''),
        (22, datetime(2026, 9, 1), '7', 'M1', 'EXPORTADOR', 'Z', 'EXPORT NOTIFY'),
        (23, datetime(2026, 10, 1), '5', 'M3', 'ACME CO', COM_NOME, 'RUA DAS FLORES 100 SAO PAULO'),
        (24, datetime(2026, 10, 10), '5', 'M1', COM_NOME, '', 'MARIA SILVA FONE 11 98765-4321'))
    for id_, criado, trafego, manifesto, embarcador, consignatario, notify in ces:
        session.add(Conhecimento(ID=id_, numeroCEmercante=f'CE{id_:03d}', create_date=criado, tipoTrafego=trafego,
                                 manifestoCE=manifesto, embarcador=embarcador, consignatario=consignatario,
                                 notify=notify, descricao='MERCADORIA DIVERSA'))
    session.add_all([
        Escala(numeroDaEscala='E1', portoDaEscala='BRRIG', dataPrevistaAtracacao='2026-08-05'),
        Escala(numeroDaEscala='E2', portoDaEscala='BRSSZ', dataPrevistaAtracacao='2026-08-10'),
        Escala(numeroDaEscala='E3', portoDaEscala='BRRIG', dataPrevistaAtracacao='2026-08-20'),
        EscalaManifesto(ID=1, manifesto='M1', escala='E1'), EscalaManifesto(ID=2, manifesto='M1', escala='E2'),
        EscalaManifesto(ID=3, manifesto='M2', escala='E3')])
    item = 0
    for ce, quantos in (('CE020', 2), ('CE023', 1), ('CE024', 7)):
        for n in range(quantos):
            item += 1
            session.add(Item(ID=item, numeroCEmercante=ce, codigoConteiner=f'C{item:03d}'))
    session.add(Item(ID=99, numeroCEmercante='CE021', codigoConteiner=''))      # item sem contêiner: não é card
    session.commit()


class TestDiagnosticoTexto(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        session = cria_sessao()
        monta(session)
        saida = io.StringIO()
        cls.diag = dtn.DiagnosticoTexto(session, saida, amostra=100, agora=datetime(2026, 10, 17, 12))
        cls.diag.roda()
        cls.log = saida.getvalue()

    def celulas(self, inicio, ultima=False):
        linhas = [linha for linha in self.log.splitlines() if linha.startswith(inicio)]
        return [c.strip() for c in linhas[-1 if ultima else 0].split('|')]

    def test_por_mes_sem_os_antigos_e_com_a_exportacao(self):
        self.assertEqual(self.celulas('2026-05'), ['2026-05', '1', '0', '1'])
        self.assertEqual(self.celulas('2026-09'), ['2026-09', '1', '1', '0'])
        self.assertEqual(self.celulas('2026-10'), ['2026-10', '2', '0', '2'])
        self.assertNotIn('2025-09', self.log)

    def test_campos_da_importacao_dos_ultimos_meses(self):
        self.assertIn('CEs de importação na janela: 4', self.log)
        self.assertEqual(self.celulas('notify ')[1], '3 (75%)')
        self.assertEqual(self.celulas('embarcador')[1], '4 (100%)')

    def test_notify(self):
        self.assertEqual(self.celulas('e-mail')[1], '1 (33%)')
        self.assertEqual(self.celulas('telefone')[1], '2 (67%)')
        self.assertEqual(self.celulas('palavra de endereço')[1], '2 (67%)')
        self.assertIn('J*** D**, ***@acme.com, TEL +# ### #### ####, ## M*** ST', self.log)   # só o formato
        self.assertNotIn('john.doe@', self.log)
        self.assertNotIn('98765', self.log)                                 # dígitos mascarados
        self.assertNotIn('MARIA', self.log)                                 # nome de pessoa não sai

    def test_cnpj_x_nome_e_nomes_cadastrados(self):
        # campo | preenchidos | são CNPJ | CNPJ com nome | distintos
        self.assertEqual(self.celulas('embarcador', ultima=True), ['embarcador', '4', '2 (50%)', '2 (100%)', '2'])
        self.assertEqual(self.celulas('consignatario', ultima=True), ['consignatario', '3', '2 (67%)', '1 (50%)', '3'])

    def test_escalas_referencia_e_cards(self):
        self.assertIn('Com escala em BRSSZ: 2 (50%); SEM data de referência (sem escala com data): 1 (25%)', self.log)
        self.assertIn('p10 -61, mediana 5, p90 9', self.log)               # 24 criado depois; 21 pela 1ª escala
        self.assertEqual(self.celulas('2  ')[1], '2 (50%)')                 # escalas do manifesto
        for faixa in ('0 (sem contêiner)', '1  ', '2 a 5', 'mais de 5'):
            with self.subTest(faixa=faixa):
                self.assertEqual(self.celulas(faixa)[-1], '1 (25%)')

    def test_tamanho_e_sem_erro(self):
        self.assertIn('3 meses: ~3 CEs', self.log)                         # (1 + 2 + 0) / 3 meses fechados
        self.assertNotIn('!! ERRO', self.log)

    def test_referencia_e_mascara(self):
        self.assertEqual(self.diag.referencia([('BRRIG', datetime(2026, 1, 1)), ('BRSSZ', datetime(2026, 1, 9))]),
                         datetime(2026, 1, 9))
        self.assertEqual(self.diag.referencia([('BRRIG', datetime(2026, 1, 9)), ('BRITJ', datetime(2026, 1, 2))]),
                         datetime(2026, 1, 2))
        self.assertIsNone(self.diag.referencia([('BRSSZ', None)]))
        self.assertEqual(dtn.mascara_notify('ana@x.com.br 1234'), '***@x.com.br ####')
        self.assertEqual(dtn.mascara_notify('ACME LTDA, RUA SAO BENTO'), 'A*** LTDA, RUA S** B****')


if __name__ == '__main__':
    unittest.main()
