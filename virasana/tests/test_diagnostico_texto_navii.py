"""Testes do diagnóstico da busca por texto (analytic_scripts/diagnostico_texto_navii.py) — F0.

Agora = 17/10/2026 12h; 12 meses de CEs por mês (11/2025 a 10/2026); amostra dos últimos 3 meses (desde 18/07/2026).
A `create_date` é a data da ÚLTIMA ALTERAÇÃO: o ID 2, criado em 09/2025 e alterado em 15/10/2026, não pode puxar a
janela para trás (foi o que fez a 1ª versão varrer a tabela em produção). IDs seguidos, como na inclusão do Mercante.
  ID 1  01/09/2025 (fora dos 12 meses)
  ID 2  criado 02/09/2025, ALTERADO em 15/10/2026 (fora)
  ID 3  20/10/2025 (fora)
  ID 4  10/05/2026 importação; ID 5 20/05 EXPORTAÇÃO; ID 6 01/07 importação (só na contagem por mês)
  ID 7  01/08 importação; embarcador CNPJ com nome; consignatário estrangeiro; notify com e-mail, telefone e endereço;
        CNPJ do notify com nome; M1 (BRRIG 05/08 e BRSSZ 10/08: a referência é a de Santos, 9 dias depois);
        2 contêineres
  ID 8  15/08 importação; consignatário CNPJ SEM nome; M2 (só BRRIG 20/08: a primeira escala, 5 dias); sem contêiner
  ID 9  01/09 EXPORTAÇÃO (fora do índice)
  ID 10 01/10 importação; consignatário CNPJ com nome; notify só endereço; CNPJ do notify que não é CNPJ; M3 sem
        escala (sem referência); 1 contêiner
  ID 11 10/10 importação; embarcador CNPJ com nome; notify com telefone; M1 (criado 61 dias DEPOIS da previsão);
        7 contêineres
"""
import io
import unittest
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError

from virasana.analytic_scripts import diagnostico_texto_navii as dtn
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Escala, EscalaManifesto, Item  # noqa: I100
from virasana.navii.busca_navii import Empresa
from virasana.tests.test_navii_busca_sqlite import cria_sessao

COM_NOME, SEM_NOME = '11222333000181', '99888777000166'
AGORA = datetime(2026, 10, 17, 12)


def monta(session):
    session.add_all([Empresa(cnpj=COM_NOME, nome='IMPORTADORA ALFA LTDA'),
                     Empresa(cnpj=SEM_NOME, nome='  ')])            # cadastrado, mas sem nome: não conta
    ces = (
        (1, datetime(2025, 9, 1), '5', 'M1', '', '', '', ''),
        (2, datetime(2026, 10, 15), '5', 'M1', 'X', 'Y', '', ''),    # alterado: a create_date é a da alteração
        (3, datetime(2025, 10, 20), '5', 'M1', 'X', 'Y', '', ''),
        (4, datetime(2026, 5, 10), '5', 'M1', 'X', 'Y', '', ''),
        (5, datetime(2026, 5, 20), '7', 'M1', 'EXP', 'Z', '', ''),
        (6, datetime(2026, 7, 1), '5', 'M1', 'X', 'Y', '', ''),
        (7, datetime(2026, 8, 1), '5', 'M1', COM_NOME, 'FOREIGN NAME LTD',
         'JOHN DOE, john.doe@acme.com, TEL +1 555 1234 5678, 12 MAIN ST', COM_NOME),
        (8, datetime(2026, 8, 15), '5', 'M2', 'ACME CO', SEM_NOME, '', ''),
        (9, datetime(2026, 9, 1), '7', 'M1', 'EXPORTADOR', 'Z', 'EXPORT NOTIFY', ''),
        (10, datetime(2026, 10, 1), '5', 'M3', 'ACME CO', COM_NOME, 'RUA DAS FLORES 100 SAO PAULO', '123'),
        (11, datetime(2026, 10, 10), '5', 'M1', COM_NOME, '', 'MARIA SILVA FONE 11 98765-4321', ''))
    for id_, data, trafego, manifesto, embarcador, consignatario, notify, cnpj_notify in ces:
        session.add(Conhecimento(ID=id_, numeroCEmercante=f'CE{id_:03d}', create_date=data, tipoTrafego=trafego,
                                 manifestoCE=manifesto, embarcador=embarcador, consignatario=consignatario,
                                 notify=notify, cnpjNotify=cnpj_notify, descricao='MERCADORIA DIVERSA'))
    session.add_all([
        Escala(numeroDaEscala='E1', portoDaEscala='BRRIG', dataPrevistaAtracacao='2026-08-05'),
        Escala(numeroDaEscala='E2', portoDaEscala='BRSSZ', dataPrevistaAtracacao='2026-08-10'),
        Escala(numeroDaEscala='E3', portoDaEscala='BRRIG', dataPrevistaAtracacao='2026-08-20'),
        EscalaManifesto(ID=1, manifesto='M1', escala='E1'), EscalaManifesto(ID=2, manifesto='M1', escala='E2'),
        EscalaManifesto(ID=3, manifesto='M2', escala='E3')])
    item = 0
    for ce, quantos in (('CE007', 2), ('CE010', 1), ('CE011', 7)):
        for n in range(quantos):
            item += 1
            session.add(Item(ID=item, numeroCEmercante=ce, codigoConteiner=f'C{item:03d}'))
    session.add(Item(ID=99, numeroCEmercante='CE008', codigoConteiner=''))      # item sem contêiner: não é card
    session.commit()


class TestDiagnosticoTexto(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.session = cria_sessao()
        monta(cls.session)
        saida, cls.progresso = io.StringIO(), []
        cls.diag = dtn.DiagnosticoTexto(cls.session, saida, amostra=100, agora=AGORA, bloco_data=2, bloco_amostra=1,
                                        progresso=cls.progresso.append)
        cls.diag.roda()
        cls.log = saida.getvalue()

    def celulas(self, inicio, ultima=False):
        linhas = [linha for linha in self.log.splitlines() if linha.startswith(inicio)]
        return [c.strip() for c in linhas[-1 if ultima else 0].split('|')]

    def test_fronteira_pelo_id_e_nao_pela_create_date(self):
        # o que a 1ª versão fazia: o menor ID com create_date na janela é o CE antigo alterado
        self.assertEqual(self.session.execute(select(func.min(Conhecimento.ID)).where(
            Conhecimento.create_date >= datetime(2025, 11, 1))).scalar(), 2)
        self.assertEqual(self.diag.id_na_data(datetime(2025, 11, 1)), 4)
        self.assertEqual(self.diag.id_na_data(datetime(2026, 7, 1)), 6)        # a data exata entra
        self.assertEqual(self.diag.id_na_data(datetime(2026, 9, 1), a_partir=7), 9)
        self.assertEqual(self.diag.id_na_data(datetime(2025, 11, 1), a_partir=7), 7)  # não volta antes de a_partir
        self.assertEqual(self.diag.id_na_data(datetime(2026, 10, 11)), 12)     # nenhum: o último + 1
        self.assertEqual(self.diag.criacao_do_id(2), datetime(2025, 10, 20))  # o vizinho não foi alterado

    def test_por_mes_pela_faixa_de_ids_e_com_a_exportacao(self):
        # mês | IDs | CEs | exportação | importação
        self.assertEqual(self.celulas('2025-11'), ['2025-11', '-', '0', '0', '0'])
        self.assertEqual(self.celulas('2026-05'), ['2026-05', '4 a 5', '2', '1', '1'])
        self.assertEqual(self.celulas('2026-07'), ['2026-07', '6 a 6', '1', '0', '1'])
        self.assertEqual(self.celulas('2026-09'), ['2026-09', '9 a 9', '1', '1', '0'])
        self.assertEqual(self.celulas('2026-10'), ['2026-10', '10 a 11', '2', '0', '2'])
        self.assertFalse([linha for linha in self.log.splitlines() if linha.startswith(('2025-09', '2025-10'))])

    def test_amostra(self):
        self.assertIn('2. AMOSTRA: blocos de IDs dos CEs criados desde 18/07/2026 (3 meses)', self.log)
        self.assertIn('Faixa: IDs 7 a 11 (5 IDs); 5 blocos de 1 IDs', self.log)
        self.assertIn('CEs lidos: 5 de 5 IDs (lacunas de ID: 0 (0%))', self.log)
        self.assertIn('Exportação: 1 (20%) (fora do índice); importação: 4', self.log)

    def test_campos_da_importacao_da_amostra(self):
        self.assertIn('3. CAMPOS CANDIDATOS (amostra de 4 CEs de importação)', self.log)
        self.assertEqual(self.celulas('notify ')[1:], ['3 (75%)', '40', '61'])  # (61 + 28 + 30) / 3; o maior
        self.assertEqual(self.celulas('embarcador')[1], '4 (100%)')
        self.assertEqual(self.celulas('cnpjNotify')[1:3], ['2 (50%)', '8'])  # média dos preenchidos: (14 + 3) / 2

    def test_notify_com_exemplos_inteiros(self):
        self.assertEqual(self.celulas('e-mail')[1], '1 (33%)')
        self.assertEqual(self.celulas('telefone')[1], '2 (67%)')
        self.assertEqual(self.celulas('palavra de endereço')[1], '2 (67%)')
        exemplos = self.log.split('Exemplos (com contato, depois sem):')[1].splitlines()[1:4]
        self.assertEqual([e.strip() for e in exemplos],
                         ['JOHN DOE, john.doe@acme.com, TEL +1 555 1234 5678, 12 MAIN ST',
                          'MARIA SILVA FONE 11 98765-4321', 'RUA DAS FLORES 100 SAO PAULO'])
        self.assertNotIn('EXPORT NOTIFY', self.log)                          # exportação fora

    def test_cnpj_x_nome_e_nomes_cadastrados(self):
        # campo | preenchidos | são CNPJ | CNPJ com nome | distintos
        self.assertEqual(self.celulas('embarcador', ultima=True), ['embarcador', '4', '2 (50%)', '2 (100%)', '2'])
        self.assertEqual(self.celulas('consignatario', ultima=True), ['consignatario', '3', '2 (67%)', '1 (50%)', '3'])
        self.assertEqual(self.celulas('cnpjNotify', ultima=True), ['cnpjNotify', '2', '1 (50%)', '1 (100%)', '2'])

    def test_escalas_referencia_e_cards(self):
        self.assertIn('Com escala em BRSSZ: 2 (50%); SEM data de referência (sem escala com data): 1 (25%)', self.log)
        self.assertIn('p10 -61, mediana 5, p90 9', self.log)               # 11 criado depois; 8 pela 1ª escala
        self.assertEqual(self.celulas('2  ')[1], '2 (50%)')                 # escalas do manifesto
        for faixa in ('0 (sem contêiner)', '1  ', '2 a 5', 'mais de 5'):
            with self.subTest(faixa=faixa):
                self.assertEqual(self.celulas(faixa)[-1], '1 (25%)')

    def test_tamanho_progresso_e_sem_erro(self):
        self.assertIn('3 meses: ~3 CEs', self.log)                         # importação (1 + 2 + 0) / 3 meses fechados
        self.assertIn('Leitura da carga inicial (todos os IDs, na taxa da amostra): 3 meses ~', self.log)
        self.assertNotIn('!! ERRO', self.log)
        self.assertEqual([p.split()[-1] for p in self.progresso], list(dtn.DiagnosticoTexto.SECOES))

    def test_amostra_em_blocos_espalhados_com_a_criacao_do_bloco(self):
        diag = dtn.DiagnosticoTexto(self.session, io.StringIO(), amostra=4, agora=AGORA, bloco_data=2,
                                    bloco_amostra=2)
        diag.secao_amostra()
        self.assertEqual([linha.ID for linha in diag.linhas], [7, 8, 10, 11])  # o 1º no início, o último no fim
        self.assertEqual(diag.criacao[11], datetime(2026, 10, 1))              # a menor do bloco (10 e 11)
        self.assertEqual(diag.criacao[8], datetime(2026, 8, 1))

    def test_blocos(self):
        diag = dtn.DiagnosticoTexto(None, io.StringIO(), amostra=20, bloco_amostra=5)
        self.assertEqual(diag._blocos(1, 100), ([1, 33, 64, 96], 5))           # o último termina no 100
        self.assertEqual(dtn.DiagnosticoTexto(None, None)._blocos(1, 3), ([1, 2, 3], 1))
        self.assertEqual(dtn.DiagnosticoTexto(None, None, amostra=500)._blocos(10, 10000), ([10], 500))

    def test_secao_com_erro_de_banco_nao_para_o_diagnostico(self):
        saida = io.StringIO()
        diag = dtn.DiagnosticoTexto(self.session, saida, amostra=100, agora=AGORA, bloco_data=2, bloco_amostra=1)

        def interrompida():
            raise OperationalError('SELECT ...', {}, Exception('max_statement_time exceeded'))
        diag.secao_meses = interrompida
        diag.roda()
        log = saida.getvalue()
        self.assertIn('!! ERRO na seção (o diagnóstico segue): max_statement_time exceeded', log)
        self.assertIn('Faixa: IDs 7 a 11', log)                                # as outras seções rodaram
        self.assertIn('Sem dados suficientes (rode as seções 1 a 3).', log)

    def test_referencia(self):
        self.assertEqual(self.diag.referencia([('BRRIG', datetime(2026, 1, 1)), ('BRSSZ', datetime(2026, 1, 9))]),
                         datetime(2026, 1, 9))
        self.assertEqual(self.diag.referencia([('BRRIG', datetime(2026, 1, 9)), ('BRITJ', datetime(2026, 1, 2))]),
                         datetime(2026, 1, 2))
        self.assertIsNone(self.diag.referencia([('BRSSZ', None)]))


if __name__ == '__main__':
    unittest.main()
