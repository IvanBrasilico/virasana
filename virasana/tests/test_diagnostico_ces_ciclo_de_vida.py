"""Testes do diagnóstico dos CEs do Ciclo de Vida (master x filhote, CE declarado nos eventos).

Cenário (SQLite): família MASTER M com filhotes H1 e H2 (manifesto MAN1), CE simples S (MAN2).
  ciclo X: CE M; contêiner nos itens de M e H1; inspeção ligada declara H1 (filhote)
  ciclo Y: CE H1; desembarque declara M (master), saída declara H1, inspeção declara H2 (irmão, sem o
           contêiner nos itens) — eventos TROCANDO master e house
  ciclo Z: CE S; inspeções declaram um BL ('6ALSK182J') e H1 (sem relação)
  sem CE:  W1 declara CE fora do Mercante; W2 declara H2 (contêiner nos itens, escala 10 dias depois);
           W3 sem CE declarado
"""
import io
import unittest
from datetime import datetime, timedelta

from virasana.analytic_scripts.ciclo_de_vida import diagnostico_ces_ciclo_de_vida as dce
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import (
    CicloDeVidaImportacao as Ciclo, CicloDeVidaImportacaoEscaneamentos as Esc,
)
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Escala, EscalaManifesto, Item, Manifesto
from virasana.tests.test_navii_busca_sqlite import cria_sessao

from bhadrasana.models.apirecintos import AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva  # noqa: I100

M, H1, H2, S, FORA = '152600000000001', '152600000000002', '152600000000003', '152600000000010', '132600000000099'
INICIO, FIM = datetime(2026, 3, 1), datetime(2026, 3, 31)
BTP = '8931356'


def d(dia, hora=8):
    return datetime(2026, 3, dia, hora)


def monta(session):
    session.add_all([
        Conhecimento(numeroCEmercante=M, manifestoCE='MAN1', tipoBLConhecimento='10', tipoTrafego='5'),
        Conhecimento(numeroCEmercante=H1, numeroCEMaster=M, manifestoCE='MAN1', tipoBLConhecimento='12',
                     tipoTrafego='5'),
        Conhecimento(numeroCEmercante=H2, numeroCEMaster=M, manifestoCE='MAN1', tipoBLConhecimento='12',
                     tipoTrafego='5'),
        Conhecimento(numeroCEmercante=S, manifestoCE='MAN2', tipoBLConhecimento='11', tipoTrafego='5'),
        Item(numeroCEmercante=M, codigoConteiner='CTRX'), Item(numeroCEmercante=H1, codigoConteiner='CTRX'),
        Item(numeroCEmercante=H1, codigoConteiner='CTRY'), Item(numeroCEmercante=S, codigoConteiner='CTRZ'),
        Item(numeroCEmercante=H2, codigoConteiner='CTRW2'),
        Manifesto(numero='MAN1'), Manifesto(numero='MAN2'),
        Escala(numeroDaEscala='E1', dataPrevistaAtracacao=(d(5) + timedelta(days=10)).strftime('%Y-%m-%d')),
        EscalaManifesto(ID=1, manifesto='MAN1', escala='E1'),
    ])
    ids = iter(range(1, 1000))
    eventos = {EmbarqueDesembarque: [], AcessoVeiculo: [], InspecaoNaoInvasiva: []}

    def novo(modelo, conteiner, data, ce, **campos):
        linha = dict(id=next(ids), numeroConteiner=conteiner, dataHoraOcorrencia=data, codigoRecinto=BTP,
                     tipoOperacao='I', numeroConhecimento=ce, **campos)
        eventos[modelo].append(linha)
        return linha['id']

    desembarque = lambda conteiner, data, ce: novo(EmbarqueDesembarque, conteiner, data, ce,  # noqa: E731
                                                   embarqueDesembarque='D')
    inspecao = lambda conteiner, data, ce: novo(InspecaoNaoInvasiva, conteiner, data, ce, vazio=False)  # noqa: E731

    dx, ix = desembarque('CTRX', d(2), None), inspecao('CTRX', d(2, 12), H1)
    dy = desembarque('CTRY', d(3), M)
    novo(AcessoVeiculo, 'CTRY', d(4), H1, direcao='S', operacao='C', vazioConteiner=False)
    iy = inspecao('CTRY', d(3, 12), H2)
    dz, iz1, iz2 = desembarque('CTRZ', d(3), None), inspecao('CTRZ', d(3, 10), '6ALSK182J'), inspecao('CTRZ', d(3, 11), H1)
    dw1, dw2, dw3 = desembarque('CTRW1', d(5), FORA), desembarque('CTRW2', d(5), H2), desembarque('CTRW3', d(5), '')
    for modelo, linhas in eventos.items():
        modelo.__table__.create(session.get_bind(), checkfirst=True)
        session.execute(modelo.__table__.insert(), linhas)

    def ciclo(ce, conteiner, desembarque_id, data, escaneamentos=()):
        c = Ciclo(numeroCEmercante=ce, numeroConteiner=conteiner, desembarque_id=desembarque_id,
                  data_desembarque=data, codigoRecintoDesembarque=BTP)
        for esc_id in escaneamentos:
            c.escaneamentos.append(Esc(escaneamento_id=esc_id, data_escaneamento=data))
        session.add(c)

    ciclo(M, 'CTRX', dx, d(2), [ix])
    ciclo(H1, 'CTRY', dy, d(3), [iy])
    ciclo(S, 'CTRZ', dz, d(3), [iz1, iz2])
    ciclo(None, 'CTRW1', dw1, d(5))
    ciclo(None, 'CTRW2', dw2, d(5))
    ciclo(None, 'CTRW3', dw3, d(5))
    session.commit()


class TestFuncoes(unittest.TestCase):
    INFO = {M: {'master': None, 'manifesto': 'MAN1'}, H1: {'master': M, 'manifesto': 'MAN1'},
            H2: {'master': M, 'manifesto': 'MAN1'}, S: {'master': None, 'manifesto': 'MAN2'},
            '152600000000004': {'master': None, 'manifesto': 'MAN1'}}

    def test_relacao(self):
        casos = [(M, M, dce.IGUAL), (M, H1, dce.FILHOTE), (H1, M, dce.MASTER), (H1, H2, dce.IRMAO),
                 (M, '152600000000004', dce.MANIFESTO), (S, H1, dce.SEM_RELACAO), (M, FORA, dce.INEXISTENTE),
                 (M, '6ALSK182J', dce.NAO_CE), (M, '', dce.NAO_INFORMADO), (M, None, dce.NAO_INFORMADO),
                 (M, '15.2600.0000.000-02', dce.FILHOTE)]
        for ce_ciclo, declarado, esperado in casos:
            with self.subTest(declarado=declarado):
                self.assertEqual(dce.relacao(ce_ciclo, declarado, self.INFO), esperado)

    def test_papel_e_onde(self):
        filhotes = {M: {H1, H2}}
        self.assertEqual(dce.papel(M, self.INFO, filhotes), 'MASTER (tem filhotes)')
        self.assertEqual(dce.papel(H1, self.INFO, filhotes), 'FILHOTE (tem master)')
        self.assertEqual(dce.papel(S, self.INFO, filhotes), 'simples (sem master/filhote)')
        itens = {M: {'CTRX'}, H1: {'CTRX'}}
        self.assertEqual(dce.onde_esta_o_conteiner('CTRX', M, self.INFO, filhotes, itens),
                         'no próprio CE + nos filhotes')
        self.assertEqual(dce.onde_esta_o_conteiner('CTRX', H1, self.INFO, filhotes, itens), 'no próprio CE + no master')
        self.assertEqual(dce.onde_esta_o_conteiner('CTRQ', H1, self.INFO, filhotes, itens), 'em nenhum')


class TestDiagnosticoCEs(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        session = cria_sessao()
        monta(session)
        saida = io.StringIO()
        diag = dce.DiagnosticoCEs(session.connection(), saida, INICIO, FIM, amostra=100, amostra_eventos=100)
        cls.falhas = dce.roda(diag)
        cls.log = saida.getvalue()
        session.close()

    def valores(self, *inicio):
        """Células seguintes da primeira linha de tabela cujas primeiras células são `inicio`."""
        for linha in self.log.splitlines():
            celulas = [c.strip() for c in linha.split('|')]
            if celulas[:len(inicio)] == list(inicio):
                return celulas[len(inicio):]
        raise AssertionError(f'linha não encontrada: {inicio}')

    def test_sem_erros(self):
        self.assertEqual(self.falhas, [])
        self.assertNotIn('!! ERRO', self.log)

    def test_papel_do_ce_do_ciclo(self):
        self.assertEqual(self.valores('MASTER (tem filhotes)'), ['10', '5', '1'])
        self.assertEqual(self.valores('FILHOTE (tem master)'), ['12', '5', '1'])
        self.assertEqual(self.valores('no próprio CE + nos filhotes'), ['1'])
        self.assertIn('5. CONSULTAS MAIS LENTAS', self.log)

    def test_inspecoes_ligadas(self):
        self.assertEqual(self.valores(dce.FILHOTE), ['sim', '-', '1'])   # X: H1 tem o contêiner
        self.assertEqual(self.valores(dce.IRMAO), ['NÃO', '-', '1'])     # Y: H2 não tem
        self.assertEqual(self.valores(dce.NAO_CE), ['-', '-', '1'])

    def test_eventos_trocam_master_e_house(self):
        self.assertEqual(self.valores('desembarque', dce.MASTER), ['1'])
        self.assertEqual(self.valores('acesso saída', dce.IGUAL), ['1'])
        self.assertEqual(self.valores('inspeção', dce.IRMAO), ['1'])
        # ciclos X e Y: só CEs da família (Y troca master x house entre os eventos)
        self.assertEqual(self.valores('o CE do ciclo e/ou sua família master/filhote (TROCA master x house)'),
                         ['2'])
        self.assertEqual(self.valores('só CE sem relação/inexistente'), ['1'])    # ciclo Z

    def test_ciclos_sem_ce(self):
        self.assertEqual(self.valores('não está no Mercante'), ['-', '-', '-', '1'])   # W1
        self.assertEqual(self.valores('FILHOTE (tem master)', 'no próprio CE', '5'), ['6 a 15 dias', '1'])  # W2
        self.assertEqual(self.valores('não informado / não é CE'), ['-', '-', '-', '1'])   # W3
        self.assertEqual(self.valores('13'), ['fora do Mercante', '1'])


if __name__ == '__main__':
    unittest.main()
