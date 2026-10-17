"""Testes do diagnóstico das Fichas de repressão na importação (analytic_scripts/diagnostico_fichas_repressao.py).

Cenário (agora = 17/10/2026; universo: Fichas tipo 1 dos últimos 24 meses):
  F1 aberta, sem verificação física informada, do atalho antigo /nova_inspecaoni, flag NAVI-I; o contêiner CONT1 já
     foi ENTREGUE (alerta da aba); bloqueio de CE informado; motivo com CPF (tem de sair mascarado)
  F2 aberta, COM verificação física informada (fora da aba)
  F3 aberta; o evento de verificação física foi EXCLUÍDO -> continua sem verificação (na aba); contêiner no seco
  F4 encerrada (fora da aba)
  F5 FMA (tipo 0) e F6 de 2023: fora do universo
"""
import io
import re
import unittest
from datetime import datetime

from sqlalchemy import create_engine, text

from virasana.analytic_scripts import diagnostico_fichas_repressao as dfr

AGORA = datetime(2026, 10, 17, 12)
CPF, CPF2 = '52998224725', '11144477735'
DDL = [
    'CREATE TABLE ovr_tiposevento (id INTEGER PRIMARY KEY, nome VARCHAR(50), fase INTEGER, eventoespecial INTEGER, '
    'ordem INTEGER)',
    'CREATE TABLE ovr_setores (id CHAR(15) PRIMARY KEY, nome CHAR(50))',
    'CREATE TABLE ovr_ovrs (id INTEGER PRIMARY KEY, numero VARCHAR(10), ano VARCHAR(4), tipooperacao INTEGER, '
    'fase INTEGER, tipoevento_id INTEGER, setor_id CHAR(15), numeroCEmercante VARCHAR(15), cnpj_fiscalizado '
    'VARCHAR(15), user_name VARCHAR(14), responsavel_cpf VARCHAR(15), cpfauditorresponsavel VARCHAR(15), '
    'datahora TIMESTAMP, create_date TIMESTAMP, observacoes VARCHAR(1000))',
    'CREATE TABLE ovr_eventos (id INTEGER PRIMARY KEY, ovr_id INTEGER, create_date TIMESTAMP, tipoevento_id INTEGER, '
    'meramente_informativo BOOLEAN, excluido BOOLEAN, motivo VARCHAR(200), user_name VARCHAR(14))',
    'CREATE TABLE ovr_verificacoesfisicas (id INTEGER PRIMARY KEY, ovr_id INTEGER, numerolote VARCHAR(20), '
    'numeroCEmercante VARCHAR(15), inspecaonaoinvasiva BOOLEAN, datahora TIMESTAMP)',
    'CREATE TABLE ovr_imagensrvf (id INTEGER PRIMARY KEY, rvf_id INTEGER)',
    'CREATE TABLE ovr_flags (id INTEGER PRIMARY KEY, nome VARCHAR(100))',
    'CREATE TABLE ovr_flags_ovr (rvf_id INTEGER, flag_id INTEGER)',
    'CREATE TABLE ciclodevida_ciclo_de_vida_importacao (ID INTEGER PRIMARY KEY, numeroConteiner VARCHAR(11), '
    'numeroCEmercante VARCHAR(15), data_desembarque DATETIME, situacao VARCHAR(20), concluido BOOLEAN, '
    'fluxo VARCHAR(20), data_saida DATETIME, data_entrada_seco DATETIME)',
]


def insere(con, tabela, linhas):
    colunas = list(dict.fromkeys(c for linha in linhas for c in linha))
    con.execute(text(f'INSERT INTO {tabela} ({", ".join(colunas)}) VALUES ({", ".join(":" + c for c in colunas)})'),
                [{c: linha.get(c) for c in colunas} for linha in linhas])


def d(dia, mes=10, ano=2026):
    return datetime(ano, mes, dia, 9)


def monta_banco(engine):
    with engine.begin() as con:
        for ddl in DDL:
            con.execute(text(ddl))
        insere(con, 'ovr_tiposevento', [
            dict(id=16, nome='Atribuição de responsável', fase=1, eventoespecial=1, ordem=1),
            dict(id=22, nome='Verificação física informada', fase=1, eventoespecial=2, ordem=5),
            dict(id=28, nome='Bloqueio de CE incluído', fase=1),
            dict(id=37, nome='Definição de Auditor Responsável', fase=1, eventoespecial=5, ordem=2),
            dict(id=40, nome='Inspeção não Invasiva', fase=1, eventoespecial=7, ordem=5),
            dict(id=46, nome='Verificação Física Agendada', fase=2),
            dict(id=15, nome='Encerramento sem resultado', fase=4, eventoespecial=9)])
        insere(con, 'ovr_setores', [dict(id='1', nome='EQUIPE A'), dict(id='2', nome='EQUIPE B')])
        base = dict(tipooperacao=1, fase=1, user_name=CPF, responsavel_cpf=CPF)
        insere(con, 'ovr_ovrs', [
            dict(base, id=1, setor_id='1', numeroCEmercante='152600000000001', datahora=d(1), tipoevento_id=28,
                 observacoes='Inspeção não invasiva Conhecimento 152600000000001 automaticamente registrada.'),
            dict(base, id=2, setor_id='2', numeroCEmercante='152600000000002', datahora=d(2), tipoevento_id=22),
            dict(base, id=3, setor_id='1', numeroCEmercante='152600000000003', datahora=d(3), tipoevento_id=37,
                 cpfauditorresponsavel=CPF2, observacoes='Carga de risco'),
            dict(base, id=4, setor_id='1', datahora=d(4), fase=4, tipoevento_id=15),
            dict(base, id=5, tipooperacao=0, setor_id='1', datahora=d(5)),
            dict(base, id=6, setor_id='1', datahora=d(1, ano=2023))])
        insere(con, 'ovr_eventos', [
            dict(id=1, ovr_id=1, create_date=d(1), tipoevento_id=16, motivo=f'De: Nenhum Para: {CPF}'),
            dict(id=2, ovr_id=1, create_date=d(1), tipoevento_id=40, motivo='RVF 1'),
            dict(id=3, ovr_id=1, create_date=d(3), tipoevento_id=28, motivo='bloqueio no CE'),
            dict(id=4, ovr_id=2, create_date=d(2), tipoevento_id=16),
            dict(id=5, ovr_id=2, create_date=d(10), tipoevento_id=22, motivo='RVF 2'),
            dict(id=6, ovr_id=3, create_date=d(3), tipoevento_id=16),
            dict(id=7, ovr_id=3, create_date=d(4), tipoevento_id=37),
            dict(id=8, ovr_id=3, create_date=d(5), tipoevento_id=22, excluido=True, motivo='lançado por engano'),
            dict(id=9, ovr_id=4, create_date=d(4), tipoevento_id=16),
            dict(id=10, ovr_id=4, create_date=d(6), tipoevento_id=15, meramente_informativo=False)])
        insere(con, 'ovr_verificacoesfisicas', [
            dict(id=1, ovr_id=1, numerolote='CONT1', numeroCEmercante='152600000000001', inspecaonaoinvasiva=True),
            dict(id=2, ovr_id=2, numerolote='CONT2', inspecaonaoinvasiva=False),
            dict(id=3, ovr_id=3, numerolote='cont3', numeroCEmercante='152600000000003')])
        insere(con, 'ovr_imagensrvf', [dict(id=1, rvf_id=1)] + [dict(id=10 + i, rvf_id=2) for i in range(6)])
        insere(con, 'ovr_flags', [dict(id=1, nome='NAVI-I')])
        insere(con, 'ovr_flags_ovr', [dict(rvf_id=1, flag_id=1)])
        insere(con, 'ciclodevida_ciclo_de_vida_importacao', [
            dict(ID=1, numeroConteiner='CONT1', numeroCEmercante='152600000000001', data_desembarque=d(2),
                 situacao='concluido', concluido=True, fluxo='operador', data_saida=d(4)),
            dict(ID=2, numeroConteiner='CONT3', numeroCEmercante='152600000000003', data_desembarque=d(3),
                 situacao='aberto', concluido=False, fluxo='tra', data_saida=d(4), data_entrada_seco=d(4))])


class TestDiagnosticoFichas(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        engine = create_engine('sqlite://')
        monta_banco(engine)
        saida = io.StringIO()
        with engine.connect() as conexao:
            cls.diag = dfr.DiagnosticoFichas(conexao, saida, agora=AGORA)
            cls.diag.roda()
        cls.log = saida.getvalue()

    def linha(self, inicio):
        return next(linha for linha in self.log.splitlines() if linha.startswith(inicio))

    def test_universo(self):
        self.assertEqual(sorted(self.diag.fichas), [1, 2, 3, 4])           # sem a FMA e sem a de 2023
        self.assertIn('2. FICHAS DE REPRESSÃO NA IMPORTAÇÃO', self.log)
        self.assertNotIn('!! ERRO', self.log)

    def test_populacao_da_aba_ignora_evento_excluido(self):
        self.assertEqual(sorted(f.id for f in self.diag.abertas_sem_verificacao()), [1, 3])
        self.assertIn('a população da aba: 2', self.log)
        self.assertIn('VERIFICAÇÃO FÍSICA INFORMADA | 1 (25%)', self.log)   # só a F2 (a da F3 foi excluída)

    def test_alerta_entregue_sem_verificacao(self):
        self.assertIn('sem verificação física informada: 1 contêiner(es)', self.log)
        self.assertRegex(self.log, r'entregue\s+\| 1')
        self.assertRegex(self.log, r'no_seco\s+\| 1')                     # contêiner gravado em minúsculas na RVF

    def test_rvfs_atalho_antigo_e_flags(self):
        self.assertRegex(self.log, r'Fichas do atalho antigo /nova_inspecaoni \| 1 \(25%\)')
        self.assertRegex(self.log, r'imagens: 5 ou mais\s+\| 1 \(33%\)')
        self.assertRegex(self.log, r'NAVI-I\s+\| 1')

    def test_catalogo_por_evento_especial(self):
        self.assertRegex(self.log, r'22 \| Verificação física informada\s+\| 1 Ativa\s+\| RVF \(verificação física '
                                   r'informada\) \| 5\s+\| 1\s+\| 1')
        self.assertIn('excluídos              | 1', self.log)

    def test_sigilo(self):
        self.assertNotIn(CPF, self.log)
        self.assertNotIn(CPF2, self.log)
        self.assertIn('529.***.***-25', self.log)
        self.assertIn('De: Nenhum Para: ###########', self.log)            # motivo: dígitos mascarados
        self.assertFalse(re.search(r'\d{11}', self.linha('   01/10/2026 09:00  Atribuição')))


class TestAuxiliares(unittest.TestCase):

    def test_percentis_e_faixas(self):
        self.assertEqual(dfr.percentis([]), '-')
        self.assertEqual(dfr.percentis([1, 2, 3, 4, 10]), 'mediana 3.0 d, p75 4.0 d, p90 10.0 d')
        self.assertEqual(dfr.faixa_idade(7), 'até 7 dias')
        self.assertEqual(dfr.faixa_idade(400), 'mais de 1 ano')
        self.assertEqual(dfr.mascara_texto('CPF 123.456.789-09 ok', 12), 'CPF ###.###.…')


if __name__ == '__main__':
    unittest.main()
