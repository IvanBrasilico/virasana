"""Testes do diagnóstico do Ciclo de Vida (analytic_scripts/ciclo_de_vida/diagnostico_ciclo_de_vida.py).

Cenário (SQLite + mongomock), operador BTP 8931356, outro operador 8931404, seco 8932000:
  C1 TRA completo e escaneado no seco (imagem no Mongo 3 h "depois" — fuso)
  C2 pendente sem saída: a saída real veio 15 dias depois (fora da janela de 72 h); só há agendamento antes
  C3 passagem com baldeação: embarque em OUTRO operador; CE tráfego 9
  C4 saida_id sem data_saida (bug do atalho) e a saída gravada é um agendamento (G)
  C5 concluído "pelo operador" sem escaneamento; a inspeção com o próprio CE foi em OUTRO recinto; entrou
     VAZIO em outro recinto 10 h depois da saída; depois foi inspecionado vazio com OUTRO CE
  C6 escaneamento ligado cuja inspeção tem OUTRO CE (ligação errada)
  C7 mesmo CE + contêiner de C2 (duplicidade)
"""
import io
import unittest
from datetime import datetime
from types import SimpleNamespace

import mongomock
from bson import ObjectId
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import virasana.navii.busca_navii  # noqa: F401 — ajusta o sys.path (ajna_commons, bhadrasana2)
from bhadrasana.models.apirecintos import AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva
from virasana.analytic_scripts.ciclo_de_vida import diagnostico_ciclo_de_vida as dcv
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import (
    CicloDeVidaImportacao as Ciclo, CicloDeVidaImportacaoEscaneamentos as Esc,
)
from virasana.integracao.mercante.mercantealchemy import Conhecimento

BTP, OUTRO_OPERADOR, SECO, OUTRO = '8931356', '8931404', '8932000', '8939999'
AGORA = datetime(2026, 3, 31)
INICIO, FIM = datetime(2026, 1, 1), datetime(2026, 3, 31)
IMAGEM = ObjectId()


def d(dia, hora=8):
    return datetime(2026, 3, dia, hora)


def monta(session):
    session.add_all([Conhecimento(numeroCEmercante=ce, tipoTrafego=t) for ce, t in
                     (('CE1', '5'), ('CE2', '5'), ('CE3', '9'), ('CE4', '5'), ('CE5', '5'), ('CE6', '5'))])
    ids = iter(range(1, 1000))
    # Eventos da API Recintos inseridos pela TABELA (sem o ORM): instanciar os modelos do bhadrasana
    # configura o registry inteiro, que falha se outro teste já carregou bhadrasana.models.ovr.
    eventos = {AcessoVeiculo: [], InspecaoNaoInvasiva: [], EmbarqueDesembarque: []}

    def novo(modelo, conteiner, data, recinto, **campos):
        linha = SimpleNamespace(id=next(ids), numeroConteiner=conteiner, dataHoraOcorrencia=data,
                                codigoRecinto=recinto, tipoOperacao='I', **campos)
        eventos[modelo].append(dict(vars(linha)))
        return linha

    def acesso(conteiner, data, recinto, direcao, operacao='C', vazio=False):
        return novo(AcessoVeiculo, conteiner, data, recinto, direcao=direcao, operacao=operacao,
                    vazioConteiner=vazio, numeroConhecimento=None)

    def inspecao(conteiner, data, recinto, ce, vazio=False):
        return novo(InspecaoNaoInvasiva, conteiner, data, recinto, numeroConhecimento=ce, vazio=vazio)

    def evento(conteiner, data, recinto, tipo):
        return novo(EmbarqueDesembarque, conteiner, data, recinto, embarqueDesembarque=tipo)

    # C1 — TRA completo
    s1, e1, ss1, i1 = (acesso('CTR1', d(2), BTP, 'S'), acesso('CTR1', d(2, 10), SECO, 'E'),
                       acesso('CTR1', d(5), SECO, 'S'), inspecao('CTR1', d(2, 12), SECO, 'CE1'))
    # C2 — saída depois de 72 h (só agendamento antes)
    acesso('CTR2', d(6), BTP, 'S', operacao='G')
    acesso('CTR2', d(20), BTP, 'S')
    # C3 — passagem com baldeação
    evento('CTR3', d(12), OUTRO_OPERADOR, 'E')
    # C4 — saida_id sem data, apontando para agendamento
    g4 = acesso('CTR4', d(8), BTP, 'S', operacao='G')
    # C5 — operador, inspeção com o CE em outro recinto, entrada vazio 10 h depois, inspeção com outro CE
    s5 = acesso('CTR5', d(2, 10), BTP, 'S')
    acesso('CTR5', d(2, 20), OUTRO, 'E', vazio=True)
    inspecao('CTR5', d(1, 20), OUTRO, 'CE5')
    inspecao('CTR5', d(3), OUTRO, 'CEX', vazio=True)
    # C6 — ligação errada
    i6 = inspecao('CTR6', d(4, 9), BTP, 'CEOUTRO')
    evento('CTR1', d(1), BTP, 'D')
    evento('CTR2', d(5), BTP, 'D')
    for modelo, linhas in eventos.items():
        session.execute(modelo.__table__.insert(), linhas)

    def ciclo(ce, conteiner, desembarque, **campos):
        c = Ciclo(numeroCEmercante=ce, numeroConteiner=conteiner, codigoRecintoDesembarque=BTP,
                  data_desembarque=desembarque, desembarque_id=next(ids), **campos)
        session.add(c)
        return c

    c1 = ciclo('CE1', 'CTR1', d(1), saida_id=s1.id, data_saida=s1.dataHoraOcorrencia, codigoRecintoArmazenagem=SECO,
               entrada_seco_id=e1.id, data_entrada_seco=e1.dataHoraOcorrencia, saida_seco_id=ss1.id,
               data_saida_seco=ss1.dataHoraOcorrencia, concluido=True)
    c1.escaneamentos.append(Esc(escaneamento_id=i1.id, data_escaneamento=i1.dataHoraOcorrencia,
                                imagem_id=str(IMAGEM)))
    ciclo('CE2', 'CTR2', d(5), concluido=False)
    ciclo('CE3', 'CTR3', d(10), concluido=False)
    ciclo('CE4', 'CTR4', d(7), saida_id=g4.id, concluido=False)
    ciclo('CE5', 'CTR5', d(1), saida_id=s5.id, data_saida=s5.dataHoraOcorrencia, concluido=True)
    c6 = ciclo('CE6', 'CTR6', d(4), concluido=False)
    c6.escaneamentos.append(Esc(escaneamento_id=i6.id, data_escaneamento=i6.dataHoraOcorrencia))
    ciclo('CE2', 'CTR2', d(5, 9), concluido=False)   # C7 duplicado
    session.commit()


def banco_mongo():
    db = mongomock.MongoClient().db_teste
    db['fs.files'].insert_many([
        {'_id': IMAGEM, 'metadata': {'dataescaneamento': d(2, 15), 'numeroinformado': 'CTR1', 'recinto': SECO,
                                     'carga': {'conhecimento': {'conhecimento': 'CE1'}},
                                     'predictions': {'vazio': False}}},
        {'metadata': {'dataescaneamento': d(3), 'numeroinformado': 'CTR5', 'predictions': {'vazio': True}}},
    ])
    return db


class TestDiagnosticoCicloDeVida(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        engine = create_engine('sqlite://')
        for modelo in (Conhecimento, Ciclo, Esc, AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva):
            modelo.__table__.create(engine)
        session = sessionmaker(bind=engine)()
        monta(session)
        saida = io.StringIO()
        with engine.connect() as conexao:
            diag = dcv.Diagnostico(conexao, saida, INICIO, FIM, agora=AGORA, amostras=3)
            cls.falhas = dcv.roda(diag, banco_mongo())
        cls.log = saida.getvalue()

    def linha(self, inicio):
        return next(lin for lin in self.log.splitlines() if lin.startswith(inicio))

    def valores(self, inicio):
        return [c.strip() for c in self.linha(inicio).split('|')[1:]]

    def test_todas_as_secoes_sem_erro(self):
        self.assertEqual(self.falhas, [])
        self.assertNotIn('!! ERRO', self.log)
        for secao in range(1, 13):
            self.assertIn(f'\n{secao}. ', self.log)

    def test_estado_e_inconsistencias(self):
        self.assertIn('Ciclos: 7; pendentes (não concluídos): 5', self.log)
        self.assertEqual(self.valores('saida_id preenchido e data_saida vazia'), ['1'])
        self.assertEqual(self.valores('(CE, contêiner) repetidos'), ['1'])

    def test_trafego(self):
        self.assertEqual(self.valores('9 '), ['1', '0', '1', '0'])  # passagem: 1 ciclo, pendente

    def test_passagem_baldeacao(self):
        self.assertEqual(self.valores('embarque em OUTRO operador'), ['1'])
        self.assertEqual(self.valores('embarque no MESMO operador'), ['0'])

    def test_saidas(self):
        # C2 e a sua duplicata C7 (mesmo contêiner)
        self.assertEqual(self.valores('com saída (acesso efetivado) existente até 93 dias'), ['2'])
        self.assertEqual(self.valores('G '), ['1'])   # C4 aponta para agendamento

    def test_transito_e_entrada_vazio(self):
        self.assertEqual(self.valores('6 a 24 h '), ['vazio', '1'])   # C5

    def test_escaneamentos(self):
        self.assertIn('recuperáveis pelo CE: 1 ciclos', self.log)
        self.assertEqual(self.valores('OUTRO recinto (terceirizado/erro)'), ['-', '1'])
        self.assertIn('OUTRO CE', self.linha('OUTRO CE'))     # C6 ligado com inspeção de outro CE

    def test_risco_de_outra_operacao(self):
        self.assertIn('com OUTRO CE      | vazio', self.log)  # C5: inspeção vazia com outro CE

    def test_amostras_com_linha_do_tempo(self):
        self.assertIn('--- pendente com embarque posterior (passagem?): 1 amostra(s)', self.log)
        self.assertIn('Ciclo ', self.log)
        self.assertIn('LIGADO', self.log)
        self.assertIn('SAÍDA do ciclo', self.log)
        self.assertNotIn('nomeMotorista', self.log)

    def test_mongo(self):
        self.assertEqual(self.valores('metadata.carga.conhecimento'), ['1', '50.0%'])
        self.assertEqual(self.valores('metadata.predictions.vazio'), ['2', '100.0%'])
        self.assertEqual(self.valores('+3 h'), ['1'])   # imagem 3 h "depois" da inspeção (fuso)

    def test_faixas(self):
        self.assertEqual(dcv._faixa(None, dcv.FAIXAS_PERMANENCIA), 'sem data')
        self.assertEqual(dcv._faixa(-5, dcv.FAIXAS_DEFASAGEM), 'antes do desembarque')
        self.assertEqual(dcv._faixa(24 * 200, dcv.FAIXAS_PERMANENCIA), '> 93 dias')


if __name__ == '__main__':
    unittest.main()
