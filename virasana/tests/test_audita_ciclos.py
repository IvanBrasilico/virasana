"""Testes da auditoria dos ciclos gravados pelas invariantes (`audita_ciclos.py`, 22/10/2026).

Ciclos (desembarque em 07/10/2026 no 8931356, porto BRSSZ pela escala ES):
  1 bom (TRA concluído com a saída do seco); 2 principal de exportação; 3 cabotagem saindo de Santos, sem escala
  gravada (porto pelo recinto); 4 TRA concluído sem a saída do seco; 5 do loop antigo e 6 fora do período: não
  auditados.
"""
import csv
import os
import tempfile
import unittest
from datetime import datetime

from sqlalchemy import create_engine

from virasana.analytic_scripts.ciclo_de_vida import audita_ciclos as ac
from virasana.analytic_scripts.ciclo_de_vida import gravacao_ciclo as gc
from virasana.analytic_scripts.ciclo_de_vida.caso_ciclo import Saida
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Escala, Item

IMP, EXP, CAB, IMP2 = '152600000000001', '152600000000007', '072600000000003', '152600000000004'
SBP = '8931356'


def banco():
    engine = create_engine('sqlite://')
    for modelo in (Conhecimento, Item, Escala):
        modelo.__table__.create(engine)
    gc.metadata.create_all(engine)
    with engine.begin() as c:
        c.execute(Escala.__table__.insert(), [{'numeroDaEscala': 'ES', 'portoDaEscala': 'BRSSZ'}])
        ces = ((IMP, '5', 'CNSHA'), (EXP, '7', 'BRSSZ'), (CAB, '3', 'BRSSZ'), (IMP2, '5', 'CNSHA'))
        for ce, trafego, origem in ces:
            c.execute(Conhecimento.__table__.insert(), {'numeroCEmercante': ce, 'tipoTrafego': trafego,
                                                        'portoOrigemCarga': origem})
        for ce, conteiner in ((IMP, 'C1'), (EXP, 'C2'), (CAB, 'C3'), (IMP2, 'C4')):
            c.execute(Item.__table__.insert(), {'numeroCEmercante': ce, 'codigoConteiner': conteiner})
        dia = datetime(2026, 10, 7, 10)
        ciclos = [(1, 'C1', IMP, 'ES', 'concluido', 'tra', datetime(2026, 10, 12), 'v2', dia),
                  (2, 'C2', EXP, 'ES', 'aberto', 'indefinido', None, 'v2', dia),
                  (3, 'C3', CAB, None, 'aberto', 'indefinido', None, 'v2', dia),
                  (4, 'C4', IMP2, 'ES', 'concluido', 'tra', None, 'v2', dia),
                  (5, 'C5', EXP, 'ES', None, None, None, None, dia),                    # loop antigo
                  (6, 'C6', EXP, 'ES', 'aberto', 'indefinido', None, 'v2', datetime(2026, 9, 1))]   # fora do período
        for id_, conteiner, ce, escala, situacao, fluxo, saida_seco, versao, data in ciclos:
            c.execute(gc.CICLOS.insert(), dict(
                ID=id_, desembarque_id=id_, numeroConteiner=conteiner, numeroCEmercante=ce, escala=escala,
                situacao=situacao, fluxo=fluxo, data_saida_seco=saida_seco, versao_heuristica=versao,
                data_desembarque=data, codigoRecintoDesembarque=SBP, confianca_ce='media'))
            c.execute(gc.CICLO_CE.insert(), dict(ciclo_de_vida_id=id_, numeroCEmercante=ce, papel='simples',
                                                 principal=True))
    return engine


class TestAuditoria(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with banco().connect() as conexao:
            auditoria = ac.Auditoria(conexao, datetime(2026, 10, 1), datetime(2026, 10, 15))
            cls.achados = auditoria.executa()
            cls.total = auditoria.total

    def test_violacoes(self):
        self.assertEqual(self.total, 4)
        self.assertEqual({a['desembarque_id']: a['violacoes'] for a in self.achados},
                         {2: 'CE principal de exportação | CE principal com a carga saindo de BRSSZ',
                          3: 'CE principal com a carga saindo de BRSSZ',     # porto pelo recinto (sem escala gravada)
                          4: 'concluído (tra) sem data_saida_seco'})

    def test_relatorio_e_csv(self):
        saida = Saida()
        with tempfile.TemporaryDirectory() as pasta:
            caminho = os.path.join(pasta, 'auditoria.csv')
            ac.relatorio(self.achados, self.total, saida, caminho)
            with open(caminho, encoding='utf-8') as arquivo:
                linhas = list(csv.DictReader(arquivo, delimiter=';'))
        texto = '\n'.join(saida.linhas)
        self.assertIn('Ciclos v2 auditados: 4; com violação: 3', texto)
        self.assertIn('CE principal com a carga saindo do porto: 2', texto)
        self.assertIn(f'{SBP} CE principal de exportação: 1', texto)
        self.assertIn('[concluído (tra) sem data_saida_seco] C4 desembarque 4', texto)
        self.assertEqual([linha['conteiner'] for linha in linhas], ['C2', 'C3', 'C4'])

    def test_regra_sem_o_porto(self):
        self.assertEqual(ac.regra('CE principal com a carga saindo de BRRIO'),
                         'CE principal com a carga saindo do porto')


if __name__ == '__main__':
    unittest.main()
