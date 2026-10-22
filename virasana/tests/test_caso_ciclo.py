"""Testes do script de CASO do ciclo de vida (`caso_ciclo.py`, 21/10/2026): extração de um banco (aqui, SQLite com os
mesmos modelos), JSON, reprocesso explicado e comparação com o gravado.

Cenário (parecido com o caso real de 21/10/2026, com números fictícios):
  IMP 152600000000101: importação (tráfego 5), manifesto MANIMP carregado em CNSHA, escala em Santos 02/10;
      contêineres FANU1111111 e CAAU2222222;
  CAB 072600000000202: cabotagem (tráfego 3) com origem em Santos (BRSSZ -> BRMAO), manifesto MANCAB carregado em
      BRSSZ, escalas em Santos 08/10 e Manaus 20/10; contêiner CAAU2222222;
  eventos: os dois desembarcam na BTP em 07/10 sem CE; o CAAU2222222 sai do recinto... e embarca na BTP em 09/10
      declarando o CAB; um acesso com placa e CPF (que NÃO podem ir para o caso);
  gravado: o ciclo do CAAU2222222 com o CAB (como em produção).
"""
import json
import unittest
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from virasana.analytic_scripts.ciclo_de_vida import caso_ciclo
from virasana.analytic_scripts.ciclo_de_vida import gravacao_ciclo as gc
from virasana.analytic_scripts.ciclo_de_vida.caso_ciclo import Saida, banco_do_caso, extrai_caso, reprocessa
from virasana.integracao.mercante.mercantealchemy import (
    Conhecimento, ConteinerVazio, Escala, EscalaManifesto, Item, Manifesto,
)
from virasana.navii.busca_navii import tabela_recintos

from bhadrasana.models.apirecintos import (  # noqa: I100
    AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva, PesagemVeiculo,
)

IMP, CAB = '152600000000101', '072600000000202'
FANU, CAAU = 'FANU1111111', 'CAAU2222222'
BTP = '8931359'
AGORA = datetime(2026, 10, 21, 12)
DESDE = datetime(2026, 8, 1)


def _insere(conexao, modelo, linhas):
    for linha in linhas:
        conexao.execute(modelo.__table__.insert(), linha)


def banco_de_origem():
    engine = create_engine('sqlite://', poolclass=StaticPool, connect_args={'check_same_thread': False})
    for modelo in (Conhecimento, Item, Escala, EscalaManifesto, Manifesto, ConteinerVazio, EmbarqueDesembarque,
                   AcessoVeiculo, InspecaoNaoInvasiva, PesagemVeiculo):
        modelo.__table__.create(engine)
    tabela_recintos.create(engine)
    gc.metadata.create_all(engine)
    with engine.begin() as c:
        c.execute(text('ALTER TABLE ovr_recintos ADD COLUMN e_operador BOOLEAN'))
        c.execute(text('ALTER TABLE ovr_recintos ADD COLUMN cod_unidade VARCHAR(7)'))
        c.execute(text('INSERT INTO ovr_recintos (id, nome, cod_siscomex, e_operador, cod_unidade) VALUES '
                       "(1, 'BTP', '8931359', 1, '0817800'), (2, 'EADI', '8932000', 0, '0817800')"))
        _insere(c, Conhecimento, [
            dict(numeroCEmercante=IMP, manifestoCE='MANIMP', tipoTrafego='5', tipoBLConhecimento='10',
                 portoOrigemCarga='CNSHA', portoDestFinal='BRSSZ', consignatario='11222333000181'),
            dict(numeroCEmercante=CAB, manifestoCE='MANCAB', tipoTrafego='3', tipoBLConhecimento='10',
                 portoOrigemCarga='BRSSZ', portoDestFinal='BRMAO'),
            dict(numeroCEmercante='152500000000999', manifestoCE='MANVELHO', tipoTrafego='5')])   # outro ano
        _insere(c, Item, [dict(numeroCEmercante=IMP, codigoConteiner=FANU, numeroSequencialItemCarga='1'),
                          dict(numeroCEmercante=IMP, codigoConteiner=CAAU, numeroSequencialItemCarga='2'),
                          dict(numeroCEmercante=CAB, codigoConteiner=CAAU, numeroSequencialItemCarga='1'),
                          dict(numeroCEmercante='152500000000999', codigoConteiner='OUTR0000000')])
        _insere(c, Manifesto, [dict(numero='MANIMP', portoCarregamento='CNSHA', portoDescarregamento='BRSSZ'),
                               dict(numero='MANCAB', portoCarregamento='BRSSZ', portoDescarregamento='BRMAO')])
        _insere(c, Escala, [
            dict(numeroDaEscala='ESCIMP', portoDaEscala='BRSSZ', dataPrevistaAtracacao='2026-10-02'),
            dict(numeroDaEscala='ESCCAB1', portoDaEscala='BRSSZ', dataPrevistaAtracacao='2026-10-08'),
            dict(numeroDaEscala='ESCCAB2', portoDaEscala='BRMAO', dataPrevistaAtracacao='2026-10-20')])
        _insere(c, EscalaManifesto, [dict(ID=1, manifesto='MANIMP', escala='ESCIMP'),
                                     dict(ID=2, manifesto='MANCAB', escala='ESCCAB1'),
                                     dict(ID=3, manifesto='MANCAB', escala='ESCCAB2')])
        evento = dict(codigoRecinto=BTP, tipoOperacao='I')
        _insere(c, EmbarqueDesembarque, [
            dict(id=11, numeroConteiner=FANU, dataHoraOcorrencia=datetime(2026, 10, 7, 10), embarqueDesembarque='D',
                 escala='ESCIMP', **evento),
            dict(id=12, numeroConteiner=CAAU, dataHoraOcorrencia=datetime(2026, 10, 7, 11), embarqueDesembarque='D',
                 escala='ESCIMP', **evento),
            dict(id=13, numeroConteiner=CAAU, dataHoraOcorrencia=datetime(2026, 10, 9, 15), embarqueDesembarque='E',
                 numeroConhecimento=CAB, escala='ESCCAB1', **evento),
            dict(id=14, numeroConteiner=CAAU, dataHoraOcorrencia=datetime(2026, 7, 1), embarqueDesembarque='D',
                 **evento)])   # antes do período: não vem
        _insere(c, AcessoVeiculo, [
            dict(id=21, numeroConteiner=FANU, dataHoraOcorrencia=datetime(2026, 10, 8, 9), operacao='C', direcao='S',
                 placa='ABC1D23', cpfMotorista='12345678901', nomeMotorista='FULANO', vazioConteiner=False,
                 **evento)])
        _insere(c, PesagemVeiculo, [dict(id=31, numeroConteiner=FANU, dataHoraOcorrencia=datetime(2026, 10, 8, 9),
                                         placa='ABC1D23', pesoBrutoBalanca=30000, **evento)])
        ciclo_id = c.execute(gc.CICLOS.insert(), dict(
            numeroConteiner=CAAU, numeroCEmercante=CAB, desembarque_id=12, data_desembarque=datetime(2026, 10, 7, 11),
            codigoRecintoDesembarque=BTP, situacao='encerrado', fluxo='passagem', confianca_ce='alta',
            versao_heuristica='v2')).inserted_primary_key[0]
        c.execute(gc.CICLO_CE.insert(), dict(ciclo_de_vida_id=ciclo_id, numeroCEmercante=CAB, papel='simples',
                                             principal=True, confianca='alta'))
    return engine


class TestExtracao(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with banco_de_origem().connect() as conexao:
            cls.caso = extrai_caso(conexao, ['1526.0000.0000.101'], [], DESDE, AGORA)

    def linhas(self, tabela):
        return self.caso['tabelas'].get(tabela, [])

    def test_conteineres_dos_itens_dos_ces_entram(self):
        self.assertEqual(self.caso['meta']['conteineres'], [CAAU, FANU])
        self.assertEqual(self.caso['meta']['ces'], [IMP])

    def test_mercante_traz_o_ce_da_cabotagem_pelo_item_e_as_escalas(self):
        self.assertEqual({c['numeroCEmercante'] for c in self.linhas('conhecimentosresumo')}, {IMP, CAB})
        self.assertEqual(len(self.linhas('itensresumo')), 3)
        self.assertEqual({m['numero'] for m in self.linhas('manifestosresumo')}, {'MANIMP', 'MANCAB'})
        self.assertEqual({e['numeroDaEscala'] for e in self.linhas('escalasresumo')}, {'ESCIMP', 'ESCCAB1', 'ESCCAB2'})
        self.assertNotIn('consignatario', self.linhas('conhecimentosresumo')[0])

    def test_eventos_do_periodo_sem_cpf_cnpj_nem_nome(self):
        self.assertEqual(sorted(e['id'] for e in self.linhas('apirecintos_embarquedesembarque')), [11, 12, 13])
        texto = json.dumps(self.caso)
        for pessoal in ('12345678901', 'FULANO', '11222333000181'):   # CPF e nome do motorista, CNPJ do consignatário
            self.assertNotIn(pessoal, texto)
        self.assertEqual(self.linhas('apirecintos_acessosveiculo')[0]['placa'], 'ABC1D23')   # a placa fica
        self.assertNotIn('placas', self.caso['meta'])

    def test_placas_apelidadas_se_pedido(self):
        with banco_de_origem().connect() as conexao:
            caso = extrai_caso(conexao, [IMP], [], DESDE, AGORA, apelidar_placas=True)
        self.assertNotIn('ABC1D23', json.dumps(caso))
        tabelas = caso['tabelas']
        acesso, pesagem = tabelas['apirecintos_acessosveiculo'][0], tabelas['apirecintos_pesagensveiculo'][0]
        self.assertEqual((acesso['placa'], pesagem['placa']), ('P000001', 'P000001'))   # a mesma placa, o mesmo apelido
        self.assertEqual(caso['meta']['placas'], 1)

    def test_gravado_e_recintos(self):
        self.assertEqual([c['numeroCEmercante'] for c in self.caso['gravado']['ciclos']], [CAB])
        self.assertEqual([c['numeroCEmercante'] for c in self.caso['gravado']['ces']], [CAB])
        self.assertEqual({r['cod_siscomex'] for r in self.caso['recintos']}, {BTP, '8932000'})

    def test_json_volta_para_um_banco_igual(self):
        engine = banco_do_caso(json.loads(json.dumps(self.caso)))
        with engine.connect() as conexao:
            self.assertEqual(conexao.execute(text('SELECT COUNT(*) FROM itensresumo')).scalar(), 3)
            self.assertEqual(conexao.execute(text(
                'SELECT dataHoraOcorrencia FROM apirecintos_embarquedesembarque WHERE id = 13')).scalar(),
                '2026-10-09 15:00:00.000000')
            self.assertEqual(conexao.execute(text('SELECT COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao')
                                             ).scalar(), 0)   # o gravado não entra no banco do reprocesso


class TestReprocesso(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with banco_de_origem().connect() as conexao:
            caso = extrai_caso(conexao, [IMP, CAB], [], DESDE, AGORA)
        cls.saida = Saida()
        cls.ciclos = reprocessa(caso, cls.saida)
        cls.texto = '\n'.join(cls.saida.linhas)

    def test_relatorio_do_caso(self):
        self.assertIn(f'CE {CAB}: tráfego 3 BL 10 master None origem BRSSZ -> destino BRMAO', self.texto)
        self.assertIn('Manifesto MANCAB: tráfego None carregamento BRSSZ -> descarregamento BRMAO', self.texto)
        self.assertIn('escala ESCCAB1: porto BRSSZ atracação prevista 2026-10-08', self.texto)
        self.assertIn('09/10/2026 15:00 8931359 BTP: EMBARQUE escala ESCCAB1', self.texto)
        self.assertIn(f'ciclo 1 (v2) {CAAU}: CE {CAB} confiança alta', self.texto)

    def test_candidatos_com_origem_carregamento_e_distancia(self):
        self.assertIn(f'candidato {CAB}: master None tráfego 3 origem BRSSZ -> destino BRMAO; manifesto MANCAB '
                      "carregado em BRSSZ; escalas ['2026-10-08 BRSSZ', '2026-10-20 BRMAO'] -> 1 dia(s) na janela, "
                      'DECLARADO, carga SAINDO de BRSSZ', self.texto)
        self.assertIn(f'candidato {IMP}: master None tráfego 5 origem CNSHA -> destino BRSSZ; manifesto MANIMP '
                      "carregado em CNSHA; escalas ['2026-10-02 BRSSZ'] -> 5 dia(s) na janela, manifesto na escala do "
                      'desembarque', self.texto)
        self.assertIn("escala declarada no desembarque: ESCIMP (manifestos dela: ['MANIMP'])", self.texto)
        self.assertIn('ESCOLHA: principal', self.texto)

    def test_servico_e_comparacao(self):
        self.assertEqual(self.ciclos[11]['principal'], IMP)   # o FANU só tem o CE de importação
        # o CAAU: a cabotagem declarada no embarque SAI de Santos (porto do desembarque) — é a perna seguinte da
        # baldeação, e o ciclo do desembarque fica com o CE de importação, concluído pelo embarque
        self.assertEqual((self.ciclos[12]['principal'], self.ciclos[12]['situacao']), (IMP, 'concluido'))
        self.assertIn('5. Serviço (código atual) x gravado', self.texto)
        self.assertRegex(self.texto, r'desembarque 12 CAAU2222222: (igual|DIFERENTE) — atual')
        self.assertIn('desembarque 11 FANU1111111: só no reprocesso — atual', self.texto)


class TestSombraComOPorto(unittest.TestCase):
    """A sombra (`sombra_ciclo_de_vida.py`) usa o porto do desembarque como o serviço e rotula a troca do CE gravado
    pela cabotagem que sai do porto como "perna seguinte"."""

    def test_sombra(self):
        import io
        from virasana.analytic_scripts.ciclo_de_vida import sombra_ciclo_de_vida as sombra
        with banco_de_origem().connect() as conexao:
            execucao = sombra.Sombra(conexao, io.StringIO(), datetime(2026, 10, 1), datetime(2026, 10, 15),
                                     conteineres=100, agora=AGORA)
            self.assertEqual(sombra.roda(execucao), [])
        linhas = {linha['conteiner']: linha for linha in execucao.linhas}
        self.assertEqual((linhas[CAAU]['ce_atual'], linhas[CAAU]['ce_novo'], linhas[CAAU]['ce_comparacao']),
                         (CAB, IMP, 'perna seguinte'))
        self.assertEqual(execucao.portos[12], 'BRSSZ')


class TestArgumentos(unittest.TestCase):

    def test_exige_ces_conteineres_ou_reprocessa(self):
        with self.assertRaises(SystemExit):
            caso_ciclo._argumentos([])
        self.assertEqual(caso_ciclo._argumentos(['--ces', IMP]).ces, [IMP])
        self.assertEqual(caso_ciclo._argumentos(['--reprocessa', 'x.json']).reprocessa, 'x.json')
        self.assertTrue(caso_ciclo._argumentos(['--ces', IMP, '--apelida-placas']).apelida_placas)


if __name__ == '__main__':
    unittest.main()
