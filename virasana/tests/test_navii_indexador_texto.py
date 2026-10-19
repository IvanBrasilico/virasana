"""Testes do indexador da busca por texto (navii/indexador_texto.py) com um Meilisearch SIMULADO e o Mercante em
SQLite.

Agora = 17/10/2026 12h; carga inicial dos CEs criados desde 18/07/2026 (3 meses). A `create_date` é a da ÚLTIMA
ALTERAÇÃO: o ID 2 (criado em 06/2026, alterado em 15/10/2026) não entra.
  ID 1  01/05/2026 importação, M2 (fora da janela: escala alterada no M2 não pode trazê-lo)
  ID 2  ALTERADO em 15/10/2026, M1 (fora);  ID 3 20/06/2026, M1 (fora)
  ID 4  20/07/2026 importação, M1 (BRRIG 05/08, BRSSZ 10/08 -> referência 10/08); embarcador CNPJ com nome em
        laudo_empresas; consignatário estrangeiro
  ID 5  10/08/2026 EXPORTAÇÃO, M2 (fora do índice)
  ID 6  01/09/2026 importação, M2 (BRRIG 20/09); consignatário CNPJ sem nome
  ID 7  01/10/2026 importação, M3 (sem escala: sem referência)
Escalas e vínculos incluídos em 01/07/2026.
"""
import re
import unittest
from datetime import datetime
from unittest import mock

import requests
from sqlalchemy.orm import sessionmaker

from virasana.tests.test_navii_busca_sqlite import cria_sessao  # 1º: o busca_navii ajusta o sys.path
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Escala, EscalaManifesto  # noqa: I100
from virasana.navii import indexador_texto as it
from virasana.navii import texto_navii as tn
from virasana.navii.busca_navii import Empresa

COM_NOME, SEM_NOME = '11222333000181', '99888777000166'
AGORA = datetime(2026, 10, 17, 12)
INCLUSAO_ESCALAS = datetime(2026, 7, 1)


class MeiliFalso:
    """O Meilisearch em memória, com a interface do ClienteMeili. `falhas`: quantas próximas escritas falham (a
    tarefa termina 'failed' e nada é aplicado)."""

    def __init__(self):
        self.indices, self.escritas, self.falhas, self._tarefas = {}, [], 0, []

    def _tarefa(self, operacao, uid, aplica):
        falha = self.falhas > 0
        if falha:
            self.falhas -= 1
        else:
            aplica()
        self._tarefas.append('failed' if falha else 'succeeded')
        self.escritas.append((operacao, uid))
        return {'taskUid': len(self._tarefas) - 1}

    def espera(self, tarefa, **_):
        if self._tarefas[tarefa['taskUid']] == 'failed':
            raise tn.ErroMeili(f'tarefa {tarefa["taskUid"]} failed')
        return {'status': 'succeeded'}

    def _novo(self, uid, chave):
        return self.indices.setdefault(uid, {'chave': chave, 'docs': {}, 'config': {}})

    def indice(self, uid):
        return {'uid': uid} if uid in self.indices else None

    def cria_indice(self, uid, chave):
        return self._tarefa('cria', uid, lambda: self._novo(uid, chave))

    def configuracao(self, uid):
        return dict(self.indices[uid]['config'])

    def configura(self, uid, configuracao):
        return self._tarefa('configura', uid, lambda: self.indices[uid]['config'].update(configuracao))

    def adiciona(self, uid, documentos, chave):
        def aplica():
            self._novo(uid, chave)['docs'].update({d[chave]: dict(d) for d in documentos})
        return self._tarefa('adiciona', uid, aplica)

    def apaga_por_filtro(self, uid, filtro):
        limite = int(re.fullmatch(r'id < (\d+)', filtro).group(1))

        def aplica():
            docs = self.indices[uid]['docs']
            for chave in [c for c, d in docs.items() if d['id'] < limite]:
                del docs[chave]
        return self._tarefa('apaga', uid, aplica)

    def documento(self, uid, chave):
        indice = self.indices.get(uid)
        return dict(indice['docs'][chave]) if indice and chave in indice['docs'] else None

    def estatisticas(self, uid):
        indice = self.indices.get(uid)
        return {'numberOfDocuments': len(indice['docs']), 'isIndexing': False} if indice else None

    def ces(self):
        return self.indices[tn.INDICE]['docs']


def adiciona_ce(session, id_, data, trafego, manifesto, embarcador='ACME CO', consignatario='FOREIGN LTD'):
    session.add(Conhecimento(ID=id_, numeroCEmercante=f'CE{id_:03d}', create_date=data, tipoTrafego=trafego,
                             manifestoCE=manifesto, embarcador=embarcador, consignatario=consignatario,
                             descricao=f'MERCADORIA {id_}'))


def monta():
    session = cria_sessao()
    session.add_all([Empresa(cnpj=COM_NOME, nome='IMPORTADORA  ALFA LTDA'), Empresa(cnpj=SEM_NOME, nome=' ')])
    for ce in ((1, datetime(2026, 5, 1), '5', 'M2'), (2, datetime(2026, 10, 15), '5', 'M1'),
               (3, datetime(2026, 6, 20), '5', 'M1'), (4, datetime(2026, 7, 20), '5', 'M1', COM_NOME),
               (5, datetime(2026, 8, 10), '7', 'M2'), (6, datetime(2026, 9, 1), '5', 'M2', 'ACME CO', SEM_NOME),
               (7, datetime(2026, 10, 1), '5', 'M3')):
        adiciona_ce(session, *ce)
    session.add_all([
        Escala(numeroDaEscala='E1', portoDaEscala='BRRIG', dataPrevistaAtracacao='2026-08-05',
               create_date=INCLUSAO_ESCALAS),
        Escala(numeroDaEscala='E2', portoDaEscala='BRSSZ', dataPrevistaAtracacao='2026-08-10',
               create_date=INCLUSAO_ESCALAS),
        Escala(numeroDaEscala='E3', portoDaEscala='BRRIG', dataPrevistaAtracacao='2026-09-20',
               create_date=INCLUSAO_ESCALAS),
        EscalaManifesto(ID=1, manifesto='M1', escala='E1', create_date=INCLUSAO_ESCALAS),
        EscalaManifesto(ID=2, manifesto='M1', escala='E2', create_date=INCLUSAO_ESCALAS),
        EscalaManifesto(ID=3, manifesto='M2', escala='E3', create_date=INCLUSAO_ESCALAS)])
    session.commit()
    return session


def indexador(session, meili, agora=AGORA, **kwargs):
    relogio = kwargs.pop('relogio', lambda: agora)
    return it.Indexador(sessionmaker(bind=session.get_bind()), meili, relogio=relogio, bloco_data=2, **kwargs)


class TestCargaInicial(unittest.TestCase):

    def setUp(self):
        self.session, self.meili = monta(), MeiliFalso()
        self.estatisticas, self.cheio = indexador(self.session, self.meili).uma_volta()

    def test_indice_criado_e_configurado(self):
        self.assertEqual(self.meili.indices[tn.INDICE]['chave'], 'ce')
        self.assertEqual(self.meili.indices[tn.INDICE]['config'], tn.CONFIGURACAO)
        self.assertEqual(self.meili.escritas[:2], [('cria', tn.INDICE), ('configura', tn.INDICE)])  # antes dos CEs

    def test_so_importacao_criada_na_janela(self):
        self.assertEqual(sorted(self.meili.ces()), ['CE004', 'CE006', 'CE007'])
        self.assertEqual(self.estatisticas['CEs novos indexados'], 3)
        self.assertEqual(self.estatisticas['IDs lidos'], 4)
        self.assertFalse(self.cheio)

    def test_documentos(self):
        ce4, ce6, ce7 = (self.meili.ces()[c] for c in ('CE004', 'CE006', 'CE007'))
        self.assertEqual((ce4['embarcador'], ce4['embarcadorCnpj']), ('IMPORTADORA ALFA LTDA', COM_NOME))
        self.assertEqual((ce4['consignatario'], ce4['consignatarioCnpj']), ('FOREIGN LTD', ''))
        self.assertEqual(tn.de_segundos(ce4['referencia']), datetime(2026, 8, 10).date())      # a de Santos
        self.assertEqual((ce6['consignatario'], ce6['consignatarioCnpj']), ('', SEM_NOME))     # sem nome cadastrado
        self.assertEqual(tn.de_segundos(ce6['referencia']), datetime(2026, 9, 20).date())      # a 1ª (só BRRIG)
        self.assertIsNone(ce7['referencia'])

    def test_marcas(self):
        marcas = self.meili.documento(tn.INDICE_ESTADO, 'marcas')
        self.assertEqual((marcas['id_inicial'], marcas['ultimo_id']), (4, 7))
        self.assertEqual(marcas['escalas_desde'], INCLUSAO_ESCALAS.isoformat())
        self.assertEqual(marcas['retencao_em'], '2026-10-17')


class TestVoltasSeguintes(unittest.TestCase):

    def setUp(self):
        self.session, self.meili = monta(), MeiliFalso()
        self.indexador = indexador(self.session, self.meili)
        self.indexador.uma_volta()

    def test_novos_e_escala_alterada(self):
        adiciona_ce(self.session, 8, datetime(2026, 10, 16), '5', 'M1')
        adiciona_ce(self.session, 9, datetime(2026, 10, 16), '7', 'M2')
        escala = self.session.query(Escala).filter(Escala.numeroDaEscala == 'E3').one()
        escala.dataPrevistaAtracacao, escala.last_modified = '2026-09-25', datetime(2026, 10, 17, 10)
        self.session.commit()
        estatisticas, _ = self.indexador.uma_volta()
        self.assertEqual(sorted(self.meili.ces()), ['CE004', 'CE006', 'CE007', 'CE008'])   # nem o 1 (M2) nem o 9
        self.assertEqual(tn.de_segundos(self.meili.ces()['CE006']['referencia']), datetime(2026, 9, 25).date())
        self.assertEqual(tn.de_segundos(self.meili.ces()['CE008']['referencia']), datetime(2026, 8, 10).date())
        self.assertEqual(estatisticas['CEs novos indexados'], 1)
        marcas = self.meili.documento(tn.INDICE_ESTADO, 'marcas')
        self.assertEqual((marcas['ultimo_id'], marcas['escalas_desde']), (9, '2026-10-17T10:00:00'))

    def test_vinculo_novo_com_escala_antiga(self):
        escala = self.session.query(Escala).filter(Escala.numeroDaEscala == 'E3').one()
        escala.last_modified = datetime(2026, 10, 17, 10)
        self.session.commit()
        self.indexador.uma_volta()                                           # marca: 10h (a E2 é de 01/07)
        # só o vínculo é novo
        self.session.add(EscalaManifesto(ID=4, manifesto='M3', escala='E2', create_date=datetime(2026, 10, 17, 11)))
        self.session.commit()
        self.indexador.uma_volta()
        self.assertEqual(tn.de_segundos(self.meili.ces()['CE007']['referencia']), datetime(2026, 8, 10).date())

    def test_escala_gravada_depois_com_hora_anterior_a_marca(self):
        escala = self.session.query(Escala).filter(Escala.numeroDaEscala == 'E3').one()
        escala.last_modified = datetime(2026, 10, 17, 10)
        self.session.commit()
        self.indexador.uma_volta()                                           # marca: 10h
        # o ETL grava depois uma alteração com hora 9h57 (transação aberta antes): dentro da sobreposição de 5 min
        escala = self.session.query(Escala).filter(Escala.numeroDaEscala == 'E1').one()
        escala.dataPrevistaAtracacao, escala.last_modified = '2026-08-01', datetime(2026, 10, 17, 9, 57)
        self.session.add(Escala(numeroDaEscala='E5', portoDaEscala='BRRIG', dataPrevistaAtracacao='2026-07-25',
                                create_date=datetime(2026, 10, 17, 9, 54)))       # fora da sobreposição: não vê
        self.session.add(EscalaManifesto(ID=5, manifesto='M3', escala='E5', create_date=datetime(2026, 10, 17, 9, 54)))
        escala_m1 = self.session.query(Escala).filter(Escala.numeroDaEscala == 'E2').one()
        escala_m1.portoDaEscala = 'BRITJ'                                    # M1 sem Santos: vale a 1ª (E1)
        escala_m1.last_modified = datetime(2026, 10, 17, 9, 56)             # (sem isso, o onupdate põe a hora real)
        self.session.commit()
        self.indexador.uma_volta()
        self.assertEqual(tn.de_segundos(self.meili.ces()['CE004']['referencia']), datetime(2026, 8, 1).date())
        self.assertIsNone(self.meili.ces()['CE007']['referencia'])
        self.assertEqual(self.meili.documento(tn.INDICE_ESTADO, 'marcas')['escalas_desde'], '2026-10-17T10:00:00')

    def test_configuracao_so_quando_muda(self):
        self.indexador.uma_volta()
        self.assertEqual(self.meili.escritas.count(('configura', tn.INDICE)), 1)
        self.meili.indices[tn.INDICE]['config']['rankingRules'] = ['words']
        self.indexador.uma_volta()
        self.assertEqual(self.meili.escritas.count(('configura', tn.INDICE)), 2)
        self.assertEqual(self.meili.indices[tn.INDICE]['config']['rankingRules'], tn.CONFIGURACAO['rankingRules'])

    def test_escrita_que_falha_nao_anda_as_marcas(self):
        adiciona_ce(self.session, 8, datetime(2026, 10, 16), '5', 'M3')
        self.session.commit()
        self.meili.falhas = 1
        with self.assertRaises(tn.ErroMeili):
            self.indexador.uma_volta()
        self.assertNotIn('CE008', self.meili.ces())
        self.assertEqual(self.meili.documento(tn.INDICE_ESTADO, 'marcas')['ultimo_id'], 7)
        self.indexador.uma_volta()                                           # a volta seguinte refaz
        self.assertIn('CE008', self.meili.ces())

    def test_indice_sumiu_recomeca(self):
        del self.meili.indices[tn.INDICE]
        self.indexador.uma_volta()
        self.assertEqual(sorted(self.meili.ces()), ['CE004', 'CE006', 'CE007'])
        self.assertEqual(self.meili.indices[tn.INDICE]['config'], tn.CONFIGURACAO)

    def test_retencao_uma_vez_por_dia(self):
        mesmo_dia = indexador(self.session, self.meili, agora=datetime(2026, 10, 17, 23))
        with mock.patch.object(it, 'id_na_data', wraps=it.id_na_data) as busca_do_id:
            self.assertNotIn('retenção: novo ID inicial', mesmo_dia.uma_volta()[0])
        busca_do_id.assert_not_called()                                      # nem calcula o limite de novo
        dia_seguinte = indexador(self.session, self.meili, agora=datetime(2026, 10, 18, 1))
        dia_seguinte.uma_volta()                                             # 12 meses atrás: antes da carga
        self.assertEqual(self.meili.documento(tn.INDICE_ESTADO, 'marcas')['id_inicial'], 4)
        self.assertNotIn(('apaga', tn.INDICE), self.meili.escritas)
        # 10 meses depois: o limite (criados desde 20/08/2026) é o ID 6
        depois = indexador(self.session, self.meili, agora=datetime(2027, 8, 20, 9))
        estatisticas, _ = depois.uma_volta()
        self.assertEqual(estatisticas['retenção: novo ID inicial'], 6)
        self.assertEqual(sorted(self.meili.ces()), ['CE006', 'CE007'])
        # escala alterada do M1 não traz de volta o CE 4 (antes do novo início)
        escala = self.session.query(Escala).filter(Escala.numeroDaEscala == 'E2').one()
        escala.last_modified = datetime(2027, 8, 20, 10)
        self.session.commit()
        depois.uma_volta()
        self.assertNotIn('CE004', self.meili.ces())

    def test_situacao(self):
        adiciona_ce(self.session, 8, datetime(2026, 10, 16), '5', 'M3')
        self.session.commit()
        situacao = self.indexador.situacao()
        self.assertEqual((situacao['último ID do Mercante'], situacao['IDs por indexar']), (8, 1))
        self.assertEqual(situacao['documentos no índice'], 3)


class TestLinhaDeComando(unittest.TestCase):

    def test_ambiente(self):
        problema = it.problema_no_ambiente
        self.assertIn('NAVII_MEILI_CHAVE_ESCRITA não está definida', problema(it._argumentos(['--uma-vez']), {}))
        self.assertIn('NAVII_MEILI_CHAVE_ESCRITA', problema(it._argumentos(['--situacao']), {}))
        self.assertIsNone(problema(it._argumentos(['--uma-vez', '--simula']), {}))      # mede sem as chaves
        self.assertIsNone(problema(it._argumentos([]), {'NAVII_MEILI_CHAVE_ESCRITA': 'e'}))
        self.assertIn('MEILI_MASTER_KEY', problema(it._argumentos(['--chaves']), {'NAVII_MEILI_CHAVE_ESCRITA': 'e'}))
        self.assertIsNone(problema(it._argumentos(['--chaves']), {'MEILI_MASTER_KEY': 'm'}))
        self.assertIn('NAVII_MEILI_CHAVE_BUSCA', problema(it._argumentos(['--busca', 'acme']),
                                                          {'NAVII_MEILI_CHAVE_ESCRITA': 'e'}))
        self.assertIsNone(problema(it._argumentos(['--busca', 'acme']), {'NAVII_MEILI_CHAVE_BUSCA': 'b'}))

    def test_argumentos_da_busca(self):
        args = it._argumentos(['--busca', 'acme co', '--de', '2026-10-01', '--ate', '2026-10-31', '--quantos', '3'])
        self.assertEqual((args.busca, args.de, args.ate, args.quantos),
                         ('acme co', datetime(2026, 10, 1).date(), datetime(2026, 10, 31).date(), 3))

    def test_mostra_busca(self):
        resultado = tn.Resultado(42, [
            tn.Achado('152605000000001', datetime(2026, 10, 20).date(),
                      [('embarcador', '\x02ACME\x03 CO'), ('descrição', '…pecas da \x02acme\x03…')]),
            tn.Achado('152605000000002', None, [])])
        self.assertEqual(it.mostra_busca(resultado).splitlines(), [
            '42 CEs (estimativa do Meilisearch); mostrando 2',
            'CE 152605000000001 — 20/10/2026', '    embarcador: [ACME] CO', '    descrição: …pecas da [acme]…',
            'CE 152605000000002 — sem data de referência'])

    def test_relatorio_de_chaves(self):
        chaves = tn.Chaves({tn.VARIAVEL_BUSCA: 'b1', tn.VARIAVEL_ESCRITA: 'e1'}, [tn.VARIAVEL_BUSCA],
                           [tn.VARIAVEL_ESCRITA], [{'uid': 'u-velha', 'description': 'NAVI-I indexador',
                                                    'createdAt': '2026-10-19T15:00:00Z'}])
        relatorio = it.relatorio_de_chaves(chaves, 'http://127.0.0.1:7700').splitlines()
        self.assertIn("export NAVII_MEILI_CHAVE_BUSCA='b1'", relatorio)
        self.assertIn("export NAVII_MEILI_CHAVE_ESCRITA='e1'", relatorio)
        self.assertIn('# criadas agora: NAVII_MEILI_CHAVE_BUSCA', relatorio)
        uid_escrita = tn.CHAVES[tn.VARIAVEL_ESCRITA]['uid']
        self.assertTrue(any(linha.startswith('# ATENÇÃO') and f'/keys/{uid_escrita}' in linha for linha in relatorio))
        self.assertIn('curl -X DELETE http://127.0.0.1:7700/keys/u-velha -H "Authorization: Bearer $MEILI_MASTER_KEY"'
                      '   # NAVI-I indexador, criada em 2026-10-19T15:00:00Z', relatorio)


class TestPaginasESimulacao(unittest.TestCase):

    def test_carga_em_paginas_com_fila(self):
        meili = MeiliFalso()
        paginado = indexador(monta(), meili, lote=2, paginas=1)
        estatisticas, cheio = paginado.uma_volta()
        self.assertTrue(cheio)                                               # IDs 4 e 5: página cheia
        self.assertEqual((sorted(meili.ces()), estatisticas['IDs lidos']), (['CE004'], 2))
        self.assertTrue(paginado.uma_volta()[1])                             # 6 e 7
        self.assertFalse(paginado.uma_volta()[1])                            # nada mais
        self.assertEqual(sorted(meili.ces()), ['CE004', 'CE006', 'CE007'])
        self.assertEqual(meili.documento(tn.INDICE_ESTADO, 'marcas')['ultimo_id'], 7)

    def test_simulacao_nao_escreve_nada(self):
        meili = MeiliFalso()
        estatisticas, _ = indexador(monta(), meili, simula=True).uma_volta()
        self.assertEqual(estatisticas['CEs novos indexados'], 3)
        self.assertEqual((meili.indices, meili.escritas), ({}, []))

    def test_simulacao_antes_de_instalar_o_meilisearch(self):
        fora_do_ar = tn.ClienteMeili('http://127.0.0.1:9', timeout=0.5)                 # porta sem ninguém
        estatisticas, _ = indexador(monta(), fora_do_ar, simula=True).uma_volta()
        self.assertEqual(estatisticas['CEs novos indexados'], 3)
        with self.assertRaises(requests.RequestException):                                # o serviço não engole
            indexador(monta(), fora_do_ar).uma_volta()


if __name__ == '__main__':
    unittest.main()
