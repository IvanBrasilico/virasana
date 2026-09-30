import random
import sys
import time
from datetime import datetime, timedelta

from pymongo import MongoClient

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')

from bhadrasana.models.apirecintos import EmbarqueDesembarque, AcessoVeiculo, InspecaoNaoInvasiva
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import CicloDeVidaImportacao, \
    CicloDeVidaImportacaoEscaneamentos, logger
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida_helpers import (
    busca_ce_mercante_conteiner_desembarque, busca_desembarque_conteiner,
    busca_acesso_veiculo_desembarque_conteiner,
    busca_escaneamentos_desembarque_conteiner, busca_mongo,
    busca_entrada_seco,
    busca_saida_seco,
    busca_escaneamentos_seco, HORAS_TRANSIT_TIME, HORAS_PERMANENCIA_MOLHADO)

OPERADORES = ('8931356', '8931359', '8931404')


class CicloDeVidaImportacaoManager():
    def __init__(self, db, session, ciclo_de_vida: CicloDeVidaImportacao):
        self.db = db
        self.session = session
        self.ciclo_de_vida = ciclo_de_vida

    def get_conhecimento(self):
        if self.ciclo_de_vida.numeroCEmercante:
            return self.ciclo_de_vida.numeroCEmercante
        if self.ciclo_de_vida.numeroConteiner is None or self.ciclo_de_vida.data_desembarque is None:
            return None
        # Busca CE por numeroConteiner E data_desembarque
        cemercante = busca_ce_mercante_conteiner_desembarque(self.session,
                                                             self.ciclo_de_vida.numeroConteiner,
                                                             self.ciclo_de_vida.data_desembarque)
        if cemercante is None:
            return None
        self.ciclo_de_vida.set_cemercante(cemercante)
        try:
            self.session.add(self.ciclo_de_vida)
            self.session.commit()
            return cemercante.numeroCEmercante
        except Exception as e:
            logger.error(e)
            self.session.rollback()
            return None

    def get_desembarque(self):
        if self.ciclo_de_vida.desembarque_id:
            return self.ciclo_de_vida.desembarque_id
        if self.ciclo_de_vida.numeroConteiner is None or self.ciclo_de_vida.data_desembarque is None:
            return None
        # Busca Desembarque por numeroConteiner E data_desembarque
        desembarque = busca_desembarque_conteiner(self.session,
                                                  self.ciclo_de_vida.numeroConteiner,
                                                  self.ciclo_de_vida.data_desembarque)
        if desembarque is None:
            return None
        self.ciclo_de_vida.set_desembarque(desembarque)
        try:
            self.session.add(self.ciclo_de_vida)
            self.session.commit()
            return desembarque.id
        except Exception as e:
            logger.error(e)
            self.session.rollback()
            return None

    def get_saida(self):
        if self.ciclo_de_vida.saida_id:
            return self.ciclo_de_vida.saida_id
        if (self.ciclo_de_vida.numeroConteiner is None or self.ciclo_de_vida.desembarque_id is None
                or self.ciclo_de_vida.codigoRecintoDesembarque is None):
            return None
        saida = busca_acesso_veiculo_desembarque_conteiner(self.session,
                                                           self.ciclo_de_vida.codigoRecintoDesembarque,
                                                           self.ciclo_de_vida.numeroConteiner,
                                                           self.ciclo_de_vida.data_desembarque)
        if saida is None:
            return None
        self.ciclo_de_vida.set_saida(saida)
        try:
            self.session.add(self.ciclo_de_vida)
            self.session.commit()
            return saida.id
        except Exception as e:
            logger.error(e)
            self.session.rollback()
            return None

    def get_escaneamentos(self):
        if (self.ciclo_de_vida.desembarque_id is None or self.ciclo_de_vida.numeroConteiner is None or
                self.ciclo_de_vida.codigoRecintoDesembarque is None):
            return []
        if self.ciclo_de_vida.data_saida is not None:
            data_saida = self.ciclo_de_vida.data_saida
        else:
            data_saida = self.ciclo_de_vida.data_desembarque + timedelta(hours=HORAS_PERMANENCIA_MOLHADO)
        escaneamentos = busca_escaneamentos_desembarque_conteiner(self.session,
                                                                  self.ciclo_de_vida.codigoRecintoDesembarque,
                                                                  self.ciclo_de_vida.numeroConteiner,
                                                                  self.ciclo_de_vida.data_desembarque,
                                                                  data_saida)
        result = []
        for escaneamento in escaneamentos:
            ciclo_escaneamento = self.session.query(CicloDeVidaImportacaoEscaneamentos).filter(
                CicloDeVidaImportacaoEscaneamentos.escaneamento_id == escaneamento.id).one_or_none()
            if ciclo_escaneamento is None:
                ciclo_escaneamento = CicloDeVidaImportacaoEscaneamentos()
                ciclo_escaneamento.ciclo_de_vida_id = self.ciclo_de_vida.ID
                ciclo_escaneamento.escaneamento_id = escaneamento.id
                ciclo_escaneamento.data_escaneamento = escaneamento.dataHoraOcorrencia
                self.session.add(ciclo_escaneamento)
                result.append(escaneamento.id)
        try:
            self.session.commit()
            return result
        except Exception as e:
            logger.error(e)
            self.session.rollback()
            return []

    def get_escaneamentos_seco(self):
        # Como ficam os escaneamentos por delegação?? Será necessário mais uma entrada e saída seco?
        # Será necessário distinguir escaneamento de um recinto/período e outro?
        # Provalmente a parte de escaneamento precisará de mais informações
        if self.ciclo_de_vida.numeroConteiner is None or self.ciclo_de_vida.entrada_seco_id is None:
            return []
        escaneamentos = busca_escaneamentos_seco(
            self.session,
            self.ciclo_de_vida.codigoRecintoArmazenagem,
            self.ciclo_de_vida.numeroConteiner,
            self.ciclo_de_vida.data_entrada_seco,
        )
        result = []
        for escaneamento in escaneamentos:
            ciclo_escaneamento = self.session.query(CicloDeVidaImportacaoEscaneamentos).filter(
                CicloDeVidaImportacaoEscaneamentos.escaneamento_id == escaneamento.id).one_or_none()
            if ciclo_escaneamento is None:
                ciclo_escaneamento = CicloDeVidaImportacaoEscaneamentos()
                ciclo_escaneamento.ciclo_de_vida_id = self.ciclo_de_vida.ID
                ciclo_escaneamento.escaneamento_id = escaneamento.id
                ciclo_escaneamento.data_escaneamento = escaneamento.dataHoraOcorrencia
                self.session.add(ciclo_escaneamento)
                result.append(escaneamento.id)
        try:
            self.session.commit()
            return result
        except Exception as e:
            logger.error(e)
            self.session.rollback()
            return []

    def get_imagens(self):
        result = []
        if self.ciclo_de_vida.numeroConteiner is None:
            return result
        for ciclo_escaneamento in self.ciclo_de_vida.escaneamentos:
            _id = busca_mongo(self.db,
                              self.ciclo_de_vida.numeroConteiner,
                              ciclo_escaneamento.data_escaneamento)
            if _id is not None:
                if _id in result:
                    logger.error(f'Tentou inserir mesmo _id duas vezes: ciclo de vida: `{self.ciclo_de_vida.ID}`'
                                 f'Escaneamento {ciclo_escaneamento.escaneamento_id}, '
                                 f'{self.ciclo_de_vida.numeroConteiner} '
                                 f'{ciclo_escaneamento.data_escaneamento}, {_id}')
                    continue
                ciclo_escaneamento.imagem_id = _id
                self.session.add(ciclo_escaneamento)
                result.append(_id)
        try:
            self.session.commit()
        except Exception as e:
            logger.error(e)
            self.session.rollback()
        return result

    def get_entrada_seco(self):
        # logger.info(f'get_entrada_seco {self.ciclo_de_vida.codigoRecintoDesembarque}, '
        #            f'{self.ciclo_de_vida.numeroConteiner}, {self.ciclo_de_vida.data_saida}')
        if self.ciclo_de_vida.entrada_seco_id:
            # logger.info('Saindo de get_entrada_seco {self.ciclo_de_vida.entrada_seco_id}')
            return self.ciclo_de_vida.entrada_seco_id
        if (self.ciclo_de_vida.numeroConteiner is None or self.ciclo_de_vida.codigoRecintoDesembarque is None
                or self.ciclo_de_vida.saida_id is None):
            # logger.info('Saindo de get_entrada_seco None')
            return None
        entrada_seco = busca_entrada_seco(
            self.session,
            self.ciclo_de_vida.codigoRecintoDesembarque,
            self.ciclo_de_vida.numeroConteiner,
            self.ciclo_de_vida.data_saida,
        )
        if entrada_seco is None:
            return None
        self.ciclo_de_vida.set_entrada_seco(entrada_seco)
        try:
            self.session.add(self.ciclo_de_vida)
            self.session.commit()
            return entrada_seco.id
        except Exception as e:
            logger.error(e)
            self.session.rollback()
            return None

    def get_saida_seco(self):
        if self.ciclo_de_vida.saida_seco_id:
            return self.ciclo_de_vida.saida_seco_id
        if (self.ciclo_de_vida.entrada_seco_id is None or self.ciclo_de_vida.numeroConteiner is None or
                self.ciclo_de_vida.codigoRecintoArmazenagem is None):
            return None
        saida_seco = busca_saida_seco(
            self.session,
            self.ciclo_de_vida.codigoRecintoArmazenagem,
            self.ciclo_de_vida.numeroConteiner,
            self.ciclo_de_vida.data_entrada_seco,
        )
        if saida_seco is None:
            return None
        self.ciclo_de_vida.saida_seco_id = saida_seco.id
        self.ciclo_de_vida.data_saida_seco = saida_seco.dataHoraOcorrencia
        try:
            self.session.add(self.ciclo_de_vida)
            self.session.commit()
            return saida_seco.id
        except Exception as e:
            logger.error(e)
            self.session.rollback()
            return None

    def check_ciclo_concluido(self):
        # Entrega pelo recinto depositário, portanto concluído
        if self.ciclo_de_vida.saida_seco_id is not None:
            self.ciclo_de_vida.concluido = True
        # Assume entrega pelo Operador se transit time estourou para não ficar preso...
        elif self.ciclo_de_vida.entrada_seco_id is None and self.ciclo_de_vida.saida_id is not None:
            if datetime.now() > self.ciclo_de_vida.data_saida + timedelta(hours=HORAS_TRANSIT_TIME):
                self.ciclo_de_vida.concluido = True
        # E se as saídas forem omitidas? Estabelecer uma validade máxima?? Usar prazo de abandono + 3 dias!
        elif self.ciclo_de_vida.desembarque_id is not None:
            if datetime.now() > self.ciclo_de_vida.data_desembarque + timedelta(days=93):
                self.ciclo_de_vida.concluido = True


def completar_ciclo_de_vida(db, session, ciclo):
    manager = CicloDeVidaImportacaoManager(db, session, ciclo)
    manager.get_conhecimento()  # Custo zero se já existir
    manager.get_desembarque()  # Custo zero se já existir
    if manager.get_saida() is None:  # Custo zero se já existir
        # logger.info('escaneamentos e imagens')
        manager.get_escaneamentos()  # Tem custo, só pesquisar se não saiu do Operador - ver se dá tolerância de tempo??
        manager.get_imagens()
    else:  # Já saiu do Operador, procurar eventos fora
        # logger.info(f'Procurando entrada seco!!!')
        manager.get_entrada_seco()  # Custo zero se já existir
        # logger.info(f'Procurando saída seco!!!')
        if manager.get_saida_seco() is not None:  # Custo zero se já existir
            manager.get_escaneamentos_seco()  # Tem custo, só pesquisar se não saiu do Recinto - ver se dá tolerância de tempo??
            manager.get_imagens()
    # E se olhou o passado e não pegou nenhum escaneamento e nenhuma imagem? Repetir tentativa? Qual o custo?
    session.refresh(manager.ciclo_de_vida)
    if manager.get_desembarque() is not None and len(manager.ciclo_de_vida.escaneamentos) == 0:
        manager.get_escaneamentos()
        manager.get_escaneamentos_seco()
        manager.get_imagens()
    manager.check_ciclo_concluido()
    try:
        session.add(ciclo)
        session.commit()
        return True
    except Exception as e:
        logger.error(e)
        session.rollback()
        return False


def processar_ciclos_pendentes(db, session, chunk_size=1000, chance_pulo=0.9, fator_pulo_min=1.0, fator_pulo_max=4.0):
    """
    Processa ciclos pendentes com saltos probabilísticos de IDs.

    A cada iteração:
      - 90% de chance de pular entre 1x e 4x o chunk_size
      - 10% de chance de avançar apenas chunk_size (sem pulo extra)

    O pulo é feito sobre os IDs, não apenas no tamanho do lote.
    """
    # Obtém o último ID dos ciclos não concluídos
    max_id_result = (
        session.query(func.max(CicloDeVidaImportacao.ID))
        .filter(CicloDeVidaImportacao.concluido.isnot(True))
        .scalar()
    )

    if max_id_result is None:
        logger.info("Nenhum ciclo pendente encontrado.")
        return 0, 0

    max_id_pendentes = max_id_result
    ultimo_id = 0
    total_ok = 0
    total_concluidos = 0
    inicio_geral = time.time()

    logger.info(f"Iniciando processamento. Último ID pendente: {max_id_pendentes}")

    while ultimo_id < max_id_pendentes:
        # === SORTEIO DO PULO ===
        if random.random() < chance_pulo:
            # 90%: pula entre 1x e 4x o chunk_size
            fator = random.uniform(fator_pulo_min, fator_pulo_max)
        else:
            # 10%: sem pulo extra, avança apenas chunk_size
            fator = 1.0

        salto = int(chunk_size * fator)
        proximo_id_alvo = ultimo_id + salto

        # Garante que não ultrapasse o max_id_pendentes
        if proximo_id_alvo > max_id_pendentes:
            proximo_id_alvo = max_id_pendentes + 1

        # === BUSCA DO LOTE A PARTIR DO ID SORTEADO ===
        lote = (
            session.query(CicloDeVidaImportacao)
            .filter(CicloDeVidaImportacao.concluido.isnot(True))
            .filter(CicloDeVidaImportacao.ID >= proximo_id_alvo)
            .order_by(CicloDeVidaImportacao.ID.asc())
            .limit(chunk_size)
            .all()
        )

        if not lote:
            # Não há mais registros a partir deste ponto
            break

        total = len(lote)
        logger.info(
            f'Processando chunk com {total} ciclos. '
            f'ID de {lote[0].ID} a {lote[-1].ID}. '
            f'Pulo aplicado: {salto} IDs (fator {fator:.2f}x). '
            f'Origem: ID {ultimo_id} -> Alvo: ID {proximo_id_alvo}'
        )

        # === PROCESSAMENTO DO LOTE ===
        for i, ciclo in enumerate(lote, 1):
            if completar_ciclo_de_vida(db, session, ciclo):
                total_ok += 1
                if ciclo.concluido:
                    total_concluidos += 1
                logger.info(
                    f'{i} de {total} concluído. Ciclo ID {ciclo.ID} '
                    f'desembarque {ciclo.desembarque_id} {ciclo.data_desembarque}.'
                )

            # Atualiza ultimo_id para o último ID processado neste lote
            ultimo_id = ciclo.ID

        session.commit()
        time.sleep(1)

    duracao = timedelta(seconds=int(time.time() - inicio_geral))
    logger.info(
        f'Finalizado em {duracao}. '
        f'{total_ok} processados, {total_concluidos} marcados como finalizados.'
    )
    return total_ok, total_concluidos


def desembarques_novos(session):
    # Inicializar ciclos de vida. Pesquisando novos desembarques, poderiam ser novos CEs cadastrados para iniciar
    # o monitoramento mais cedo no ciclo.
    desembarques = (
        session.query(EmbarqueDesembarque)
        .filter(EmbarqueDesembarque.codigoRecinto.in_(OPERADORES))
        .filter(EmbarqueDesembarque.embarqueDesembarque == 'D')
    )
    max_id_desembarque = session.query(func.max(CicloDeVidaImportacao.desembarque_id)).scalar()
    if max_id_desembarque is None:  # Ciclo de vida zerado, inicializar 7 dias atrás
        data_inicial = datetime.now() - timedelta(days=7)
        desembarques = (desembarques
                        .filter(EmbarqueDesembarque.dataHoraOcorrencia >= data_inicial)
                        )
    else:  # Ciclo de vida inicializado, continuar puxando desembarques novos
        desembarques = (desembarques
                        .filter(EmbarqueDesembarque.id > max_id_desembarque)
                        )
    desembarques = (desembarques
                    .order_by(EmbarqueDesembarque.id.asc())
                    .limit(2000)  # definir se puxa novos desembarques ou não
                    .all()
                    )
    logger.info('%s desembarques encontrados', len(desembarques))
    for desembarque in desembarques:
        ciclo_de_vida = CicloDeVidaImportacao()
        ciclo_de_vida.set_desembarque(desembarque)
        session.add(ciclo_de_vida)
    try:
        session.commit()
        logger.info('Desembarques novos incluídos com sucesso no monitoramento de ciclo de vida.')
    except Exception as e:
        logger.error(e)
        session.rollback()

def saidas_novas_operadores(session):
    # Completar ciclos de vida com saídas. Atalho para a pesquisa completa do completar_ciclo
    max_id_saida_operador = session.query(func.max(CicloDeVidaImportacao.saida_id)).scalar()
    if max_id_saida_operador is None:
        return
    saidas = (
        session.query(AcessoVeiculo)
        .filter(AcessoVeiculo.codigoRecinto.in_(OPERADORES))
        .filter(AcessoVeiculo.direcao == 'S')
        .filter(AcessoVeiculo.numeroConteiner.isnot(None))
        .filter(AcessoVeiculo.numeroConteiner != '')
        .order_by(AcessoVeiculo.id.asc())
        .limit(2000)
        .all()
    )
    logger.info('%s saídas encontradas', len(saidas))
    saidas_incluidas = 0
    for saida in saidas:
        fim = saida.dataHoraOcorrencia
        inicio = fim - timedelta(hours=HORAS_PERMANENCIA_MOLHADO)
        ciclo_de_vida = (
            session.query(CicloDeVidaImportacao)
            .filter(CicloDeVidaImportacao.codigoRecintoDesembarque == saida.codigoRecinto)
            .filter(CicloDeVidaImportacao.data_desembarque < fim)
            .filter(CicloDeVidaImportacao.data_desembarque > inicio)
            .filter(CicloDeVidaImportacao.numeroConteiner == saida.numeroConteiner)
            .one_or_none()
        )
        if ciclo_de_vida:
            ciclo_de_vida.saida_id = saida.id
            session.add(ciclo_de_vida)
            saidas_incluidas += 1
    try:
        session.commit()
        logger.info(f'{saidas_incluidas} saídas novas incluídas com sucesso no monitoramento de ciclo de vida.')
    except Exception as e:
        logger.error(e)
        session.rollback()


def busca_escaneamentos_novos(session):
    """Completar ciclos de vida com escaneamentos.

    Há muitas possibilidades, na importação, de escaneamentos fora da regra base, que é:
    1 - Escaneamento no Operador Portuário: ocorre escaneamento pelo CodigoRecinto do Operador,
    no período entre desembarque e saída.
    2 - Escaneamento no TRA: ocorre escaneamento pelo CodigoRecinto do TRA, após uma entrada neste
    e antes da saída (na verdade, deve ocorrer logo após a entrada neste)

    A taxa de assertividade para estas regras ficou muito baixa para alguns recintos. Pesquisando os
    dados, viram-se casos como: transmissão com codigoRecinto diferente, escaneamento "terceirizado", etc.

    """
    max_id_escaneamento = session.query(func.max(CicloDeVidaImportacaoEscaneamentos.escaneamento_id)).scalar()
    if max_id_escaneamento is None:
        return
    escaneamentos_novos = (
        session.query(InspecaoNaoInvasiva)
        .filter(InspecaoNaoInvasiva.id > max_id_escaneamento)
        .filter(InspecaoNaoInvasiva.numeroConhecimento.isnot(None))
        .filter(InspecaoNaoInvasiva.numeroConhecimento != '')
        .filter(InspecaoNaoInvasiva.numeroConteiner.isnot(None))
        .filter(InspecaoNaoInvasiva.numeroConteiner != '')
        .order_by(InspecaoNaoInvasiva.id.asc())
        .limit(2000)
        .all()
    )
    logger.info('%s novas inspeções não invasivas encontradas', len(escaneamentos_novos))
    escaneamentos_incluidos = 0
    for escaneamento in escaneamentos_novos:
        ciclo_de_vida = (
            session.query(CicloDeVidaImportacao)
            .filter(CicloDeVidaImportacao.numeroCEmercante == escaneamento.numeroConhecimento)
            .filter(CicloDeVidaImportacao.numeroConteiner == escaneamento.numeroConteiner)
            .one_or_none()
        )
        if ciclo_de_vida:
            ciclo_escaneamento = CicloDeVidaImportacaoEscaneamentos()
            ciclo_escaneamento.ciclo_de_vida_id = ciclo_de_vida.ID
            ciclo_escaneamento.escaneamento_id = escaneamento.id
            ciclo_escaneamento.data_escaneamento = escaneamento.dataHoraOcorrencia
            session.add(ciclo_escaneamento)
            escaneamentos_incluidos += 1
    try:
        session.commit()
        logger.info(f'{escaneamentos_incluidos} escaneamentos incluídos com sucesso no '
                    f'monitoramento de ciclo de vida.')
    except Exception as e:
        logger.error(e)
        session.rollback()


def busca_escaneamentos_faltantes_por_ce(session):
    """Completar ciclos de vida com escaneamentos, por número do CE.

    Há muitas possibilidades, na importação, de escaneamentos fora da regra base, que é:
    1 - Escaneamento no Operador Portuário: ocorre escaneamento pelo CodigoRecinto do Operador,
    no período entre desembarque e saída.
    2 - Escaneamento no TRA: ocorre escaneamento pelo CodigoRecinto do TRA, após uma entrada neste
    e antes da saída (na verdade, deve ocorrer logo após a entrada neste)

    A taxa de assertividade para estas regras ficou muito baixa para alguns recintos. Pesquisando os
    dados, viram-se casos como: transmissão com codigoRecinto diferente, escaneamento "terceirizado", etc.

    """
    # Limite de 90 dias atrás
    data_limite = datetime.now() - timedelta(days=90)

    query = (
        session.query(CicloDeVidaImportacao)
        .outerjoin(CicloDeVidaImportacao.escaneamentos)  # ou .outerjoin(CicloDeVidaImportacaoEscaneamentos)
        .filter(
            CicloDeVidaImportacao.data_desembarque.isnot(None),
            CicloDeVidaImportacao.numeroCEmercante.isnot(None),
            CicloDeVidaImportacao.numeroCEmercante != '',
            CicloDeVidaImportacao.data_desembarque >= data_limite,
            CicloDeVidaImportacaoEscaneamentos.ID.is_(None)  # sem escaneamentos
        )
        .order_by(CicloDeVidaImportacao.data_desembarque.desc())
        .limit(1000)
    )

    ciclos_sem_escaneamento = query.all()
    logger.info('%s ciclos sem escaneamento encontrados', len(ciclos_sem_escaneamento))
    escaneamentos_incluidos = 0
    for ciclo_de_vida in ciclos_sem_escaneamento:
        escaneamentos = (session.query(InspecaoNaoInvasiva)
                         .filter(InspecaoNaoInvasiva.numeroConhecimento == ciclo_de_vida.numeroCEmercante)
                         .filter(InspecaoNaoInvasiva.numeroConteiner == ciclo_de_vida.numeroConteiner)
                         ).all()
        for escaneamento in escaneamentos:
            ciclo_escaneamento = CicloDeVidaImportacaoEscaneamentos()
            ciclo_escaneamento.ciclo_de_vida_id = ciclo_de_vida.ID
            ciclo_escaneamento.escaneamento_id = escaneamento.id
            ciclo_escaneamento.data_escaneamento = escaneamento.dataHoraOcorrencia
            session.merge(ciclo_escaneamento)
            escaneamentos_incluidos += 1
    try:
        session.commit()
        logger.info(f'{escaneamentos_incluidos} escaneamentos incluídos com sucesso no '
                    f'monitoramento de ciclo de vida.')
    except Exception as e:
        logger.error(e)
        session.rollback()


if __name__ == '__main__':
    # Popular Ciclos de Vida
    # Escolher um ponto de partida para inicializar os Ciclos de Vida!
    # Depois, basta ficar populando os ciclos de vida até detetar uma conclusão...
    from ajna_commons.flask.conf import SQL_URI, MONGODB_URI, DATABASE
    from sqlalchemy import create_engine, func
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    conn = MongoClient(host=MONGODB_URI)
    mongodb = conn[DATABASE]
    while True:
        logger.info('Iniciando...')
        # As funções abaixo são "atalhos" para buscar novos ciclos de vida ou completar existentes
        # Os atalhos darão um tratamento mais célere ao problema de monitorar o ciclo de vida,
        # diminuindo custos computacionais e encurtando tempos de busca e atualização
        desembarques_novos(session)
        saidas_novas_operadores(session)
        busca_escaneamentos_novos(session)
        # Popular ciclos de vida
        ok, total = processar_ciclos_pendentes(mongodb, session, 1000)
        busca_escaneamentos_faltantes_por_ce(session)
        logger.info(f'Processados com sucesso: {ok}/{total}')
        time.sleep(600)
