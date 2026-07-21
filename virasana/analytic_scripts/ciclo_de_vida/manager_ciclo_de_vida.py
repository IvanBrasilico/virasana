import sys

sys.path.append('.')
sys.path.insert(0, '../ajna_docs/commons')
sys.path.insert(0, '../bhadrasana')

from ajna_commons.flask.log import logger
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import CicloDeVidaImportacao
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida_helpers import (
    busca_ce_mercante_conteiner_desembarque, busca_desembarque_conteiner)


class CicloDeVidaImportacaoManager():
    def __init__(self, session, ciclo_de_vida: CicloDeVidaImportacao):
        self.session = session
        self.ciclo_de_vida = ciclo_de_vida

    def get_conhecimento(self):
        if self.ciclo_de_vida is None:
            return None
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
        self.ciclo_de_vida.numeroCEmercante = cemercante.numeroCEmercante
        try:
            self.session.add(self.ciclo_de_vida)
            self.session.commit()
        except Exception as e:
            logger.error(e)
            self.session.rollback()

    def get_desembarque(self):
        if self.ciclo_de_vida is None:
            return None
        if self.ciclo_de_vida.desembarque_id:
            return self.ciclo_de_vida.desembarque_id
        if self.ciclo_de_vida.numeroConteiner is None or self.ciclo_de_vida.data_desembarque is None:
            return None
        # Busca Desembarque por numeroConteiner E data_desembarque
        desembarque = busca_desembarque_conteiner(self.session,
                                                  self.ciclo_de_vida.numeroConteiner,
                                                  self.ciclo_de_vida.data_desembarque)
        self.ciclo_de_vida.desembarque_id = desembarque.id
        try:
            self.session.add(self.ciclo_de_vida)
            self.session.commit()
        except Exception as e:
            logger.error(e)
            self.session.rollback()
