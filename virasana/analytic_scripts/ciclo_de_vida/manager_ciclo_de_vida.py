from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import CicloDeVidaImportacao


class CicloDeVidaImportacaoManager():
    def __init__(self, session, ciclo_de_vida: CicloDeVidaImportacao):
        self.session = session
        self.ciclo_de_vida = ciclo_de_vida

    def get_conhecimento(self):
        if self.ciclo_de_vida is None:
            return None
        if self.ciclo_de_vida.numeroConteiner is None or self.ciclo_de_vida.data_desembarque is None:
            return None


    def get_desembarque(self):
        pass