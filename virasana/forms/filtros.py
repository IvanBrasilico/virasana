from datetime import datetime, timedelta, date

from ajna_commons.utils.sanitiza import mongo_sanitizar
from bhadrasana.models.apirecintos_risco import DescricaoRiscoMotorista
from flask import g
from flask_login import current_user
from flask_wtf import FlaskForm
from virasana.integracao.bagagens.viajantesalchemy import ClasseRisco
from virasana.models.auditoria import Auditoria
from virasana.models.models import Tags
from wtforms import BooleanField, DateField, IntegerField, FloatField, \
    SelectField, StringField
from wtforms.fields import DateTimeLocalField
from wtforms.validators import optional

MAXROWS = 50
MAXPAGES = 100


class FilesForm(FlaskForm):
    """Valida pesquisa de arquivos.

    Usa wtforms para facilitar a validação dos campos de pesquisa da tela
    search_files.html

    """
    numero = StringField(u'Número', validators=[optional()], default='')
    start = DateField('Start', validators=[optional()])
    end = DateField('End', validators=[optional()])
    alerta = BooleanField('Alerta', validators=[optional()], default=False)
    ranking = BooleanField('Ranking', validators=[optional()], default=False)
    pagina_atual = IntegerField('Pagina', default=1)
    filtro_auditoria = SelectField(u'Filtros de Auditoria',
                                   default=0)
    tag_usuario = BooleanField('Exclusivamente Tag do usuário',
                               validators=[optional()], default=False)
    filtro_tags = SelectField(u'Tags de usuário',
                              default=[0])
    texto_ocorrencia = StringField(u'Texto Ocorrência',
                                   validators=[optional()], default='')
    classe = SelectField(u'Classe de contêiner detectado', default='0')
    colormap = SelectField('Mapa de cores para visualizar imagem',
                           validators=[optional()], default='Contraste')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.colormap.choices = ['original', 'Contraste', 'Equaliza histograma',
                                 'flag', 'winter', 'hsv',
                                 'gist_rainbow', 'gist_stern', 'nipy_spectral_r',
                                 'jet', 'gnuplot', 'tab10']


class ColorMapForm(FlaskForm):
    colormap = SelectField('Mapa de cores para visualizar imagem',
                           validators=[optional()], default='original')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.colormap.choices = ['original', 'Contraste', 'Equaliza histograma',
                                 'flag', 'winter', 'hsv',
                                 'gist_rainbow', 'gist_stern', 'nipy_spectral_r',
                                 'jet', 'gnuplot', 'tab10']


class FormFiltro(FlaskForm):
    """Valida pesquisa de arquivos.

    Usa wtforms para facilitar a validação dos campos de pesquisa da tela
    search_files.html

    """
    pagina_atual = IntegerField('Pagina', default=1)
    # order = None
    numero = StringField(u'Número', validators=[optional()], default='')
    start = DateField('Start', validators=[optional()])
    end = DateField('End', validators=[optional()])
    alerta = BooleanField('Alerta', validators=[optional()], default=False)
    zscore = FloatField('Z-Score', validators=[optional()], default=3.)
    contrast = BooleanField(validators=[optional()], default=False)
    color = BooleanField(validators=[optional()], default=False)
    filtro_auditoria = SelectField(u'Filtros de Auditoria',
                                   validators=[optional()], default=0)
    tag_usuario = BooleanField('Exclusivamente Tag do usuário',
                               validators=[optional()], default=False)
    filtro_tags = SelectField(u'Filtrar por estas tags',
                              validators=[optional()], default=0)
    texto_ocorrencia = StringField(u'Texto Ocorrência',
                                   validators=[optional()], default='')

    def initialize(self, db):
        self.db = db
        self.auditoria_object = Auditoria(db)
        self.tags_object = Tags(db)
        self.filtro_tags.choices = self.tags_object.tags_text
        self.filtro_auditoria.choices = self.auditoria_object.filtros_auditoria_desc

    def recupera_filtro_personalizado(self):
        """Usa variável global para guardar filtros personalizados entre posts."""
        key = 'filtros' + current_user.id
        self.filtro_personalizado = g.get(key)

    def valida(self):
        """Lê formulário e adiciona campos ao filtro se necessário."""
        if self.validate():  # configura filtro básico
            self.filtro = {}
            # pagina_atual = self.pagina_atual.data
            numero = self.numero.data
            start = self.start.data
            end = self.end.data
            alerta = self.alerta.data
            zscore = self.zscore.data
            if numero == 'None':
                numero = None
            if start and end:
                start = datetime.combine(start, datetime.min.time())
                end = datetime.combine(end, datetime.max.time())
                self.filtro['metadata.dataescaneamento'] = {'$lte': end, '$gte': start}
            if numero:
                self.filtro['metadata.numeroinformado'] = \
                    {'$regex': '^' + mongo_sanitizar(self.numero), '$options': 'i'}
            if alerta:
                self.filtro['metadata.xml.alerta'] = True
            if zscore:
                self.filtro['metadata.zscore'] = {'$gte': zscore}
            # Auditoria
            filtro_escolhido = self.filtro_auditoria.data
            if filtro_escolhido and filtro_escolhido != '0':
                auditoria_object = Auditoria(self.db)
                filtro_auditoria = \
                    auditoria_object.dict_auditoria.get(filtro_escolhido)
                if filtro_auditoria:
                    self.filtro.update(filtro_auditoria.get('filtro'))
                    order = filtro_auditoria.get('order')
            tag_escolhida = self.filtro_tags.data
            tag_usuario = self.tag_usuario.data
            if tag_escolhida and tag_escolhida != '0':
                filtro_tag = {'tag': tag_escolhida}
                if tag_usuario:
                    filtro_tag.update({'usuario': current_user.id})
                self.filtro['metadata.tags'] = {'$elemMatch': filtro_tag}
            texto_ocorrencia = self.texto_ocorrencia.data
            if texto_ocorrencia:
                self.filtro.update(
                    {'metadata.ocorrencias': {'$exists': True},
                     'metadata.ocorrencias.texto':
                         {'$regex':
                              '^' + mongo_sanitizar(texto_ocorrencia), '$options': 'i'
                          }
                     })
            print('FILTRO: ', self.filtro)
            return True
        return False


class FormFiltroData(FlaskForm):
    """Valida filtragem por datas

    Usa wtforms para facilitar a validação dos campos de pesquisa da tela
    search_files.html

    """
    start = DateField('Start', validators=[optional()])
    end = DateField('End', validators=[optional()])
    recinto = StringField(u'Nome do Recinto',
                          validators=[optional()], default='')
    pagina_atual = IntegerField('Pagina', default=1)


class FormFiltroConformidade(FlaskForm):
    """Valida filtragem por datas

    Usa wtforms para facilitar a validação dos campos de pesquisa da tela
    search_files.html

    """
    start = DateField('Start', validators=[optional()])
    end = DateField('End', validators=[optional()])
    pagina_atual = IntegerField('Pagina', default=1)
    recinto = StringField(u'Nome do Recinto',
                          validators=[optional()], default='')
    order = StringField(u'Nome do campo para ordenar',
                        validators=[optional()], default='')
    reverse = BooleanField('Order by DESC', default=False)
    isocode_size = SelectField(u'Iso Code size',
                               validators=[optional()], default='')
    isocode_group = SelectField(u'Iso Code Group',
                                validators=[optional()], default='')


class FormFiltroAlerta(FlaskForm):
    """Valida filtragem por datas

    Usa wtforms para facilitar a validação dos campos de pesquisa da tela
    search_files.html

    """
    start = DateField('Start', validators=[optional()])
    end = DateField('End', validators=[optional()])
    pagina_atual = IntegerField('Pagina', default=1)
    recinto = StringField(u'Nome do Recinto',
                          validators=[optional()], default='')
    order = StringField(u'Nome do campo para ordenar',
                        validators=[optional()], default='')
    reverse = BooleanField('Order by DESC', default=False)


class FormFiltroBagagem(FlaskForm):
    """Valida filtragem por datas

    Usa wtforms para facilitar a validação dos campos de pesquisa da tela
    search_files.html

    """
    start = DateField('Start', default=date.today() - timedelta(days=3))
    end = DateField('End', default=date.today())
    recinto = StringField(u'Nome do Recinto',
                          validators=[optional()], default='')
    portoorigem = StringField(u'Porto de Origem',
                              validators=[optional()], default='')
    cpf_cnpj = StringField(u'CPF ou CNPJ do consignatario',
                           validators=[optional()], default='')
    conteiner = StringField(u'Número do contêiner',
                            validators=[optional()], default='')
    portodestino = StringField(u'Porto de Destino',
                               validators=[optional()], default='BRSSZ')
    ncm = StringField(u'NCM a pesquisar',
                      validators=[optional()], default='9797')
    colormap = SelectField('Mapa de cores para visualizar imagem',
                           validators=[optional()], default='contraste')
    classificados = BooleanField('Exibir descartados', default=False)
    selecionados = BooleanField('Exibir selecionados', default=False)
    concluidos = BooleanField('Exibir selecionados', default=False)
    semimagem = BooleanField('Exibir somente os sem imagem ou sem pesagem', default=False)
    filtrar_dsi = BooleanField('Filtrar e ordenar por data de emissão da DSI', default=False)
    ordenar_rvf = BooleanField('Ordenar por data da RVF', default=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.colormap.choices = ['original', 'contraste', 'flag', 'winter', 'hsv',
                                 'gist_rainbow', 'gist_stern', 'nipy_spectral_r',
                                 'jet', 'gnuplot', 'tab10']


class FormClassificacaoRisco(FlaskForm):
    """Interface para classe ClassificacaoRisco.

    Usa wtforms para facilitar a validação dos campos da tela.

    """
    numeroCEmercante = StringField('Número do CE Mercante')
    classerisco = SelectField('Classificação de risco',
                              validators=[optional()], default=0)
    descricao = StringField('Descrição do motivo da classificação', default='')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.classerisco.choices = [(i.value, i.name) for i in ClasseRisco]


class FormFiltroAPIRecintos(FlaskForm):
    """Valida filtragem por datas

    Usa wtforms para facilitar a validação dos campos de pesquisa da tela
    search_files.html

    """
    start = DateTimeLocalField('Start', format='%Y-%m-%dT%H:%M')
    end = DateTimeLocalField('End', format='%Y-%m-%dT%H:%M')
    placa = StringField(u'Placa do Cavalo ou reboque',
                        validators=[optional()], default='')
    numeroConteiner = StringField(u'Número do contêiner',
                                  validators=[optional()], default='')
    cpfMotorista = StringField(u'CPF do Motorista',
                               validators=[optional()], default='')
    motoristas_de_risco = BooleanField('Motoristas de Risco', default=False)
    motoristas_de_risco_select = SelectField('Motoristas de Risco', default='0')
    codigoRecinto = SelectField('Recinto Aduaneiro', default=-1)
    tempo_permanencia = IntegerField('Tempo entre entrada e saída', default=0)
    missao = SelectField('Missão do acesso ao Recinto', default=99)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.start.data:
            self.start.data = datetime.now() - timedelta(days=3)
        if not self.end.data:
            self.end.data = datetime.now()
        self.codigoRecinto.choices = [['', 'Selecione']]
        if kwargs.get('recintos'):
            self.codigoRecinto.choices.extend(kwargs.get('recintos'))
        self.missao.choices = [[99, 'Selecione']]
        if kwargs.get('missoes'):
            self.missao.choices.extend(kwargs.get('missoes'))
        self.motoristas_de_risco_select.choices = [('0', 'Ignorar'), ('99', 'TODOS'),
                                                   *[(k, v) for k, v in DescricaoRiscoMotorista.items()]]


class FormFiltroEscaneamento(FlaskForm):
    """Valida filtragem por datas

    Usa wtforms para facilitar a validação dos campos de pesquisa da tela
    search_files.html

    """
    start = DateField('Start', validators=[optional()])
    end = DateField('End', validators=[optional()])
    recinto = StringField(u'Nome do Recinto',
                          validators=[optional()], default='')
    sentido = SelectField(u'EXPO ou IMPO',
                          validators=[optional()], default='EXPO')
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sentido.choices = [['EXPO', 'EXPO'], ['IMPO', 'IMPO']]
