from flask_wtf import FlaskForm
from flask_wtf.file import FileAllowed
from wtforms import StringField, FileField, SubmitField
from wtforms.validators import DataRequired


class InspecaoNaoInvasivaForm(FlaskForm):
    dataHoraOcorrencia = StringField('Data/Hora Ocorrência', validators=[DataRequired()])
    codigoRecinto = StringField('Código Recinto', validators=[DataRequired()])
    numeroConteiner = StringField('Número do Contêiner', validators=[DataRequired()])
    imagem = FileField('Imagem JPEG',
                       validators=[DataRequired(), FileAllowed(['jpg', 'jpeg'], 'Apenas arquivos .jpeg!')])
    submit = SubmitField('Enviar')
