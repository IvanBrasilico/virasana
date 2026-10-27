from datetime import date, timedelta

from flask_wtf import FlaskForm
from wtforms import DateField
from wtforms.validators import DataRequired, ValidationError


def sete_dias_atras():
    return date.today() - timedelta(days=7)


class AnaliseCompletudeFiltroForm(FlaskForm):
    class Meta:
        csrf = False  # formulário GET, idempotente — CSRF não se aplica

    datainicio = DateField(
        'Data Início', validators=[DataRequired()], format='%Y-%m-%d',
        default=sete_dias_atras,
    )
    datafim = DateField(
        'Data Fim', validators=[DataRequired()], format='%Y-%m-%d',
        default=date.today,
    )

    def validate_datafim(self, field):
        if self.datainicio.data and field.data and field.data < self.datainicio.data:
            raise ValidationError('Data fim não pode ser anterior à data início.')