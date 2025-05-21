# exportacao_app.py
from flask import Blueprint, render_template

exportacao_app = Blueprint(
    'exportacao_app',
    __name__,
    url_prefix='/exportacao'
)

def configure(app):
    app.register_blueprint(exportacao_app)

@exportacao_app.route('/', methods=['GET'])
def index():
    return render_template('exportacao.html')
