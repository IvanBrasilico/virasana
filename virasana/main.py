"""Módulo de entrada da aplicação web do módulo Virasana.

Módulo Virasana é o Servidor de imagens e a interface para carga,
consulta e integração das imagens com outras bases.

"""

import ajna_commons.flask.log as log
from ajna_commons.flask import api_login
from ajna_commons.flask.flask_log import configure_applog
from sqlalchemy.orm import sessionmaker, scoped_session
from virasana.db import mongodb, mysql, mongodb_risco
from virasana.views import configure_app, csrf

# from bhadrasana.models import db_session

db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=mysql))

app = configure_app(mongodb, mysql, mongodb_risco)
configure_applog(app)
api = api_login.configure(app)
csrf.exempt(api)
log.logger.info('Servidor (re)iniciado!')


@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()
    log.logger.info('db_session remove (não ativado - ver se é necessário ativar)')


if __name__ == '__main__':  # pragma: no cover
    app.run(debug=app.config['DEBUG'])
