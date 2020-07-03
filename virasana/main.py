"""Módulo de entrada da aplicação web do módulo Virasana.

Módulo Virasana é o Servidor de imagens e a interface para carga,
consulta e integração das imagens com outras bases.

"""

import ajna_commons.flask.log as log
from ajna_commons.flask import api_login
from ajna_commons.flask.flask_log import configure_applog

from virasana.db import mongodb, mysql, mongodb_risco
from virasana.views import configure_app, csrf

app = configure_app(mongodb, mysql, mongodb_risco)
configure_applog(app)
api = api_login.configure(app)
csrf.exempt(api)
log.logger.info('Servidor (re)iniciado!')

if __name__ == '__main__':  # pragma: no cover
    app.run(debug=app.config['DEBUG'])
