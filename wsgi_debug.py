import os
import sys
from werkzeug.serving import run_simple
from werkzeug.middleware.dispatcher import DispatcherMiddleware


sys.path.insert(0, '.')
sys.path.insert(0, '../ajna_docs/commons')
sys.path.insert(0, '../commons')
sys.path.append('../bhadrasana2')

from ajna_commons.flask.conf import VIRASANA_URL

os.environ['DEBUG'] = '1'
from virasana.main import app

if __name__ == '__main__':
    port = 5000
    if VIRASANA_URL:
        port = int(VIRASANA_URL.split(':')[-1])
    application = DispatcherMiddleware(app,
                                    {
                                        '/virasana': app
                                    })
    run_simple('localhost', port, application, use_reloader=True)
