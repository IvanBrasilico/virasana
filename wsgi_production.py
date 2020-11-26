import logging
try:
    from werkzeug.wsgi import DispatcherMiddleware
except ImportError:
    from werkzeug.middleware.dispatcher import DispatcherMiddleware

from virasana.main import app


gunicorn_logger = logging.getLogger('gunicorn.error')
app.logger.handlers = gunicorn_logger.handlers
app.logger.setLevel(gunicorn_logger.level)

application = DispatcherMiddleware(app,
                                   {
                                       '/virasana': app
                                   })
