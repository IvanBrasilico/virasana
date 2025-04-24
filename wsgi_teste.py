from flask import Flask, request
from werkzeug.serving import run_simple

app = Flask(__name__)


@app.route('/')
def index():
    return str(request.environ)


if __name__ == '__main__':
    run_simple('localhost', 5010, app, use_reloader=True)
