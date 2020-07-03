"""Configura a conexão com Banco de Dados."""
from pymongo import MongoClient

from ajna_commons.flask.conf import DATABASE, MONGODB_URI
from sqlalchemy import create_engine

from ajna_commons.flask.conf import SQL_URI

conn = MongoClient(host=MONGODB_URI)
mongodb = conn[DATABASE]
mongodb_risco = conn['risco']
mysql = create_engine(SQL_URI)

