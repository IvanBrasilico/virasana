import sys

from sqlalchemy import Column, Integer, String, text, create_engine
from sqlalchemy.orm import declarative_base, Session

Base = declarative_base()

class Teste(Base):
    __tablename__ = "teste_charset"
    id = Column(Integer, primary_key=True)
    txt = Column(String(255))  # coluna latin1 no banco

sys.path.append('../ajna_docs/commons')
from ajna_commons.flask.conf import SQL_URI

engine = create_engine(SQL_URI)
# insere um caractere fora de latin1, ex.: emoji 🚀 (U+1F680)
with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS teste_charset"))
    conn.execute(
        text(
            """
            CREATE TABLE teste_charset (
                id INT AUTO_INCREMENT PRIMARY KEY,
                txt VARCHAR(255)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            """
        )
    )
    # conn.commit()

with Session(engine) as session:
    obj = Teste(txt="Teste 🚀")
    session.add(obj)
    try:
        session.commit()
        print("Insert OK")
    except Exception as e:
        print("Erro:", e)