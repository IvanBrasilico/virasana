from pymongo import MongoClient
from datetime import datetime, timedelta
import gridfs
from typing import Optional

# Configurações - ADAPTE AQUI
SRC_URI = "mongodb://localhost/test"
DST_URI = "mongodb://localhost/test"


def connect_fs(uri: str):
    """Conecta e retorna GridFS + sua .files collection."""
    client = MongoClient(uri)
    db = client["ajna"]
    fs = gridfs.GridFS(db)
    return db, fs


def get_min_upload_date_dst(dst_files):
    """Menor uploadDate no destino."""
    pipeline = [{"$group": {"_id": None, "min_date": {"$min": "$uploadDate"}}}]
    result = list(dst_files.aggregate(pipeline))
    return result[0]["min_date"] if result else None


def copy_day_window(start_date: datetime, src_fs, dst_fs):
    """Copia janela diária usando fs.get() e fs.put()."""
    end_date = start_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    query = {"uploadDate": {"$gte": start_date, "$lte": end_date}}

    total = src_fs.files.count_documents(query)
    print(f"📊 Dia {start_date.date()}: {total:,} arquivos")

    if total == 0:
        return 0

    copied = 0
    for file_doc in src_fs.files.find(query):
        file_id = file_doc["_id"]

        try:
            # Lê com fs.get()
            src_file = src_fs.get(file_id)
            data = src_file.read()

            # Prepara kwargs (metadados + MESMO _id)
            kwargs = dict(file_doc)
            kwargs.pop("_id")  # Será sobrescrito
            kwargs["_id"] = file_id  # MANTER ID!

            # Grava com fs.put() **kwargs (SOBRESCREVE se existir)
            dst_fs.put(data, **kwargs)
            copied += 1

            if copied % 1000 == 0:
                print(f"Progresso: {copied}/{total} ({copied / total * 100:.1f}%)")

        except gridfs.errors.NoFile:
            print(f"Arquivo não encontrado: {file_id}")
        except Exception as e:
            print(f"Erro {file_id}: {e}")

    print(f"✅ Copiados: {copied}/{total}")
    return copied


def main():
    srd_db, src_fs = connect_fs(SRC_URI)
    dst_db, dst_fs = connect_fs(DST_URI)

    min_dst = get_min_upload_date_dst(dst_db)

    if min_dst is None:
        start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        start_date = min_dst.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

    print(f"Iniciando dia: {start_date.date()}")
    copy_day_window(start_date, src_fs, dst_fs)


if __name__ == "__main__":
    main()
