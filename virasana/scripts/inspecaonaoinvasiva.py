import io
import json
import zipfile

import requests


def upload(url, files, data):
    r = requests.post(url, files=files, data=data, verify=False)
    print(f'Status code: {r.status_code}')
    print('Response:', r.json())
    return r


def upload_file(url, image_path, json_path):
    # TODO: load json_data from json_path
    json_data = {'jsonOriginal': {
        "dataHoraOcorrencia": "2025-07-17T22:58:17.170-0300",
        "codigoRecinto": "8931305",
        "listaConteineresUld": [
            {"numeroConteiner": "MSMU2186544"}
        ]
    }}
    print(image_path)
    with open(image_path, 'rb') as img_file:
        files = {
            'imagem': (image_path, img_file, 'image/jpeg')
        }
        data = {
            'json': json.dumps(json_data)
        }
        return upload(url, files, data)


def upload_zip(zip_filename):
    with zipfile.ZipFile(open(zip_filename, 'rb')) as zip_file:
        for file_info in zip_file.infolist():
            filename = file_info.filename
            base_name = filename.rsplit('.', 1)[0]
            parts = base_name.split('-')
            if len(parts) < 3:
                print(f'Arquivo {filename} não tem as informações esperadas no nome')
                continue  # Não tem metadata no nome conforme esperado
            numeroinformado, recinto, data_str = parts[0], parts[1], parts[2]
            json_data = {'jsonOriginal': {
                "dataHoraOcorrencia": data_str,
                "codigoRecinto": recinto,
                "listaConteineresUld": [
                    {"numeroConteiner": numeroinformado}
                ]
            }}
            with zip_file.open(file_info) as file:
                file_bytes = io.BytesIO(file.read())
            files = {
                'imagem': (image_path, file_bytes, 'image/jpeg')
            }
            upload(url, files, json_data)


if __name__ == '__main__':
    # TODO: load from_zip, filename, url, image_path, json_path from args
    from_zip = True
    if from_zip:
        filename = "inspecoes.zip"
        upload_zip(filename)
    else:
        # url = 'https://ajna1.rfoc.srf/virasana/api/inspecaonaoinvasiva'
        url = 'http://localhost:5001/api/inspecaonaoinvasiva'
        image_path = 'C:/Users/25052288840/Downloads/MSMU2186544-17072025-2258.jpg'
        upload_file(url, image_path, None)
