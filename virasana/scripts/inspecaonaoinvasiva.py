import requests
import json

url = 'https://ajna1.rfoc.srf/virasana/api/inspecaonaoinvasiva'  # ajuste se necessário

#
json_data = {
    "dataHoraOcorrencia": "2025-07-17T22:58:17.170-0300",
    "codigoRecinto": "8931305",
    "listaConteineresUld": [
        {"numeroConteiner": "MSMU2186544"}
    ]
}

image_path = 'MSMU2186544-17072025-2258.jpg'

with open(image_path, 'rb') as img_file:
    files = {
        'imagem': (image_path, img_file, 'image/jpeg')
    }
    data = {
        'json': json.dumps(json_data)
    }
    r = requests.post(url, files=files, data=data)

print(f'Status code: {r.status_code}')
print('Response:', r.json())


