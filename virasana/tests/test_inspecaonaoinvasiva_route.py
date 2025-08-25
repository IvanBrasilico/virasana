import io
import json
import sys
import unittest
from unittest.mock import patch, MagicMock


sys.path.append('.')
sys.path.append('../bhadrasana2')
sys.path.append('../ajna_docs/commons')
from virasana.views import configure_app

from virasana.routes import conformidade_app
from virasana.routes import inspecaonaoinvasiva_app
from virasana.routes.inspecaonaoinvasiva_app import processar_inspecaonaoinvasiva

class TestProcessarInspecaoNaoInvasiva(unittest.TestCase):

    @patch('seu_modulo.gridfs.GridFS')  # mock GridFS dentro do módulo
    def test_processar_inspecaonaoinvasiva_chama_put_corretamente(self, mock_gridfs_cls):
        mock_fs_instance = MagicMock()
        mock_gridfs_cls.return_value = mock_fs_instance
        mock_fs_instance.put.return_value = "id_falso_123"

        # Mock banco e arquivo
        mock_mongodb = MagicMock()
        mock_file = MagicMock()
        mock_file.stream = io.BytesIO(b"conteudo_imagem")
        mock_file.filename = "imagem_teste.jpeg"

        json_original = {
            "dataHoraOcorrencia": "2025-07-17T22:58:17.170-0300",
            "codigoRecinto": "8931305",
            "listaConteineresUld": [{"numeroConteiner": "MSMU2186544"}]
        }

        file_id = processar_inspecaonaoinvasiva(mock_mongodb, json_original, mock_file)

        self.assertEqual(file_id, "id_falso_123")

        mock_gridfs_cls.assert_called_once_with(mock_mongodb)
        mock_fs_instance.put.assert_called_once()

        args, kwargs = mock_fs_instance.put.call_args

        self.assertEqual(kwargs['filename'], "imagem_teste.jpeg")

        metadata = kwargs.get('metadata')
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata['dataescaneamento'], "2025-07-17T22:58:17.170-0300")
        self.assertEqual(metadata['numeroinformado'], "MSMU2186544")
        self.assertEqual(metadata['unidade'], "ALFSTS")
        self.assertEqual(metadata['recinto'], "TRANSBRASA")


class TestApiInspecaoNaoInvasivaEndpoint(unittest.TestCase):
    def setUp(self):
        self.app = configure_app(MagicMock(), None, None)
        conformidade_app.configure(self.app)
        inspecaonaoinvasiva_app.configure(self.app)

        self.client = self.app.test_client()

    @patch('seu_modulo.processar_inspecaonaoinvasiva')
    def test_api_inspecaonaoinvasiva_post(self, mock_processar):
        mock_processar.return_value = "id_mock_456"

        json_payload = {
            "dataHoraOcorrencia": "2025-07-17T22:58:17.170-0300",
            "codigoRecinto": "8931305",
            "listaConteineresUld": [{"numeroConteiner": "MSMU2186544"}]
        }

        img_bytes = b"imagem_fake"
        data = {
            'json': json.dumps(json_payload),
            'imagem': (io.BytesIO(img_bytes), 'arquivo.jpeg')
        }

        response = self.client.post('/api/inspecaonaoinvasiva',
                                    data=data,
                                    content_type='multipart/form-data')

        self.assertEqual(response.status_code, 201)
        resp_json = response.get_json()
        self.assertEqual(resp_json['message'], "Salvo com sucesso")
        self.assertEqual(resp_json['file_id'], "id_mock_456")

        mock_processar.assert_called_once()
        args, kwargs = mock_processar.call_args
        self.assertEqual(args[1], json_payload)
        # args[2] deve ser file storage - checamos filename
        self.assertTrue(hasattr(args[2], 'filename'))
        self.assertEqual(args[2].filename, 'arquivo.jpeg')

if __name__ == '__main__':
    unittest.main()
