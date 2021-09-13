"""Funções para Auditoria/comparação nos metadados de fs.files."""
import json
import sys
from collections import defaultdict

from bson import ObjectId

sys.path.append('.')
sys.path.append('../ajna_docs/commons/')
from ajna_commons.flask.log import logger

from virasana.integracao import carga, info_ade02, xmli


class Auditoria:
    """Recebe params, monta consultas de auditoria entre campos fs.files."""

    FILTROS_AUDITORIA = {
        '0': {'filtro': {},
              'order': [],
              'descricao': 'Selecione'
              },
        '1': {'filtro': {'metadata.carga.vazio': True,
                         'metadata.predictions.vazio': False,
                         #                         'metadata.recinto': {'$ne': 'EMBRAPORT'}
                         },
              'order': [('metadata.predictions.peso', -1)],
              'descricao': 'Contêineres informados como vazios mas detectados ' +
                           'como não vazios (ordem decrescente de peso detectado)'
              },
        '2': {'filtro': {'metadata.carga.vazio': False,
                         'metadata.predictions.vazio': True,
                         #                         'metadata.recinto': {'$ne': 'EMBRAPORT'}
                         },
              'order': [('metadata.predictions.peso', 1)],
              'descricao': 'Contêineres informados como contendo carga mas ' +
                           'detectados como vazios (ordem de peso detectado)'
              },
        '3': {'filtro': {'metadata.alertapeso': True},
              'order': [('metadata.diferencapeso', -1)],
              'descricao': 'Contêineres com maiores divergências de peso (Imagem)'
              },
        '4': {'filtro': {'metadata.alertapeso2': True},
              'order': [('metadata.diferencapeso2', -1)],
              'descricao': 'Contêineres com maiores divergências de peso (Balança)'
              },
        '5': {'filtro': {'metadata.contentType': 'image/jpeg',
                         'metadata.predictions.bbox': {'$exists': False}},
              'order': [('metadata.dataescaneamento', 1)],
              'descricao': 'Imagens sem contêiner detectado'
              },
        '6': {'filtro': carga.FALTANTES,
              'order': [('metadata.dataescaneamento', 1)],
              'descricao': 'Imagens sem informação do CARGA integrada'
              },
        '7': {'filtro': xmli.FALTANTES,
              'order': [('metadata.dataescaneamento', 1)],
              'descricao': 'Imagens sem informação do XML integrada'
              },
        '8': {'filtro': info_ade02.FALTANTES,
              'order': [('metadata.dataescaneamento', 1)],
              'descricao': 'Imagens sem pesagem integrada'
              },
        '9': {'filtro': {'metadata.contentType': 'image/jpeg',
                         'metadata.carga.ncm': {'$size': 1},
                         'metadata.carga.container.indicadorusoparcial': {'$ne': 's'}
                         },
              'order': [('metadata.dataescaneamento', 1)],
              'descricao': 'Imagens com NCM único'
              },
        '10': {'filtro': {'metadata.contentType': 'image/jpeg',
                          'metadata.carga.ncm': {'$size': 2},
                          'metadata.carga.container.indicadorusoparcial': {'$ne': 's'}
                          },
               'order': [('metadata.dataescaneamento', 1)],
               'descricao': 'Imagens com dois NCMs'
               },
        '11': {'filtro': {'metadata.contentType': 'image/jpeg',
                          'metadata.predictions.reefer.reefer_contaminado': True,
                          'metadata.predictions.reefer.reefer_class': 0,
                          'metadata.carga.manifesto.tipomanifesto': {'$ne': 'lci'},
                          'metadata.recinto': {'$in': ['Santos Brasil', 'BTP1', 'BTP2']},
                          },
               'order': [('metadata.dataescaneamento', 1)],
               'descricao': 'Motor Reefer com suspeita de contaminação'
               },
        '12': {'filtro': {'metadata.contentType': 'image/jpeg',
                          'metadata.carga.ncm': {'$size': 1},
                          'metadata.predictions.0.ncm.0.ncm.divergent': True
                          },
               'order': [('metadata.dataescaneamento', 1)],
               'descricao': 'NCM Único com declaração divergente'
               },
        '13': {'filtro': {'metadata.contentType': 'image/jpeg',
                          'metadata.predictions.reefer.reefer_class': 0,
                          'metadata.carga.manifesto.tipomanifesto': {'$ne': 'lci'},
                          'metadata.recinto': {'$in': ['Santos Brasil', 'BTP1', 'BTP2']},
                          },
               'order': [('metadata.dataescaneamento', 1)],
               'descricao': 'Motor Reefers - total'
               },
        # '13': {'filtro': {'_id': {'$in': [ObjectId('60993da9364ebd08e9b5ec63'),
        #                                   ObjectId('5fc1224cc19b6697bda04aea'),
        #                                   ObjectId('5f3d31e08f17e86b2f3f6f07'),
        #                                   ObjectId('61128ae941cf8b44f40fce23'),
        #                                   ObjectId('5f8da2932860640c37492f33'),
        #                                   ObjectId('60929d32364ebd08e9b59e88'),
        #                                   ObjectId('5f5113ef93363e841b089fb0'),
        #                                   ObjectId('6026848e475b3868bc7f74ff'),
        #                                   ObjectId('5efa306ff8734183a94fe03a'),
        #                                   ObjectId('5f58f9a193363e841b099aba'),
        #                                   ObjectId('6061f459061ba9a83a765832'),
        #                                   ObjectId('6067e2dbb261939fbff51f57'),
        #                                   ObjectId('5fc121e6c19b6697bda03909'), ]}
        #                   },
        #        'order': [('metadata.dataescaneamento', -1)],
        #        'descricao': 'Reefers contaminados'
        #        },
    }

    def __init__(self, db):
        """Init.

        Args:
            db: conexão ao MongoDB a ser usada
        """
        self.db = db
        self.relatorios = {}
        self.dict_auditoria = {}
        self.filtros_auditoria_desc = []
        self.mount_filtros()

    def mount_filtros(self):
        """Para evitar a criação desmesurada de filtros eles serão centralizados.

        Aqui, se a tabela não existir no banco, cria algumas hard_coded.
        Depois, o administrador poderá criar novas no BD.
        """
        cursor = self.db['Auditorias'].find()
        auditorias = list(cursor)
        if len(auditorias) == 0:
            logger.debug('Criando tabela Auditorias...')
            # Se não existe tabela, cria, preenche e chama de novo mesmo método
            for id, campos in self.FILTROS_AUDITORIA.items():
                logger.debug(id + ' ' + campos['descricao'])
                self.db['Auditorias'].insert_one(
                    {'id': id,
                     'filtro': json.dumps(campos['filtro']),
                     'order': json.dumps(campos['order']),
                     'descricao': campos['descricao']
                     })
                # self.add_relatorio(id, **campos)
            self.mount_filtros()
            return
        for row in auditorias:
            id = row['id']
            self.dict_auditoria[id] = {
                'filtro': json.loads(row['filtro']),
                'order': json.loads(row['order']),
                'descricao': row['descricao']
            }
            self.filtros_auditoria_desc.append((id, row['descricao']))
        #   logger.debug(self.filtros_auditoria_desc)
        self.filtros_auditoria_desc = sorted(self.filtros_auditoria_desc)
        # logger.debug(self.dict_auditoria)

    def add_relatorio(self, id: str,
                      filtro: dict,
                      order: list,
                      descricao: str
                      ) -> bool:
        """Adiciona um relatório a rodar.

        Recebe um dicionário no formato campos: list e operador:str

        Lendo este dicionário, monta uma checagem por 'metaprogramação'
        """
        self.db['Auditorias'].find_one_and_replace(
            {'id': id},
            {'id': id,
             'filtro': json.dumps(filtro),
             'order': json.dumps(order),
             'descricao': descricao
             }
        )
        return True

    def reporta(self) -> dict:
        """Executa relatórios configurados.

        Retorna um dicionário com uma lista por chave
        """
        result = defaultdict(list)
        for key, value in self.relatorios:
            cursor = self.db['fs.files'].find(value)
            lista = []
            for linha in cursor:
                lista.append(linha)
            result[key] = lista
        return result


if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    certodisso = input('Esta ação apaga TODOS os filtros e restaura o padrão.'
                       'Digite SIM para confirmar.\n')
    if certodisso == 'SIM':
        print('Recriando filtros Auditoria...')
        db['Auditorias'].delete_many({})
        Auditoria(db)
