from datetime import datetime, timedelta

from pymongo import MongoClient

# import pprint

# from virasana.integracao.carga import busca_info_container

# TODO: Query to know what CARGA Containers(Vazio e Cheio) do not have images!!

db = MongoClient()['test']

dataatracacaoiso = datetime(2017, 9, 1)

linha = db['fs.files'].find(
    {'metadata.contentType': 'image/jpeg'}
).sort('metadata.dataescaneamento', 1).limit(1)
linha = next(linha)
datainicio = linha.get('metadata').get('dataescaneamento')
print('Menor data de escaneamento (IMAGENS)', datainicio)

linha = db['fs.files'].find(
    {'metadata.contentType': 'image/jpeg'}
).sort('metadata.dataescaneamento', -1).limit(1)
linha = next(linha)
datafim = linha.get('metadata').get('dataescaneamento')
print('Maior data de escaneamento (IMAGENS)', datafim)
datafim = datafim - timedelta(days=3)

linha = db['CARGA.AtracDesatracEscala'].find().sort(
    'dataatracacaoiso', 1).limit(1)
linha = next(linha)
iniciocarga = linha.get('dataatracacaoiso')
print('Menor data de atracação (CARGA)', iniciocarga)
linha = db['CARGA.AtracDesatracEscala'].find().sort(
    'dataatracacaoiso', -1).limit(1)
linha = next(linha)
fimcarga = linha.get('dataatracacaoiso')
print('Maior data de atracação (CARGA)', fimcarga)

agg_vazios = db['CARGA.AtracDesatracEscala'].aggregate(
    [{'$match': {'dataatracacaoiso':
                 {'$gt': datainicio,
                  '$lt': datafim}
                 }},
        {'$lookup':
         {'from': 'CARGA.EscalaManifesto',
          'localField': 'escala',
          'foreignField': 'escala',
          'as': 'manifestos'}
         },
     {'$lookup':
      {'from': 'CARGA.ContainerVazio',
       'localField': 'manifestos.manifesto',
       'foreignField': 'manifesto',
       'as': 'vazios'}
      },
     ])

agg_conhecimentos = db['CARGA.AtracDesatracEscala'].aggregate(
    [
        {'$match': {'dataatracacaoiso':
                    {'$gt': datainicio,
                     '$lt': datafim}
                    }},
        {'$lookup':
         {'from': 'CARGA.EscalaManifesto',
          'localField': 'escala',
          'foreignField': 'escala',
          'as': 'manifestos'}
         },
        {'$lookup':
            {'from': 'CARGA.ManifestoConhecimento',
             'localField': 'manifestos.manifesto',
             'foreignField': 'manifesto',
             'as': 'conhecimentos'}
         },
    ]
)

conhecimentos = []
for escala in agg_conhecimentos:
    for conhecimento in escala['conhecimentos']:
        conhecimentos.append(conhecimento['conhecimento'])


conhecimentos = []
containers = []
for conhecimento in conhecimentos:
    agg_conhecimentos_2 = db['CARGA.Conhecimento'].find(
        {'conhecimento': conhecimento,
         'codigoportodestino': 'brssz'},
        {'conhecimento': 1}
    )
    if agg_conhecimentos_2.count() > 0:
        agg_containers = db['CARGA.Container'].find(
            {'conhecimento': conhecimento['conhecimento']}
        )
        for container in agg_containers:
            containers.append(container['container'].lower())


vazios = []
for escala in agg_vazios:
    for vazio in escala['vazios']:
        vazios.append(vazio['container'])


# print('conhecimentos', conhecimentos)
# print('containers', containers)
# print('vazios', vazios)

container_carga = set(containers)

container_arquivos = db['fs.files'].find(
    {},
    ['metadata.numeroinformado']
)

container_arquivo = set()
for container in container_arquivos:
    container_arquivo.add(container['metadata']['numeroinformado'].lower())

print('Total de números de container únicos ' +
      'importados do CARGA', len(container_carga))
print('Total de arquivos de imagem de escaneamento',
      len(container_arquivo))
print('Contâineres CARGA sem escaneamento',
      len(container_carga - container_arquivo))


cursor = db['fs.files'].find(
    {'metadata.carga.atracacao.escala': None,
     'metadata.contentType': 'image/jpeg',
     'metadata.dataescaneamento':
     {'$gt': iniciocarga, '$lt': fimcarga + timedelta(days=1)}
     },
    {'metadata.carga.atracacao.escala': 1,
     'metadata.contentType': 1,
     'metadata.dataescaneamento': 1})
print('Imagens de escaneamento sem dados CARGA', cursor.count())

print('Critério: Foram consultados todos os números da importação' +
      'CARGA da data inicial até a data final menos 3 dias')
