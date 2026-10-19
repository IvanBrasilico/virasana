# coding: utf-8
"""
Busca por TEXTO do NAVI-I — índice de CEs no Meilisearch (plano em `navii/CONTEXTO.md`, "Busca por texto").

Um documento por CE de importação (chave `ce` = numeroCEmercante), só com os campos que estão no SQL
(`conhecimentosresumo` + nome do CNPJ em `laudo_empresas` + escalas do manifesto). Os CNPJs vão em campos próprios,
SEM tolerância a erro de digitação (senão um CNPJ acharia outros que diferem em 2 dígitos); o texto (nomes, notify,
descrição) com tolerância. `referencia` é a previsão de atracação de referência (escala no porto — padrão Santos —,
senão a primeira), em segundos (meia-noite UTC do dia): filtra o período e desempata pelos mais recentes.

O Meilisearch fica só em 127.0.0.1 (ver `navii/MEILISEARCH.md`); o navegador nunca fala com ele. Variáveis de
ambiente: `NAVII_MEILI_URL` (padrão http://127.0.0.1:7700), `NAVII_MEILI_CHAVE_BUSCA` (o virasana) e
`NAVII_MEILI_CHAVE_ESCRITA` (o indexador, `navii/indexador_texto.py`).

`notify` e `cnpjNotify` estão hoje sempre vazios no banco (o parser da carga não os lê — Pendência 5 do CONTEXTO);
já vão no documento para valer assim que a carga for corrigida.
"""
import calendar
import html
import os
import re
import time
from datetime import date, datetime, timedelta
from typing import Callable, Dict, Iterable, List, NamedTuple, Optional, Tuple

import requests

INDICE = 'navii_ces'
INDICE_ESTADO = 'navii_texto_estado'
CHAVE = 'ce'
URL_PADRAO = 'http://127.0.0.1:7700'
TIPO_TRAFEGO_EXPORTACAO = '7'
CNPJ = re.compile(r'^\d{14}$')

# ordem = peso (o campo antes pesa mais): nomes, notify, descrição, depois CNPJs e números
CAMPOS_BUSCA = ['embarcador', 'consignatario', 'notify', 'descricao', 'embarcadorCnpj', 'consignatarioCnpj',
                'cnpjNotify', 'transportador', 'ce', 'master', 'bl', 'manifesto']
SEM_ERRO_DE_DIGITACAO = ['embarcadorCnpj', 'consignatarioCnpj', 'cnpjNotify', 'transportador', 'ce', 'master', 'bl',
                         'manifesto']
CONFIGURACAO = {
    'searchableAttributes': CAMPOS_BUSCA,
    'filterableAttributes': ['referencia', 'id', 'origem', 'destino'],
    'sortableAttributes': ['referencia', 'id'],
    'rankingRules': ['words', 'typo', 'proximity', 'attribute', 'sort', 'exactness', 'referencia:desc'],
    'typoTolerance': {'enabled': True, 'disableOnAttributes': SEM_ERRO_DE_DIGITACAO},
}
ROTULOS = {'embarcador': 'embarcador', 'consignatario': 'consignatário', 'notify': 'notify', 'descricao': 'descrição',
           'embarcadorCnpj': 'CNPJ do embarcador', 'consignatarioCnpj': 'CNPJ do consignatário',
           'cnpjNotify': 'CNPJ do notify', 'transportador': 'transportador', 'ce': 'CE', 'master': 'CE master',
           'bl': 'BL', 'manifesto': 'manifesto'}
MARCA_INICIO, MARCA_FIM = '\x02', '\x03'     # marcas do trecho encontrado (o texto do CE não é HTML)
TAMANHO_TRECHO = 20                          # palavras em volta do que foi encontrado

# Chaves de API com UID FIXO. O Meilisearch calcula o valor da chave a partir do UID e da chave mestra: criar de novo
# não duplica (o UID repetido é recusado) e o valor sai sempre o mesmo (`GET /keys/<uid>`) enquanto a mestra não
# mudar. Sem UID, cada `POST /keys` cria uma chave nova (foi o que confundiu a 1ª instalação, 19/10/2026).
VARIAVEL_MESTRA = 'MEILI_MASTER_KEY'
VARIAVEL_BUSCA = 'NAVII_MEILI_CHAVE_BUSCA'
VARIAVEL_ESCRITA = 'NAVII_MEILI_CHAVE_ESCRITA'
DESCRICAO_CHAVES = 'NAVI-I'
CHAVES = {
    VARIAVEL_BUSCA: {'uid': '60a06578-9886-4ce7-b119-c46f8b7b0f38', 'name': 'navii-busca',
                     'description': 'NAVI-I busca (virasana)', 'actions': ['search'], 'indexes': [INDICE],
                     'expiresAt': None},
    VARIAVEL_ESCRITA: {'uid': 'decbba99-9480-42e9-b988-4f794eefc34e', 'name': 'navii-indexador',
                       'description': 'NAVI-I indexador',
                       'actions': ['documents.*', 'indexes.create', 'indexes.get', 'settings.*', 'tasks.get',
                                   'stats.get'],
                       'indexes': [INDICE, INDICE_ESTADO], 'expiresAt': None},
}


class ErroMeili(Exception):
    pass


def _texto(valor) -> str:
    return ' '.join(str(valor).split()) if valor is not None else ''


# ---------------------------------------------------------------- cliente HTTP
class ClienteMeili:
    """O mínimo da API REST do Meilisearch 1.x, por `requests` (sem SDK)."""

    def __init__(self, url: str = URL_PADRAO, chave: str = '', timeout: float = 30, sessao=None,
                 dorme: Callable[[float], None] = time.sleep):
        self.url, self.timeout, self.dorme = url.rstrip('/'), timeout, dorme
        self.sessao = sessao or requests.Session()
        self.cabecalhos = {'Authorization': f'Bearer {chave}'} if chave else {}

    def _pede(self, metodo: str, caminho: str, nao_achou=None, **kwargs):
        resposta = self.sessao.request(metodo, f'{self.url}{caminho}', headers=self.cabecalhos,
                                       timeout=self.timeout, **kwargs)
        if resposta.status_code == 404 and nao_achou is not None:
            return nao_achou()
        if resposta.status_code >= 400:
            try:
                detalhe = resposta.json().get('message', '')
            except ValueError:
                detalhe = resposta.text[:200]
            if resposta.status_code in (401, 403):
                detalhe += (f' — chave ausente ou sem permissão: confira {VARIAVEL_ESCRITA} (indexador) ou '
                            f'{VARIAVEL_BUSCA} (virasana); para gerar, indexador_texto.py --chaves')
            raise ErroMeili(f'{metodo} {caminho}: HTTP {resposta.status_code} {detalhe}')
        return resposta.json() if resposta.content else {}

    def saude(self) -> dict:
        return self._pede('GET', '/health')

    def indice(self, uid: str) -> Optional[dict]:
        return self._pede('GET', f'/indexes/{uid}', nao_achou=lambda: None)

    def cria_indice(self, uid: str, chave_primaria: str) -> dict:
        return self._pede('POST', '/indexes', json={'uid': uid, 'primaryKey': chave_primaria})

    def configuracao(self, uid: str) -> dict:
        return self._pede('GET', f'/indexes/{uid}/settings')

    def configura(self, uid: str, configuracao: dict) -> dict:
        return self._pede('PATCH', f'/indexes/{uid}/settings', json=configuracao)

    def adiciona(self, uid: str, documentos: List[dict], chave_primaria: str) -> dict:
        """Inclui ou SUBSTITUI os documentos inteiros (POST /documents)."""
        return self._pede('POST', f'/indexes/{uid}/documents', params={'primaryKey': chave_primaria}, json=documentos)

    def apaga_por_filtro(self, uid: str, filtro: str) -> dict:
        return self._pede('POST', f'/indexes/{uid}/documents/delete', json={'filter': filtro})

    def documento(self, uid: str, chave: str) -> Optional[dict]:
        return self._pede('GET', f'/indexes/{uid}/documents/{chave}', nao_achou=lambda: None)

    def estatisticas(self, uid: str) -> Optional[dict]:
        return self._pede('GET', f'/indexes/{uid}/stats', nao_achou=lambda: None)

    def busca(self, uid: str, corpo: dict) -> dict:
        return self._pede('POST', f'/indexes/{uid}/search', json=corpo)

    def tarefa(self, uid_tarefa: int) -> dict:
        return self._pede('GET', f'/tasks/{uid_tarefa}')

    def espera(self, tarefa: dict, limite: float = 900, pausa: float = 0.5) -> dict:
        """Espera a tarefa (resposta de uma escrita) terminar; ErroMeili se falhou, foi cancelada ou passou do
        limite."""
        uid, inicio = tarefa['taskUid'], time.monotonic()
        while True:
            situacao = self.tarefa(uid)
            status = situacao.get('status')
            if status == 'succeeded':
                return situacao
            if status in ('failed', 'canceled'):
                raise ErroMeili(f'tarefa {uid} {status}: {(situacao.get("error") or {}).get("message")}')
            if time.monotonic() - inicio > limite:
                raise ErroMeili(f'tarefa {uid} ainda {status} depois de {limite:.0f} s')
            self.dorme(pausa)

    # chaves de API (só com a chave mestra)
    def chave(self, uid: str) -> Optional[dict]:
        return self._pede('GET', f'/keys/{uid}', nao_achou=lambda: None)

    def cria_chave(self, definicao: dict) -> dict:
        return self._pede('POST', '/keys', json=definicao)

    def chaves(self) -> List[dict]:
        return self._pede('GET', '/keys', params={'limit': 1000}).get('results', [])


def cliente_do_ambiente(variavel_chave: str, ambiente=os.environ, timeout: float = 30) -> ClienteMeili:
    """Cliente com a URL de `NAVII_MEILI_URL` e a chave da variável indicada (busca, escrita ou a mestra)."""
    return ClienteMeili(ambiente.get('NAVII_MEILI_URL') or URL_PADRAO, ambiente.get(variavel_chave, ''), timeout)


class Chaves(NamedTuple):
    valores: Dict[str, str]          # variável de ambiente -> valor da chave
    criadas: List[str]               # variáveis cuja chave foi criada agora
    divergentes: List[str]           # chave existente com ações ou índices diferentes da definição
    avulsas: List[dict]              # outras chaves "NAVI-I" (criadas sem UID fixo): podem ser apagadas


def garante_chaves(cliente_mestre: ClienteMeili) -> Chaves:
    """Cria as chaves de busca e de escrita que faltam (UID fixo) e devolve os valores. Rodar de novo devolve os
    mesmos valores, sem criar nada."""
    valores, criadas, divergentes = {}, [], []
    for variavel, definicao in CHAVES.items():
        existente = cliente_mestre.chave(definicao['uid'])
        if existente is None:
            existente = cliente_mestre.cria_chave(definicao)
            criadas.append(variavel)
        elif (sorted(existente.get('actions') or []) != sorted(definicao['actions']) or
              sorted(existente.get('indexes') or []) != sorted(definicao['indexes'])):
            divergentes.append(variavel)
        valores[variavel] = existente['key']
    fixas = {definicao['uid'] for definicao in CHAVES.values()}
    avulsas = [c for c in cliente_mestre.chaves()
               if (c.get('description') or '').startswith(DESCRICAO_CHAVES) and c.get('uid') not in fixas]
    return Chaves(valores, criadas, divergentes, avulsas)


# ---------------------------------------------------------------- documento
def referencia(escalas: Iterable[Tuple[str, Optional[datetime]]], porto: str) -> Optional[datetime]:
    """A previsão de atracação de referência: a da escala no porto; sem ela, a primeira (a mais cedo)."""
    escalas = list(escalas)
    no_porto = [d for p, d in escalas if p == porto and d]
    datas = [d for _, d in escalas if d]
    return min(no_porto) if no_porto else (min(datas) if datas else None)


def em_segundos(dia) -> Optional[int]:
    """Meia-noite UTC do dia, em segundos (o Meilisearch filtra números)."""
    if dia is None:
        return None
    return calendar.timegm(date(dia.year, dia.month, dia.day).timetuple())


def de_segundos(segundos: Optional[int]) -> Optional[date]:
    return None if segundos is None else date(1970, 1, 1) + timedelta(seconds=int(segundos))


def configuracao_difere(atual: dict, desejada: dict = CONFIGURACAO) -> List[str]:
    """As chaves da configuração desejada que o índice não tem. O Meilisearch devolve os atributos filtráveis,
    ordenáveis e sem tolerância em outra ordem (conjunto); campos de busca e regras de ranking valem pela ordem."""
    diferentes = []
    for chave, valor in desejada.items():
        if chave == 'typoTolerance':
            tolerancia = atual.get(chave) or {}
            igual = (tolerancia.get('enabled') == valor['enabled'] and
                     set(tolerancia.get('disableOnAttributes') or []) == set(valor['disableOnAttributes']))
        elif chave in ('filterableAttributes', 'sortableAttributes'):
            igual = set(atual.get(chave) or []) == set(valor)
        else:
            igual = list(atual.get(chave) or []) == list(valor)
        if not igual:
            diferentes.append(chave)
    return diferentes


def documento_do_ce(ce, nomes: Dict[str, str], data_referencia: Optional[datetime]) -> Optional[dict]:
    """O documento do CE (linha do `conhecimentosresumo`); None sem número. `nomes`: CNPJ -> nome
    (`laudo_empresas`). Embarcador e consignatário guardam o CNPJ da empresa brasileira ou o nome da estrangeira."""
    numero = _texto(ce.numeroCEmercante)
    if not numero:
        return None
    documento = {'ce': numero, 'id': int(ce.ID), 'manifesto': _texto(ce.manifestoCE),
                 'master': _texto(ce.numeroCEMaster), 'bl': _texto(ce.numConhecimento),
                 'notify': _texto(ce.notify), 'cnpjNotify': _texto(ce.cnpjNotify), 'descricao': _texto(ce.descricao),
                 'transportador': _texto(ce.codigoEmpresaNavegacao), 'origem': _texto(ce.portoOrigemCarga),
                 'destino': _texto(ce.portoDestFinal), 'referencia': em_segundos(data_referencia)}
    for campo in ('embarcador', 'consignatario'):
        valor = _texto(getattr(ce, campo))
        e_cnpj = bool(CNPJ.match(valor))
        documento[campo] = nomes.get(valor, '') if e_cnpj else valor
        documento[f'{campo}Cnpj'] = valor if e_cnpj else ''
    return documento


def cnpjs_do_ce(ce) -> List[str]:
    """Os CNPJs do CE que podem ter nome em `laudo_empresas`."""
    return [v for v in (_texto(ce.embarcador), _texto(ce.consignatario)) if CNPJ.match(v)]


def e_importacao(ce) -> bool:
    return _texto(ce.tipoTrafego) != TIPO_TRAFEGO_EXPORTACAO


# ---------------------------------------------------------------- busca
class Achado(NamedTuple):
    ce: str
    referencia: Optional[date]
    trechos: List[Tuple[str, str]]          # (rótulo do campo, trecho com MARCA_INICIO/MARCA_FIM)


class Resultado(NamedTuple):
    total: int
    achados: List[Achado]


def corpo_da_busca(texto: str, inicio: Optional[date] = None, fim: Optional[date] = None, limite: int = 50,
                   deslocamento: int = 0) -> dict:
    """O pedido ao Meilisearch: período pela `referencia` (fim inclusive), trechos marcados e cortados."""
    corpo = {'q': texto, 'limit': limite, 'offset': deslocamento, 'attributesToRetrieve': ['ce', 'referencia'],
             'attributesToHighlight': CAMPOS_BUSCA, 'attributesToCrop': CAMPOS_BUSCA, 'cropLength': TAMANHO_TRECHO,
             'highlightPreTag': MARCA_INICIO, 'highlightPostTag': MARCA_FIM, 'showMatchesPosition': True}
    filtros = []
    if inicio is not None:
        filtros.append(f'referencia >= {em_segundos(inicio)}')
    if fim is not None:
        filtros.append(f'referencia < {em_segundos(fim + timedelta(days=1))}')
    if filtros:
        corpo['filter'] = ' AND '.join(filtros)
    return corpo


def le_resultado(resposta: dict) -> Resultado:
    """Os CEs na ordem do Meilisearch e, de cada um, os trechos dos campos onde o texto foi encontrado (na ordem de
    peso dos campos)."""
    achados = []
    for hit in resposta.get('hits', []):
        posicoes, formatado = hit.get('_matchesPosition') or {}, hit.get('_formatted') or {}
        trechos = [(ROTULOS[campo], _texto(formatado.get(campo))) for campo in CAMPOS_BUSCA
                   if campo in posicoes and _texto(formatado.get(campo))]
        achados.append(Achado(hit['ce'], de_segundos(hit.get('referencia')), trechos))
    return Resultado(int(resposta.get('estimatedTotalHits') or resposta.get('totalHits') or 0), achados)


def busca(cliente: ClienteMeili, texto: str, inicio: Optional[date] = None, fim: Optional[date] = None,
          limite: int = 50, deslocamento: int = 0) -> Resultado:
    return le_resultado(cliente.busca(INDICE, corpo_da_busca(texto, inicio, fim, limite, deslocamento)))


def trecho_html(trecho: str) -> str:
    """O trecho para a tela: texto escapado e o que foi encontrado em <mark>."""
    return html.escape(trecho).replace(MARCA_INICIO, '<mark>').replace(MARCA_FIM, '</mark>')
