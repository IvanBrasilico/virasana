# coding: utf-8
"""
ABRIR FICHA pelo card do NAVI-I — o tratamento do risco (plano aprovado em 17/10/2026; `CONTEXTO.md`, "Tratamento do
risco: Fichas pelo NAVI-I").

Monta, a partir do card do contêiner (o mesmo card da tela), o que a regra do Fichas grava
(`bhadrasana.models.navii_ficha`, repo bhadrasana2 — só aditivo, uma transação): o resumo do risco (observações da
Ficha, até 500 caracteres, editável no diálogo), a descrição da carga declarada (RVF), a memória de cálculo do risco
(`descricao_interna` da RVF, interna), as imagens de escaneamento (lidas do Mongo do VIRASANA) e o recinto onde o
contêiner está.

As imagens vão para o Mongo do FICHAS (`MONGODB_RISCO`, banco `risco`) — outro servidor/banco que o do virasana. O
`mongodb_risco` de `virasana/db.py` é `conn['risco']` no servidor do virasana: NÃO é o banco do Fichas.

O módulo do Fichas é importado só quando usado (`regra_do_fichas`): se ele falhar ao carregar, só esta função fica
indisponível — o resto do NAVI-I segue.
"""
import logging
import os
from datetime import datetime
from typing import List, Optional, Tuple

from bson import ObjectId
from gridfs import GridFS

from virasana.navii import fichas_ovr, referencias
from virasana.navii.busca_navii import busca_ces_navii, monta_cards_navii

TAMANHO_RESUMO = 500          # = bhadrasana.models.navii_ficha.TAMANHO_RESUMO (o teste confere)
TAMANHO_TEXTO_RVF = 5000
MAX_IMAGENS = 4
SINAIS_NO_RESUMO = 3
_MONGO_FICHAS = {}
logger = logging.getLogger(__name__)


def regra_do_fichas():
    """`bhadrasana.models.navii_ficha`, importado na hora (carrega o registry do Fichas)."""
    from bhadrasana.models import navii_ficha
    return navii_ficha


def mongodb_fichas(config) -> Optional[object]:
    """Banco de imagens do Fichas: `config['mongodb_fichas']` (testes) ou `MONGODB_RISCO` (uma conexão por processo).
    None = não configurado (a abertura de Ficha fica indisponível)."""
    if config.get('mongodb_fichas') is not None:
        return config['mongodb_fichas']
    uri = os.environ.get('MONGODB_RISCO')
    if not uri:
        return None
    if uri not in _MONGO_FICHAS:
        from pymongo import MongoClient
        _MONGO_FICHAS[uri] = MongoClient(host=uri)['risco']
    return _MONGO_FICHAS[uri]


def normaliza_conteiner(numero: Optional[str]) -> str:
    return ''.join(c for c in (numero or '').upper() if c.isalnum())


def card_do_conteiner(session, ce: str, conteiner: str) -> Optional[dict]:
    """O card do NAVI-I do contêiner no CE (sem limite de data: filtro por CE e contêiner)."""
    alvo = normaliza_conteiner(conteiner)
    resultados = busca_ces_navii(session, 'efetiva', {'ce': [ce], 'conteiner': [alvo]}, limit=5)
    cards = monta_cards_navii(session, resultados)
    return next((c for c in cards if normaliza_conteiner(c.get('numeroConteiner')) == alvo), None)


# ---------------------------------------------------------------- textos

def _data(valor) -> str:
    if isinstance(valor, datetime):
        return valor.strftime('%d/%m/%Y %H:%M')
    if isinstance(valor, str) and len(valor) >= 10 and valor[4] == '-':
        return f'{valor[8:10]}/{valor[5:7]}/{valor[:4]}'
    return '-'


def _corta(texto: str, limite: int) -> str:
    texto = texto.strip()
    return texto if len(texto) <= limite else texto[:limite - 1] + '…'


def recinto_atual(card: dict) -> Optional[str]:
    """Onde o contêiner está (ou esteve por último): o recinto seco, se entrou e não saiu; senão, o do desembarque."""
    linha = card.get('linhaDoTempo') or {}
    seco = linha.get('entradaRecintoSeco') or {}
    if seco.get('recinto'):
        return seco['recinto']
    return (linha.get('desembarque') or {}).get('recinto')


def _risco_curto(card: dict) -> List[str]:
    risco = card.get('risco')
    if not risco or not risco.get('rotuloFaixa'):
        return ['Risco histórico: não calculado.']
    sinais = [f'{p["dimensao"]} {p["valor"]}' for p in (risco.get('parcelas') or [])[:SINAIS_NO_RESUMO]]
    pontos = f' ({risco["pontos"]:.0f} pts)' if risco.get('pontos') is not None else ''
    return [f'Risco histórico {risco["rotuloFaixa"].upper()}{pontos}' + (f': {"; ".join(sinais)}.' if sinais else '.')]


def resumo_padrao(card: dict) -> str:
    """Observações da Ficha (o analista edita no diálogo): selo, contêiner, risco histórico, CORAD e situação."""
    partes = [f'[NAVI-I] Contêiner {card.get("numeroConteiner") or "-"}, CE {card.get("numeroCEmercante") or "-"}.']
    partes += _risco_curto(card)
    if card.get('corad'):
        partes.append(f'CORAD {card["corad"].get("rotulo")}.')
    estagio = (card.get('estagio') or {}).get('rotulo')
    if card.get('semDesembarque'):
        previsao = card.get('previsaoAtracacao') or {}
        partes.append(f'Ainda sem desembarque (atracação prevista {_data(previsao.get("data"))}).')
    elif estagio:
        partes.append(f'Situação: {estagio}.')
    return _corta(' '.join(partes), TAMANHO_RESUMO)


def _linhas_conteiner(card: dict) -> List[str]:
    alvo = normaliza_conteiner(card.get('numeroConteiner'))
    do_card = next((c for c in card.get('conteineres') or [] if normaliza_conteiner(c.get('codigoConteiner')) == alvo),
                   {})
    tipo = do_card.get('tipoConteiner') or {}
    linhas = [f'Contêiner: {card.get("numeroConteiner")}'
              + (f' (tipo {tipo.get("codigo")} — {tipo.get("descricao")})' if tipo else '')
              + (f'; lacre {do_card["lacre"]}' if do_card.get('lacre') else '')
              + (f'; peso bruto {do_card["pesoBruto"]:.0f} kg' if do_card.get('pesoBruto') else '')]
    for ncm in do_card.get('ncms') or []:
        linhas.append(f'NCM {ncm.get("identificacaoNCM") or "-"}'
                      + (f' — {ncm["descricaoPosicao"]}' if ncm.get('descricaoPosicao') else '')
                      + (f' (declarado: {ncm["descritivo"]})' if ncm.get('descritivo') else ''))
    return linhas


def _linhas_ciclo(card: dict) -> List[str]:
    linha = card.get('linhaDoTempo') or {}
    rotulos = (('desembarque', 'Desembarque'), ('saida', 'Saída do operador'),
               ('entradaRecintoSeco', 'Entrada no recinto seco'), ('saidaRecintoSeco', 'Saída do recinto seco'))
    linhas = [f'{rotulo}: {_data(etapa.get("data"))} em {etapa.get("recinto")} {etapa.get("nomeRecinto") or ""}'.strip()
              for chave, rotulo in rotulos for etapa in [linha.get(chave) or {}] if etapa.get('data')]
    for esc in card.get('escaneamentos') or []:
        linhas.append(f'Escaneamento: {_data(esc.get("data"))} em {esc.get("recinto") or "-"}'
                      + (' (imagem anexada)' if esc.get('temImagem') else ' (sem imagem)'))
    if card.get('semDesembarque'):
        linhas.append(f'Ainda sem desembarque; atracação prevista: '
                      f'{_data((card.get("previsaoAtracacao") or {}).get("data"))}')
    return linhas or ['Ciclo de vida: não localizado.']


def descricao_carga(card: dict) -> str:
    """RVF.descricao: a carga declarada e o que o ciclo de vida sabe do contêiner."""
    nome_consignatario = card.get('nomeConsignatario') or ''
    origem = card.get('portoOrigemCarga')
    linhas = ['Inspeção não invasiva — Ficha aberta pelo NAVI-I (análise de risco na importação).',
              *_linhas_conteiner(card),
              f'CE Mercante {card.get("numeroCEmercante")} ({card.get("tipoBL") or "tipo de BL não informado"}); '
              f'manifesto {(card.get("manifesto") or {}).get("numero") or "-"}; '
              f'navio {card.get("nomeEmbarcacao") or card.get("embarcacao") or "-"}',
              f'Consignatário: {card.get("consignatario") or "-"} {nome_consignatario}'.strip(),
              f'Embarcador: {card.get("embarcador") or "-"}',
              f'Origem {origem or "-"} ({referencias.descreve_porto(origem) or "-"})'
              f' → destino {card.get("portoDestFinal") or "-"}',
              f'Mercadoria declarada: {card.get("descricaoMercadoria") or "-"}',
              *_linhas_ciclo(card)]
    return _corta('\n'.join(linhas), TAMANHO_TEXTO_RVF)


def memoria_risco(card: dict) -> str:
    """RVF.descricao_interna: a memória de cálculo completa do risco (interna; não vai a documento do importador)."""
    risco = card.get('risco') or {}
    linhas = ['Memória do risco — NAVI-I']
    if risco.get('rotuloFaixa'):
        linhas.append(f'Risco histórico: {risco["rotuloFaixa"]} ({risco.get("pontos")} pontos); versão dos '
                      f'parâmetros {risco.get("versao")}, calculado em {_data(risco.get("calculadoEm"))}')
        for p in risco.get('parcelas') or []:
            fichas = ', '.join(str(f['id']) for f in p.get('fichas_links') or [])
            linhas.append(f'- {p.get("grupo")} / {p.get("dimensao")}: {p.get("valor")} {p.get("descricao") or ""}'
                          f' — peso {p.get("peso")}; {p.get("fichas")} ficha(s); {p.get("pct")}% dos CEs; '
                          f'{p.get("vezes")}x a base' + (f'; Fichas {fichas}' if fichas else ''))
    else:
        linhas.append('Risco histórico: não calculado.')
    if card.get('corad'):
        linhas.append(f'CORAD do importador: {card["corad"].get("rotulo")}')
    for dta in card.get('dtas') or []:
        linhas.append(f'DTA {dta.get("numero") or "-"} ({dta.get("tipo") or "-"})')
    fichas = [f for f in card.get('fichasOvr') or []]
    if fichas:
        linhas.append('Fichas já ligadas ao CE/contêiner: '
                      + ', '.join(f'{f.get("id")} ({f.get("tipo") or "-"}, {f.get("fase") or "-"})' for f in fichas))
    return _corta('\n'.join(linhas), TAMANHO_TEXTO_RVF)


def imagens_do_card(mongodb, card: dict) -> List[Tuple[bytes, str]]:
    """(conteúdo, nome) das imagens de escaneamento do card, lidas do GridFS do VIRASANA (até MAX_IMAGENS); a que
    não for achada fica de fora (com aviso no log) — não impede a Ficha."""
    if mongodb is None:
        return []
    fs, imagens = GridFS(mongodb), []
    for imagem_id in (card.get('imagens_ids') or [])[:MAX_IMAGENS]:
        try:
            arquivo = fs.get(ObjectId(str(imagem_id)))
            imagens.append((arquivo.read(), arquivo.filename or f'{imagem_id}.jpg'))
        except Exception as err:  # noqa: imagem ausente não impede a Ficha
            logger.warning(f'NAVI-I/Ficha: imagem {imagem_id} do contêiner {card.get("numeroConteiner")} '
                           f'não lida do Mongo do virasana: {err}')
    return imagens


def dados_da_ficha(regra, mongodb, card: dict, resumo: Optional[str] = None, bloqueio: bool = False,
                   agendada: bool = False):
    """`DadosFicha` do Fichas a partir do card (o resumo editado no diálogo, se veio)."""
    return regra.DadosFicha(
        numeroCEmercante=card.get('numeroCEmercante'), conteiner=card.get('numeroConteiner'),
        resumo_risco=_corta(resumo, TAMANHO_RESUMO) if resumo and resumo.strip() else resumo_padrao(card),
        descricao_carga=descricao_carga(card), memoria_risco=memoria_risco(card),
        imagens=imagens_do_card(mongodb, card), recinto_siscomex=recinto_atual(card),
        cnpj_consignatario=card.get('consignatario'), bloqueio_incluido=bool(bloqueio),
        verificacao_agendada=bool(agendada))


def url_da_ficha(ovr_id) -> str:
    return fichas_ovr.URL_FICHA.format(ovr_id)
