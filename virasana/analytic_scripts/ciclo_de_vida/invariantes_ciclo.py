# coding: utf-8
"""
INVARIANTES do ciclo de vida (22/10/2026): o que vale para QUALQUER ciclo gravado, com dado bom ou ruim. O dado ruim
(evento faltando, CE errado, D/E trocados, relógio...) pode deixar o ciclo aberto, incompleto, sem CE ou com
confiança baixa — mas nunca produzir um destes absurdos:

  1. CE principal de exportação (tráfego 7);
  2. CE principal com a carga SAINDO do porto do desembarque (origem = porto; caso CAAU5545018);
  3. CE principal sem o contêiner nos itens do Mercante;
  4. CE principal fora da família gravada, família sem principal, confiança fora de alta/média/baixa, confiança sem
     CE;
  5. etapas fora de ordem: desembarque <= saída do operador <= entrada no seco <= saída do seco; embarque antes do
     desembarque;
  6. concluído sem o evento que conclui: TRA sem a saída do seco, passagem/baldeação sem o embarque. (Pelo operador
     pode concluir sem a saída informada — a entrada seguinte com o contêiner vazio, `linha_do_tempo`.)

Uso: `test_casos_reais_ciclo.py` verifica todos os ciclos de todos os casos reais; a mesma função serve para auditar
os ciclos gravados em produção (ciclo = linha de `ciclodevida_ciclo_de_vida_importacao` + 'familia').
"""
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
from virasana.analytic_scripts.ciclo_de_vida.ces_do_ciclo import TRAFEGO_EXPORTACAO, normaliza_ce

CONFIANCAS = (ldt.ALTA, ldt.MEDIA, ldt.BAIXA)
ORDEM_DAS_ETAPAS = ('data_desembarque', 'data_saida', 'data_entrada_seco', 'data_saida_seco')
CONCLUSAO = {ldt.TRA: 'data_saida_seco', ldt.PASSAGEM: 'data_embarque', ldt.BALDEACAO: 'data_embarque'}


def _t(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def _dt(valor) -> Optional[datetime]:
    if valor is None or isinstance(valor, datetime):
        return valor
    return datetime.fromisoformat(str(valor))


def contexto_do_caso(caso: dict) -> Tuple[Dict[str, dict], Set[Tuple[str, str]]]:
    """(info dos CEs: tráfego e origem; itens (CE, contêiner)) de um caso extraído por `caso_ciclo.py`."""
    tabelas = caso['tabelas']
    info = {normaliza_ce(c['numeroCEmercante']): {'trafego': _t(c.get('tipoTrafego')),
                                                  'origem': _t(c.get('portoOrigemCarga')).upper()}
            for c in tabelas.get('conhecimentosresumo', [])}
    itens = {(normaliza_ce(i['numeroCEmercante']), _t(i['codigoConteiner'])) for i in tabelas.get('itensresumo', [])}
    return info, itens


def _sem_principal(familia: Set[str], confianca) -> List[str]:
    return (['família sem CE principal'] if familia else []) + \
        ([f'confiança {confianca} sem CE principal'] if confianca else [])


def _do_principal(ciclo: dict, info: Dict[str, dict], itens: Set[Tuple[str, str]], porto: Optional[str]) -> List[str]:
    principal = normaliza_ce(ciclo.get('numeroCEmercante'))
    familia = {normaliza_ce(ce) for ce in ciclo.get('familia') or []}
    confianca = ciclo.get('confianca_ce')
    if not principal:
        return _sem_principal(familia, confianca)
    dados = info.get(principal, {})
    regras = ((dados.get('trafego') == TRAFEGO_EXPORTACAO, 'CE principal de exportação'),
              (bool(porto) and dados.get('origem') == porto, f'CE principal com a carga saindo de {porto}'),
              ((principal, _t(ciclo.get('numeroConteiner'))) not in itens, 'contêiner fora dos itens do CE principal'),
              (principal not in familia, 'CE principal fora da família gravada'),
              (confianca not in CONFIANCAS, f'confiança inválida: {confianca}'))
    return [mensagem for viola, mensagem in regras if viola]


def _da_ordem(ciclo: dict) -> List[str]:
    datas = [(campo, _dt(ciclo[campo])) for campo in ORDEM_DAS_ETAPAS if ciclo.get(campo)]
    erros = [f'{anterior} depois de {seguinte}' for (anterior, data_a), (seguinte, data_s) in zip(datas, datas[1:])
             if data_a > data_s]
    embarque, desembarque = _dt(ciclo.get('data_embarque')), _dt(ciclo.get('data_desembarque'))
    if embarque and desembarque and embarque < desembarque:
        erros.append('embarque antes do desembarque')
    return erros


def _da_conclusao(ciclo: dict) -> List[str]:
    campo = CONCLUSAO.get(ciclo.get('fluxo'))
    if ciclo.get('situacao') != ldt.CONCLUIDO or campo is None or ciclo.get(campo):
        return []
    return [f'concluído ({ciclo["fluxo"]}) sem {campo}']


def violacoes(ciclo: dict, info: Dict[str, dict], itens: Set[Tuple[str, str]], porto: Optional[str] = None
              ) -> List[str]:
    """As invariantes que o ciclo viola ([] = nenhuma). `info`/`itens`: Mercante (ver `contexto_do_caso`); `porto`:
    o do desembarque (`CargaCiclo.portos_dos_desembarques`), None = não verifica a regra 2."""
    return _do_principal(ciclo, info, itens, porto) + _da_ordem(ciclo) + _da_conclusao(ciclo)
