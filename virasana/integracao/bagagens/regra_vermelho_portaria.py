"""
Implementa a regra contida na Ordem de Serviço ALFSTS de DSI de Canal Vermelho

Resumidamente, a regra diz:

SE dígito verificador da DSI é igual ao dia de registro da DSI,
então colocar em canal vermelho SE o dígito anterior for
 igual a 2 vezes o dígito, menos 2 ou menos 1,
 conforme dicionário abaixo, reiniciando no cinco (módulo de 5) e considerando zero = 10.
"""

from datetime import date
from typing import Tuple

segundo_digito_valido = {
    '1': '5',
    '2': '6',
    '3': '7',
    '4': '8',
    '5': '9',
    '6': '0',
    '7': '1',
    '8': '2',
    '9': '3',
    '0': '4',
}

terceiro_digito_valido = {
'1': '7',
'2': '8',
'3': '9',
'4': '0',
'5': '1',
'6': '2',
'7': '3',
'8': '4',
'9': '5',
'0': '6',
}


def regra_matematica_digito(digito: int, anterior: int):
    valor = (digito + 4) % 10
    return valor == anterior

def regra_matematica_digito_terceiro(digito: int, anterior: int):
    valor = (digito + 6) % 10
    return valor == anterior

def e_canal_vermelho(num_dsi: str, data_registro: date)->Tuple[bool, str]:
    if len(num_dsi) < 10:
        raise ValueError(f'Número de DSI {num_dsi} inválido!')
    dia_str = str(data_registro.day)
    final_dia = dia_str[-1:]
    digito_verificador = num_dsi[-1:]
    digito_anterior = num_dsi[-2:-1]
    penultimo_digito = num_dsi[-3:-2]
    if digito_verificador != final_dia:  # Regra 1 NÃO atendida
        return False, 'REGRA_1_VIOLADA'
    if not regra_matematica_digito(int(digito_verificador), int(digito_anterior)): # Regra 2 não atendida
        return False, 'REGRA_2_VIOLADA'
    if terceiro_digito_valido[digito_verificador] != penultimo_digito:
        return False, 'REGRA_3_VIOLADA'
    return True, 'VALIDO'


if __name__ == '__main__':
    # Testes de Unidade!

    for digito, anteriores in segundo_digito_valido.items():
        assert regra_matematica_digito(int(digito), int(anteriores[0]))
        # assert regra_matematica_digito(int(digito), int(anteriores[1]))

    for digito, anterior in terceiro_digito_valido.items():
        assert regra_matematica_digito_terceiro(int(digito), int(anterior))

    # Casos que devem ser considerados VERDE (não canal vermelho)
    DSIs_verdes = [
        ['2200000002', date(2022, 1, 1)],
        ['2200000001', date(2022, 1, 2)],
        ['2200000011', date(2022, 1, 3)],
        ['2200000022', date(2022, 1, 4)],
        ['2200000032', date(2022, 1, 5)],
        ['2200000043', date(2022, 1, 6)],
        ['2200000053', date(2022, 1, 7)],
        ['2200000064', date(2022, 1, 8)],
        ['2200000074', date(2022, 1, 9)],
        ['2200000085', date(2022, 1, 10)],
        ['2200000065', date(2022, 1, 5)],
        ['2200000001', date(2022, 1, 1)],
        ['2200000001', date(2022, 1, 21)],
        ['2200000001', date(2022, 1, 31)],
        ['2200000011', date(2022, 1, 11)],
        ['2200000022', date(2022, 1, 2)],
        ['2200000032', date(2022, 1, 12)],
        ['2200000043', date(2022, 1, 3)],
        ['2200000053', date(2022, 1, 13)],
        ['2200000064', date(2022, 1, 14)],
        ['2200000074', date(2022, 1, 24)],
        ['2200000085', date(2022, 1, 25)],
    ]

    # Casos que devem ser considerados VERMELHO (canal vermelho)
    DSIs_vermelhas = [
        ['2200000751', date(2022, 1, 1)],
        ['2200000751', date(2022, 1, 21)],
        ['2200000751', date(2022, 1, 31)],
        ['2200000751', date(2022, 1, 11)],
        ['2200000862', date(2022, 1, 2)],
        ['2200000862', date(2022, 1, 12)],
        ['2200000973', date(2022, 1, 3)],
        ['2200000973', date(2022, 1, 13)],
        ['2200000084', date(2022, 1, 14)],
        ['2200000084', date(2022, 1, 24)],
        ['2200000195', date(2022, 1, 25)],
        ['2200000195', date(2022, 1, 5)],
        ['2200000206', date(2022, 1, 16)],
        ['2200000206', date(2022, 1, 6)],
        ['2200000317', date(2022, 1, 7)],
        ['2200000317', date(2022, 1, 27)],
        ['2200000428', date(2022, 1, 18)],
        ['2200000428', date(2022, 1, 28)],
        ['2200000539', date(2022, 1, 9)],
        ['2200000539', date(2022, 1, 29)],
        ['2200000640', date(2022, 1, 10)],
        ['2200000640', date(2022, 1, 30)],
    ]
    for (dsi, data_registro) in DSIs_verdes:
        canal_vermelho, motivo = e_canal_vermelho(dsi, data_registro)
        if (canal_vermelho is True):
            print(f'Erro na DSI verde: {dsi} - retornado vermelho: {motivo}')

    for (dsi, data_registro) in DSIs_vermelhas:
        canal_vermelho, motivo = e_canal_vermelho(dsi, data_registro)
        if (canal_vermelho is False):
            print(f'Erro na DSI vermelha: {dsi} {data_registro}- retornado verde: {motivo}')
