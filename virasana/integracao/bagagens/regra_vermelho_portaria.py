"""
Implementa a regra contida na Ordem de Serviço ALFSTS de DSI de Canal Vermelho

Resumidamente, a regra diz:

SE dígito verificador da DSI é igual ao dia de registro da DSI,
então colocar em canal vermelho SE o dígito anterior for
 igual a 2 vezes o dígito, menos 2 ou menos 1,
 conforme dicionário abaixo, reiniciando no cinco (módulo de 5) e considerando zero = 10.
"""

from datetime import date

segundo_digito_valido = {
    '1': '01',
    '2': '23',
    '3': '45',
    '4': '67',
    '5': '89',
    '6': '01',
    '7': '23',
    '8': '45',
    '9': '67',
    '0': '89',
}


def regra_matematica_digito(digito: int, anterior: int):
    valor = (digito % 5) * 2
    if valor == 0:
        valor = 10
    return (anterior < valor) and (anterior > valor - 3)


def e_canal_vermelho(num_dsi: str, data_registro: date):
    if len(num_dsi) < 10:
        raise ValueError(f'Número de DSI {num_dsi} inválido!')
    dia_str = str(data_registro.day)
    final_dia = dia_str[-1:]
    digito_verificador = num_dsi[-1:]
    digito_anterior = num_dsi[-2:-1]
    if digito_verificador != final_dia:  # Regra 1 NÃO atendida
        return False
    return regra_matematica_digito(int(digito_verificador), int(digito_anterior))
    # Para fazer com dicionário:
    # return digito_anterior in segundo_digito_valido[digito_verificador]


if __name__ == '__main__':
    # Testes de Unidade!

    for digito, anteriores in segundo_digito_valido.items():
        assert regra_matematica_digito(int(digito), int(anteriores[0]))
        assert regra_matematica_digito(int(digito), int(anteriores[1]))

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
    ]
    DSIs_vermelhas = [
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
        ['2200000095', date(2022, 1, 5)],
        ['2200000006', date(2022, 1, 16)],
        ['2200000016', date(2022, 1, 6)],
        ['2200000027', date(2022, 1, 7)],
        ['2200000037', date(2022, 1, 27)],
        ['2200000048', date(2022, 1, 18)],
        ['2200000058', date(2022, 1, 28)],
        ['2200000069', date(2022, 1, 9)],
        ['2200000079', date(2022, 1, 29)],
        ['2200000080', date(2022, 1, 10)],
        ['2200000090', date(2022, 1, 30)],
    ]
    for dsi in DSIs_verdes:
        assert e_canal_vermelho(dsi[0], dsi[1]) is False

    for dsi in DSIs_vermelhas:
        assert e_canal_vermelho(dsi[0], dsi[1]) is True
