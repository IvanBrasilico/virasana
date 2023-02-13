import pandas as pd


df = pd.read_excel('relatorio.xlsx')
print(df.groupby('Motivo').Recinto.count())
# print(df.head())