import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


path_dados = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\data.csv'
path_data_funcionamento = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\data_funcionamento.csv'
path_limite_conama = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\limites_conama_506.csv'

limite_conama = pd.read_csv(path_limite_conama, sep=';', encoding='utf-8')
dados = pd.read_csv(path_dados, sep=',', encoding='utf-8')
data_funcionamento = pd.read_csv(path_data_funcionamento, sep=',', encoding='utf-8')

print('dados:')
print(dados.columns)
print('data_funcionamento:')
print(data_funcionamento.columns)
print('limite_conama:')
print(limite_conama.columns)

# join data_funcionamento with dados on 'Estacao' and 'Poluente'
dados = dados.merge(data_funcionamento, how='left', on=['Estacao', 'Poluente'])

# join dados with limite_conama on 'Poluente'
dados = dados.merge(limite_conama, how='left', on=['Poluente'])

if 'Periodo' == '24h':
    # liberação de poluentes em 24h
    pass
if 'Periodo' == 'med. arit. anual':
    # média aritmética anual
    pass
if 'Periodo' == 'max. med. hor. do dia (1h)':
    # máxima média horária obtida no dia
    pass
if 'Periodo' == 'max. med. mov. do dia (8h)':
    # máxima média móvel obtida no dia
    pass
if 'Periodo' == 'med. geom. anual':
    # média geométrica anual
    pass



