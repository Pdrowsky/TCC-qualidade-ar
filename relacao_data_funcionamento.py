import pandas as pd
import numpy as np

# load csv file
file_path = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\data.csv'
data = pd.read_csv(file_path, sep=',', encoding='utf-8')


# create date time column
data['Data'] = pd.to_datetime(data['Data'], format='%Y-%m-%d')

# locate rows with only HH:MM data to add seconds
hora = data['Hora']
linhas_horaminuto = hora.str.count(':') == 1
hora.loc[linhas_horaminuto] += ':00'
hor = hora.replace('24:00:00', '00:00:00')

data['Hora'] = pd.to_datetime(data['Hora'], format='%H:%M:%S').dt.time
data['Data_Hora'] = pd.to_datetime(data['Data'].astype(str) + ' ' + data['Hora'].astype(str), format='%Y-%m-%d %H:%M:%S')

# group by Estacao and Poluente and get the first and last date
data_grouped = data.groupby(['Estacao', 'Poluente']).agg({'Data_Hora': ['min', 'max']}).reset_index()
data_grouped.columns = ['Estacao', 'Poluente', 'Data_Hora_Inicio', 'Data_Hora_Fim']

# save the result to a new csv file on the same folder
output_path = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\data_funcionamento.csv'
data_grouped.to_csv(output_path, sep=',', encoding='utf-8', index=False)