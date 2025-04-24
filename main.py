import os
import pandas as pd
import numpy as np

raw_data_path = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\raw_data'

# extract all data
data_dict = {}

for folder in os.listdir(raw_data_path):
    state = folder
    state_data = []
    
    for file in os.listdir(os.path.join(raw_data_path, folder)):
        file_path = os.path.join(raw_data_path, folder, file)
        df = pd.read_csv(file_path, sep=',', encoding='latin1')
        state_data.append(df)

    state_data = pd.concat(state_data, ignore_index=True)
    data_dict[state] = state_data

# concat everythin on data_dict into a single dataframe creating a new column called 'state'
data = pd.DataFrame()
for state, records in data_dict.items():
    records['state'] = state
    data = pd.concat([data, records], ignore_index=True)

# fill column 'Data' where it is null with the column 'ï»¿Data'
data['Data'] = data['Data'].fillna(data['ï»¿Data'])

# eclude 'ï»¿Data' column
data = data.drop(columns=['ï»¿Data'])

# unify PM10 and MP10
data['Poluente'] = data['Poluente'].replace({'PM10': 'MP10'})

# clean the unit column
variacoes_unidade = ['ug/m3', 'µg/m3', 'µg/m³', 'Âµg/mÂ³']
data['Unidade'] = data['Unidade'].replace(variacoes_unidade, 'µg/m³')

# molar masses
massa_molar = {
    'NO2': 46.0055,
    'O3': 48.00,
    'SO2': 64.066,
    'CO': 28.01,
    'NO': 30.01
}
# TODO: verificar se podemos usar 24.45 L/mol (25°C e 1 atm) pode ser usado para todos os poluentes em todas as localidades
PPM_CONVERSION = 24.45

# initialize new columns
data['Valor_Padronizado'] = np.nan
data['Unidade_Padronizada'] = 'µg/m³'

# unit for CO is ppm
co_mask = data['Poluente'] == 'CO'
data.loc[co_mask, 'Unidade_Padronizada'] = 'ppm'

# CO conversions (vectorized)
co_ppm_mask = co_mask & (data['Unidade'] == 'ppm')
co_ppb_mask = co_mask & (data['Unidade'] == 'ppb')
data.loc[co_ppm_mask, 'Valor_Padronizado'] = data.loc[co_ppm_mask, 'Valor']
data.loc[co_ppb_mask, 'Valor_Padronizado'] = data.loc[co_ppb_mask, 'Valor'] / 1000

# other pollutants conversions (vectorized)
other_mask = ~co_mask
other_ppm_mask = other_mask & (data['Unidade'] == 'ppm')
other_ppb_mask = other_mask & (data['Unidade'] == 'ppb')
other_ugm3_mask = other_mask & (data['Unidade'] == 'µg/m³')

# if unity is ug/m³
data.loc[other_ugm3_mask, 'Valor_Padronizado'] = data.loc[other_ugm3_mask, 'Valor']

# if unity is ppm
for poluente, mm in massa_molar.items():
    if poluente == 'CO': continue
    mask = other_ppm_mask & (data['Poluente'] == poluente)
    data.loc[mask, 'Valor_Padronizado'] = (data.loc[mask, 'Valor'] * mm * 1000) / PPM_CONVERSION

# if unity is ppb
for poluente, mm in massa_molar.items():
    if poluente == 'CO': continue
    mask = other_ppb_mask & (data['Poluente'] == poluente)
    data.loc[mask, 'Valor_Padronizado'] = (data.loc[mask, 'Valor'] * mm) / PPM_CONVERSION


# load lat lon data from csv
lat_lon_path = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\support_data\Monitoramento_QAr_BR_latlon_2024.csv'
lat_lon_data = pd.read_csv(lat_lon_path, sep=',', encoding='utf-8')

# select wanted columns from lat lon data
lat_lon_data = lat_lon_data[['ESTAÇÃO', 'LATITUDE', 'LONGITUDE']]

# left join data on 'ESTAÇÃO' = 'Estacao'
data = data.merge(lat_lon_data, how='left', left_on='Estacao', right_on='ESTAÇÃO')
data = data.drop(columns=['ESTAÇÃO'])

print(data.columns)

# save data df to csv 
data_path = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\processed_data'
data.to_csv(os.path.join(data_path, 'data.csv'), sep=',', encoding='utf-8', index=False)

