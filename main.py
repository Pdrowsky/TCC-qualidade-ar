import os
import pandas as pd

raw_data_path = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\raw_data'

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

# eclude ï»¿Data column
data = data.drop(columns=['ï»¿Data'])

print(data['Poluente'].unique())

co_data = data[data['Poluente'] == 'CO']
mp10_data = data[data['Poluente'] == 'MP10']
no2_data = data[data['Poluente'] == 'NO2']
o3_data = data[data['Poluente'] == 'O3']
so2_data = data[data['Poluente'] == 'SO2']
mp25_data = data[data['Poluente'] == 'MP2.5']
pts_data = data[data['Poluente'] == 'PTS']
fmc_data = data[data['Poluente'] == 'FMC']
no_data = data[data['Poluente'] == 'NO']
pm10_data = data[data['Poluente'] == 'PM10']

print('co_data: ', co_data['Unidade'].unique())
print('mp10_data: ', mp10_data['Unidade'].unique())
print('no2_data: ', no2_data['Unidade'].unique())
print('o3_data: ', o3_data['Unidade'].unique())
print('so2_data: ', so2_data['Unidade'].unique())
print('mp25_data: ', mp25_data['Unidade'].unique())
print('pts_data: ', pts_data['Unidade'].unique())
print('fmc_data: ', fmc_data['Unidade'].unique())
print('no_data: ', no_data['Unidade'].unique())
print('pm10_data: ', pm10_data['Unidade'].unique())


print(data.columns)

