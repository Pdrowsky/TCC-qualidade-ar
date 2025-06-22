import pandas as pd
import os

# Carregar dados de localização
lat_lon_path = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\support_data\Mapa de estações de monitoramento_data.csv'
lat_lon_data = pd.read_csv(lat_lon_path, sep=';', encoding='utf-8')
lat_lon_data = lat_lon_data[['Estacao1', 'Latitude', 'Longitude']]

lat_lon_data['Latitude'] = lat_lon_data['Latitude'].str.replace(',', '.').astype(float)
lat_lon_data['Longitude'] = lat_lon_data['Longitude'].str.replace(',', '.').astype(float)

# data path
INPUT_DIR = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\data_poluentes_parquet'
OUTPUT_DIR = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\resultados_poluentes_parquet_loc'

# Processar cada arquivo de poluente
for file_name in os.listdir(INPUT_DIR):
    if not file_name.endswith('.parquet'):
        continue
    
    file_path = os.path.join(INPUT_DIR, file_name)
    data = pd.read_parquet(file_path)

    data = data.merge(
        lat_lon_data.rename(columns={'Estacao1': 'Estacao'}),
        on='Estacao',
        how='left'
    )

    save_path = os.path.join(OUTPUT_DIR, str(file_name.split('.')[0]))

    data.to_parquet(f'{save_path}.parquet')