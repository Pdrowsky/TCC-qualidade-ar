import os
import pandas as pd
import numpy as np

raw_data_path = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\raw_data'

# Extrair todos os dados
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

# Concatenar tudo em um único DataFrame
data = pd.DataFrame()
for state, records in data_dict.items():
    records['state'] = state
    data = pd.concat([data, records], ignore_index=True)

# Preencher coluna 'Data' onde for nula com 'ï»¿Data'
if 'ï»¿Data' in data.columns:
    data['Data'] = data['Data'].fillna(data['ï»¿Data'])
    data = data.drop(columns=['ï»¿Data'])

# Unificar PM10 e MP10
data['Poluente'] = data['Poluente'].replace({
    'PM10': 'MP10', 
    'pm10': 'MP10', 
    'Pm10': 'MP10',
    'MP2.5': 'MP2.5'
})

# Padronizar coluna de unidade
unidades_corretas = {
    'ug/m3': 'µg/m³',
    'µg/m3': 'µg/m³',
    'µg/m³': 'µg/m³',
    'Âµg/mÂ³': 'µg/m³',
    'ppm': 'ppm',
    'ppb': 'ppb'
}
data['Unidade'] = data['Unidade'].map(unidades_corretas).fillna(data['Unidade'])

# Converter 'Valor' para numérico
def clean_numeric(value):
    if isinstance(value, str):
        # Remover caracteres não numéricos exceto ponto, vírgula e sinal negativo
        cleaned = ''.join(c for c in value if c in '0123456789.,-')
        # Substituir vírgula por ponto
        cleaned = cleaned.replace(',', '.')
        # Remover múltiplos pontos (caso haja)
        if cleaned.count('.') > 1:
            parts = cleaned.split('.')
            cleaned = parts[0] + '.' + ''.join(parts[1:])
        return cleaned
    return value

data['Valor'] = data['Valor'].apply(clean_numeric)
data['Valor'] = pd.to_numeric(data['Valor'], errors='coerce')

# Massas molares
massa_molar = {
    'NO2': 46.0055,
    'O3': 48.00,
    'SO2': 64.066,
    'CO': 28.01,
    'NO': 30.01
}
PPM_CONVERSION = 24.45  # 25°C e 1 atm

# Inicializar colunas padronizadas
data['Valor_Padronizado'] = np.nan
data['Unidade_Padronizada'] = 'µg/m³'

# Unidade para CO deve ser ppm
co_mask = data['Poluente'] == 'CO'
data.loc[co_mask, 'Unidade_Padronizada'] = 'ppm'

# Conversão para CO
co_ppm_mask = co_mask & (data['Unidade'] == 'ppm')
co_ppb_mask = co_mask & (data['Unidade'] == 'ppb')
co_ugm3_mask = co_mask & (data['Unidade'] == 'µg/m³')

data.loc[co_ppm_mask, 'Valor_Padronizado'] = data.loc[co_ppm_mask, 'Valor']
data.loc[co_ppb_mask, 'Valor_Padronizado'] = data.loc[co_ppb_mask, 'Valor'] / 1000

# Converter CO de µg/m³ para ppm
if co_ugm3_mask.any():
    data.loc[co_ugm3_mask, 'Valor_Padronizado'] = (
        data.loc[co_ugm3_mask, 'Valor'] * PPM_CONVERSION / (massa_molar['CO'] * 1000)
    )

# Outros poluentes (todos convertidos para µg/m³)
other_mask = ~co_mask
other_ppm_mask = other_mask & (data['Unidade'] == 'ppm')
other_ppb_mask = other_mask & (data['Unidade'] == 'ppb')
other_ugm3_mask = other_mask & (data['Unidade'] == 'µg/m³')

# Se unidade já é µg/m³, manter o valor
data.loc[other_ugm3_mask, 'Valor_Padronizado'] = data.loc[other_ugm3_mask, 'Valor']

# Converter de ppm para µg/m³
for poluente, mm in massa_molar.items():
    if poluente == 'CO': 
        continue
        
    mask = other_ppm_mask & (data['Poluente'] == poluente)
    data.loc[mask, 'Valor_Padronizado'] = (data.loc[mask, 'Valor'] * mm * 1000) / PPM_CONVERSION

# Converter de ppb para µg/m³
for poluente, mm in massa_molar.items():
    if poluente == 'CO': 
        continue
        
    mask = other_ppb_mask & (data['Poluente'] == poluente)
    data.loc[mask, 'Valor_Padronizado'] = (data.loc[mask, 'Valor'] * mm) / PPM_CONVERSION

# Tratar partículas (MP10, MP2.5) - sem conversão necessária
for particula in ['MP10', 'MP2.5']:
    part_mask = (data['Poluente'] == particula) & other_mask
    ugm3_mask = part_mask & (data['Unidade'] == 'µg/m³')
    mgm3_mask = part_mask & (data['Unidade'] == 'mg/m³')  # Se houver mg/m³
    
    data.loc[ugm3_mask, 'Valor_Padronizado'] = data.loc[ugm3_mask, 'Valor']
    
    if mgm3_mask.any():
        data.loc[mgm3_mask, 'Valor_Padronizado'] = data.loc[mgm3_mask, 'Valor'] * 1000

# Verificar nulos e reportar
print("\nRelatório de valores nulos:")
print(f"Total de registros: {len(data)}")
print(f"Valores nulos em 'Valor': {data['Valor'].isna().sum()}")
print(f"Valores nulos em 'Valor_Padronizado': {data['Valor_Padronizado'].isna().sum()}")

if data['Valor_Padronizado'].isna().sum() > 0:
    nulos = data[data['Valor_Padronizado'].isna()]
    print("\nMotivos para valores nulos em Valor_Padronizado:")
    print(nulos.groupby(['Poluente', 'Unidade']).size().reset_index(name='count'))
    
    # Salvar dados problemáticos para análise
    nulos.to_csv(os.path.join(raw_data_path, 'dados_problematicos.csv'), index=False)

# Salvar dados por poluente
parquet_path = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\data_poluentes_parquet'
os.makedirs(parquet_path, exist_ok=True)

for poluente in data['Poluente'].unique():
    pollutant_data = data[data['Poluente'] == poluente].copy()
    
    # Remover registros sem valores padronizados
    initial_count = len(pollutant_data)
    pollutant_data = pollutant_data.dropna(subset=['Valor_Padronizado'])
    final_count = len(pollutant_data)
    
    print(f"\nPoluente: {poluente}")
    print(f"  Registros antes: {initial_count}")
    print(f"  Registros após remoção de nulos: {final_count}")
    print(f"  Registros perdidos: {initial_count - final_count}")
    
    # Salvar
    pollutant_data.to_parquet(
        os.path.join(parquet_path, f"{poluente}.parquet"),
        index=False
    )