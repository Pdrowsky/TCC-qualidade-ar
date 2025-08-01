import os
import pandas as pd
import polars as pl
import numpy as np
from datetime import timedelta

def extrair_datas_unicas(df):
    """Retorna um DataFrame com os valores únicos da coluna 'Data', ordenados."""
    datas_unicas = df['Data'].unique()
    datas_ordenadas = sorted(datas_unicas)
    return pd.DataFrame(datas_ordenadas, columns=['Data'])

def aggregate_data(df, periodo):
    # converter para numerico
    df['Valor_Padronizado'] = pd.to_numeric(df['Valor_Padronizado'], errors='coerce')
    
    # MODIFICADO: Incluir 'Estado' em todos os agrupamentos
    if periodo == '24h':
        df['Date'] = df['Data_Hora'].dt.date
        aggregated = df.groupby(['Estado', 'Estacao', 'Date']).agg(  # Adicionado 'Estado'
            Valor_Padronizado=('Valor_Padronizado', 'max'),
            Latitude=('Latitude', 'first'),
            Longitude=('Longitude', 'first')
        ).reset_index()
        
    elif periodo == 'med. arit. anual':
        df['Year'] = df['Data_Hora'].dt.year
        aggregated = df.groupby(['Estado', 'Estacao', 'Year']).agg(  # Adicionado 'Estado'
            Valor_Padronizado=('Valor_Padronizado', 'mean'),
            Latitude=('Latitude', 'first'),
            Longitude=('Longitude', 'first')
        ).reset_index()
        
    elif periodo == 'max. med. hor. do dia (1h)':
        df['Date'] = df['Data_Hora'].dt.date
        aggregated = df.groupby(['Estado', 'Estacao', 'Date', 'Hora']).agg(  # Adicionado 'Estado'
            Valor_Padronizado=('Valor_Padronizado', 'mean'),
            Latitude=('Latitude', 'first'),
            Longitude=('Longitude', 'first')
        ).reset_index()
        aggregated = aggregated.groupby(['Estado', 'Estacao', 'Date']).agg(  # Adicionado 'Estado'
            Valor_Padronizado=('Valor_Padronizado', 'max'),
            Latitude=('Latitude', 'first'),
            Longitude=('Longitude', 'first')
        ).reset_index()
        
    elif periodo == 'max. med. mov. do dia (8h)':
        df = df.sort_values(['Estado', 'Estacao', 'Data_Hora'])  # Adicionado 'Estado'
        df['rolling_8h'] = (
            df.groupby(['Estado', 'Estacao'])['Valor_Padronizado']  # Adicionado 'Estado'
            .rolling(8, min_periods=1).mean()
            .reset_index(level=[0,1], drop=True))
        df['Date'] = df['Data_Hora'].dt.date
        aggregated = df.groupby(['Estado', 'Estacao', 'Date']).agg(  # Adicionado 'Estado'
            Valor_Padronizado=('rolling_8h', 'max'),
            Latitude=('Latitude', 'first'),
            Longitude=('Longitude', 'first')
        ).reset_index()
        
    elif periodo == 'med. geom. anual':
        df['Year'] = df['Data_Hora'].dt.year
        aggregated = df.groupby(['Estado', 'Estacao', 'Year']).agg(  # Adicionado 'Estado'
            Valor_Padronizado=('Valor_Padronizado', lambda x: np.exp(np.mean(np.log(x)))),
            Latitude=('Latitude', 'first'),
            Longitude=('Longitude', 'first')
        ).reset_index()
        
    else:
        raise ValueError(f"Unsupported period: {periodo}")
    
    return aggregated

def corrigir_data_hora(df):
    df = df.copy()

    # Step 1: Create boolean mask for '24:00:00'
    mask_24h = df['Hora'] == '24:00:00'

    # Step 2: Prepare corrected date column
    df['Data_corrigida'] = pd.to_datetime(df['Data'], errors='coerce')
    df.loc[mask_24h, 'Data_corrigida'] += timedelta(days=1)

    # Step 3: Replace "24:00:00" with "00:00:00"
    df['Hora_corrigida'] = df['Hora'].where(~mask_24h, '00:00:00')

    # Step 4: Combine and convert
    df['Data_Hora'] = pd.to_datetime(
        df['Data_corrigida'].astype(str) + ' ' + df['Hora_corrigida'],
        errors='coerce'
    )

    return df

path_dados = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_chunks_com_loc'
path_data_funcionamento = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\data_funcionamento.csv'
path_limite_conama = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\limites_conama_506.csv'

data_funcionamento = pl.read_csv(path_data_funcionamento, separator=',')
limite_conama = pl.read_csv(path_limite_conama, separator=';')

def run_all(poluente, limite_conama, data_funcionamento):
    # Obter todas as estações únicas para o poluente
    pol_dir = os.path.join(path_dados, poluente)
    all_files = [os.path.join(pol_dir, f) for f in os.listdir(pol_dir) if f.endswith('.parquet')]

    poluente = poluente.split('_')[0]
    if poluente == 'MP2.5':
        poluente = 'MP2,5'
    
    # MODIFICADO: Incluir 'Estado' na seleção de colunas
    station_query = pl.scan_parquet(all_files).select(['Estado', 'Estacao']).unique().collect()
    stations = station_query[['Estado', 'Estacao']].unique().to_dicts()
    
    print(f'Encontradas {len(stations)} estações para {poluente}')
    
    # Preparar DataFrames auxiliares
    limite_conama_pd = limite_conama.to_pandas()
    for col in ['PI-1', 'PI-2', 'PI-3', 'PI-4', 'PF']:
        limite_conama_pd[col] = pd.to_numeric(limite_conama_pd[col], errors='coerce')
    
    all_results = []
    
    # Processar cada estação individualmente
    for i, station_info in enumerate(stations):
        estado = station_info['Estado']
        station = station_info['Estacao']
        print(f'Processando estação {i+1}/{len(stations)}: {estado} - {station}')
        
        # MODIFICADO: Filtrar por estado e estação
        station_dados = pl.concat([
            pl.scan_parquet(file)
            .filter((pl.col('Estado') == estado) & (pl.col('Estacao') == station))
            .collect()
            for file in all_files
        ]).unique()
        
        if station_dados.is_empty():
            print(f"  Sem dados para estação {station} em {estado}")
            continue
        
        # Converter para pandas e processar
        station_df = station_dados.to_pandas()
        station_df = corrigir_data_hora(station_df)
        
        # Verificar se temos o poluente necessário
        if station_df.empty:
            print(f"  Sem dados do poluente {poluente} para estação {station} em {estado}")
            continue
            
        # Obter os períodos aplicáveis
        limites_pol = limite_conama_pd[limite_conama_pd['Sigla'] == poluente]
        station_results = []
        
        for _, row in limites_pol.iterrows():
            periodo = row['Periodo']
            try:
                aggregated = aggregate_data(station_df, periodo)
            except ValueError as e:
                print(f"  Skipping {estado} - {station} ({periodo}): {e}")
                continue
            
            # Adicionar limites e excedências
            for limit_col in ['PI-1', 'PI-2', 'PI-3', 'PI-4', 'PF']:
                aggregated[limit_col] = row[limit_col]
                aggregated[f'exceed_{limit_col}'] = aggregated['Valor_Padronizado'] > aggregated[limit_col]
            
            aggregated['Poluente'] = poluente
            aggregated['Periodo'] = periodo
            station_results.append(aggregated)
        
        if station_results:
            station_final = pd.concat(station_results, ignore_index=True)
            all_results.append(station_final)
    
    if not all_results:
        print(f"Nenhum resultado para {poluente}")
        return
    
    final_results = pd.concat(all_results, ignore_index=True)
    
    # Salvar resultados
    output_dir = os.path.join(r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_violacoes_completo')
    os.makedirs(output_dir, exist_ok=True)
    
    # Salvar combinado
    combined_path = os.path.join(output_dir, f'{poluente}_combinado.parquet')
    final_results.to_parquet(combined_path)
    print(f'Resultado combinado salvo: {combined_path}')

# Obter lista de poluentes (subdiretórios)
poluentes_de_interesse = [d for d in os.listdir(path_dados) 
                         if os.path.isdir(os.path.join(path_dados, d))]

for poluente in poluentes_de_interesse:
    print(f"\nIniciando processamento para {poluente}")
    run_all(poluente, limite_conama, data_funcionamento)
    print(f"Concluído processamento para {poluente}")