import os
import pandas as pd
import numpy as np

DATA_DIR = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_violacoes_completo'
OUTPUT_DIR = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_sincronicidade'

# Equação de haversine
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # raio da Terra em km
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    d_phi = np.radians(lat2 - lat1)
    d_lambda = np.radians(lon2 - lon1)

    a = np.sin(d_phi/2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(d_lambda/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c  # distância em km

# função para calcular SC
def calcular_SC(df_violation, delta_t_hours=24, limiar_f=0.5, max_dist_km=1000, step_km=10):
    result_list = []

    unique_dates = df_violation['Date'].dt.floor('H').unique()  # datas de referência

    for ref_date in unique_dates:
        janela_inicio = ref_date - pd.Timedelta(hours=delta_t_hours)
        janela_fim = ref_date + pd.Timedelta(hours=delta_t_hours)

        # Entradas válidas na janela
        df_janela = df_violation[(df_violation['Date'] >= janela_inicio) &
                                 (df_violation['Date'] <= janela_fim)]

        for idx, ref_row in df_violation[df_violation['Date'] == ref_date].iterrows():
            ref_lat = ref_row['Latitude']
            ref_lon = ref_row['Longitude']
            ref_estacao = ref_row['Estacao']
            ref_estado = ref_row['Estado']

            # Estações diferentes da referência
            df_outras = df_janela[df_janela['Estacao'] != ref_estacao]

            if df_outras.empty:
                result_list.append((ref_estacao, ref_date, 0))
                continue

            # Calcular distâncias
            df_outras = df_outras.copy()
            df_outras['dist_km'] = df_outras.apply(
                lambda row: haversine(ref_lat, ref_lon, row['Latitude'], row['Longitude']), axis=1)

            SC = 0

            # Testar distâncias crescentes
            for d in range(step_km, max_dist_km + step_km, step_km):
                dentro_raio = df_outras[df_outras['dist_km'] <= d]
                total_est = dentro_raio['Estacao'].nunique()
                if total_est == 0:
                    continue

                violacoes = dentro_raio['Estacao'].nunique()  # todas são violações

                f_d = violacoes / total_est

                if f_d > limiar_f:
                    SC = d
                    break

            result_list.append((ref_estacao, ref_date, ref_lat, ref_lon, SC, ref_estado))

    return pd.DataFrame(result_list, columns=['Estacao', 'Data', 'Latitude', 'Longitude', 'SC_km', 'Estado'])


# calculando para cada arquivo de poluentes
for file_name in os.listdir(DATA_DIR):
    if not file_name.endswith('.parquet'):
        continue
    
    poluente = file_name.split('.')[0].split('_')[0]
    file_path = os.path.join(DATA_DIR, file_name)
    df = pd.read_parquet(file_path)

    # Conversões de tipo
    df['Latitude'] = df['Latitude'].str.replace(',', '.').astype(float)
    df['Longitude'] = df['Longitude'].str.replace(',', '.').astype(float)
    df['Date'] = pd.to_datetime(df['Date'])

    for col in df.columns:
        if col.startswith('exceed_'):
            df_violation = df[df[col] == True]
            df_resultado_sincronia = calcular_SC(df_violation)

            # save dataframe as parquet on output directory
            output_file = os.path.join(OUTPUT_DIR, f'sincronicidade_{poluente}_{col.split('_')[1]}.parquet')
            df_resultado_sincronia.to_parquet(output_file, index=False)
            print(f'Sincronicidade calculada e salva para {poluente} - {col.split("_")[1]} em {output_file}')





