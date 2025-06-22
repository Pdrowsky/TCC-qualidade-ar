import os
import pandas as pd
import pymannkendall as mk
from tqdm import tqdm

# Configurações
INPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_chunks_com_loc"
OUTPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_testes_mannkendall"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def calculate_trend(series):
    """Executa o teste de Mann-Kendall e retorna um resumo"""
    if len(series) < 3:
        return None
    
    try:
        result = mk.original_test(series)
        return {
            'trend': result.trend,
            'h': result.h,
            'p': result.p,
            'z': result.z,
            'Tau': result.Tau,
            's': result.s,
            'slope': result.slope,
            'intercept': result.intercept
        }
    except:
        return None

print("Iniciando cálculo de tendências...")

# Processar cada subdiretório de poluente
for poluente_dir in os.listdir(INPUT_DIR):
    poluente_path = os.path.join(INPUT_DIR, poluente_dir)
    
    # Verificar se é um diretório
    if not os.path.isdir(poluente_path):
        continue
    
    print(f"\nProcessando poluente: {poluente_dir}")
    
    # Listar todos os arquivos parquet deste poluente
    all_files = []
    for file_name in os.listdir(poluente_path):
        if file_name.endswith('.parquet'):
            file_path = os.path.join(poluente_path, file_name)
            all_files.append(file_path)
    
    if not all_files:
        print(f"  Nenhum arquivo encontrado para {poluente_dir}")
        continue
    
    # Carregar e combinar todos os arquivos do poluente
    df_list = []
    for file_path in tqdm(all_files, desc=f"Carregando {poluente_dir}"):
        try:
            df_chunk = pd.read_parquet(file_path)
            df_list.append(df_chunk)
        except Exception as e:
            print(f"  Erro ao carregar {file_path}: {str(e)}")
    
    if not df_list:
        print(f"  Nenhum dado carregado para {poluente_dir}")
        continue
    
    df = pd.concat(df_list, ignore_index=True)
    
    # Converter coordenadas para float
    if df['Latitude'].dtype == 'object':
        df['Latitude'] = df['Latitude'].str.replace(',', '.').astype(float)
    if df['Longitude'].dtype == 'object':
        df['Longitude'] = df['Longitude'].str.replace(',', '.').astype(float)
    
    # Passo 1: Agregar dados diariamente (média diária)
    print(f"Aggregando dados diariamente para {poluente_dir}...")
    
    # Garantir que temos coluna de data
    if 'Data_Hora' in df.columns:
        df['Data'] = pd.to_datetime(df['Data_Hora']).dt.date
    elif 'Data' in df.columns:
        df['Data'] = pd.to_datetime(df['Data']).dt.date
    else:
        print(f"  Nenhuma coluna de data encontrada para {poluente_dir}")
        continue
    
    # Calcular média diária por estação
    daily_agg = df.groupby(['Estacao', 'Latitude', 'Longitude', 'Data'])['Valor_Padronizado'].mean().reset_index()
    
    # Passo 2: Calcular tendências usando dados diários agregados
    trends = []
    grouped = daily_agg.groupby(['Estacao', 'Latitude', 'Longitude'])
    
    print(f"Calculando tendências para {len(grouped)} estações usando dados diários...")
    for (estacao, lat, lon), group in tqdm(grouped, desc=f"Processando {poluente_dir}"):
        group = group.sort_values('Data')
        series = group['Valor_Padronizado'].values
        
        trend_result = calculate_trend(series)
        if trend_result is None:
            continue
        
        trends.append({
            'Poluente': poluente_dir,
            'Estacao': estacao,
            'Latitude': lat,
            'Longitude': lon,
            'Tendencia': trend_result['trend'],
            'p_valor': trend_result['p'],
            'slope': trend_result['slope'],
            'z': trend_result['z'],
            'Tau': trend_result['Tau'],
            'n_dias': len(series)  # Adicionado número de dias usados
        })
    
    if not trends:
        print(f"  Nenhuma tendência calculada para {poluente_dir}")
        continue
    
    # Converter para DataFrame e salvar
    trends_df = pd.DataFrame(trends)
    output_path = os.path.join(OUTPUT_DIR, f"mk_{poluente_dir}.parquet")
    trends_df.to_parquet(output_path)
    print(f"  Tendências salvas: {output_path}")
    print(f"  Estações processadas: {len(trends_df)}")
    print(f"  Dias médios por estação: {trends_df['n_dias'].mean():.1f}")

print("\nProcessamento concluído! Dados de tendência salvos em:", OUTPUT_DIR)