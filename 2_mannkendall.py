import os
import pandas as pd
import pymannkendall as mk
from tqdm import tqdm

# Configurações
INPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_chunks_com_loc"
OUTPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_testes_mannkendall_ano"
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
            # Verificar se a coluna Estado existe
            if 'Estado' not in df_chunk.columns:
                print(f"  ATENÇÃO: 'Estado' não encontrado em {file_path}")
            df_list.append(df_chunk)
        except Exception as e:
            print(f"  Erro ao carregar {file_path}: {str(e)}")
    
    if not df_list:
        print(f"  Nenhum dado carregado para {poluente_dir}")
        continue
    
    df = pd.concat(df_list, ignore_index=True)
    
    # Converter coordenadas para float
    for coord in ['Latitude', 'Longitude']:
        if coord in df.columns and df[coord].dtype == 'object':
            df[coord] = df[coord].str.replace(',', '.').astype(float)
    
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
    
    # Verificar se a coluna Estado está presente
    if 'Estado' not in df.columns:
        print(f"  ERRO CRÍTICO: Coluna 'Estado' não encontrada para {poluente_dir}")
        continue

    # 1. Converter Data para datetime e criar colunas auxiliares
    df['Data'] = pd.to_datetime(df['Data'])
    df['Ano'] = df['Data'].dt.year
    df['Mes'] = df['Data'].dt.month
    df['Dia'] = df['Data'].dt.date  # apenas parte da data, sem hora

    # 2. Etapa hora → dia: média diária se dia tiver ≥ 18 horas válidas
    df_valid_hours = (
        df.groupby(['Estado', 'Estacao', 'Latitude', 'Longitude', 'Dia'])
        .agg(
            Valor_Medio_Dia=('Valor_Padronizado', 'mean'),
            n_horas=('Valor_Padronizado', 'count')
        )
        .reset_index()
    )

    # manter apenas dias com ≥ 18 horas válidas
    df_valid_days = df_valid_hours[df_valid_hours['n_horas'] >= 18].copy()
    df_valid_days['Ano'] = pd.to_datetime(df_valid_days['Dia']).dt.year
    df_valid_days['Mes'] = pd.to_datetime(df_valid_days['Dia']).dt.month

    # 3. Etapa dia → mês: média mensal se mês tiver ≥ 20 dias válidos
    monthly_agg = (
        df_valid_days
        .groupby(['Estado', 'Estacao', 'Latitude', 'Longitude', 'Ano', 'Mes'])
        .agg(
            Valor_Padronizado=('Valor_Medio_Dia', 'mean'),
            n_dias_validos=('Dia', 'nunique')
        )
        .reset_index()
    )

    # manter apenas meses com ≥ 20 dias válidos
    monthly_agg = monthly_agg[monthly_agg['n_dias_validos'] >= 20].copy()

    
    # Passo 2: Calcular tendências usando dados diários agregados
    trends = []
    # Agrupar por Estado + Estação (MODIFICADO)
    grouped = monthly_agg.groupby(['Estado', 'Estacao', 'Latitude', 'Longitude'])
    
    print(f"Calculando tendências para {len(grouped)} estações usando dados diários...")
    for (estado, estacao, lat, lon), group in tqdm(grouped, desc=f"Processando {poluente_dir}"):
        group = group.sort_values(['Ano', 'Mes'])
        series = group['Valor_Padronizado'].values
        
        trend_result = calculate_trend(series)
        if trend_result is None:
            continue
        
        trends.append({
            'Poluente': poluente_dir,
            'Estado': estado,  # NOVO CAMPO ADICIONADO
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
    print(f"  Estados representados: {trends_df['Estado'].nunique()}")  # NOVA ESTATÍSTICA
    print(f"  Dias médios por estação: {trends_df['n_dias'].mean():.1f}")

print("\nProcessamento concluído! Dados de tendência salvos em:", OUTPUT_DIR)