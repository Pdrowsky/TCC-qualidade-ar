import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
import seaborn as sns

# Configurações
MK_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_testes_mannkendall"
OUTPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\graphtable_analise_tendencias"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Processando dados para análise de tendências...")

def formatar_para_excel_ptbr(df):
    """Formata o DataFrame para compatibilidade com Excel PT-BR"""
    # Converter colunas float para string com vírgula decimal
    for col in df.select_dtypes(include=['float']).columns:
        df[col] = df[col].apply(
            lambda x: f"{x:.6f}".replace('.', ',') if pd.notna(x) else ''
        )
    return df

# Para cada arquivo de tendência
for file_name in os.listdir(MK_DIR):
    if not file_name.startswith('mk_') or not file_name.endswith('.parquet'):
        continue
    
    poluente = file_name.split('_')[1].split('.')[0]
    file_path = os.path.join(MK_DIR, file_name)
    mk_df = pd.read_parquet(file_path)
    
    # Converter coordenadas para float
    for col in ['Latitude', 'Longitude']:
        if mk_df[col].dtype == object:
            mk_df[col] = mk_df[col].str.replace(',', '.').astype(float)
    
    # Filtrar tendências significativas (p < 0.05)
    mk_df_significativo = mk_df[mk_df['p_valor'] < 0.05].copy()

    # Calcular magnitude absoluta da tendência
    mk_df_significativo['magnitude_absoluta'] = mk_df_significativo['slope'].abs()
    
    # Classificar tendências
    mk_df_significativo['Direcao_Tendencia'] = np.where(
        mk_df_significativo['slope'] > 0, 'Aumento', 'Redução'
    )

    # Ordenar tabela completa pela magnitude absoluta (decrescente)
    mk_df_significativo = mk_df_significativo.sort_values(
        'magnitude_absoluta', 
        ascending=False
    )
    
    # Salvar tabela completa ordenada
    mk_df_significativo.to_csv(
        os.path.join(OUTPUT_DIR, f"tendencias_significativas_{poluente}.csv"),
        index=False,
        encoding='utf-8-sig'
    )

    # Top 10 estações com maior magnitude de tendência (independente da direção)
    top_magnitude = mk_df_significativo.sort_values(
        'magnitude_absoluta', 
        ascending=False
    ).head(10)
    
    formatar_para_excel_ptbr(top_magnitude).to_csv(
        os.path.join(OUTPUT_DIR, f"top_magnitude_{poluente}.csv"),
        index=False,
        encoding='utf-8-sig',
        sep=';'
    )
    
    # Top 10 estações com maior aumento
    top_aumento = mk_df_significativo[mk_df_significativo['slope'] > 0].sort_values(
        'slope', ascending=False
    ).head(10)
    
    top_aumento.to_csv(
        os.path.join(OUTPUT_DIR, f"top_aumento_{poluente}.csv"),
        index=False,
        encoding='utf-8-sig'
    )
    
    # Top 10 estações com maior redução
    top_reducao = mk_df_significativo[mk_df_significativo['slope'] < 0].sort_values(
        'slope'
    ).head(10)
    
    top_reducao.to_csv(
        os.path.join(OUTPUT_DIR, f"top_reducao_{poluente}.csv"),
        index=False,
        encoding='utf-8-sig'
    )
    
    # Gráfico de distribuição das tendências
    plt.figure(figsize=(10, 6))
    sns.histplot(
        data=mk_df_significativo,
        x='slope',
        hue='Direcao_Tendencia',
        kde=True,
        palette={'Aumento': 'red', 'Redução': 'blue'},
        element='step'
    )
    plt.title(f'Distribuição das Tendências Significativas - {poluente}')
    plt.xlabel('Magnitude da Tendência (slope)')
    plt.ylabel('Número de Estações')
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f"distribuicao_tendencias_{poluente}.png"), dpi=200)
    plt.close()

print("Análise de tendências concluída! Resultados salvos em:", OUTPUT_DIR)