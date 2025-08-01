import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd
import matplotlib as mpl
from shapely.geometry import Point

# Configurações
DATA_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_violacoes_completo"
OUTPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\graphtable_frequencia_violacoes"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# URLs para shapefiles
STATES_URL = r"https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_1_states_provinces.zip"
MUNICIPIOS_URL = r"https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/BR_Municipios_2022.zip"

print("Carregando shapefiles...")
# Carregar estados brasileiros
states = gpd.read_file(STATES_URL)
brazil_states = states[states['admin'] == 'Brazil'].to_crs(epsg=3857)

# Carregar municípios brasileiros
municipios = gpd.read_file(MUNICIPIOS_URL).to_crs(epsg=3857)
print(f"Municípios carregados: {len(municipios)}")

print("Processando dados para análise de frequência de violações...")

# Listas para acumular dados de todos os poluentes
todos_resultados_gerais = []
todos_resultados_estacoes = []

# Para cada arquivo (cada poluente)
for file_name in os.listdir(DATA_DIR):
    if not file_name.endswith('.parquet'):
        continue
    
    file_path = os.path.join(DATA_DIR, file_name)
    df = pd.read_parquet(file_path)
    poluente = df['Poluente'].iloc[0] if 'Poluente' in df.columns else file_name.split('_')[0]

    for coord_col in ['Longitude', 'Latitude']:
        if coord_col in df.columns:
            # Converter para string, substituir vírgula por ponto, depois para float
            df[coord_col] = df[coord_col].astype(str).str.replace(',', '.').astype(float)
    
    # Identificar padrões
    padroes = [col for col in df.columns if col.startswith('exceed_')]
    
    # Tabela para armazenar resultados
    resultados_gerais = []
    resultados_estacoes = []
    
    for padrao in padroes:
        # Calcular total de medições e violações
        total_medicoes = len(df)
        total_violacoes = df[padrao].sum()
        taxa = total_violacoes / total_medicoes * 100
        
        # Armazenar resultados gerais
        resultados_gerais.append({
            'Poluente': poluente,
            'Padrao': padrao.split('_')[-1],
            'Total_Medicoes': total_medicoes,
            'Total_Violacoes': total_violacoes,
            'Taxa_Violacao_Perc': taxa
        })
        
        # Calcular por estação
        agg = df.groupby(['Estacao', 'Latitude', 'Longitude']).agg(
            total_violacoes=(padrao, 'sum'),
            total_medicoes=(padrao, 'count')
        ).reset_index()
        
        agg['taxa_violacao'] = agg['total_violacoes'] / agg['total_medicoes'] * 100
        agg['Poluente'] = poluente
        agg['Padrao'] = padrao.split('_')[-1]
        
        resultados_estacoes.append(agg)
    
    # Salvar tabelas por poluente
    df_geral = pd.DataFrame(resultados_gerais)
    df_geral.to_csv(
        os.path.join(OUTPUT_DIR, f"frequencia_geral_{poluente}.csv"),
        index=False,
        encoding='utf-8-sig'
    )
    
    df_estacoes = pd.concat(resultados_estacoes)
    df_estacoes.to_csv(
        os.path.join(OUTPUT_DIR, f"frequencia_estacoes_{poluente}.csv"),
        index=False,
        encoding='utf-8-sig'
    )
    
    # Adicionar aos acumuladores
    todos_resultados_gerais.append(df_geral)
    todos_resultados_estacoes.append(df_estacoes)
    
    # Gráfico de barras para taxas de violação por padrão (individual por poluente)
    plt.figure(figsize=(10, 6))
    plt.bar(df_geral['Padrao'], df_geral['Taxa_Violacao_Perc'], color='#ff6b6b')
    plt.title(f'Taxa de Violação por Padrão - {poluente}')
    plt.xlabel('Padrão de Qualidade')
    plt.ylabel('Taxa de Violação (%)')
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f"taxa_padroes_{poluente}.png"), dpi=200)
    plt.close()
    
    # Top 10 estações com maior taxa de violação
    for padrao in df_estacoes['Padrao'].unique():
        df_padrao = df_estacoes[df_estacoes['Padrao'] == padrao]
        top_estacoes = df_padrao.sort_values('taxa_violacao', ascending=False).head(10)
        
        plt.figure(figsize=(12, 6))
        plt.barh(
            top_estacoes['Estacao'], 
            top_estacoes['taxa_violacao'],
            color='#4e79a7'
        )
        plt.title(f'Top 10 Estações - {poluente} (Padrão: {padrao})')
        plt.xlabel('Taxa de Violação (%)')
        plt.gca().invert_yaxis()
        plt.grid(axis='x', alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, f"top_estacoes_{poluente}_{padrao}.png"), dpi=200)
        plt.close()

# ========================================================================================
# NOVAS VISUALIZAÇÕES
# ========================================================================================

# Combinar dados de todos os poluentes
df_geral_todos = pd.concat(todos_resultados_gerais, ignore_index=True)
df_estacoes_todos = pd.concat(todos_resultados_estacoes, ignore_index=True)

# Converter para GeoDataFrame
df_estacoes_todos['geometry'] = df_estacoes_todos.apply(
    lambda row: Point(row['Longitude'], row['Latitude']), 
    axis=1
)
gdf_estacoes = gpd.GeoDataFrame(
    df_estacoes_todos, 
    geometry='geometry',
    crs="EPSG:4326"
).to_crs(epsg=3857)

# Paleta de cores para mapas
cores = ["#2c7bb6", "#abd9e9", "#ffffbf", "#fdae61", "#d7191c"]
cmap = mpl.colors.LinearSegmentedColormap.from_list("custom_diverging", cores)

# ========================================================================================
# 1. FIGURA COM SUBPLOTS: MAPAS DE TAXA DE VIOLAÇÃO POR PADRÃO
# ========================================================================================
print("Gerando mapas de taxa de violação por padrão...")

# Obter lista de padrões únicos
padroes = df_geral_todos['Padrao'].unique()
n_padroes = len(padroes)

# Configurar figura
ncols = min(3, n_padroes)
nrows = int(np.ceil(n_padroes / ncols))
fig, axs = plt.subplots(nrows, ncols, figsize=(20, 6 * nrows))
axs = axs.flatten() if nrows > 1 or ncols > 1 else [axs]

# Para cada padrão
for i, padrao in enumerate(padroes):
    ax = axs[i]
    
    # Filtrar dados para o padrão
    df_padrao = gdf_estacoes[gdf_estacoes['Padrao'] == padrao]
    
    # Plotar mapa do Brasil
    brazil_states.plot(
        ax=ax,
        color='lightgrey',
        edgecolor='darkgrey',
        linewidth=0.8
    )
    
    # Plotar municípios
    municipios.plot(
        ax=ax,
        color='none',
        edgecolor='lightgrey',
        linewidth=0.2,
        alpha=0.3
    )
    
    # Normalização para a escala de cores
    vmin = df_padrao['taxa_violacao'].quantile(0.05)
    vmax = df_padrao['taxa_violacao'].quantile(0.95)
    norm = mpl.colors.Normalize(vmin=vmin, vmax=vmax)
    
    # Plotar estações
    scatter = ax.scatter(
        df_padrao.geometry.x,
        df_padrao.geometry.y,
        c=df_padrao['taxa_violacao'],
        cmap=cmap,
        norm=norm,
        s=30,
        alpha=0.9
    )
    
    # Adicionar barra de cores
    cbar = fig.colorbar(scatter, ax=ax, shrink=0.7)
    cbar.set_label('Taxa de Violação (%)', fontsize=10)
    
    # Configurações do gráfico
    ax.set_title(f'Taxa de Violação - Padrão {padrao}', fontsize=14)
    ax.tick_params(axis='both', labelsize=12)
    ax.set_axis_off()
    
    # Adicionar contador de estações
    ax.annotate(
        f"Estações: {len(df_padrao)}", 
        xy=(0.02, 0.02),
        xycoords='axes fraction',
        fontsize=10,
        bbox=dict(boxstyle='round,pad=0.3', fc='white', alpha=0.7)
    )

# Ocultar eixos extras
for j in range(len(padroes), len(axs)):
    axs[j].axis('off')

# Ajustar layout e salvar
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "mapas_taxa_violacao_por_padrao.png"), dpi=300, bbox_inches='tight')
plt.close()
print("Mapas de taxa de violação por padrão salvos!")

# ========================================================================================
# 2. FIGURA COM SUBPLOTS: GRÁFICOS DE BARRAS COMBINADOS POR PADRÃO
# ========================================================================================
print("Gerando gráficos de barras combinados por padrão...")

# Configurar figura
ncols = min(3, n_padroes)
nrows = int(np.ceil(n_padroes / ncols))
fig, axs = plt.subplots(nrows, ncols, figsize=(20, 6 * nrows))
axs = axs.flatten() if nrows > 1 or ncols > 1 else [axs]

# Para cada padrão
for i, padrao in enumerate(padroes):
    ax = axs[i]
    
    # Filtrar dados para o padrão
    df_padrao = df_geral_todos[df_geral_todos['Padrao'] == padrao]
    
    # Ordenar poluentes por taxa de violação
    df_padrao = df_padrao.sort_values('Taxa_Violacao_Perc', ascending=False)
    
    # Plot de barras
    sns.barplot(
        data=df_padrao,
        x='Poluente',
        y='Taxa_Violacao_Perc',
        ax=ax,
        palette="viridis"
    )
    
    # Configurações do gráfico
    ax.set_title(f'Taxa de Violação - Padrão {padrao}', fontsize=20)
    ax.set_xlabel("Poluente", fontsize=18)
    ax.set_ylabel("Taxa de Violação (%)", fontsize=18)
    ax.tick_params(axis='x', rotation=45)
    ax.grid(axis='y', alpha=0.3)
    ax.tick_params(axis='both', labelsize=18)
    
    # FIXAR ESCALA DE 0 A 100%
    ax.set_ylim(0, 100)
    
    # Adicionar valores nas barras
    for p in ax.patches:
        height = p.get_height()
        ax.annotate(
            f"{height:.1f}%",
            (p.get_x() + p.get_width() / 2., height),
            ha='center', va='center',
            xytext=(0, 5),
            textcoords='offset points',
            fontsize=9
        )

# Ocultar eixos extras
for j in range(len(padroes), len(axs)):
    axs[j].axis('off')

# Ajustar layout e salvar
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "barras_taxa_violacao_por_padrao.png"), dpi=300, bbox_inches='tight')
plt.close()
print("Gráficos de barras combinados por padrão salvos!")

print("Análise de frequência de violações concluída! Resultados salvos em:", OUTPUT_DIR)