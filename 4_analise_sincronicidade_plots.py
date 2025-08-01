import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib as mpl
import geopandas as gpd
from shapely.geometry import Point
import math

# ========================================================================================
# CONFIGURAÇÕES INICIAIS
# ========================================================================================
SC_DIR = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_sincronicidade'
OUTPUT_DIR = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\graphtable_sincronicidade'
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Carregando dados de sincronicidade...")

# ========================================================================================
# CARREGAR E PREPARAR OS DADOS
# ========================================================================================
# Lista para armazenar todos os DataFrames
dfs = []

# Carregar todos os arquivos de sincronicidade
for file in os.listdir(SC_DIR):
    if file.startswith('sincronicidade_') and file.endswith('.parquet'):
        parts = file.split('_')
        poluente = parts[1]
        parametro = parts[2].split('.')[0]  # remover a extensão
        
        file_path = os.path.join(SC_DIR, file)
        df = pd.read_parquet(file_path)
        df['Poluente'] = poluente
        df['Parametro'] = parametro
        dfs.append(df)

# Combinar todos os DataFrames
df_sincronia = pd.concat(dfs, ignore_index=True)

# Verificar se a coluna 'Estado' existe
if 'Estado' not in df_sincronia.columns:
    raise ValueError("Coluna 'Estado' não encontrada no DataFrame. Verifique os dados de entrada.")

# ========================================================================================
# CARREGAR SHAPEFILES
# ========================================================================================
print("Carregando shapefiles...")

# URLs para shapefiles
STATES_URL = r"https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_1_states_provinces.zip"
MUNICIPIOS_URL = r"https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/BR_Municipios_2022.zip"

# Carregar estados brasileiros
states = gpd.read_file(STATES_URL)
brazil_states = states[states['admin'] == 'Brazil'].to_crs(epsg=3857)
brazil_states.rename(columns={'postal': 'Estado'}, inplace=True)  # Renomear coluna para facilitar junção

# Carregar municípios brasileiros
municipios = gpd.read_file(MUNICIPIOS_URL).to_crs(epsg=3857)
print(f"Municípios carregados: {len(municipios)}")

# Calcular média de SC_km por estação e poluente
media_por_estacao = df_sincronia.groupby(['Estacao', 'Latitude', 'Longitude', 'Estado', 'Poluente'])['SC_km'].mean().reset_index()

# Converter estações para GeoDataFrame com CRS 3857
gdf_estacoes = gpd.GeoDataFrame(
    media_por_estacao,
    geometry=gpd.points_from_xy(media_por_estacao.Longitude, media_por_estacao.Latitude),
    crs="EPSG:4326"
).to_crs(epsg=3857)

# Lista de poluentes únicos
poluentes = media_por_estacao['Poluente'].unique()

# Paleta de cores para mapas
cores = ["#ffee00", "#ffae00", "#ff7300", "#ff3300", "#920002"]
cmap = mpl.colors.LinearSegmentedColormap.from_list("custom_diverging", cores)

# ========================================================================================
# 1. FIGURA GERAL: MÉDIA DE SINCRONICIDADE POR ESTADO (SUBPLOTS POR POLUENTE)
# ========================================================================================
print("Gerando gráficos de média por estado...")

# Calcular média de SC_km por estado e poluente
media_por_estado = df_sincronia.groupby(['Estado', 'Poluente'])['SC_km'].mean().reset_index()

# Configurar figura
ncols = 2
nrows = int(np.ceil(len(poluentes) / ncols))
fig, axs = plt.subplots(nrows, ncols, figsize=(20, 6 * nrows))
axs = axs.flatten() if nrows > 1 or ncols > 1 else [axs]

# Para cada poluente
for i, pol in enumerate(poluentes):
    ax = axs[i]
    df_pol = media_por_estado[media_por_estado['Poluente'] == pol]
    
    # Ordenar estados por média de SC_km
    ordem_estados = df_pol.sort_values('SC_km', ascending=False)['Estado'].unique()
    
    # Plot de barras
    barplot = sns.barplot(
        data=df_pol,
        x='Estado',
        y='SC_km',
        order=ordem_estados,
        ax=ax,
        palette="viridis"
    )
    
    # Configurações do gráfico
    ax.set_title(f"Média de Sincronicidade - {pol}", fontsize=16)
    ax.set_xlabel("Estado", fontsize=12)
    ax.set_ylabel("SC_km (média)", fontsize=12)
    ax.set_ylim(0, 600)
    ax.tick_params(axis='x', rotation=45)
    ax.grid(axis='y', alpha=0.3)
    
    # Adicionar valores nas barras
    for p in barplot.patches:
        height = p.get_height()
        ax.annotate(
            f"{height:.1f}",
            (p.get_x() + p.get_width() / 2., height),
            ha='center', va='center',
            xytext=(0, 5),
            textcoords='offset points',
            fontsize=9
        )

# Ocultar eixos extras
for j in range(len(poluentes), len(axs)):
    axs[j].axis('off')

# Ajustar layout e salvar
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "media_sincronicidade_por_estado.png"), dpi=300, bbox_inches='tight')
plt.close()
print("Gráficos de média por estado salvos!")

# ========================================================================================
# 2. BOXPLOT DE SINCRONICIDADE POR ESTADO (TODOS POLUENTES EM UMA FIGURA)
# ========================================================================================
print("Gerando boxplots combinados por estado...")

# Configurar figura principal
ncols = 2
nrows = int(np.ceil(len(poluentes) / ncols))
fig, axs = plt.subplots(nrows, ncols, figsize=(20, 10 * nrows))
axs = axs.flatten() if nrows > 1 or ncols > 1 else [axs]

# Para cada poluente
for i, pol in enumerate(poluentes):
    ax = axs[i]
    df_pol = df_sincronia[df_sincronia['Poluente'] == pol]
    
    # Ordenar estados por ordem alfabética
    ordem_estados = df_pol['Estado'].value_counts().index.tolist()
    ordem_estados.sort()
    
    # Boxplot por estado
    boxplot = sns.boxplot(
        data=df_pol,
        x='Estado',
        y='SC_km',
        order=ordem_estados,
        ax=ax,
        color='steelblue',
        showfliers=False
    )
    
    # Configurar título e rótulos
    ax.set_title(f"Distribuição de Sincronicidade - {pol}", fontsize=20)
    ax.set_xlabel("Estado", fontsize=18)
    ax.set_ylabel("SC_km", fontsize=18)
    ax.set_ylim(0, 1000)
    ax.tick_params(axis='x', rotation=45)
    ax.tick_params(axis='both', labelsize=18)
    ax.grid(axis='y', alpha=0.3)

# Ocultar eixos extras
for j in range(len(poluentes), len(axs)):
    axs[j].axis('off')

# Ajustar layout e salvar
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "boxplot_sincronicidade_por_estado.png"), dpi=300, bbox_inches='tight')
plt.close()
print("Boxplots combinados salvos!")

# ========================================================================================
# 3. MAPAS DAS ESTAÇÕES COM ESCALA DE COR PARA SINCRONICIDADE (POR POLUENTE) - FOCO NO BRASIL
# ========================================================================================
print("Gerando mapas por estação com foco no Brasil...")

# Obter limites do Brasil
xmin, ymin, xmax, ymax = brazil_states.total_bounds
buffer = 100000  # Buffer de 100km

# Para cada poluente
for pol in poluentes:
    print(f"Processando mapa Brasil - {pol}...")
    
    # Filtrar dados do poluente
    df_pol = gdf_estacoes[gdf_estacoes['Poluente'] == pol]
    
    if df_pol.empty:
        print(f"Nenhum dado para {pol}. Pulando...")
        continue

    # Ordenar por SC_km (menores primeiro para que maiores sejam sobrepostos)
    df_pol = df_pol.sort_values('SC_km')
    
    # Criar figura
    fig, ax = plt.subplots(1, 1, figsize=(14, 12))
    
    # Plotar estados brasileiros
    brazil_states.plot(
        ax=ax,
        color='lightgrey',
        edgecolor='darkgrey',
        linewidth=0.8
    )
    
    # Definir limites do mapa para o Brasil
    ax.set_xlim(xmin - buffer, xmax + buffer)
    ax.set_ylim(ymin - buffer, ymax + buffer)
    
    # Plotar estações
    scatter = ax.scatter(
        df_pol.geometry.x,
        df_pol.geometry.y,
        c=df_pol['SC_km'],
        cmap=cmap,
        vmin=0,
        vmax=550,
        s=30,
        alpha=0.9
    )
    
    # Adicionar barra de cores
    cbar = fig.colorbar(scatter, ax=ax, shrink=0.7)
    cbar.set_label('SC_km (média)', fontsize=12)
    
    # Configurações do gráfico
    ax.set_title(f"Sincronicidade Média por Estação - {pol}", fontsize=16)
    ax.set_axis_off()
    
    # Salvar figura
    plt.savefig(
        os.path.join(OUTPUT_DIR, f"mapa_sincronicidade_brasil_{pol}.png"), 
        dpi=300, 
        bbox_inches='tight'
    )
    plt.close()

# ========================================================================================
# 4. MAPAS POR ESTADO COM FOCO NAS ESTAÇÕES (LAYOUT DINÂMICO)
# ========================================================================================
print("Gerando mapas por estado com foco nas estações...")

# Para cada poluente
for pol in poluentes:
    print(f"Processando mapas por estado para {pol}...")
    
    # Filtrar dados do poluente
    df_pol = gdf_estacoes[gdf_estacoes['Poluente'] == pol]
    
    if df_pol.empty:
        continue
    
    # Filtrar estados que têm dados
    estados_com_dados = df_pol['Estado'].unique()
    n_estados = len(estados_com_dados)
    
    if n_estados == 0:
        continue
    
    # Calcular layout ideal (evitar muitas colunas para poucos estados)
    ncols = min(4, math.ceil(math.sqrt(n_estados)))
    nrows = math.ceil(n_estados / ncols)
    
    # Criar figura
    fig, axs = plt.subplots(nrows, ncols, figsize=(6 * ncols, 5 * nrows))
    
    # Converter para array 1D para facilitar iteração
    axs = axs.flatten()
    
    # Contador para estados
    estado_count = 0
    
    # Para cada estado
    for estado in estados_com_dados:
        ax = axs[estado_count]
        
        # Filtrar estações do estado
        df_estado = df_pol[df_pol['Estado'] == estado]
        
        # Obter shape do estado
        estado_shape = brazil_states[brazil_states['Estado'] == estado]
        
        if estado_shape.empty:
            continue
        
        # Plotar estado
        estado_shape.plot(
            ax=ax,
            color='lightgrey',
            edgecolor='darkgrey',
            linewidth=0.8
        )
        
        # Definir limites do estado
        xmin, ymin, xmax, ymax = estado_shape.total_bounds
        buffer = 20000  # 20km
        ax.set_xlim(xmin - buffer, xmax + buffer)
        ax.set_ylim(ymin - buffer, ymax + buffer)
        
        # Ordenar por SC_km (menores primeiro)
        df_estado = df_estado.sort_values('SC_km')
        
        # Plotar estações
        scatter = ax.scatter(
            df_estado.geometry.x,
            df_estado.geometry.y,
            c=df_estado['SC_km'],
            cmap=cmap,
            vmin=0,
            vmax=550,
            s=80,
            alpha=0.9
        )
        
        # Configurações do gráfico
        ax.set_title(estado, fontsize=12)
        ax.set_axis_off()
        
        estado_count += 1
    
    # Ocultar eixos extras
    for j in range(estado_count, len(axs)):
        axs[j].axis('off')
    
    # Adicionar barra de cores única
    fig.subplots_adjust(right=0.85)
    cbar_ax = fig.add_axes([0.88, 0.15, 0.02, 0.7])
    cbar = fig.colorbar(scatter, cax=cbar_ax)
    cbar.set_label('SC_km (média)', fontsize=12)
    
    # Ajustar layout e salvar
    plt.suptitle(f"Sincronicidade por Estado - {pol}", fontsize=16)
    plt.tight_layout(rect=[0, 0, 0.85, 0.95])
    plt.savefig(os.path.join(OUTPUT_DIR, f"mapa_por_estado_{pol}.png"), dpi=300, bbox_inches='tight')
    plt.close()

# ========================================================================================
# 5. MAPA DE CALOR POR ESTADO E POLUENTE
# ========================================================================================
print("Gerando mapas de calor por estado e poluente...")

# Calcular média de SC_km por estado e poluente
media_por_estado_pol = df_sincronia.groupby(['Estado', 'Poluente'])['SC_km'].mean().reset_index()

# Juntar com shapefile dos estados
brazil_states_media = brazil_states.merge(
    media_por_estado_pol, 
    on='Estado', 
    how='left'
)

# Configurar figura
ncols = 2
nrows = int(np.ceil(len(poluentes) / ncols))
fig, axs = plt.subplots(nrows, ncols, figsize=(20, 10 * nrows))
axs = axs.flatten() if nrows > 1 or ncols > 1 else [axs]

# Paleta de cores para os estados
cores_estados = ["#2c7bb6", "#ffffbf", "#d7191c"]  # Azul (baixo), Amarelo (médio), Vermelho (alto)
cmap_estados = mpl.colors.LinearSegmentedColormap.from_list("custom_estados", cores_estados)

# Normalização baseada em todos os valores
vals = media_por_estado_pol['SC_km'].dropna()
vmin, vmax = vals.min(), vals.max()

# Para armazenar o último mappable (usado na colorbar)
mappable = None

# Para cada poluente
for i, pol in enumerate(poluentes):
    ax = axs[i]
    
    # Filtrar dados do poluente
    df_pol = brazil_states_media[brazil_states_media['Poluente'] == pol]
    
    # Plotar estados com a média de SC_km
    mappable = df_pol.plot(
        column='SC_km',
        ax=ax,
        cmap=cmap_estados,
        vmin=vmin,
        vmax=vmax,
        legend=False,
        missing_kwds={'color': 'lightgrey'}
    )

    # Adicionar rótulos para os estados
    for idx, row in df_pol.iterrows():
        centroid = row.geometry.centroid
        ax.annotate(
            row['Estado'], 
            (centroid.x, centroid.y),
            ha='center',
            va='center',
            fontsize=10,
            bbox=dict(boxstyle='round,pad=0.2', fc='white', alpha=0.7)
        )
    
    # Configurações do gráfico
    ax.set_title(f'Sincronicidade Média - {pol}', fontsize=16)
    ax.set_axis_off()

# Ocultar eixos extras
for j in range(len(poluentes), len(axs)):
    axs[j].axis('off')

# Adicionar barra de cores única
fig.subplots_adjust(right=0.85)
cbar_ax = fig.add_axes([0.88, 0.15, 0.02, 0.7])

# Criar colorbar com base no último mapeamento
cbar = fig.colorbar(
    mappable.get_children()[0],
    cax=cbar_ax
)
cbar.set_label('SC_km (média)', fontsize=12)

# Ajustar layout e salvar
plt.tight_layout(rect=[0, 0, 0.85, 1])
plt.savefig(os.path.join(OUTPUT_DIR, "mapa_calor_estados_poluente.png"), dpi=300, bbox_inches='tight')
plt.close()

print("Todos os mapas salvos!")
print("Processo concluído com sucesso!")