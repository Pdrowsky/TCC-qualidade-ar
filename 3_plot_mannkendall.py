import os
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
import matplotlib as mpl
import numpy as np

# Configurações
INPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_testes_mannkendall"
OUTPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\plots_tendencia"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configurar estilo
plt.style.use('default')
plt.rcParams['figure.figsize'] = (12, 10)
plt.rcParams['font.size'] = 10
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.facecolor'] = 'white'

# URLs para shapefiles
COUNTRY_URL = "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
STATES_URL = "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_1_states_provinces.zip"

print("Carregando shapefiles...")
# Carregar países da América do Sul
south_america = gpd.read_file(COUNTRY_URL)
south_america = south_america[south_america['CONTINENT'] == 'South America'].to_crs(epsg=3857)

# Carregar estados brasileiros
states = gpd.read_file(STATES_URL)
brazil_states = states[states['admin'] == 'Brazil'].to_crs(epsg=3857)  # Filtrar e converter CRS

print("Iniciando geração de mapas...")

# Processar cada arquivo de tendência
for file_name in os.listdir(INPUT_DIR):
    if not file_name.startswith('mk_') or not file_name.endswith('.parquet'):
        continue
    
    file_path = os.path.join(INPUT_DIR, file_name)
    trends_df = pd.read_parquet(file_path)
    poluente = trends_df['Poluente'].iloc[0]
    
    print(f"Gerando mapa para {poluente}...")
     
    # Converter para GeoDataFrame
    gdf = gpd.GeoDataFrame(
        trends_df,
        geometry=gpd.points_from_xy(trends_df.Longitude, trends_df.Latitude),
        crs="EPSG:4326"
    ).to_crs(epsg=3857)  # Converter para Web Mercator
    
    # Criar figura e eixo
    fig, ax = plt.subplots(figsize=(12, 10))
    
    # Plotar países vizinhos (fundo cinza claro)
    south_america.plot(ax=ax, color='#f0f0f0', edgecolor='#d9d9d9', linewidth=0.5)
    
    # Plotar estados brasileiros
    brazil_states.boundary.plot(ax=ax, color='#737373', linewidth=0.6)
    brazil_states.plot(ax=ax, color='white', edgecolor='none')
    
    # Criar escala de cores
    cmap = mpl.colors.LinearSegmentedColormap.from_list(
        'divergente_vivida', 
        ['#006837', '#ffff00', '#a50026'],
        N=256
    )
    
    # Normalizar os valores de slope
    max_abs_slope = gdf['slope'].abs().max()
    if max_abs_slope == 0:
        max_abs_slope = 1e-9
    norm = mpl.colors.Normalize(vmin=-max_abs_slope, vmax=max_abs_slope)
    
    # Plotar estações com borda preta
    gdf.plot(
        ax=ax,
        column='slope',
        cmap=cmap,
        norm=norm,
        markersize=25,
        alpha=0.9,
        edgecolor='black',  # Borda preta
        linewidth=0.5       # Espessura da borda
    )

    # Obter limites do Brasil
    minx, miny, maxx, maxy = brazil_states.total_bounds
    # xlim = (minx - 100000, maxx + 100000)
    # ylim = (miny - 100000, maxy + 100000)

    # Obter limites regiões sul sudeste e centro-oeste
    xlim = (-62e5, -39e5)
    ylim = (-33e5, -15e5)

    # Obter limites região nordeste
    # xlim = (-48e5, -34e5)
    # ylim = (-15e5, -3e5)

    ax.set_xlim(xlim)
    ax.set_ylim(ylim)
    
    # Remover eixos
    ax.set_axis_off()
    
    # Adicionar barra de cores
    cax = fig.add_axes([0.78, 0.15, 0.02, 0.2])
    cbar = fig.colorbar(
        mpl.cm.ScalarMappable(norm=norm, cmap=cmap),
        cax=cax,
        orientation='vertical'
    )
    
    # Configurar rótulos da barra de cores
    cbar.set_ticks([-max_abs_slope, 0, max_abs_slope])
    cbar.set_ticklabels(['Diminuição', 'Neutro', 'Aumento'])
    cbar.ax.tick_params(labelsize=9)
    cbar.set_label('Intensidade da Tendência', size=9)
    
    # Salvar o mapa
    output_path = os.path.join(OUTPUT_DIR, f"mapa_tendencia_{poluente}.png")
    plt.savefig(output_path, dpi=200, bbox_inches='tight', pad_inches=0, facecolor='white')
    plt.close()
    
    print(f"  Mapa salvo: {output_path}")

print("Geração de mapas concluída! Mapas salvos em:", OUTPUT_DIR)