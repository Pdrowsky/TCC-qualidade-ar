import os
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
import matplotlib as mpl
import numpy as np
from shapely.geometry import Point

# Configurações
INPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_testes_mannkendall"
OUTPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\plots_tendencia"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configurar estilo
plt.style.use('default')
plt.rcParams['figure.figsize'] = (10, 8)
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
brazil_states = states[states['admin'] == 'Brazil'].to_crs(epsg=3857)

# Criar lista de estados brasileiros
estados_brasileiros = brazil_states['name'].unique().tolist()
print(f"Estados encontrados: {len(estados_brasileiros)}")

# Configurar cores
cmap = mpl.colors.LinearSegmentedColormap.from_list(
    'divergente_vivida', 
    ['#006837', "#B9B9B9", '#a50026'],
    N=256
)

# Processar cada arquivo de tendência
for file_name in os.listdir(INPUT_DIR):
    if not file_name.startswith('mk_') or not file_name.endswith('.parquet'):
        continue
    
    file_path = os.path.join(INPUT_DIR, file_name)
    trends_df = pd.read_parquet(file_path)
    poluente = trends_df['Poluente'].iloc[0]
    
    print(f"\nGerando mapas para {poluente}...")
    
    # Converter para GeoDataFrame
    gdf = gpd.GeoDataFrame(
        trends_df,
        geometry=gpd.points_from_xy(trends_df.Longitude, trends_df.Latitude),
        crs="EPSG:4326"
    ).to_crs(epsg=3857)
    
    # Normalizar os valores de slope
    max_abs_slope = gdf['slope'].abs().max()
    if max_abs_slope == 0:
        max_abs_slope = 1e-9
    norm = mpl.colors.Normalize(vmin=-max_abs_slope, vmax=max_abs_slope)
    
    # Gerar mapa para cada estado
    for estado in estados_brasileiros:
        print(f"  Processando estado: {estado}")
        
        # Filtrar o shape do estado
        estado_shp = brazil_states[brazil_states['name'] == estado]
        
        if estado_shp.empty:
            print(f"    Shapefile não encontrado para {estado}")
            continue
        
        # Filtrar estações dentro do estado
        pontos_no_estado = gdf[gdf.geometry.within(estado_shp.unary_union)]
        
        if pontos_no_estado.empty:
            print(f"    Nenhuma estação encontrada em {estado}")
            continue

        # Ordenar do menor para o maior
        pontos_no_estado = pontos_no_estado.sort_values(
            by='slope', 
            key=lambda x: x.abs(), 
            ascending=True  # Menores magnitudes primeiro
        )
        
        # Criar figura e eixo
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Plotar outros estados brasileiros (cinza claro)
        outros_estados = brazil_states[brazil_states['name'] != estado]
        outros_estados.plot(ax=ax, color='#e0e0e0', edgecolor='#bdbdbd', linewidth=0.5)
        
        # Plotar estado atual (branco)
        estado_shp.plot(ax=ax, color='white', edgecolor='#636363', linewidth=1.0)
        
        # Plotar estações com borda preta
        pontos_no_estado.plot(
            ax=ax,
            column='slope',
            cmap=cmap,
            norm=norm,
            markersize=70,  # Tamanho maior para mapas estaduais
            alpha=0.9,
            edgecolor='black',
            linewidth=0.8
        )
        
        # Definir limites do mapa (zoom no estado)
        minx, miny, maxx, maxy = estado_shp.total_bounds
        margin = 0.1 * (maxx - minx)  # 10% de margem
        ax.set_xlim(minx - margin, maxx + margin)
        ax.set_ylim(miny - margin, maxy + margin)
        
        # Remover eixos
        ax.set_axis_off()
        
        # Adicionar título
        plt.title(f"Tendência de {poluente} em {estado}", fontsize=12, pad=10)
        
        # Adicionar barra de cores
        cax = fig.add_axes([0.82, 0.15, 0.02, 0.2])
        cbar = fig.colorbar(
            mpl.cm.ScalarMappable(norm=norm, cmap=cmap),
            cax=cax,
            orientation='vertical'
        )
        
        # Configurar rótulos da barra de cores
        cbar.set_ticks([-max_abs_slope, 0, max_abs_slope])
        cbar.set_ticklabels(['Diminuição', 'Neutro', 'Aumento'])
        cbar.ax.tick_params(labelsize=8)
        cbar.set_label('Intensidade da Tendência', size=8)
        
        # Salvar o mapa
        estado_nome_formatado = estado.replace(" ", "_").lower()
        output_path = os.path.join(OUTPUT_DIR, f"mapa_tendencia_{poluente}_{estado_nome_formatado}.png")
        plt.savefig(output_path, dpi=200, bbox_inches='tight', pad_inches=0.1, facecolor='white')
        plt.close()
        
        print(f"    Mapa salvo: {output_path}")

print("\nGeração de mapas concluída! Mapas salvos em:", OUTPUT_DIR)