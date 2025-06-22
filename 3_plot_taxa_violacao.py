import os
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
import matplotlib as mpl
import numpy as np

# Configurações
DATA_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_violacoes_completo"
OUTPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\plots_taxa_violacao"
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
brazil_states = states[states['admin'] == 'Brazil'].to_crs(epsg=3857)

# Obter limites do Brasil
minx, miny, maxx, maxy = brazil_states.total_bounds
xlim = (minx - 100000, maxx + 100000)
ylim = (miny - 100000, maxy + 100000)

# Obter limites regiões sul sudeste e centro-oeste
# xlim = (-62e5, -39e5)
# ylim = (-33e5, -15e5)

# Obter limites região nordeste
# xlim = (-48e5, -34e5)
# ylim = (-15e5, -3e5)

print("Iniciando geração de mapas...")

# Processar cada arquivo de poluente
for file_name in os.listdir(DATA_DIR):
    if not file_name.endswith('.parquet'):
        continue
    
    file_path = os.path.join(DATA_DIR, file_name)
    df = pd.read_parquet(file_path)
    
    # Corrigir coordenadas
    for col in ['Latitude', 'Longitude']:
        if df[col].dtype == object:
            df[col] = df[col].str.replace(',', '.', regex=False).astype(float)
    
    poluente = df['Poluente'].iloc[0]
    
    # Identificar todos os padrões disponíveis
    padroes = [col for col in df.columns if col.startswith('exceed_')]
    
    # Processar cada padrão individualmente
    for padrao in padroes:
        print(f"Processando {poluente} - {padrao}...")
        
        # Agregar violações por estação
        agg = df.groupby(['Estacao', 'Latitude', 'Longitude']).agg(
            total_violacoes=(padrao, 'sum'),
            total_medicoes=(padrao, 'count')
        ).reset_index()
        
        # Remover estações sem coordenadas
        agg = agg.dropna(subset=['Latitude', 'Longitude'])
        
        # Calcular taxa de violação
        agg['taxa_violacao'] = np.where(
            agg['total_medicoes'] > 0,
            agg['total_violacoes'] / agg['total_medicoes'] * 100,
            0
        )
        
        # Converter para GeoDataFrame
        gdf = gpd.GeoDataFrame(
            agg,
            geometry=gpd.points_from_xy(agg.Longitude, agg.Latitude),
            crs="EPSG:4326"
        ).to_crs(epsg=3857)

        # Ordenar para colocar as piores estações no topo do plot
        gdf = gdf.sort_values('taxa_violacao', ascending=True)
        
        # Criar figura e eixo
        fig, ax = plt.subplots(figsize=(12, 10))
        
        # Plotar fundo - países sul-americanos
        south_america.plot(ax=ax, color='#f0f0f0', edgecolor='#d9d9d9', linewidth=0.5)
        
        # Plotar estados brasileiros
        brazil_states.boundary.plot(ax=ax, color='#737373', linewidth=0.6)
        brazil_states.plot(ax=ax, color='white', edgecolor='none')
        
        # Criar escala de cores ciano-roxo
        cmap = mpl.colors.LinearSegmentedColormap.from_list(
            'taxa_violacao', 
            ["#77ffd2", "#25c8ff", "#0d7eff", "#0051ff", "#250EA8"],
            N=256
        )
        
        # Normalizar os valores de taxa de violação (0-100%)
        norm = mpl.colors.Normalize(vmin=0, vmax=100)
        
        # Plotar estações
        gdf.plot(
            ax=ax,
            column='taxa_violacao',
            cmap=cmap,
            norm=norm,
            markersize=30,
            alpha=1
        )
        
        # Ajustar limites do mapa para o Brasil
        ax.set_xlim(*xlim)
        ax.set_ylim(*ylim)
        
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
        cbar.set_label('Taxa de Violação (%)', size=9)
        cbar.ax.tick_params(labelsize=8)
        
        # Salvar o mapa
        nome_padrao = padrao.split('_')[-1]
        output_path = os.path.join(OUTPUT_DIR, f"taxa_violacao_{poluente}_{nome_padrao}.png")
        plt.savefig(output_path, dpi=200, bbox_inches='tight', pad_inches=0, facecolor='white')
        plt.close()
        
        print(f"  Mapa salvo: {output_path}")

print("Processamento concluído! Mapas salvos em:", OUTPUT_DIR)