import os
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
import matplotlib as mpl
import numpy as np
from shapely.geometry import Point, box
import contextily as ctx

# Configurações
DATA_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_violacoes_completo"
OUTPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\plots_num_violacao"
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
MUNICIPIOS_URL = "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/BR_Municipios_2022.zip"

print("Carregando shapefiles...")
# Carregar países da América do Sul
south_america = gpd.read_file(COUNTRY_URL)
south_america = south_america[south_america['CONTINENT'] == 'South America'].to_crs(epsg=3857)

# Carregar estados brasileiros
states = gpd.read_file(STATES_URL)
brazil_states = states[states['admin'] == 'Brazil'].to_crs(epsg=3857)

# Carregar municípios brasileiros
municipios = gpd.read_file(MUNICIPIOS_URL).to_crs(epsg=3857)
print("Municípios carregados:", len(municipios))

# Definir regiões metropolitanas
regioes_metropolitanas = {
    "São Paulo": {
        "name": "Região Metropolitana de São Paulo",
        "bbox": (-47.5, -24.2, -45.8, -23.3),  # (minx, miny, maxx, maxy) em graus
        "cities": [
            "São Paulo", "Guarulhos", "São Bernardo do Campo", "Santo André",
            "Osasco", "Mauá", "Diadema", "Carapicuíba", "Itapevi", "Barueri",
            "Cotia", "Ferraz de Vasconcelos", "Itapecerica da Serra",
            "Itaquaquecetuba", "Mogi das Cruzes", "Poá", "Ribeirão Pires", "Rio Grande da Serra",
            "Santana de Parnaíba", "Suzano", "Taboão da Serra"
        ]
    },
    "Minas Gerais": {
        "name": "Região Metropolitana de Belo Horizonte",
        "bbox": (-44.5, -20.2, -43.5, -19.7),  # (minx, miny, maxx, maxy) em graus
        "cities": [
            "Belo Horizonte", "Contagem", "Betim", "Sete Lagoas", "Ibirité",
            "Santa Luzia", "Vespasiano", "Ribeirão das Neves", "Sabará", "Nova Lima",
            "Caeté", "Brumadinho", "Igarapé", "Sarzedo", "Mário Campos",
            "São Joaquim de Bicas", "Juatuba"
        ]
    },
    # ADICIONADO: REGIÃO METROPOLITANA DE SALVADOR
    "Bahia": {
        "name": "Região Metropolitana de Salvador",
        "bbox": (-38.7, -13.2, -38.2, -12.7),  # Bounding box para Salvador
        "cities": [
            "Salvador", "Camaçari", "Lauro de Freitas", "Simões Filho", "Dias d'Ávila",
            "Candeias", "Madre de Deus", "São Sebastião do Passé",
            "Vera Cruz", "Itaparica"
        ]
    }
}

# Criar lista de estados brasileiros
estados_brasileiros = brazil_states['name'].unique().tolist()
print(f"Estados encontrados: {len(estados_brasileiros)}")

# Configurar cores
cmap = mpl.colors.LinearSegmentedColormap.from_list(
    'violacoes_total', 
    ["#ffd000", "#e0b700", "#FFBB00", "#ff9100", "#ff6a00", "#ff2a00", "#cc0003", "#7c0019", "#61001D"],
    N=256
)

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
        print(f"\nProcessando {poluente} - {padrao}...")
        
        # Agregar violações por estação
        agg = df.groupby(['Estacao', 'Latitude', 'Longitude']).agg(
            total_violacoes=(padrao, 'sum'),
            total_medicoes=(padrao, 'count')
        ).reset_index()
        
        # Remover estações sem coordenadas
        agg = agg.dropna(subset=['Latitude', 'Longitude'])
        
        # Converter para GeoDataFrame
        gdf = gpd.GeoDataFrame(
            agg,
            geometry=gpd.points_from_xy(agg.Longitude, agg.Latitude),
            crs="EPSG:4326"
        ).to_crs(epsg=3857)
        
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
            
            # Ordenar por total de violações (do menor para o maior)
            pontos_no_estado = pontos_no_estado.sort_values(
                'total_violacoes', 
                ascending=True  # Menores violações primeiro
            )
            
            # Normalizar os valores de violações para este estado
            max_violacoes = pontos_no_estado['total_violacoes'].max()
            if max_violacoes == 0:
                max_violacoes = 1  # Evitar erro
            norm = mpl.colors.Normalize(vmin=0, vmax=max_violacoes)
            
            # ==============================================
            # Mapa de todo o estado
            # ==============================================
            fig, ax = plt.subplots(figsize=(10, 8))
            
            # Plotar fundo da América do Sul
            south_america.plot(ax=ax, color='#f0f0f0', edgecolor='#d9d9d9', linewidth=0.5)
            
            # Plotar outros estados brasileiros (cinza claro)
            outros_estados = brazil_states[brazil_states['name'] != estado]
            outros_estados.plot(ax=ax, color='#e0e0e0', edgecolor='#bdbdbd', linewidth=0.5)
            
            # Plotar estado atual (branco)
            estado_shp.plot(ax=ax, color='white', edgecolor='#636363', linewidth=1.0)
            
            # Plotar estações individualmente em ordem
            for idx, row in pontos_no_estado.iterrows():
                ax.scatter(
                    row.geometry.x,
                    row.geometry.y,
                    c=[row['total_violacoes']],
                    cmap=cmap,
                    norm=norm,
                    s=80,  # Tamanho do marcador
                    alpha=1,
                    edgecolor='black',
                    linewidth=0.5,
                    zorder=10 + row['total_violacoes']  # Z-order baseado no total
                )
            
            # Definir limites do mapa (zoom no estado)
            minx, miny, maxx, maxy = estado_shp.total_bounds
            margin = 0.1 * (maxx - minx)  # 10% de margem
            ax.set_xlim(minx - margin, maxx + margin)
            ax.set_ylim(miny - margin, maxy + margin)
            
            # Remover eixos
            ax.set_axis_off()
            
            # Adicionar título
            nome_padrao = padrao.split('_')[-1]
            plt.title(f"Viol. {poluente} ({nome_padrao}) em {estado}", fontsize=12, pad=10)
            
            # Adicionar barra de cores
            cax = fig.add_axes([0.82, 0.15, 0.02, 0.2])
            cbar = fig.colorbar(
                mpl.cm.ScalarMappable(norm=norm, cmap=cmap),
                cax=cax,
                orientation='vertical'
            )
            
            # Configurar rótulos da barra de cores
            cbar.set_label('Total de Violações', size=8)
            cbar.ax.tick_params(labelsize=8)
            
            # Salvar o mapa
            estado_nome_formatado = estado.replace(" ", "_").lower()
            nome_padrao_formatado = nome_padrao.lower()
            output_path = os.path.join(OUTPUT_DIR, f"violacoes_{poluente}_{nome_padrao_formatado}_{estado_nome_formatado}.png")
            plt.savefig(output_path, dpi=200, bbox_inches='tight', pad_inches=0.1, facecolor='white')
            plt.close()
            
            print(f"    Mapa salvo: {output_path}")
            
            # ==============================================
            # Mapa focado na região metropolitana (SP, MG e BA)
            # ==============================================
            if estado in regioes_metropolitanas:
                rm = regioes_metropolitanas[estado]
                print(f"  Gerando mapa para região metropolitana de {estado}: {rm['name']}")
                
                # Converter bounding box para EPSG:3857
                minx_deg, miny_deg, maxx_deg, maxy_deg = rm['bbox']
                bbox_4326 = box(minx_deg, miny_deg, maxx_deg, maxy_deg)
                bbox_3857 = gpd.GeoSeries([bbox_4326], crs='EPSG:4326').to_crs('EPSG:3857')
                minx_rm, miny_rm, maxx_rm, maxy_rm = bbox_3857.total_bounds
                
                # Filtrar todos os municípios na bounding box (incluindo estados vizinhos)
                municipios_vizinhos = municipios.cx[minx_rm:maxx_rm, miny_rm:maxy_rm]
                
                # Filtrar municípios da região metropolitana
                municipios_rm = municipios_vizinhos[municipios_vizinhos['NM_MUN'].isin(rm['cities'])]
                
                # Filtrar pontos na região metropolitana
                pontos_rm = pontos_no_estado.cx[minx_rm:maxx_rm, miny_rm:maxy_rm]
                
                if not pontos_rm.empty:
                    # Criar figura e eixo para o mapa da RM
                    fig_rm, ax_rm = plt.subplots(figsize=(12, 10))
                    
                    # Plotar todos os municípios na área (fundo cinza claro)
                    municipios_vizinhos.plot(
                        ax=ax_rm, 
                        color='#f0f0f0', 
                        edgecolor='#d9d9d9', 
                        linewidth=0.5, 
                        alpha=0.7
                    )
                    
                    # Plotar municípios da região metropolitana (branco)
                    municipios_rm.plot(
                        ax=ax_rm, 
                        color='white', 
                        edgecolor='#636363', 
                        linewidth=0.7
                    )
                    
                    # Plotar estações na RM
                    for idx, row in pontos_rm.iterrows():
                        ax_rm.scatter(
                            row.geometry.x,
                            row.geometry.y,
                            c=[row['total_violacoes']],
                            cmap=cmap,
                            norm=norm,
                            s=60,
                            alpha=1,
                            edgecolor='black',
                            linewidth=0.6,
                            zorder=10
                        )
                    
                    # Adicionar rótulos das cidades (em preto com fundo branco)
                    for idx, row in municipios_rm.iterrows():
                        centroid = row.geometry.centroid
                        ax_rm.text(
                            centroid.x,
                            centroid.y,
                            row['NM_MUN'],
                            fontsize=6,  # Fonte pequena
                            color='black',
                            ha='center',
                            va='center',
                            bbox=dict(
                                facecolor='white', 
                                alpha=0.7, 
                                edgecolor='none',
                                boxstyle='round,pad=0.1'
                            ),
                            zorder=20  # Colocar por cima de tudo
                        )
                    
                    # Definir limites para a região metropolitana
                    margin_rm = 0.05 * (maxx_rm - minx_rm)  # 5% de margem
                    ax_rm.set_xlim(minx_rm - margin_rm, maxx_rm + margin_rm)
                    ax_rm.set_ylim(miny_rm - margin_rm, maxy_rm + margin_rm)
                    
                    # Remover eixos
                    ax_rm.set_axis_off()
                    
                    # Adicionar título
                    plt.title(f"Viol. {poluente} ({nome_padrao}) em {rm['name']}", fontsize=12, pad=10)
                    
                    # Adicionar barra de cores
                    cax_rm = fig_rm.add_axes([0.82, 0.15, 0.02, 0.2])
                    cbar_rm = fig_rm.colorbar(
                        mpl.cm.ScalarMappable(norm=norm, cmap=cmap),
                        cax=cax_rm,
                        orientation='vertical'
                    )
                    
                    # Configurar rótulos da barra de cores
                    cbar_rm.set_label('Total de Violações', size=8)
                    cbar_rm.ax.tick_params(labelsize=8)
                    
                    # Salvar o mapa da região metropolitana
                    rm_nome_formatado = rm['name'].replace(" ", "_").replace("de", "").replace("ã", "a").lower()
                    output_path_rm = os.path.join(OUTPUT_DIR, f"violacoes_{poluente}_{nome_padrao_formatado}_{rm_nome_formatado}.png")
                    plt.savefig(output_path_rm, dpi=300, bbox_inches='tight', pad_inches=0.1, facecolor='white')
                    plt.close()
                    
                    print(f"    Mapa RM salvo: {output_path_rm}")
                else:
                    print(f"    Nenhuma estação encontrada na região metropolitana de {estado}")

print("\nProcessamento concluído! Mapas salvos em:", OUTPUT_DIR)