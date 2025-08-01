import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
import seaborn as sns
import contextily as ctx
import matplotlib as mpl
from shapely.geometry import Point, box
import zipfile

# Configurações
MK_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_testes_mannkendall"
OUTPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\graphtable_analise_tendencias_ano"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# URLs para shapefiles (MESMO do primeiro código)
COUNTRY_URL = r"https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
STATES_URL = r"https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_1_states_provinces.zip"
MUNICIPIOS_ZIP_LOCAL = r"support_data\BR_Municipios_2022.zip"
MUNICIPIOS_EXTRACT_DIR = r"support_data\BR_Municipios_2022"

# Extrair shapefile dos municípios
if not os.path.exists(MUNICIPIOS_EXTRACT_DIR):
    with zipfile.ZipFile(MUNICIPIOS_ZIP_LOCAL, 'r') as zip_ref:
        zip_ref.extractall(MUNICIPIOS_EXTRACT_DIR)

print("Carregando shapefiles...")
# Carregar países da América do Sul
south_america = gpd.read_file(COUNTRY_URL)
south_america = south_america[south_america['CONTINENT'] == 'South America'].to_crs(epsg=3857)

# Carregar estados brasileiros
states = gpd.read_file(STATES_URL)
brazil_states = states[states['admin'] == 'Brazil'].to_crs(epsg=3857)

# Carregar municípios brasileiros a partir do shapefile extraído
# Procura automaticamente o primeiro .shp na pasta extraída
for file in os.listdir(MUNICIPIOS_EXTRACT_DIR):
    if file.lower().endswith('.shp'):
        municipios_shp_path = os.path.join(MUNICIPIOS_EXTRACT_DIR, file)
        break

municipios = gpd.read_file(municipios_shp_path).to_crs(epsg=3857)
print("Municípios carregados:", len(municipios))

# Definir regiões metropolitanas (MESMO do primeiro código)
regioes_metropolitanas = {
    "SP": (-47.16, -24.11, -45.8, -23.21),  # São Paulo
    "MG": (-44.4, -20.7, -42.1, -19.2),     # Belo Horizonte
    "RJ": (-43.8, -23.1, -42.99, -22.6),    # Rio de Janeiro
    "BA": (-38.65, -13.15, -38.1, -12.525)  # Salvador
}

print("Processando dados para análise de tendências...")

def formatar_para_excel_ptbr(df):
    """Formata o DataFrame para compatibilidade com Excel PT-BR"""
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
    mk_df_significativo['magnitude_absoluta'] = mk_df_significativo['slope'].abs()
    mk_df_significativo['Direcao_Tendencia'] = np.where(
        mk_df_significativo['slope'] > 0, 'Aumento', 'Redução'
    )

    # Criar GeoDataFrame com as tendências
    geometry = [Point(xy) for xy in zip(mk_df_significativo['Longitude'], mk_df_significativo['Latitude'])]
    gdf_tendencias = gpd.GeoDataFrame(
        mk_df_significativo,
        geometry=geometry,
        crs="EPSG:4326"
    ).to_crs(epsg=3857)

    # Filtrar tendências significativas (p < 0.05)
    mk_df_completo = mk_df.copy()
    mk_df_completo['magnitude_absoluta'] = mk_df_completo['slope'].abs()
    mk_df_completo['Direcao_Tendencia'] = np.where(
        mk_df_completo['slope'] > 0, 'Aumento', 'Redução'
    )

    # Criar GeoDataFrame com as tendências
    geometry = [Point(xy) for xy in zip(mk_df_completo['Longitude'], mk_df_completo['Latitude'])]
    gdf_tendencias = gpd.GeoDataFrame(
        mk_df_completo,
        geometry=geometry,
        crs="EPSG:4326"
    ).to_crs(epsg=3857)

    
    # Salvar tabelas (mantido do original)
    mk_df_significativo.to_csv(
        os.path.join(OUTPUT_DIR, f"tendencias_significativas_{poluente}.csv"),
        index=False,
        encoding='utf-8-sig'
    )
    
    
    # ========================================================================================
    # MAPAS POR ESTADO (MESMO ESTILO DO PRIMEIRO CÓDIGO)
    # ========================================================================================
    
    # Paleta de cores divergente (vermelho para aumento, azul para redução)
    cmap_divergente = mpl.colors.LinearSegmentedColormap.from_list(
        'divergente', 
        ['#1f78b4', '#ffffff', '#e31a1c']  # Azul -> Branco -> Vermelho
    )
    
    estados_presentes = gdf_tendencias['Estado'].unique()
    
    for estado in estados_presentes:
        print(f"Gerando mapa de tendências para {estado} - {poluente}")
        
        # Filtrar tendências do estado
        gdf_estado = gdf_tendencias[gdf_tendencias['Estado'] == estado]
        
        # Obter shape do estado
        estado_shp = brazil_states[brazil_states['postal'] == estado]
        
        if estado_shp.empty:
            print(f"  Shapefile não encontrado para {estado}")
            continue
            
        if gdf_estado.empty:
            print(f"  Nenhuma tendência significativa em {estado}")
            continue
            
        # Verificar se estado tem região metropolitana
        tem_rm = estado in regioes_metropolitanas
            
        # Configurar plot - criar figura com 1 ou 2 subplots
        if tem_rm:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(24, 10))
        else:
            fig, ax1 = plt.subplots(figsize=(12, 10))
        
        # Lista de eixos
        axes = [ax1]
        if tem_rm:
            axes.append(ax2)
        
        # Determinar limites de cores (simétricos)
        max_abs = max(gdf_estado['slope'].abs().max(), 0.001)  # Evitar zero
        vmin, vmax = -max_abs, max_abs
        
        # Plotar mapa do estado completo
        estado_shp.plot(ax=ax1, color='#f0f0f0', edgecolor='#636363', linewidth=1.2, zorder=1)
        municipios_estado = municipios[municipios['SIGLA_UF'] == estado]
        
        # Plotar tendências
        sc = gdf_estado.plot(
            ax=ax1,
            column='slope',
            cmap=cmap_divergente,
            vmin=vmin,
            vmax=vmax,
            markersize=80,
            edgecolor='black',
            linewidth=0.5,
            legend=True,
            legend_kwds={
                'shrink': 0.6, 
                'label': "Magnitude da Tendência",
                'format': "%.4f"
            }
        )
        
        # Aumentar tamanho das fontes da barra de cores
        # Método mais confiável: acessar através da figura
        if len(fig.axes) > 1:
            # A barra de cores é sempre o último eixo criado
            cbar_ax = fig.axes[-1]
            
            # Aumentar tamanho dos valores
            cbar_ax.tick_params(labelsize=14)
            
            # Aumentar tamanho do título
            # Obter o texto atual do rótulo
            current_label = cbar_ax.get_ylabel()
            # Definir o rótulo novamente com tamanho maior
            cbar_ax.set_ylabel(current_label, fontsize=16)
        
        # Definir limites do território
        minx, miny, maxx, maxy = estado_shp.total_bounds
        ax1.set_xlim(minx, maxx)
        ax1.set_ylim(miny, maxy)
        
        # Destacar região metropolitana se aplicável
        rm_patch = None
        if tem_rm:
            bbox = regioes_metropolitanas[estado]
            minx, miny, maxx, maxy = bbox
            bbox_4326 = box(minx, miny, maxx, maxy)
            bbox_3857 = gpd.GeoSeries([bbox_4326], crs='EPSG:4326').to_crs('EPSG:3857')
            minx_rm, miny_rm, maxx_rm, maxy_rm = bbox_3857.total_bounds
            
            # Adicionar retângulo de destaque
            rect1 = plt.Rectangle(
                (minx_rm, miny_rm), 
                maxx_rm - minx_rm, 
                maxy_rm - miny_rm,
                fill=False, 
                color='red', 
                linewidth=1.8, 
                linestyle='--'
            )
            ax1.add_patch(rect1)
            rm_patch = rect1
            
            # Plotar mapa da região metropolitana (zoom)
            municipios_estado.plot(ax=ax2, color='white', edgecolor='#636363', linewidth=0.8, alpha=0.7)

            # Plotar tendências
            gdf_estado.plot(
                ax=ax2,
                column='slope',
                cmap=cmap_divergente,
                vmin=vmin,
                vmax=vmax,
                markersize=130,
                edgecolor='black',
                linewidth=0.5,
                legend=False
            )
            
            # Definir limites da região metropolitana
            ax2.set_xlim(minx_rm, maxx_rm)
            ax2.set_ylim(miny_rm, maxy_rm)
            # Aumentar fonte do título do zoom
            ax2.set_title("Área de maior densidade de estações", fontsize=16)
            
            # Adicionar retângulo no zoom
            rect2 = plt.Rectangle(
                (minx_rm, miny_rm), 
                maxx_rm - minx_rm, 
                maxy_rm - miny_rm,
                fill=False, 
                color='red', 
                linewidth=1.8, 
                linestyle='--'
            )
            ax2.add_patch(rect2)
        
        # Configurar título principal
        fig.suptitle(
            f"Tendências de Concentração ({poluente}) - {estado}\n"
            f"Estações com significância estatística (p < 0.05)",
            fontsize=20
        )
        
        # Adicionar legenda do retângulo
        if tem_rm and rm_patch:
            from matplotlib.patches import Patch
            rm_legend = Patch(
                facecolor='none', 
                edgecolor='red', 
                linestyle='--', 
                linewidth=1.8, 
                label='Região de maior densidade de estações'
            )
            ax1.legend(handles=[rm_legend], loc='upper right')
        
        # Desligar eixos
        for ax in axes:
            ax.axis('off')
        
        plt.tight_layout(rect=[0, 0, 1, 0.96])
        
        # Salvar mapa
        estado_nome = estado.replace("/", "_").replace(" ", "_")
        plt.savefig(
            os.path.join(OUTPUT_DIR, f"tendencias_map_{estado_nome}_{poluente}.png"), 
            dpi=300, 
            bbox_inches='tight'
        )
        plt.close()
    
    # ========================================================================================
    # MAPA COMBINADO: SLOPE + P-VALUE (COR + TRANSPARÊNCIA) - APENAS TENDÊNCIAS SIGNIFICATIVAS
    # ========================================================================================
    print(f"Gerando mapa combinado slope/p-value para {poluente}")

    # Filtrar apenas tendências significativas (p < 0.05)
    gdf_significativo = gdf_tendencias[gdf_tendencias['p_valor'] < 0.05].copy()

    # Calcular tendência média e p-value médio por estado (apenas significativos)
    if not gdf_significativo.empty:
        agregado_por_estado = gdf_significativo.groupby('Estado').agg(
            media_slope=('slope', 'mean'),
            media_pvalue=('p_valor', 'mean')
        ).reset_index()
    else:
        agregado_por_estado = pd.DataFrame(columns=['Estado', 'media_slope', 'media_pvalue'])

    # Juntar com shapefile dos estados
    brasil_map = brazil_states.merge(agregado_por_estado, left_on='postal', right_on='Estado', how='left')

    # Configurar paleta de cores sequencial (amarelo para laranja)
    cores = ["#ffee00", "#ff9100", '#fc4e2a', '#e31a1c', '#800026']
    cmap_slope = mpl.colors.LinearSegmentedColormap.from_list('amarelo_laranja', ["#ffee00", '#fc4e2a', '#800026'])

    # Determinar limites para normalização do slope
    if not agregado_por_estado.empty:
        max_slope = agregado_por_estado['media_slope'].abs().max()
        norm_slope = mpl.colors.Normalize(vmin=-max_slope, vmax=max_slope)
    else:
        max_slope = 1  # Valor padrão se não houver dados
        norm_slope = mpl.colors.Normalize(vmin=-1, vmax=1)

    # Função para mapear p-value para transparência (apenas duas categorias)
    def pvalue_to_alpha(p):
        if pd.isna(p):
            return 0
        elif p < 0.01:
            return 1.0  # Máxima opacidade
        else:  # 0.01 <= p < 0.05
            return 0.5  # Opacidade moderada

    # Criar figura principal para o mapa
    fig, ax = plt.subplots(1, 1, figsize=(14, 14))

    # Plotar estados sem dados primeiro (fundo)
    sem_dados = brasil_map[brasil_map['media_slope'].isna()]
    if not sem_dados.empty:
        for idx, row in sem_dados.iterrows():
            gpd.GeoSeries(row.geometry).plot(
                ax=ax,
                color='lightgrey',
                edgecolor='darkgrey',
                hatch='////',
                linewidth=0.8
            )

    # Plotar estados com dados (aplicando cor + transparência)
    for idx, row in brasil_map.iterrows():
        if pd.notna(row['media_slope']):
            # Converter slope para cor
            rgba = cmap_slope(norm_slope(row['media_slope']))
            
            # Converter p-value para transparência
            alpha = pvalue_to_alpha(row['media_pvalue'])
            
            # Criar cor com transparência
            facecolor = list(rgba[:3]) + [alpha]
            
            # Plotar
            gpd.GeoSeries(row.geometry).plot(
                ax=ax,
                facecolor=facecolor,
                edgecolor='black',
                linewidth=0.8
            )

    # =================================================================
    # ADICIONAR AS ESTAÇÕES AO MAPA
    # =================================================================
    if not gdf_significativo.empty:
        # Criar cópia para não modificar o original
        estacoes_plot = gdf_significativo.copy()
        
        # Definir cores das estações baseadas na direção da tendência
        estacoes_plot['cor'] = estacoes_plot['slope'].apply(
            lambda x: "#43b62c" if x < 0 else '#d7191c'  # Verde para redução, vermelho para aumento
        )
        
        # Plotar estações como pontos
        estacoes_plot.plot(
            ax=ax,
            marker='o',
            markersize=25,  # Tamanho pequeno
            color=estacoes_plot['cor'],
            edgecolor='black',
            linewidth=0.5,
            alpha=0.8  # Leve transparência
        )

    # =================================================================
    # NOVA LEGENDA PARA P-VALUE (APENAS 2 CATEGORIAS)
    # =================================================================
    legend_ax = fig.add_axes([0.15, 0.01, 0.7, 0.08])
    legend_ax.set_ylim(0, 6)
    legend_ax.set_xlim(0, 15)
    legend_ax.axis('off')

    # Definir os níveis de transparência e rótulos (apenas 2 categorias)
    alphas = [1.0, 0.7]
    labels = [
        'p < 0.01 (Alta confiança)',
        '0.01 ≤ p < 0.05 (Moderada confiança)'
    ]

    # Configurações das barras
    bar_height = 1.0
    bar_width = 9.0
    spacing = 0.5
    y_positions = [2.5, 1.5]  # Apenas duas posições

    # Criar cada barra com gradiente
    for i, (alpha, label, y) in enumerate(zip(alphas, labels, y_positions)):
        # Criar gradiente
        gradient = np.linspace(0, 1, 512).reshape(1, -1)
        gradient = np.vstack((gradient, gradient))
        
        # Plotar a barra de gradiente
        im = legend_ax.imshow(
            gradient, 
            aspect='auto', 
            cmap=cmap_slope,
            alpha=alpha,
            extent=[0, bar_width, y, y + bar_height]
        )
        
        # Adicionar rótulo
        legend_ax.text(
            bar_width + spacing, 
            y + bar_height/2, 
            label, 
            ha='left', 
            va='center', 
            fontsize=25
        )

    # Título da legenda
    legend_ax.text(
        bar_width/2, 
        4,  # Posição acima das barras
        'slope (direção da tendência)', 
        ha='center', 
        fontsize=25,
        weight='bold'
    )

    # Rótulos das extremidades (mais destacados)
    legend_ax.text(
        0, 
        0.5, 
        'Diminuição', 
        ha='center', 
        fontsize=25, 
        color="#9b7e00",
        weight='bold'
    )
    legend_ax.text(
        bar_width, 
        0.5, 
        'Aumento', 
        ha='center', 
        fontsize=25, 
        color="#6b0002",
        weight='bold'
    )

    # =================================================================
    # TÍTULOS PRINCIPAIS
    # =================================================================
    fig.suptitle(
        f"Tendências de {(poluente if poluente != 'MP2' else 'MP2,5')} por Estado: Direção e Confiança",
        fontsize=16,
        y=0.9
    )
    fig.text(
        0.5, 0.92,
        "Cor = Direção/Magnitude | Transparência = Significância Estatística",
        ha='center',
        fontsize=12
    )

    # =================================================================
    # LEGENDA PARA SEM DADOS
    # =================================================================
    sem_dados_patch = mpl.patches.Patch(
        facecolor='lightgrey', 
        edgecolor='darkgrey', 
        hatch='////', 
        label='Sem dados significativos'
    )
    ax.legend(
        handles=[sem_dados_patch], 
        loc='upper right',
        fontsize=9
    )

    # Ajustar layout e salvar
    plt.tight_layout(rect=[0, 0.12, 1, 0.90])

    plt.savefig(
        os.path.join(OUTPUT_DIR, f"mapa_combinado_slope_pvalue_{poluente}.png"), 
        dpi=300, 
        bbox_inches='tight'
    )
    plt.close()

    # ========================================================================================
    # BOXPLOT POR ESTADO PARA CADA POLUENTE (apenas estados com >=4 estações)
    # ========================================================================================
    print("Gerando boxplots por estado (com filtro mínimo de 4 estações)...")

    # Collect data for all pollutants first
    data_dict = {}
    all_slopes = []
    pollutants = []

    print("Coletando dados para todos os poluentes...")
    for file_name in os.listdir(MK_DIR):
        if not file_name.startswith('mk_') or not file_name.endswith('.parquet'):
            continue
        
        poluente = file_name.split('_')[1].split('.')[0]
        file_path = os.path.join(MK_DIR, file_name)
        mk_df = pd.read_parquet(file_path)
        
        # Filter significant trends (p < 0.05)
        mk_df_significativo = mk_df[mk_df['p_valor'] < 0.05].copy()
        
        if mk_df_significativo.empty:
            print(f"  Nenhuma tendência significativa para {poluente}. Pulando.")
            continue
        
        # Filter states with at least 4 stations
        contagem_estados = mk_df_significativo['Estado'].value_counts()
        estados_validos = contagem_estados[contagem_estados >= 4].index.tolist()
        
        if not estados_validos:
            print(f"  Nenhum estado com pelo menos 4 estações para {poluente}. Pulando.")
            continue
            
        mk_df_filtrado = mk_df_significativo[mk_df_significativo['Estado'].isin(estados_validos)]
        data_dict[poluente] = mk_df_filtrado
        pollutants.append(poluente)
        all_slopes.extend(mk_df_filtrado['slope'].tolist())

    # Create combined plot if we have data
    if pollutants:
        print("Criando gráficos combinados...")
        # Calculate global y-limits (symmetric)
        abs_max = max(abs(np.percentile(all_slopes, 1)), 
                    abs(np.percentile(all_slopes, 99)))
        y_lim = (-0.1, 0.1)

        # Create subplot grid
        n_pols = len(pollutants)
        ncols = 2
        nrows = (n_pols + 1) // ncols
        fig, axes = plt.subplots(nrows, ncols, figsize=(16, nrows * 6), squeeze=False)
        axes = axes.flatten()
        
        for i, pol in enumerate(pollutants):
            ax = axes[i]
            df = data_dict[pol]
            
            # Order states by alfabetical order
            order = df['Estado'].value_counts().index.tolist()
            order.sort()
            
            # Create boxplot without outliers and a single color
            sns.boxplot(
                data=df,
                y='slope',
                x='Estado',
                order=order,
                color='steelblue',
                whis=1.5,
                showfliers=False,  # Exclude outliers
                linewidth=1.5,
                ax=ax
            )
            
            # Reference line at zero
            ax.axhline(y=0, color='blue', linestyle='--', linewidth=1.5, alpha=0.7)

            # Labels and titles
            ax.set_title(f"Tendências de {(pol if pol != 'MP2' else 'MP2,5')}", fontsize=18)
            ax.set_ylabel("slope", fontsize=22)
            ax.set_xlabel("")
            
            # Rotate state labels
            ax.set_xticklabels(ax.get_xticklabels(), rotation=75, ha='right', fontsize=16)
            
            # Set consistent y-axis limits
            ax.set_yscale('symlog', linthresh=1e-4)
            ax.set_ylim(-abs_max, abs_max)
            
            # Add station counts to x-axis labels
            new_labels = []
            for estado in order:
                count = len(df[df['Estado'] == estado])
                new_labels.append(f"{estado}\n(n={count})")
            ax.set_xticklabels(new_labels, fontsize=16)
            
            # Grid lines
            ax.grid(axis='y', alpha=0.3)

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis('off')
        
        # Main title
        fig.suptitle(
            "Distribuição das Tendências por Estado\n"
            "(Estados com pelo menos 4 estações significativas, p-value < 0.05)",
            fontsize=24,
            y=0.98
        )
        
        plt.tight_layout(rect=[0, 0, 1, 0.96])
        plt.savefig(
            os.path.join(OUTPUT_DIR, "combined_boxplot_tendencias.png"), 
            dpi=300, 
            bbox_inches='tight'
        )
        plt.close()
        print("Gráfico combinado salvo!")
    else:
        print("Nenhum dado válido encontrado para criar os gráficos.")

# ========================================================================================
# MAPA COMBINADO PARA TODOS POLUENTES EM UMA ÚNICA FIGURA
# ========================================================================================
print("Gerando mapa combinado para todos poluentes")

# Listar todos poluentes disponíveis
poluentes = []
gdfs_significativos = {}
agregados_por_estado = {}

# Coletar dados
for file_name in os.listdir(MK_DIR):
    if not file_name.startswith('mk_') or not file_name.endswith('.parquet'):
        continue
    
    poluente = file_name.split('_')[1].split('.')[0]
    poluentes.append(poluente)
    file_path = os.path.join(MK_DIR, file_name)
    mk_df = pd.read_parquet(file_path)
    
    # Converter coordenadas se necessário
    for col in ['Latitude', 'Longitude']:
        if mk_df[col].dtype == object:
            mk_df[col] = mk_df[col].str.replace(',', '.').astype(float)
    
    # Filtrar tendências significativas
    gdf_significativo = mk_df[mk_df['p_valor'] < 0.05].copy()
    gdfs_significativos[poluente] = gdf_significativo
    
    # Calcular tendência média por estado
    if not gdf_significativo.empty:
        agregado = gdf_significativo.groupby('Estado').agg(
            media_slope=('slope', 'mean'),
            media_pvalue=('p_valor', 'mean')
        ).reset_index()
        agregados_por_estado[poluente] = agregado

# Configurar figura principal
n_pols = len(poluentes)
ncols = 2
nrows = (n_pols + 1) // ncols
fig, axs = plt.subplots(nrows, ncols, figsize=(20, 10 * nrows))
axs = axs.flatten()

# Configurar paleta de cores sequencial com intervalo fixo
cmap_slope = mpl.colors.LinearSegmentedColormap.from_list('ciano_azulmarinho', [
    "#006400", "#00FF00", "#FFFF00", "#FF0000", "#8B0000"
])

# Calcular limites globais para a barra de cores
all_slopes = []
for pol in poluentes:
    if pol in agregados_por_estado:
        all_slopes.extend(agregados_por_estado[pol]['media_slope'].dropna().values)

if not all_slopes:
    # Caso não haja dados, define valores padrão
    vmin, vmax = 0, 1
else:
    if abs(min(all_slopes)) > abs(max(all_slopes)):
        vmin, vmax = -abs(min(all_slopes)), abs(min(all_slopes))
    else:
        vmin, vmax = -abs(max(all_slopes)), abs(max(all_slopes))

norm_fixo = mpl.colors.Normalize(vmin=vmin, vmax=vmax)

# Plotar cada poluente
for i, pol in enumerate(poluentes):
    ax = axs[i]
    print(f"Processando {pol}...")
    
    # Obter dados para este poluente
    gdf_significativo = gdfs_significativos.get(pol, gpd.GeoDataFrame())
    agregado = agregados_por_estado.get(pol, pd.DataFrame())
    
    # Juntar com shapefile dos estados
    if not agregado.empty:
        brasil_map = brazil_states.merge(agregado, left_on='postal', right_on='Estado', how='left')
    else:
        brasil_map = brazil_states.copy()
        brasil_map['media_slope'] = np.nan
    
    # Plotar estados sem dados
    sem_dados = brasil_map[brasil_map['media_slope'].isna()]
    if not sem_dados.empty:
        for idx, row in sem_dados.iterrows():
            gpd.GeoSeries(row.geometry).plot(
                ax=ax,
                color='lightgrey',
                edgecolor='darkgrey',
                hatch='////',
                linewidth=0.8
            )
    
    # Plotar estados com dados - usando intervalo fixo
    for idx, row in brasil_map.iterrows():
        if pd.notna(row['media_slope']):
            # Usar intervalo fixo para determinar a cor
            gpd.GeoSeries(row.geometry).plot(
                ax=ax,
                color=cmap_slope(norm_fixo(row['media_slope'])),
                edgecolor='black',
                linewidth=0.8,
                alpha=1.0
            )
    
    # Plotar estações
    if not gdf_significativo.empty:
        geometry = [Point(xy) for xy in zip(gdf_significativo['Longitude'], gdf_significativo['Latitude'])]
        gdf_estacoes = gpd.GeoDataFrame(
            gdf_significativo,
            geometry=geometry,
            crs="EPSG:4326"
        ).to_crs(epsg=3857)
        
        gdf_estacoes['cor'] = gdf_estacoes['slope'].apply(
            lambda x: '#43b62c' if x < 0 else '#d7191c'
        )
        
        gdf_estacoes.plot(
            ax=ax,
            marker='o',
            markersize=30,
            color=gdf_estacoes['cor'],
            alpha=0.8
        )
    
    # Configurar título do subplot
    ax.set_title(f"Tendências de {pol}", fontsize=25, y=0.94)
    
    # Adicionar mapa de fundo
    ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, zoom=5)
    ax.set_axis_off()

# Ocultar eixos extras
for j in range(i + 1, len(axs)):
    axs[j].axis('off')

# ==================================================================
# BARRA DE CORES NA PARTE INFERIOR
# ==================================================================

# Criar eixos para a barra de cores na parte inferior
cbar_ax = fig.add_axes([0.25, 0.03, 0.5, 0.02])  # [left, bottom, width, height]
sm = plt.cm.ScalarMappable(cmap=cmap_slope, norm=norm_fixo)
sm.set_array([])

# Adicionar barra de cores horizontal
cbar = fig.colorbar(sm, cax=cbar_ax, orientation='horizontal')
cbar.set_label('Taxa de Mudança Média por Estado (unidade/ano)', fontsize=22, labelpad=10)
cbar.ax.tick_params(labelsize=18)

# ==================================================================
# TÍTULO E AJUSTES FINAIS
# ==================================================================

# Título principal
fig.suptitle(
    "Tendências de Poluentes por Estado com Localização das Estações",
    fontsize=40,
    y=0.98
)

# Ajustar layout para dar espaço para a barra de cores na parte inferior
plt.tight_layout(rect=[0, 0.05, 1, 0.97])

# Salvar
plt.savefig(
    os.path.join(OUTPUT_DIR, "mapa_combinado_todos_poluentes.png"), 
    dpi=300, 
    bbox_inches='tight'
)
plt.close()
print("Mapa combinado salvo com sucesso!")

print("Análise de tendências concluída! Resultados salvos em:", OUTPUT_DIR)