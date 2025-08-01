import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, box
import contextily as ctx
import matplotlib as mpl

# Configurações
DATA_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_violacoes_completo"
OUTPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\graphtable_analise_sazonalidade"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Dicionário para armazenar dados de MSI por estado (para boxplot combinado)
msi_data = {}

# URLs para shapefiles
COUNTRY_URL = r"https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
STATES_URL = r"https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_1_states_provinces.zip"
MUNICIPIOS_URL = r"https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/BR_Municipios_2022.zip"

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

print("Processando dados para análise de sazonalidade...")

# Para cada poluente
for file_name in os.listdir(DATA_DIR):
    if not file_name.endswith('.parquet'):
        continue
    
    poluente = file_name.split('.')[0].split('_')[0]
    file_path = os.path.join(DATA_DIR, file_name)
    df = pd.read_parquet(file_path)
    
    # CONVERSÃO CRÍTICA: Converter coordenadas para float
    for coord_col in ['Longitude', 'Latitude']:
        if coord_col in df.columns:
            # Converter para string, substituir vírgula por ponto, depois para float
            df[coord_col] = df[coord_col].astype(str).str.replace(',', '.').astype(float)
    
    # Converter data
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Extrair mês e estação do ano
    df['Mes'] = df['Date'].dt.month
    df['Estacao_Ano'] = df['Date'].dt.month.map({
        12: 'Verão', 1: 'Verão', 2: 'Verão',
        3: 'Outono', 4: 'Outono', 5: 'Outono',
        6: 'Inverno', 7: 'Inverno', 8: 'Inverno',
        9: 'Primavera', 10: 'Primavera', 11: 'Primavera'
    })

    print(f"Gerando tabela de resumo para {poluente}...")
    
    # Lista de colunas de violação
    padroes_exceed = [col for col in df.columns if col.startswith('exceed_')]
    padroes = [col.split('_')[1] for col in padroes_exceed]
    
    # Criar tabela resumo
    resumo_data = []
    
    # Para cada padrão de violação
    for padrao_col, padrao_nome in zip(padroes_exceed, padroes):
        # Filtrar apenas violações deste padrão
        violacoes = df[df[padrao_col] == 1]
        total_violacoes = len(violacoes)
        
        if total_violacoes > 0:
            # Contar violações por mês
            violacoes_por_mes = violacoes.groupby('Mes').size()
            violacoes_por_mes = violacoes_por_mes.reindex(range(1, 13), fill_value=0)
            
            # Adicionar ao resumo
            resumo_data.append({
                'Poluente': poluente,
                'Padrão': padrao_nome,
                'Total_Violações': total_violacoes,
                **{f'Mes_{mes}': count for mes, count in violacoes_por_mes.items()}
            })
    
    # Criar DataFrame com os resultados
    if resumo_data:
        df_resumo = pd.DataFrame(resumo_data)
        
        # Renomear colunas de meses
        meses_map = {i: pd.to_datetime(f'2023-{i}-1').strftime('%b') for i in range(1, 13)}
        df_resumo = df_resumo.rename(columns={
            f'Mes_{mes_num}': meses_map[mes_num] for mes_num in range(1, 13)
        })
        
        # Adicionar linha com total de registros
        total_registros = pd.DataFrame([{
            'Poluente': poluente,
            'Padrão': 'TOTAL_REGISTROS',
            'Total_Violações': len(df),
            **{meses_map[i]: len(df[df['Mes'] == i]) for i in range(1, 13)}
        }])
        
        df_resumo = pd.concat([df_resumo, total_registros], ignore_index=True)
        
        # Salvar tabela
        resumo_path = os.path.join(OUTPUT_DIR, f"resumo_violacoes_{poluente}.csv")
        df_resumo.to_csv(resumo_path, index=False, encoding='utf-8-sig')
        print(f"  Tabela salva: {resumo_path}")
    else:
        print(f"  Nenhuma violação encontrada para {poluente}")
    
    # Calcular estatísticas mensais
    media_mensal = df.groupby('Mes')['Valor_Padronizado'].mean().reset_index()
    mediana_mensal = df.groupby('Mes')['Valor_Padronizado'].median().reset_index()
    max_mensal = df.groupby('Mes')['Valor_Padronizado'].max().reset_index()
    
    # Calcular estatísticas por estação do ano
    media_estacao = df.groupby('Estacao_Ano')['Valor_Padronizado'].mean().reset_index()
    
    # Salvar tabelas
    media_mensal.to_csv(
        os.path.join(OUTPUT_DIR, f"media_mensal_{poluente}.csv"),
        index=False,
        encoding='utf-8-sig'
    )
    
    media_estacao.to_csv(
        os.path.join(OUTPUT_DIR, f"media_estacao_{poluente}.csv"),
        index=False,
        encoding='utf-8-sig'
    )
    
    # Gráfico de linha sazonal
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=media_mensal, x='Mes', y='Valor_Padronizado', marker='o')
    plt.title(f'Variação Sazonal - {poluente}')
    plt.xlabel('Mês')
    plt.ylabel('Concentração Média')
    plt.xticks(range(1,13), labels=['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'])
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f"sazonalidade_{poluente}.png"), dpi=200)
    plt.close()
    
    # 1. Boxplot por mês para cada estado
    if 'Estado' in df.columns:
        print(f"Gerando boxplot por estado para {poluente}...")

        # Filtrar estados válidos
        valid_df = df.dropna(subset=['Estado'])
        estados = valid_df['Estado'].unique()
        n_estados = len(estados)
        
        if n_estados > 0:
            # Configurar grid de plots
            n_cols = 2
            n_rows = int(np.ceil(n_estados / n_cols))
            
            # Criar figura
            fig, axs = plt.subplots(n_rows, n_cols, figsize=(15, 5*n_rows))
            fig.suptitle(f'Concentrações Mensais por Estado - {poluente}', fontsize=16)
            
            # Se houver apenas 1 estado
            if n_estados == 1:
                axs = np.array([axs])
            
            # Plotar para cada estado
            for i, estado in enumerate(estados):
                ax = axs.flat[i]
                estado_df = valid_df[valid_df['Estado'] == estado].copy()
                
                valores_originais = len(estado_df)
                
                # Plotar boxplot com intervalo completo
                sns.boxplot(
                    data=estado_df,
                    x='Mes',
                    y='Valor_Padronizado',
                    ax=ax,
                    showfliers=True,
                    whis=[0, 100]
                )
                
                # Configurações do plot
                ax.set_title(estado)
                ax.set_xlabel('Mês')
                ax.set_ylabel(f'Concentração ({"µg/m³" if poluente != "CO" else "ppm"})')
                ax.set_xticklabels(['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'])
                ax.grid(alpha=0.3)
                
                # Adicionar limites de padrão
                limites_cols = ['PI-1', 'PI-2', 'PI-3', 'PI-4', 'PF']
                cores = ['green', 'blue', 'orange', 'red', 'purple']
                
                for col, cor in zip(limites_cols, cores):
                    if col in df.columns:
                        if not df[col].dropna().empty:
                            limite = df[col].dropna().iloc[0]
                            ax.axhline(
                                y=limite,
                                color=cor,
                                linestyle='--',
                                alpha=0.7,
                                label=f'{col} ({limite})'
                            )
                
                if i == 0:  # Apenas uma legenda
                    ax.legend()
            
            # Remover eixos vazios
            for j in range(i+1, n_rows*n_cols):
                fig.delaxes(axs.flat[j])
                
            plt.tight_layout(rect=[0, 0, 1, 0.96])
            plt.savefig(os.path.join(OUTPUT_DIR, f"boxplot_estado_{poluente}.png"), dpi=200)
            plt.close()
            print(f"  Boxplot por estado salvo: {OUTPUT_DIR}/boxplot_estado_{poluente}.png")
    
    # 2. Índice de Sazonalidade de Markham por estação
    if 'Estacao' in df.columns:
        # Garantir que a coluna de data está em formato datetime
        df['Date'] = pd.to_datetime(df['Date'])

        # Extrair o mês
        df['Mes'] = df['Date'].dt.month

        # Calcular média mensal por estação
        monthly = df.groupby(['Estacao', 'Mes'])['Valor_Padronizado'].mean().reset_index()

        # Calcular soma total por estação para normalizar
        monthly['Total'] = monthly.groupby('Estacao')['Valor_Padronizado'].transform('sum')
        monthly['p_i'] = monthly['Valor_Padronizado'] / monthly['Total']

        # Transformar em formato wide (colunas = meses)
        wide = monthly.pivot(index='Estacao', columns='Mes', values='p_i').fillna(0)

        # Calcular MSI real: MSI = 0.5 * ∑|p_i - 1/12|
        msi = wide.apply(lambda row: 0.5 * sum(abs(p - 1/12) for p in row), axis=1).reset_index()
        msi.columns = ['Estacao', 'MSI']

        # Armazenar dados para o boxplot combinado
        if 'Estado' in df.columns:
            # Juntar dados de MSI com estados
            estacoes_info = df.groupby('Estacao').first().reset_index()[['Estacao', 'Estado']]
            msi_com_info = msi.merge(estacoes_info, on='Estacao', how='left')
            msi_com_info['Poluente'] = poluente
            
            # Adicionar ao dicionário de dados combinados
            if poluente in msi_data:
                msi_data[poluente] = pd.concat([msi_data[poluente], msi_com_info])
            else:
                msi_data[poluente] = msi_com_info

        # Ordenar por MSI e selecionar top 20
        msi_top20 = msi.sort_values('MSI', ascending=False).dropna().head(20)

        # Plotar gráfico com as 20 principais
        plt.figure(figsize=(12, 6))
        sns.barplot(
            data=msi_top20,
            x='Estacao',
            y='MSI',
            color='skyblue'
        )
        plt.title(f'Índice de Sazonalidade (Top 20) - {poluente}')
        plt.xlabel('Estação')
        plt.ylabel('MSI')
        plt.xticks(rotation=45, ha='right')
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, f"markham_index_{poluente}.png"), dpi=200)
        plt.close()

    # 3. Mapa de Índice de Markham por estado (foco nas regiões metropolitanas)
    if 'Estado' in df.columns and 'Latitude' in df.columns and 'Longitude' in df.columns:
        # Juntar dados de MSI com coordenadas das estações
        estacoes_info = df.groupby('Estacao').first().reset_index()[['Estacao', 'Longitude', 'Latitude', 'Estado']]
        msi_com_info = msi.merge(estacoes_info, on='Estacao', how='left')
        
        # Converter para GeoDataFrame
        gdf = gpd.GeoDataFrame(
            msi_com_info,
            geometry=gpd.points_from_xy(msi_com_info.Longitude, msi_com_info.Latitude),
            crs="EPSG:4326"
        ).to_crs(epsg=3857)

        # Definir regiões metropolitanas com bounding boxes
        regioes_metropolitanas = {
            "SP": (-47.16, -24.11, -45.8, -23.21),
            "MG": (-44.4, -20.7, -42.1, -19.2),
            "RJ": (-43.8, -23.1, -42.99, -22.6),
            "BA": (-38.65, -13.15, -38.1, -12.525)
        }
        
        # Criar colormap personalizado: do ciano (#00FFFF) ao roxo (#800080)
        cmap_cyan_purple = mpl.colors.LinearSegmentedColormap.from_list(
            'cyan_to_purple', 
            ['#00FFFF', '#800080']
        )
        
        # Para cada estado, criar um mapa individual
        estados_presentes = gdf['Estado'].unique()
        
        # plot do MSI
        for estado in estados_presentes:
            print(f"Gerando mapa de Markham para {estado} - {poluente}")
            
            # Filtrar estações deste estado
            gdf_estado = gdf[gdf['Estado'] == estado]
            
            # Obter shape do estado
            estado_shp = brazil_states[brazil_states['postal'] == estado]
            
            if estado_shp.empty:
                print(f"  Shapefile não encontrado para {estado}")
                continue
                
            if gdf_estado.empty:
                print(f"  Nenhuma estação encontrada em {estado}")
                continue
                
            # Verificar se estado tem região metropolitana definida
            tem_rm = estado in regioes_metropolitanas
                
            # Configurar plot - criar figura com 1 ou 2 subplots
            if tem_rm:
                fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(24, 10))
            else:
                fig, ax1 = plt.subplots(figsize=(12, 10))
            
            # Lista de eixos que serão preenchidos
            axes = [ax1]
            if tem_rm:
                axes.append(ax2)
            
            # Plotar mapa do estado completo
            estado_shp.plot(ax=ax1, color='#f0f0f0', edgecolor='#636363', linewidth=1.2, zorder=1)
            municipios_estado = municipios[municipios['SIGLA_UF'] == estado]
            municipios_estado.plot(ax=ax1, color='white', edgecolor='#d9d9d9', linewidth=0.3, alpha=0.7)
            
            # Plotar estações com índice de Markham
            sc = gdf_estado.plot(
                ax=ax1,
                column='MSI',
                cmap=cmap_cyan_purple,
                vmin=0,
                vmax=1,
                markersize=80,
                edgecolor='black',
                linewidth=0.5,
                legend=True,
                legend_kwds={
                    'shrink': 0.6, 
                    'label': "Índice de Markham",
                    'format': "%.2f"
                }
            )
            
            # Definir limites pelo território do estado
            minx, miny, maxx, maxy = estado_shp.total_bounds
            ax1.set_xlim(minx, maxx)
            ax1.set_ylim(miny, maxy)
            
            # Destacar região metropolitana se aplicável
            rm_patch = None  # Para armazenar o objeto do retângulo
            if tem_rm:
                bbox = regioes_metropolitanas[estado]
                minx, miny, maxx, maxy = bbox
                bbox_4326 = box(minx, miny, maxx, maxy)
                bbox_3857 = gpd.GeoSeries([bbox_4326], crs='EPSG:4326').to_crs('EPSG:3857')
                minx_rm, miny_rm, maxx_rm, maxy_rm = bbox_3857.total_bounds
                
                # Criar retângulo para região metropolitana
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
                rm_patch = rect1  # Salvar para usar na legenda
                
                # Plotar mapa da região metropolitana (zoom)
                estado_shp.plot(ax=ax2, color='#f0f0f0', edgecolor='#636363', linewidth=0.8)
                municipios_estado.plot(ax=ax2, color='white', edgecolor='#d9d9d9', linewidth=0.3, alpha=0.7)
                
                # Plotar estações com índice de Markham (mesmo estilo)
                gdf_estado.plot(
                    ax=ax2,
                    column='MSI',
                    cmap=cmap_cyan_purple,
                    vmin=0,
                    vmax=1,
                    markersize=80,
                    edgecolor='black',
                    linewidth=0.5,
                    legend=False  # Não repetir a legenda
                )
                
                # Definir limites para a região metropolitana
                ax2.set_xlim(minx_rm, maxx_rm)
                ax2.set_ylim(miny_rm, maxy_rm)
                ax2.set_title("Área com maior densidade de estações", fontsize=12)
                
                # CORREÇÃO: Criar novo retângulo em vez de copiar
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

                # Filtrar municípios visíveis na região metropolitana
                municipios_visiveis = municipios_estado.cx[minx_rm:maxx_rm, miny_rm:maxy_rm]

                # Verificar a coluna de nome do município
                nome_col = 'NM_MUN' if 'NM_MUN' in municipios_visiveis.columns else 'NOME'

                # Adicionar anotações para cada município
                for idx, municipio in municipios_visiveis.iterrows():
                    try:
                        # Calcular centroide
                        centroid = municipio.geometry.centroid
                        
                        # Adicionar texto
                        ax2.annotate(
                            text=municipio[nome_col],
                            xy=(centroid.x, centroid.y),
                            fontsize=6,
                            color='black',
                            ha='center',
                            va='center',
                            backgroundcolor='none',
                            alpha=0.8,
                            zorder=1000  # Garantir que fique por cima
                        )
                    except Exception as e:
                        print(f"Erro ao anotar município: {e}")
                        continue
            
            # Configurar título e layout
            fig.suptitle(f"Índice de Sazonalidade de Markham (MSI) - {estado} - {poluente}", fontsize=16)
            
            # Adicionar legenda única para o retângulo vermelho
            if tem_rm and rm_patch:
                # Criar um patch customizado para a legenda
                from matplotlib.patches import Patch
                rm_legend = Patch(facecolor='none', edgecolor='red', linestyle='--', 
                                 linewidth=1.8, label='Área de foco')
                
                # Adicionar legenda em um local que não sobreponha
                ax1.legend(handles=[rm_legend], loc='lower left')
            
            # Desligar eixos para todos os subplots
            for ax in axes:
                ax.axis('off')
            
            plt.tight_layout(rect=[0, 0, 1, 0.96])  # Ajustar para suptitle
            
            # Salvar mapa
            estado_nome = estado.replace("/", "_").replace(" ", "_")
            plt.savefig(os.path.join(OUTPUT_DIR, f"markham_map_{estado_nome}_{poluente}.png"), dpi=300)
            plt.close()

# ========================================================================================
# BOXPLOT POR ESTADO COM MSI (TODOS POLUENTES EM UMA FIGURA)
# ========================================================================================
print("Gerando boxplots combinados para todos poluentes")

# Configurar figura principal
poluentes = list(msi_data.keys())
n_pols = len(poluentes)
ncols = 2
nrows = (n_pols + 1) // ncols
fig, axs = plt.subplots(nrows, ncols, figsize=(20, 10 * nrows))
axs = axs.flatten()

# Para cada poluente
for i, pol in enumerate(poluentes):
    ax = axs[i]
    df_pol = msi_data[pol]
    
    # Ordenar estados por mediana do MSI (do maior para o menor)
    ordem_estados = df_pol.groupby('Estado')['MSI'].median().sort_values(ascending=False).index
    
    # Boxplot por estado
    sns.boxplot(
        data=df_pol,
        x='Estado',
        y='MSI',
        order=ordem_estados,
        ax=ax,
        palette="viridis"
    )
    
    # Configurar título e rótulos
    ax.set_title(f"Distribuição do MSI por Estado - {pol}", fontsize=20)
    ax.set_xlabel("Estado", fontsize=16)
    ax.set_ylabel("MSI", fontsize=16)
    ax.tick_params(axis='x', rotation=45)
    ax.grid(axis='y', alpha=0.3)
    
    # Adicionar pontos individuais para estações
    sns.stripplot(
        data=df_pol,
        x='Estado',
        y='MSI',
        order=ordem_estados,
        ax=ax,
        color='black',
        alpha=0.5,
        size=4,
        jitter=True
    )

# Ocultar eixos extras
for j in range(i + 1, len(axs)):
    axs[j].axis('off')

# Ajustar layout e salvar
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "boxplot_MSI_por_estado_todos_poluentes.png"), dpi=300, bbox_inches='tight')
plt.close()
print("Boxplots combinados salvos com sucesso!")
    
print("Análise de sazonalidade concluída! Resultados salvos em:", OUTPUT_DIR)