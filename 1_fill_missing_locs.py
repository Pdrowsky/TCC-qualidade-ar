import polars as pl
import os

data_folder = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\data_poluentes_parquet'

poluentes = ['CO', 'MP2.5', 'MP10', 'NO2', 'O3', 'SO2']

estacoes_sem_coord = set()

for poluente in poluentes:
    print('checando poluente')
    # Carrega o arquivo parquet
    data_path = os.path.join(data_folder, f'{poluente}.parquet')
    data = pl.read_parquet(data_path, columns=['Estacao'])

    # Cria as colunas Latitude e Longitude

    data = data.with_columns([
        pl.lit(None).alias('Latitude'),
        pl.lit(None).alias('Longitude')
    ])

    # Filtra linhas com LATITUDE ou LONGITUDE nulas
    missing_coords = data.filter(
        pl.col('Latitude').is_null() | pl.col('Longitude').is_null()
    )

    # Extrai estações únicas e adiciona ao set
    estacoes = missing_coords.select('Estacao').unique().to_series().to_list()
    estacoes_sem_coord.update(estacoes)
    print('terminou de checar o poluente')

# Converte o set em DataFrame Polars
df_estacoes_sem_coord = pl.DataFrame({'Estacao': list(estacoes_sem_coord)})

# adicionando as coordenadas faltantes aos dataframes -------------------

# Caminho do arquivo de coordenadas
coords_path = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\support_data\Mapa de estações de monitoramento_data.csv'
coords_df = pl.read_csv(coords_path, separator=';')

# Renomeia a coluna 'Estacao1' para 'Estacao' para facilitar o join
coords_df = coords_df.rename({'Estacao1': 'Estacao'})

# Mantém só as colunas relevantes
coords_df = coords_df.select(['Estacao', 'Latitude', 'Longitude'])

# Caminho dos resultados parciais
path_locs_parciais = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\data_poluentes_parquet'
lista_poluentes = ['O3', 'SO2']

path_locs_completas = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\resultados_poluentes_parquet_loc'

for poluente in lista_poluentes:
    print('fazendo join')
    path_doc = os.path.join(path_locs_parciais, f'{poluente}.parquet')

    # Carrega os dados
    df_loc_parcial = pl.read_parquet(path_doc)

    # Faz o join com coordenadas
    df_loc_parcial = df_loc_parcial.join(coords_df, on='Estacao', how='left')

    # seleciona apenas linhas onde Latitude e Longitude não são nulas
    df_loc_parcial = df_loc_parcial.filter(
        (pl.col("Latitude").is_not_null()) & (pl.col("Longitude").is_not_null())
    )

    # (Opcional) Salva o resultado com coordenadas incluídas
    df_loc_parcial.write_parquet(os.path.join(path_locs_completas, f'{poluente}_result_com_coords.parquet'))

    print(f'{poluente}: join com coordenadas completo.')