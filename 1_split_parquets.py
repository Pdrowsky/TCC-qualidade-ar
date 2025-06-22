import polars as pl
import os
import math

# ========== CONFIGURATION ==========
input_folder = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\resultados_poluentes_parquet_loc'
output_folder = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_chunks_com_loc'
n_splits = 5  # Number of chunks
# ===================================

os.makedirs(output_folder, exist_ok=True)

for file_name in os.listdir(input_folder):
    if file_name.endswith('.parquet'):
        file_path = os.path.join(input_folder, file_name)
        base_name = os.path.splitext(file_name)[0]

        print(f"\nProcessando {file_name}...")

        # Lê o DataFrame completo
        df = pl.read_parquet(file_path)

        # Obtém todas as estações únicas
        estacoes = df.select("Estacao").unique().to_series().to_list()
        estacoes.sort()  # Para garantir consistência

        # Divide as estações em n_splits grupos quase iguais
        tamanho_grupo = math.ceil(len(estacoes) / n_splits)
        estacoes_split = [estacoes[i:i + tamanho_grupo] for i in range(0, len(estacoes), tamanho_grupo)]

        # Cria subpasta para o arquivo
        subfolder = os.path.join(output_folder, base_name)
        os.makedirs(subfolder, exist_ok=True)

        for i, grupo_estacoes in enumerate(estacoes_split):
            # Filtra o DataFrame com base nas estações do grupo
            df_chunk = df.filter(pl.col("Estacao").is_in(grupo_estacoes))

            output_path = os.path.join(subfolder, f'part_{i+1}.parquet')
            df_chunk.write_parquet(output_path)

            print(f"  -> Salvo {output_path} ({df_chunk.height} linhas, {len(grupo_estacoes)} estações)")
