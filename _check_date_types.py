import pandas as pd
import os

file_path = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_violacoes_completo'

for file in os.listdir(file_path):
    if not file.endswith('.parquet'):
        pass
    else:
        path = os.path.join(file_path, file)
        df = pd.read_parquet(path)

        print(f'Lido arquivo {file}')