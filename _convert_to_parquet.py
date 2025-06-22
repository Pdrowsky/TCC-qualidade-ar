import polars as pl

path_dados = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\data.csv"

pl.read_csv(path_dados, separator=",").write_parquet(r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\data.parquet")