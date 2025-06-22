import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Configurações
DATA_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_violacoes_completo"
OUTPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\graphtable_frequencia_violacoes"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Processando dados para análise de frequência de violações...")

# Para cada arquivo (cada poluente)
for file_name in os.listdir(DATA_DIR):
    if not file_name.endswith('.parquet'):
        continue
    
    file_path = os.path.join(DATA_DIR, file_name)
    df = pd.read_parquet(file_path)
    poluente = df['Poluente'].iloc[0]
    
    # Identificar padrões
    padroes = [col for col in df.columns if col.startswith('exceed_')]
    
    # Tabela para armazenar resultados
    resultados_gerais = []
    resultados_estacoes = []
    
    for padrao in padroes:
        # Calcular total de medições e violações
        total_medicoes = len(df)
        total_violacoes = df[padrao].sum()
        taxa = total_violacoes / total_medicoes * 100
        
        # Armazenar resultados gerais
        resultados_gerais.append({
            'Poluente': poluente,
            'Padrao': padrao.split('_')[-1],
            'Total_Medicoes': total_medicoes,
            'Total_Violacoes': total_violacoes,
            'Taxa_Violacao_Perc': taxa
        })
        
        # Calcular por estação
        agg = df.groupby(['Estacao', 'Latitude', 'Longitude']).agg(
            total_violacoes=(padrao, 'sum'),
            total_medicoes=(padrao, 'count')
        ).reset_index()
        
        agg['taxa_violacao'] = agg['total_violacoes'] / agg['total_medicoes'] * 100
        agg['Poluente'] = poluente
        agg['Padrao'] = padrao.split('_')[-1]
        
        resultados_estacoes.append(agg)
    
    # Salvar tabelas por poluente
    df_geral = pd.DataFrame(resultados_gerais)
    df_geral.to_csv(
        os.path.join(OUTPUT_DIR, f"frequencia_geral_{poluente}.csv"),
        index=False,
        encoding='utf-8-sig'
    )
    
    df_estacoes = pd.concat(resultados_estacoes)
    df_estacoes.to_csv(
        os.path.join(OUTPUT_DIR, f"frequencia_estacoes_{poluente}.csv"),
        index=False,
        encoding='utf-8-sig'
    )
    
    # Gráfico de barras para taxas de violação por padrão
    plt.figure(figsize=(10, 6))
    plt.bar(df_geral['Padrao'], df_geral['Taxa_Violacao_Perc'], color='#ff6b6b')
    plt.title(f'Taxa de Violação por Padrão - {poluente}')
    plt.xlabel('Padrão de Qualidade')
    plt.ylabel('Taxa de Violação (%)')
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f"taxa_padroes_{poluente}.png"), dpi=200)
    plt.close()
    
    # Top 10 estações com maior taxa de violação
    for padrao in df_estacoes['Padrao'].unique():
        df_padrao = df_estacoes[df_estacoes['Padrao'] == padrao]
        top_estacoes = df_padrao.sort_values('taxa_violacao', ascending=False).head(10)
        
        plt.figure(figsize=(12, 6))
        plt.barh(
            top_estacoes['Estacao'], 
            top_estacoes['taxa_violacao'],
            color='#4e79a7'
        )
        plt.title(f'Top 10 Estações - {poluente} (Padrão: {padrao})')
        plt.xlabel('Taxa de Violação (%)')
        plt.gca().invert_yaxis()
        plt.grid(axis='x', alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, f"top_estacoes_{poluente}_{padrao}.png"), dpi=200)
        plt.close()

print("Análise de frequência de violações concluída! Resultados salvos em:", OUTPUT_DIR)