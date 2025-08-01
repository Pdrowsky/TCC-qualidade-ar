import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from calendar import month_abbr
from matplotlib.ticker import MaxNLocator

# Configurações
DATA_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_violacoes_completo"
OUTPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\graphtable_mensal_violacoes"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Processando dados para análise mensal de violações...")

# Para cada arquivo (cada poluente)
for file_name in os.listdir(DATA_DIR):
    if not file_name.endswith('.parquet'):
        continue
    
    file_path = os.path.join(DATA_DIR, file_name)
    df = pd.read_parquet(file_path)
    poluente = df['Poluente'].iloc[0]
    
    # Converter data
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Extrair ano e mês
    df['Ano'] = df['Date'].dt.year
    df['Mes'] = df['Date'].dt.month
    
    # Identificar padrões
    padroes = [col for col in df.columns if col.startswith('exceed_')]
    
    for padrao in padroes:
        nome_padrao = padrao.split('_')[-1]
        
        # Agregar violações por mês/ano
        agg = df.groupby(['Ano', 'Mes']).agg(
            total_violacoes=(padrao, 'sum'),
            total_medicoes=(padrao, 'count')
        ).reset_index()
        
        agg['taxa_violacao'] = agg['total_violacoes'] / agg['total_medicoes'] * 100
        
        # Salvar tabela
        agg.to_csv(
            os.path.join(OUTPUT_DIR, f"violacoes_mensais_{poluente}_{nome_padrao}.csv"),
            index=False,
            encoding='utf-8-sig'
        )
        
        # Calcular média histórica por mês
        media_mensal = agg.groupby('Mes').agg(
            media_violacoes=('total_violacoes', 'mean'),
            media_taxa=('taxa_violacao', 'mean')
        ).reset_index()
        
        # Gráfico de calor de violações por mês/ano
        pivot_table = agg.pivot_table(
            index='Ano', 
            columns='Mes', 
            values='total_violacoes', 
            fill_value=0
        )
        
        plt.figure(figsize=(12, 8))
        sns.heatmap(
            pivot_table, 
            cmap="YlOrRd", 
            annot=True, 
            fmt=".0f", 
            linewidths=.5
        )
        plt.title(f'Violações por Mês/Ano - {poluente} (Padrão: {nome_padrao})')
        plt.xlabel('Mês')
        plt.ylabel('Ano')
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, f"heatmap_mensal_{poluente}_{nome_padrao}.png"), dpi=200)
        plt.close()
        
        # Gráfico de linha da média mensal histórica
        plt.figure(figsize=(10, 6))
        plt.plot(
            media_mensal['Mes'], 
            media_mensal['media_violacoes'], 
            marker='o', 
            color='#e15759'
        )
        plt.title(f'Média Histórica de Violações por Mês - {poluente} (Padrão: {nome_padrao})')
        plt.xlabel('Mês')
        plt.ylabel('Número Médio de Violações')
        plt.xticks(range(1,13), labels=list(month_abbr)[1:13])
        plt.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, f"media_mensal_{poluente}_{nome_padrao}.png"), dpi=200)
        plt.close()

    # GRÁFICO FINAL DE BARRAS EMPILHADAS POR MÊS/PATAMAR (UMA FIGURA POR POLUENTE)
    if padroes:
        fig, axs = plt.subplots(
            nrows=len(padroes),
            ncols=1,
            figsize=(14, 3.5 * len(padroes)),  # altura mais achatada
            constrained_layout=True  # melhora o uso do espaço
        )
        if len(padroes) == 1:
            axs = [axs]

        for idx, padrao in enumerate(padroes):
            nome_padrao = padrao.split('_')[-1]

            # Agregar por mês
            agg_padrao = df.groupby('Mes').agg(
                violacoes=(padrao, 'sum'),
                total=(padrao, 'count')
            ).reset_index()

            agg_padrao['nao_violacoes'] = agg_padrao['total'] - agg_padrao['violacoes']
            agg_padrao['percent_viol'] = agg_padrao['violacoes'] / agg_padrao['total'] * 100
            agg_padrao['percent_ok'] = 100 - agg_padrao['percent_viol']

            ax = axs[idx]
            if poluente in ['CO', 'NO2']:
                ax.set_yscale('log')
                ax.set_ylim(1, agg_padrao['total'].max() * 1.2)  # Ajustar limite Y para log

            # Plotar barras (violação embaixo, não violação em cima)
            bar1 = ax.bar(
                agg_padrao['Mes'],
                agg_padrao['violacoes'],
                label='Violação',
                color='red'
            )
            bar2 = ax.bar(
                agg_padrao['Mes'],
                agg_padrao['nao_violacoes'],
                bottom=agg_padrao['violacoes'],
                label='Não violação',
                color='lightgreen'
            )

            # Anotar número e percentual acima da barra vermelha (violação)
            for i in range(len(agg_padrao)):
                viol = agg_padrao.loc[i, 'violacoes']
                percent = agg_padrao.loc[i, 'percent_viol']
                altura = viol
                if altura > 0:
                    ax.text(
                        agg_padrao.loc[i, 'Mes'],
                        altura + altura * 0.03,
                        f'{viol}\n({percent:.1f}%)',
                        ha='center',
                        va='bottom',
                        fontsize=13
                    )

            ax.set_title(f'Violações {nome_padrao} - {poluente}', fontsize=17)
            ax.set_xticks(range(1, 13))
            ax.set_xticklabels(list(month_abbr)[1:13], fontsize=13)
            ax.set_ylabel('Número de Medições', fontsize=14)
            ax.tick_params(axis='y', labelsize=13)
            ax.grid(alpha=0.3)
            ax.legend(fontsize=13)

            # Reduzir número de ticks no eixo Y
            yticks = ax.get_yticks()
            ax.set_yticks(yticks[::2])

        plt.savefig(os.path.join(OUTPUT_DIR, f"violacoes_empilhadas_{poluente}.png"), dpi=200)
        plt.close()

print("Análise mensal de violações concluída! Resultados salvos em:", OUTPUT_DIR)