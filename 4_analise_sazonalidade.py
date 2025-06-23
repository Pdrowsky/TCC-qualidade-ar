import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Configurações
DATA_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_violacoes_completo"
OUTPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\graphtable_analise_sazonalidade"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Processando dados para análise de sazonalidade...")

# Para cada poluente
for file_name in os.listdir(DATA_DIR):
    if not file_name.endswith('.parquet'):
        continue
    
    poluente = file_name.split('.')[0].split('_')[0]
    file_path = os.path.join(DATA_DIR, file_name)
    df = pd.read_parquet(file_path)
    
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
    
    # Boxplot por estação do ano com limites
    plt.figure(figsize=(12, 8))
    ax = sns.boxplot(
        data=df, 
        x='Estacao_Ano', 
        y='Valor_Padronizado', 
        order=['Verão','Outono','Inverno','Primavera']
    )
    
    # Extrair limites do dataframe
    limites_cols = ['PI-1', 'PI-2', 'PI-3', 'PI-4', 'PF']
    limites = []
    for col in limites_cols:
        if col in df.columns:
            # Pegar primeiro valor não nulo
            valor = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if valor is not None:
                limites.append(valor)
    
    # Adicionar linhas horizontais para os limites
    if limites:
        cores = ['green', 'blue', 'orange', 'red', 'purple']
        rotulos = ['Padrão I', 'Padrão II', 'Padrão III', 'Padrão IV', 'Padrão Final']
        
        for i, (limite, cor, rotulo) in enumerate(zip(limites, cores, rotulos)):
            plt.axhline(
                y=limite, 
                color=cor, 
                linestyle='--', 
                alpha=0.7,
                label=f'{rotulo} ({limite} {"µg/m³" if poluente != "CO" else "ppm"})'
            )
    
    plt.title(f'Distribuição por Estação do Ano - {poluente}')
    plt.xlabel('Estação do Ano')
    plt.ylabel(f'Concentração ({"µg/m³" if poluente != "CO" else "ppm"})')
    plt.legend(loc='upper right', fontsize=9)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f"boxplot_estacao_{poluente}.png"), dpi=200)
    plt.close()
    
    # Análise de sazonalidade das violações
    padroes_exceed = [col for col in df.columns if col.startswith('exceed_')]
    padroes = [col.split('_')[1] for col in padroes_exceed]  # Extrair nomes dos padrões
    
    if padroes_exceed:
        # Calcular taxa de violação por estação do ano e padrão
        violacoes = []
        for padrao_col, padrao_nome in zip(padroes_exceed, padroes):
            agg = df.groupby('Estacao_Ano').agg(
                taxa_violacao=(padrao_col, 'mean')
            ).reset_index()
            agg['Padrao'] = padrao_nome
            violacoes.append(agg)
        
        df_violacoes = pd.concat(violacoes)
        
        # Salvar tabela de violações
        df_violacoes.to_csv(
            os.path.join(OUTPUT_DIR, f"violacoes_sazonais_{poluente}.csv"),
            index=False,
            encoding='utf-8-sig'
        )
        
        # Gráfico de boxplot das violações por estação
        plt.figure(figsize=(12, 8))
        sns.boxplot(
            data=df, 
            x='Estacao_Ano', 
            y='Valor_Padronizado', 
            order=['Verão','Outono','Inverno','Primavera'],
            showfliers=False
        )
        
        # Adicionar limites no gráfico de violações
        if limites:
            for i, (limite, cor, rotulo) in enumerate(zip(limites, cores, rotulos)):
                plt.axhline(
                    y=limite, 
                    color=cor, 
                    linestyle='--', 
                    alpha=0.7,
                    label=f'{rotulo} ({limite})'
                )
        
        # Adicionar pontos para violações
        for padrao_col, cor in zip(padroes_exceed, cores):
            violacoes = df[df[padrao_col] == 1]
            if not violacoes.empty:
                # Jitter para evitar sobreposição
                jitter = np.random.uniform(-0.2, 0.2, size=len(violacoes))
                posicoes = {
                    'Verão': 0,
                    'Outono': 1,
                    'Inverno': 2,
                    'Primavera': 3
                }
                pos = violacoes['Estacao_Ano'].map(posicoes) + jitter
                
                plt.scatter(
                    pos,
                    violacoes['Valor_Padronizado'],
                    alpha=0.3,
                    s=20,
                    color=cor,
                    label=f'Viol. {padrao_col.split("_")[1]}'
                )
        
        plt.title(f'Concentrações e Violações por Estação - {poluente}')
        plt.xlabel('Estação do Ano')
        plt.ylabel(f'Concentração ({"µg/m³" if poluente != "CO" else "ppm"})')
        plt.legend(loc='upper right', fontsize=9)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, f"violacoes_estacao_{poluente}.png"), dpi=200)
        plt.close()
        
        # Gráfico de barras para taxas de violação
        plt.figure(figsize=(12, 8))
        sns.barplot(
            data=df_violacoes,
            x='Estacao_Ano',
            y='taxa_violacao',
            hue='Padrao',
            order=['Verão','Outono','Inverno','Primavera'],
            palette=cores[:len(padroes)]  # Usar cores consistentes com os limites
        )
        plt.title(f'Taxa de Violação por Estação e Padrão - {poluente}')
        plt.xlabel('Estação do Ano')
        plt.ylabel('Taxa de Violação')
        plt.legend(title='Padrão', loc='upper right')
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, f"taxa_violacoes_{poluente}.png"), dpi=200)
        plt.close()

    

print("Análise de sazonalidade concluída! Resultados salvos em:", OUTPUT_DIR)