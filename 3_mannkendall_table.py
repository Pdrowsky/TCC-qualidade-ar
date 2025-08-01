import os
import pandas as pd
import numpy as np

# Configurações
INPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\z_testes_mannkendall_ano"
OUTPUT_DIR = r"C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\graphtable_analise_tendencias_ano"

# Criar diretório de saída se não existir
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Listar todos os arquivos de resultados
arquivos = [f for f in os.listdir(INPUT_DIR) if f.endswith('.parquet') and f.startswith('mk_')]

if not arquivos:
    print("Nenhum arquivo de tendência encontrado!")
    exit()

print(f"Encontrados {len(arquivos)} arquivos de tendência\n")

# Processar cada arquivo/poluente
for arquivo in arquivos:
    try:
        # Extrair nome do poluente do nome do arquivo
        poluente = arquivo.split('_')[1].split('.')[0]
        
        # Carregar dados
        df = pd.read_parquet(os.path.join(INPUT_DIR, arquivo))
        
        # Verificar se temos dados
        if df.empty:
            print(f"  Arquivo vazio: {arquivo}")
            continue
        
        print(f"Processando poluente: {poluente}")
        print(f"  Estações encontradas: {len(df)}")
        
        # Selecionar e renomear colunas importantes
        df = df.rename(columns={
            'p_valor': 'Valor_p',
            'slope': 'Inclinação',
            'z': 'Z-Score',
            'Tau': 'Tau_Kendall',
            'Tendencia': 'Direção_Tendência'
        })
        
        # Reordenar colunas
        colunas = ['Poluente', 'Estado', 'Estacao', 'Latitude', 'Longitude', 
                   'Direção_Tendência', 'Inclinação', 'Valor_p', 'Z-Score', 
                   'Tau_Kendall', 'n_dias']
        
        # Adicionar coluna de poluente
        df['Poluente'] = poluente
        
        # Selecionar apenas colunas desejadas
        df = df[colunas].copy()
        
        # Converter tendência para português
        df['Direção_Tendência'] = df['Direção_Tendência'].map({
            'increasing': 'Crescente',
            'decreasing': 'Decrescente',
            'no trend': 'Sem tendência'
        })
        
        # Adicionar significância estatística
        df['Significância'] = np.where(df['Valor_p'] < 0.05, 'Significativo', 'Não significativo')
        
        # Calcular magnitude da tendência (classificação)
        conditions = [
            (df['Inclinação'].abs() < 0.01),
            (df['Inclinação'].abs() < 0.05),
            (df['Inclinação'].abs() < 0.1),
            (df['Inclinação'].abs() >= 0.1)
        ]
        choices = ['Muito fraca', 'Fraca', 'Moderada', 'Forte']
        df['Magnitude'] = np.select(conditions, choices, default='Não definida')
        
        # Ordenar resultados por inclinação (maiores tendências primeiro)
        df = df.sort_values(by='Inclinação', ascending=False)
        
        # Salvar como CSV
        output_file = os.path.join(OUTPUT_DIR, f"tendencias_{poluente}.csv")
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"  Resultados salvos: {output_file}")
        print(f"  Tendência média: {df['Inclinação'].mean():.5f} unidades/ano")
        print(f"  Estações com tendência significativa: {len(df[df['Significância'] == 'Significativo'])}")
        print(f"  Proporção de tendências crescentes: {len(df[df['Direção_Tendência'] == 'Crescente'])/len(df):.1%}\n")
        
    except Exception as e:
        print(f"Erro ao processar {arquivo}: {str(e)}\n")

print("Processamento concluído!")
print(f"Arquivos salvos em: {OUTPUT_DIR}")