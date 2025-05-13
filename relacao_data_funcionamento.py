import pandas as pd

# Load data in chunks if memory is tight (optional)
file_path = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\data.csv'
data = pd.read_csv(file_path)

# Convert date column first
data['Data'] = pd.to_datetime(data['Data'], format='%Y-%m-%d')

# Process time column with vectorized operations
# Convert to string and clean time data
data['Hora'] = data['Hora'].astype(str)

# Add seconds to incomplete times
data['Hora'] = data['Hora'].str.replace(r'(\d+:\d{2})(?::\d{2})?$', r'\1:00', regex=True)

# Handle 24:00:00 cases
is_24h = data['Hora'].str.contains('24:00:00')
data['Hora'] = data['Hora'].str.replace('24:00:00', '00:00:00')

# Increment date where we had 24:00:00
data.loc[is_24h, 'Data'] += pd.DateOffset(days=1)

# Create datetime column using vectorized operation
data['Data_Hora'] = pd.to_datetime(
    data['Data'].dt.strftime('%Y-%m-%d') + ' ' + data['Hora'],
    format='%Y-%m-%d %H:%M:%S'
)

# Group and aggregate
data_grouped = data.groupby(['Estacao', 'Poluente'], as_index=False).agg(
    Data_Hora_Inicio=('Data_Hora', 'min'),
    Data_Hora_Fim=('Data_Hora', 'max')
)

# Save results
output_path = r'C:\Users\pedro\Desktop\UFSC\TCC\TCC-qualidade-ar\proccessed_data\data_funcionamento.csv'
data_grouped.to_csv(output_path, index=False)