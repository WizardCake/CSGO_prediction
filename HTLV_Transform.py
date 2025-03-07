import os
import pandas as pd

# Verifica se o arquivo NDJSON existe e não está vazio
ndjson_file = 'data/extração_partidas.ndjson'
if not os.path.exists(ndjson_file) or os.path.getsize(ndjson_file) == 0:
    print("Arquivo NDJSON não encontrado ou vazio. Encerrando.")
    exit(0)

# Read NDJSON File
data = pd.read_json(ndjson_file, lines=True)

# Se já existir um CSV com dados processados, filtra os registros duplicados
csv_file = 'data/transformacao_intermediaria.csv'
if os.path.exists(csv_file):
    processed_data = pd.read_csv(csv_file, sep=';')
    if 'match_url' in processed_data.columns:
        processed_links = set(processed_data['match_url'])
        data = data[~data['link'].isin(processed_links)]
        if data.empty:
            print("Nenhum dado novo para processar. Encerrando.")
            exit(0)

# Tratamento
# Define Match_ID
data['match_id'] = data['link'].str.extract(r'matches/(\d+)')

# Remove linhas inválidas
data.dropna(subset=['match_id', 'jogo'], inplace=True)

# Split da coluna 'jogo'
data['jogo_split'] = data['jogo'].str.split('\n', expand=False)

# Garante que a lista tenha pelo menos 5 elementos antes de expandir
data['jogo_split'] = data['jogo_split'].apply(lambda x: x + [None] * (5 - len(x)) if isinstance(x, list) else [None] * 5)

data[['time_1', 'resultado', 'time_2', 'competicao', 'best_of']] = pd.DataFrame(data['jogo_split'].tolist(), index=data.index)

# Preenche valores nulos antes de dividir o resultado
data['resultado'] = data['resultado'].fillna('0 x 0')

# Split do resultado
data['resultado_split'] = data['resultado'].str.split(' ')

# Verifica se todas as listas têm pelo menos 3 elementos; caso contrário, define valores padrão
data['resultado_split'] = data['resultado_split'].apply(lambda x: x if isinstance(x, list) and len(x) == 3 else ['0', 'x', '0'])

# Divide os valores do resultado
data[['score_t1', '_', 'score_t2']] = pd.DataFrame(data['resultado_split'].tolist(), index=data.index)
data.drop(columns=['_'], inplace=True)  # Remove coluna extra do separador 'x'

# Drop de colunas desnecessárias e remoção de NaNs
data.drop(columns=['jogo', 'jogo_split', 'resultado', 'resultado_split'], inplace=True)
data.dropna(inplace=True)
data.reset_index(drop=True, inplace=True)

# Renomeia colunas para padronização
data.rename(columns={
    'link': 'match_url',
    'time_1': 'team_A',
    'time_2': 'team_B',
    'score_t1': 'score_tA',
    'score_t2': 'score_tB',
    'competicao': 'competition',
    'best_of': 'type_of_match'
}, inplace=True)

# Salva os dados processados:
# Se o arquivo CSV já existe, os novos registros são adicionados (append) sem cabeçalho.
# Caso contrário, um novo arquivo é criado.
if os.path.exists(csv_file):
    data.to_csv(csv_file, sep=';', index=False, mode='a', header=False)
else:
    data.to_csv(csv_file, sep=';', index=False)
