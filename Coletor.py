import requests
import pandas as pd
import datetime
from google.cloud import bigquery
import os

# Função extrair dados da API de Clima
def weather_get(openweathermap_api_key, origin, destination, units, lang, pais):
    if not origin:
        if destination:
            city = destination
        else:
            return None
    else:
        city = origin

    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={openweathermap_api_key}&units={units}&lang={lang}"
    response = requests.get(url)
    if response.status_code == 200:
        json_temperatura = response.json()
        descricao_tempo = json_temperatura["weather"][0]["description"]
        temperatura_tempo = json_temperatura["main"]["temp"]
        temperatura_max_tempo = json_temperatura["main"]["temp_max"]
        temperatura_min_tempo = json_temperatura["main"]["temp_min"]
        sensacao_termica_tempo = json_temperatura["main"]["feels_like"]
        cidade_captura_tempo = json_temperatura["name"]
        pais_tempo = pais
        data_captura_tempo = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return [
            descricao_tempo, temperatura_tempo, temperatura_max_tempo, 
            temperatura_min_tempo, sensacao_termica_tempo, data_captura_tempo, 
            cidade_captura_tempo, pais_tempo
        ]
    else:
        print(f"Erro na chamada à API: {response.status_code} - {response.text}")
        return None

# Função para extrair dados da API de Trânsito
def google_traffic_get(google_maps_api_key, origin, pais_origem, destination, pais_destino, language, units, mode):
    url = f"https://maps.googleapis.com/maps/api/directions/json?origin={origin}+{pais_origem}&destination={destination}+{pais_destino}&key={google_maps_api_key}&language={language}&units={units}&mode={mode}"
    response = requests.get(url)
    if response.status_code == 200:
        json_transporte = response.json()
        duracao_transporte = json_transporte["routes"][0]["legs"][0]["duration"]["text"]
        distancia_transporte = json_transporte["routes"][0]["legs"][0]["distance"]["text"]
        paradas_transporte = json_transporte['routes'][0]['legs'][0]['steps']
        modo_transporte = mode
        origem_transporte = origin
        pais_origem_transporte = pais_origem
        destino_transporte = destination
        pais_destino_transporte = pais_destino
        total_paradas_transporte = len(paradas_transporte)
        data_captura_transporte = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        
        return [
            duracao_transporte, distancia_transporte,
            total_paradas_transporte, modo_transporte, origem_transporte, destino_transporte,
            data_captura_transporte, pais_origem_transporte, pais_destino_transporte
        ]
    else:
        print(f"Erro na chamada à API: {response.status_code} - {response.text}")
        return None

# Função para carga dos dados no BigQuery
def carga_bigquery(df, table_id, schema):
    v_ambiente = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    client = bigquery.Client.from_service_account_json(f'{v_ambiente}')
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Dados carregados na tabela {table_id}")

# Configurações
# Lista de todas as capitais do Brasil
capitais_brasil = ["Rio Branco", "Maceió", "Macapá", "Manaus", "Salvador", "Fortaleza", "Brasília",
                   "Vitória", "Goiânia", "São Luís", "Cuiabá", "Campo Grande", "Belo Horizonte",
                   "Belém", "João Pessoa", "Curitiba", "Recife", "Teresina", "Rio de Janeiro",
                   "Natal", "Porto Alegre", "Porto Velho", "Boa Vista", "Florianópolis",
                   "São Paulo", "Aracaju", "Palmas"]

# Lista para armazenar os DataFrames de temperatura e tráfego
temperatura_dfs = []
transporte_dfs = []

# Configurações
openweathermap_api_key = os.getenv('OPEN_WEATHER_KEY')
google_maps_api_key = os.getenv('GOOGLE_MAPS_KEY') 
pais = "Brasil"
lang = "pt_BR"
language = "pt_BR"
units = "metric"
mode = "driving"
pais_origem = "Brasil"
pais_destino = "Brasil"
## Temperatura
tabela_temperatura = 'mapa-temperatura.Staging_Area.tabela_temperatura' 
schema_temperatura = [
    bigquery.SchemaField('descricao_tempo', 'STRING'),
    bigquery.SchemaField('temperatura_tempo', 'STRING'),
    bigquery.SchemaField('temperatura_max_tempo', 'STRING'),   
    bigquery.SchemaField('temperatura_min_tempo', 'STRING'),
    bigquery.SchemaField('sensacao_termica_tempo', 'STRING'),
    bigquery.SchemaField('data_captura_tempo', 'STRING'),
    bigquery.SchemaField('cidade_captura_tempo', 'STRING'),
    bigquery.SchemaField('pais_tempo', 'STRING'),
    bigquery.SchemaField('alias', 'STRING')
]
## Transporte
tabela_transporte = 'mapa-temperatura.Staging_Area.tabela_transporte' 
schema_transporte = [
    bigquery.SchemaField('duracao_transporte', 'STRING'),
    bigquery.SchemaField('distancia_transporte', 'STRING'),   
    bigquery.SchemaField('total_paradas_transporte', 'STRING'),
    bigquery.SchemaField('modo_transporte', 'STRING'),
    bigquery.SchemaField('origem_transporte', 'STRING'),
    bigquery.SchemaField('destino_transporte', 'STRING'),
    bigquery.SchemaField('data_captura_transporte', 'STRING'),
    bigquery.SchemaField('pais_origem_transporte', 'STRING'),
    bigquery.SchemaField('pais_destino_transporte', 'STRING')
]

# Iteração sobre todas as combinações de origens e destinos
for origin in capitais_brasil:
    for destination in capitais_brasil:
        if origin != destination:
            # Extrai dados de Clima
            temperatura_origem = weather_get(openweathermap_api_key, origin, None, units, lang, pais)
            temperatura_destino = weather_get(openweathermap_api_key, destination, None, units, lang, pais)

            if temperatura_origem and temperatura_destino:
                # Criando um DataFrame para os dados de temperatura da origem
                temperatura_origem_df = pd.DataFrame([temperatura_origem], columns=[
                    "descricao_tempo", "temperatura_tempo", "temperatura_max_tempo", 
                    "temperatura_min_tempo", "sensacao_termica_tempo", "data_captura_tempo", 
                    "cidade_captura_tempo", "pais_tempo"
                ])
                temperatura_origem_df["alias"] = f"temperatura_{origin}"

                # Criando um DataFrame para os dados de temperatura do destino
                temperatura_destino_df = pd.DataFrame([temperatura_destino], columns=[
                    "descricao_tempo", "temperatura_tempo", "temperatura_max_tempo", 
                    "temperatura_min_tempo", "sensacao_termica_tempo", "data_captura_tempo", 
                    "cidade_captura_tempo", "pais_tempo"
                ])
                temperatura_destino_df["alias"] = f"temperatura_{destination}"

                # Concatenando os DataFrames de temperatura
                temperatura_df = pd.concat([temperatura_origem_df, temperatura_destino_df], ignore_index=True)


            # Extrai dados de Trânsito
            traffic_data = google_traffic_get(google_maps_api_key, origin, pais_origem, destination, pais_destino, language, units, mode)
            if traffic_data:
                # Criando um DataFrame para os dados de tráfego
                transporte_df = pd.DataFrame([traffic_data], columns=[
                    "duracao_transporte", "distancia_transporte",
                    "total_paradas_transporte", "modo_transporte", "origem_transporte", 
                    "destino_transporte", "data_captura_transporte", "pais_origem_transporte", 
                    "pais_destino_transporte"
                ])
                
                # Adicionando o DataFrame à lista
                transporte_dfs.append(transporte_df)

# Verifica se há dados para concatenar
if temperatura_dfs:
    temperatura_df_final = pd.concat(temperatura_dfs, ignore_index=True)
    # Converter dados do DF para adequar ao do BigQuery
    temperatura_df_final['descricao_tempo'] = temperatura_df_final['descricao_tempo'].astype(str)
    temperatura_df_final['temperatura_tempo'] = temperatura_df_final['temperatura_tempo'].astype(str)
    temperatura_df_final['temperatura_max_tempo'] = temperatura_df_final['temperatura_max_tempo'].astype(str)
    temperatura_df_final['temperatura_min_tempo'] = temperatura_df_final['temperatura_min_tempo'].astype(str)
    temperatura_df_final['sensacao_termica_tempo'] = temperatura_df_final['sensacao_termica_tempo'].astype(str)
    temperatura_df_final['data_captura_tempo'] = temperatura_df_final['data_captura_tempo'].astype(str)
    temperatura_df_final['cidade_captura_tempo'] = temperatura_df_final['cidade_captura_tempo'].astype(str)
    temperatura_df_final['pais_tempo'] = temperatura_df_final['pais_tempo'].astype(str)
    temperatura_df_final['alias'] = temperatura_df_final['alias'].astype(str)
    carga_bigquery(temperatura_df_final, tabela_temperatura, schema_temperatura)
else:
    print("Nenhum dado de temperatura foi coletado.")

if transporte_dfs:
    transporte_df_final = pd.concat(transporte_dfs, ignore_index=True)
    # Converter dados do DF para adequar ao do BigQuery
    transporte_df_final['duracao_transporte'] = transporte_df_final['duracao_transporte'].astype(str)
    transporte_df_final['distancia_transporte'] = transporte_df_final['distancia_transporte'].astype(str)
    transporte_df_final['total_paradas_transporte'] = transporte_df_final['total_paradas_transporte'].astype(str)
    transporte_df_final['modo_transporte'] = transporte_df_final['modo_transporte'].astype(str)
    transporte_df_final['origem_transporte'] = transporte_df_final['origem_transporte'].astype(str)
    transporte_df_final['destino_transporte'] = transporte_df_final['destino_transporte'].astype(str)
    transporte_df_final['data_captura_transporte'] = transporte_df_final['data_captura_transporte'].astype(str)
    transporte_df_final['pais_origem_transporte'] = transporte_df_final['pais_origem_transporte'].astype(str)
    transporte_df_final['pais_destino_transporte'] = transporte_df_final['pais_destino_transporte'].astype(str)
    carga_bigquery(transporte_df_final, tabela_transporte, schema_transporte)
else:
    print("Nenhum dado de transporte foi coletado.")