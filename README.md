# TempoRota
 Projeto com o intuíto de verificar rotas entre às capitais do Brasil e alertar sobre a situação climatica deles.

## Para rodar o projeto em sua maquina, é preciso configurar algumas váriaveis de ambiente: 
- GOOGLE_APPLICATION_CREDENTIALS: Nessa váriavel encontra-se o caminho das credenciais da conta de serviço criada para acessar as tabelas do BigQuery do projeto. 
- OPEN_WEATHER_KEY: Essa váriavel contem a chave de acesso à API Open Weather, que fornece os dados de tempo.
- GOOGLE_MAPS_KEY: Essa váriavel contem a chave de acesso à API Directions, que fornece os dados de rotas.

## Arquivos do projeto:
- Coletor.py
- ETL_Temperatura.py (A ser desenvolvido)
- ETL_Transporte.py (A ser desenvolvido)
- Unificacao.py (A ser desenvolvido)

## Etapa de construção da base de dados (Em andamento)
### Coletor (Etapa Finalizada)
 Esse Script é responsável por capturar os dados das APIs citadas acima (openweathermap e google maps directions), armazenar esses dados em um dataframe e por fim realizar sua ingestão no conjunto de dados Stagin_Area, que representa o conceito da camada RAW/Bronze em um Data Lake.

### ETL Temperatura (A ser desenvolvido)
 Esse Script, quando concluído, será responsável por realizar a limpeza e tratamento dos dados de temperatura, mantendo suas peculiaridades e realizando sua padronização, após padronização será realizado a etapa de ingestão no conjunto de dados dos dados tratados, que seria o mesmo que a camada Prata, nos conceitos de Data Lake. 

### ETL Transporte (A ser desenvolvido)
 Esse Script, quando concluído, será responsável por realizar a limpeza e tratamento dos dados de rotas, mantendo suas peculiaridades e realizando sua padronização, após padronização será realizado a etapa de ingestão no conjunto de dados dos dados tratados, que seria o mesmo que a camada Prata, nos conceitos de Data Lake.

### Unificação dos dados (A ser desenvolvido)
 Nesse Script ocorre a unificação das bases, afim de ser utilizado os dados na próxima etapa (Data Viz), ele representaria a etapa Ouro, no modelo de Data Lake.  


## Etapa de Data Viz (A ser desenvolvido)
 Modelo visual desenvolvido no Looker Studio, afim de gerar informação com os dados coletados das API's.
