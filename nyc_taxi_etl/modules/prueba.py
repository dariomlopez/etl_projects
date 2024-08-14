import pandas as pd

# URL del archivo Parquet
url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'

taxi_df = pd.read_parquet(url, engine='pyarrow')
# Mostrar las primeras filas del DataFrame
print(taxi_df.head())

taxi_df = taxi_df[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'RatecodeID', 'PULocationID', 'DOLocationID', 'payment_type', 'total_amount']]
print(taxi_df)

    # Column normalization
taxi_df.columns = (
  taxi_df.columns
  .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
  .str.lower()
)

    # ---- Changing the name for clarity
taxi_df.rename(columns={
        'tpep_pickup_datetime':'pickup_datetime', 
        'tpep_dropoff_datetime':'dropoff_datetime',
        'pulocation_id':'pickup_location', 'dolocation_id':'dropoff_location'}, inplace=True)

print(taxi_df)
