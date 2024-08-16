# Añadir alguna columna
# Modificar alguna columna
import pandas as pd
import numpy as np

# import our own modules
from modules.utils import postgres_connection

def load_taxi_tripdata():
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
    connection = postgres_connection()

    # ---- Extract ----
    taxi_df = pd.read_parquet(url, engine='pyarrow')

    print(taxi_df.head())

    # ---- Transform ----
    # Choosing the columns we are insterested in
    taxi_df = taxi_df[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'RatecodeID', 'PULocationID', 'DOLocationID', 'payment_type', 'total_amount']]
    print(taxi_df)

    # Column normalization
    taxi_df.columns = (
        taxi_df.columns
        .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
        .str.lower()
    )

    # ---- Changing names for clarity
    taxi_df.rename(columns={
        'tpep_pickup_datetime':'pickup_datetime', 
        'tpep_dropoff_datetime':'dropoff_datetime',
        'pulocation_id':'pickup_location', 'dolocation_id':'dropoff_location'}, inplace=True)

    # # ---- Changing the types of datetime columns ----
    # taxi_df['pickup_datetime'] = taxi_df['pickup_datetime'].astype("datetime64[ns]")
    # taxi_df['dropoff_datetime'] = taxi_df['dropoff_datetime'].astype("datetime64[ns]")

    # print(taxi_df.columns)

    # ---- Cleaning columns: erase where RatecodeID is > 6, erase negative values in total_amount and drop null data ----
    taxi_df_filtered = taxi_df[(taxi_df['ratecode_id'] <= 6) & (taxi_df['total_amount'] > 0)]

    taxi_df_filtered = taxi_df_filtered.dropna()

    # ---- Changing the types of some columns ----

    for column in taxi_df_filtered.columns:
        if 'ID' in column or 'payment_type' in column:
            taxi_df_filtered.loc[:,column] = taxi_df_filtered[column].astype('str')

    # ---- Deleting years that are not 2024 and months different to january

    taxi_data_prep = taxi_df_filtered.drop(
        taxi_df_filtered[
        (taxi_df_filtered['pickup_datetime'].dt.year != 2024)
        ].index)

    taxi_data_prep = taxi_data_prep.drop(
        taxi_data_prep[
            (taxi_data_prep['pickup_datetime'].dt.month > 1) |
            (taxi_df_filtered['dropoff_datetime'].dt.month > 1)
            ].index)
    
    # ---- droping outliers in trip distance and total amount ----
    conditions = taxi_data_prep[    
    (taxi_data_prep['trip_distance'] <= 1) & (taxi_data_prep['total_amount'] > 10) &
    (taxi_data_prep['trip_distance'] <= 2) & (taxi_data_prep['total_amount'] > 15) &
    (taxi_data_prep['trip_distance'] <= 3) & (taxi_data_prep['total_amount'] > 20) &
    (taxi_data_prep['trip_distance'] <= 5) & (taxi_data_prep['total_amount'] > 35) &
    (taxi_data_prep['trip_distance'] <= 10) & (taxi_data_prep['total_amount'] > 50) &
    (taxi_data_prep['trip_distance'] <= 20) & (taxi_data_prep['total_amount'] > 100)
    ].index

    print('''---- outliers ----''')
    print(conditions.shape[0])

    taxi_df_cleaned = taxi_data_prep.drop(~conditions, inplace=True)

    print(taxi_df_cleaned)
#     results = [True, True, True, True, True, True]

#     # Crear una nueva columna 'tarifa_muy_alta' que aplica estas condiciones
#     taxi_data_prep['excessive_amount'] = np.select(conditions, results, default=False)

# # Mostrar algunas filas para verificar el resultado
#     taxi_data_prep = taxi_data_prep.drop(taxi_data_prep[taxi_data_prep['excessive_amount'] == True].index)

    insert_with_progress(
    connection,
    taxi_df_cleaned,
    table_name='yellow_taxi_2024_01',
    schema='public'
    )




# ---- Load ----
def chunker(seq, size):
    # from http://stackoverflow.com/a/434328
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def insert_with_progress(con, df, table_name, schema):
    total = int(len(df))
    chunksize = 1000
    for i, cdf in enumerate(chunker(df, chunksize)):
        replace = "replace" if i == 0 else "append"
        cdf.to_sql(
            con=con,
            name=table_name, schema=schema, if_exists=replace, method='multi',
            index=False)
        print(f'{i}, {i * chunksize}, {total}, {int(i * chunksize / total * 10000) / 100.0}%')