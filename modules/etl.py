# Añadir alguna columna
# Cuanto se gasto de media por mes
# Modificar alguna columna

import pandas as pd
import numpy as np

from sqlalchemy import text

# import our own modules

from modules.utils import postgres_connection

def load_taxi_tripdata():
    connection = postgres_connection()

    # ---- Extract ----
    taxi_df = pd.read_csv('./data/yellow_tripdata_2024-01.csv',
    low_memory=False)

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

    insert_with_progress(
    connection,
    taxi_df,
    table_name='yellow_taxi_2024',
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