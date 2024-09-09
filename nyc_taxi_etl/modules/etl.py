import pandas as pd

# import our own modules
from modules.utils import postgres_connection, calculate_thresholds

def etl_taxi_tripdata():
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
    connection = postgres_connection()

    # ---- Extract ----
    # Extract a parquet file form a URL
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
        'pulocation_id':'pickup_location', 
        'dolocation_id':'dropoff_location'}, inplace=True)

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

    taxi_df_cleaned = taxi_data_prep.drop(
        taxi_data_prep[
            (taxi_data_prep['pickup_datetime'].dt.month > 1) |
            (taxi_df_filtered['dropoff_datetime'].dt.month > 1)
            ].index)
    
    # ---- droping outliers in trip distance and total amount ----
    lower_amount, upper_amount = calculate_thresholds(taxi_df_cleaned, 'total_amount')
    lower_distance, upper_distance = calculate_thresholds(taxi_df_cleaned, 'trip_distance')

    # Filter the data to remove outliers
    taxi_df_cleaned = taxi_df_cleaned[
        (taxi_df_cleaned['total_amount'] >= lower_amount) & (taxi_df_cleaned['total_amount'] <= upper_amount) &
        (taxi_df_cleaned['trip_distance'] >= lower_distance) & (taxi_df_cleaned['trip_distance'] <= upper_distance)
    ]

    # ---- Loading data into database
    insert_with_progress(
    connection,
    taxi_df_cleaned,
    table_name='yellow_taxi_2024_01',
    schema='public'
    )




# ---- Load in chunks----
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