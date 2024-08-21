import requests
import pandas as pd
import random

years = list(range(2010, 2025))  # From 2009 to 2024
months = list(range(1, 13))  # From January (1) to December (12)

def drop_na(df):
  na_count = df.isna().sum().sum()
  if na_count > 0:
    print("Found NaN")
  else:
    print("No NaN or null values found")

def wrong_ratecode(df):
  ratecodes_count = (df['RatecodeID'] > 6).sum()

  if ratecodes_count > 0:
    print(f"Found {ratecodes_count} rows with wrong ratecodeID")
  else:
    print("RatecodeID is OK, polilla")

def negative_amount(df):
  negative_count = (df['total_amount'] < 0).sum()

  if negative_count > 0:
    print(f"Found {negative_count} rows with negative total_amount values.")
  else:
    print("No negative total_amount values found.")

def different_year_and_month(df):
  pass

def identify_outliers(df):
  pass

for _ in range(15):
#   Select random years and months
  random_year = random.choice(years)

  if random_year == 2024:
    months = [month for month in months if month <= 6]
  else:
    months = months

  random_month = random.choice(months)
  random_month = f"{random_month:02d}"

  print(f"Random Year: {random_year}")
  print(f"Random Month: {random_month}")
  
  url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{random_year}-{random_month}.parquet'
  response = requests.get(url)

  taxi_df = pd.read_parquet(url, engine='pyarrow')

  # taxi_df = taxi_df[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'RatecodeID', 'PULocationID', 'DOLocationID', 'payment_type', 'total_amount']]

  print(f"Data for {random_year}-{random_month}:")

  # print(taxi_df.head())

  drop_na(taxi_df)
  negative_amount(taxi_df)
  wrong_ratecode(taxi_df)