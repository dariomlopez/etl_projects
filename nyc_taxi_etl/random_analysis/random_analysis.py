import requests
import pandas as pd
# import polars as pl
import random

from modules import utils

years = list(range(2010, 2025))  # From 2009 to 2024
months = list(range(1, 13))  # From January (1) to December (12)

for _ in range(15):
#   Select random years and months
  random_year = random.choice(years)

  if random_year == 2024:
    months = [month for month in months if month <= 6]
  else:
    months = months

  random_month = random.choice(months)

  print(f"Random Year: {random_year}")
  print(f"Random Month: {random_month}")
  
  url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{random_year}-{random_month:02d}.parquet'

  taxi_df = pd.read_parquet(url, engine='pyarrow')

  print(f"Data for {random_year}-{random_month:02d}:")

  print(f"""Head: {taxi_df.head(3)}
  Size: {taxi_df.size}
  Rows: {len(taxi_df)}""")
  
  taxi_df=utils.find_na(taxi_df)
  utils.negative_amount(taxi_df)
  utils.wrong_ratecode(taxi_df)
  utils.different_year(taxi_df, random_year)
  utils.different_month(taxi_df, random_month)

  # outliers_total = utils.find_outliers(taxi_df, column=[column for column in taxi_df.columns if 'total' in column.lower()])
  # print(f'Outliers in total amount: {len(outliers_total)}')

  # outliers_trip_distance = utils.find_outliers(taxi_df, column=[column for column in taxi_df.columns if 'trip_distance' in column.lower()])
  # print(f'Outliers in trip_distance: {len(outliers_trip_distance)}')