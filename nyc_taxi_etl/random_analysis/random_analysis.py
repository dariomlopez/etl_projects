import requests
import pandas as pd
# import polars as pl
import random

from modules import utils
from modules.logger import logger

# years = list(range(2020, 2025))  # From 2009 to 2024
# months = list(range(1, 13))  # From January (1) to December (12)



year_ranges = [
    (2011, 2015),
    (2016, 2019),
    (2021, 2025)
]

months = list(range(1, 13))

def select_random_year_month(year_range):
    start_year, end_year = year_range
    random_year = random.choice(range(start_year, end_year + 1))
    
    # Ajuste de meses para el año 2024, si aplica
    if random_year == 2024:
        available_months = [month for month in months if month <= 6]
    else:
        available_months = months
    
    random_month = random.choice(available_months)
    return random_year, random_month

def process_data_for_year_range(year_range):
    for _ in range(15):
        random_year, random_month = select_random_year_month(year_range)
        
        print(f"Random Year: {random_year}")
        print(f"Random Month: {random_month}")
        
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{random_year}-{random_month:02d}.parquet'
        
        try:
            taxi_df = pd.read_parquet(url, engine='pyarrow')
        except Exception as e:
            logger.info(f"Error loading data for {random_year}-{random_month:02d}: {e}")
            continue
        
        logger.info("\n" + '-'*5 + f" Data for {random_year}-{random_month:02d} " + '-'*10 + "\n")
        print(f"Head: {taxi_df.head(3)}")
        logger.info(f"Size of df: {taxi_df.size}")
        logger.info(f"Rows: {len(taxi_df)}")
        
        # Aplicar funciones de validación
        taxi_df = utils.find_na(taxi_df)
        # na_count = utils.find_na(taxi_df)
        negative_count = utils.negative_amount(taxi_df)
        wrong_ratecode_count = utils.wrong_ratecode(taxi_df)
        year_diff_count = utils.different_year(taxi_df, random_year)
        month_diff_count = utils.different_month(taxi_df, random_month)
        total_amount_outliers = utils.find_outliers(taxi_df, column=[column for column in taxi_df.columns if 'total' in column.lower()])
        trip_distance_outliers = utils.find_outliers(taxi_df, column=[column for column in taxi_df.columns if 'trip_distance' in column.lower()])
        
        # utils.find_outliers(taxi_df, column=[column for column in taxi_df.columns if 'total' in column.lower()])
        # utils.find_outliers(taxi_df, column=[column for column in taxi_df.columns if 'trip_distance' in column.lower()])

# Procesar los datos para cada rango de años
for year_range in year_ranges:
    process_data_for_year_range(year_range)














# for _ in range(15):
# #   Select random years and months
#   random_year = random.choice(years)

#   if random_year == 2024:
#     months = [month for month in months if month <= 6]
#   else:
#     months = months

#   random_month = random.choice(months)

#   print(f"Random Year: {random_year}")
#   print(f"Random Month: {random_month}")
  
#   url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{random_year}-{random_month:02d}.parquet'

#   taxi_df = pd.read_parquet(url, engine='pyarrow')

#   print(f"Data for {random_year}-{random_month:02d}:")

#   print(f"""Head: {taxi_df.head(3)}
#   Size: {taxi_df.size}
#   Rows: {len(taxi_df)}""")
  
#   taxi_df=utils.find_na(taxi_df)
#   utils.negative_amount(taxi_df)
#   utils.wrong_ratecode(taxi_df)
#   utils.different_year(taxi_df, random_year)
#   utils.different_month(taxi_df, random_month)

#   utils.find_outliers(taxi_df, column=[column for column in taxi_df.columns if 'total' in column.lower()])
#   # print(f'Outliers in total amount: {len(outliers_total)}')

#   utils.find_outliers(taxi_df, column=[column for column in taxi_df.columns if 'trip_distance' in column.lower()])
  # print(f'Outliers in trip_distance: {len(outliers_trip_distance)}')