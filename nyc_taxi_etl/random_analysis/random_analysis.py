import requests
import pandas as pd
import random

from modules import utils
from modules.logger import logger

#  years previous to 2011 throw errors because of different datatypes. At the end are just two years left outside
year_ranges = [
  (2011, 2015),
  (2016, 2019),
  (2021, 2024)
]

months = list(range(1, 13))

# handling duplicated year and month
dup_year, dup_month = set(), set()

def select_random_year(year_range):
  start_year, end_year = year_range
  random_year = random.choice(range(start_year, end_year + 1))

  return random_year

def select_random_month(year):
  if year == 2024:
      available_months = [month for month in range(1, 13) if month <= 6]
  else:
      available_months = range(1, 13)

  available_months = [month for month in available_months if month not in dup_month]
  if not available_months:
    dup_month.clear()
    available_months = [month for month in available_months if month not in dup_month]
  
  random_month = random.choice(available_months)
  dup_month.add(random_month)
  print(dup_month)
  return random_month

def select_random_year_month(year_range):
  """
  Select a random year and random month.
  """
  year = select_random_year(year_range)
  month = select_random_month(year)
  
  return year, month

def process_data(year_range):
  # change range() as needed, if you use bigger number you will have less months left
  for _ in range(9):
    random_year, random_month = select_random_year_month(year_range)

    if random_month is None:
        logger.info(f"No available months for year {random_year}")
        continue
    
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
    
    # Apply functions
    taxi_df = utils.find_na(taxi_df)
    # na_count = utils.find_na(taxi_df)
    negative_count = utils.negative_amount(taxi_df)
    wrong_ratecode_count = utils.wrong_ratecode(taxi_df)
    year_diff_count = utils.different_year(taxi_df, random_year)
    month_diff_count = utils.different_month(taxi_df, random_month)
    total_amount_outliers = utils.find_outliers(taxi_df, column=[column for column in taxi_df.columns if 'total' in column.lower()])
    trip_distance_outliers = utils.find_outliers(taxi_df, column=[column for column in taxi_df.columns if 'trip_distance' in column.lower()])

# Process data for year range
for year_range in year_ranges:
    process_data(year_range)
