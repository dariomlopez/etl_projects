import requests
import pandas as pd
import random

from modules import utils
from modules.logger import logger

#  years previous to 2011 throw errors because of different datatypes. At the end are just two years left outside
years = list(range(2011, 2025))

months = list(range(1, 13))

# handling duplicated months by year
dup_month = {}

def select_random_year(years):
  """
    Selects a random year from a provided list of years.

    Args:
        years (list): A list of years from which to select randomly.

    Returns:
        int: A randomly selected year from the list.
  """
  random_year = random.choice(years)

  return random_year

def select_random_month(year):
  """
    Selects a random month for a given year, ensuring that the month has not been used already.

    Args:
        year (int): The year for which to select a month.

    Returns:
        int: A randomly selected month for the given year, or None if no months are available.
    
    This function ensures that each month is selected only once per year. It handles special cases like 
    the year 2024, where only months up to June are available. If all months have been used, 
    the function clears the used months and attempts to select a new month.
  """
  if year not in dup_month:
    dup_month[year] = set()

  if year == 2024:
      available_months = [month for month in range(1, 13) if month <= 6]
  else:
      available_months = range(1, 13)

  available_months = [month for month in available_months if month not in dup_month]

  if not available_months:
    dup_month[year].clear()
    available_months = list(range(1, 7)) if year == 2024 else list(range(1, 13))
  
  random_month = random.choice(available_months)
  dup_month[year].add(random_month)

  print(dup_month)
  return random_month

def select_random_year_month(year):
  """
    Selects a random year and month.

    Returns:
        tuple: A tuple containing a randomly selected year and a randomly selected month.
    
    This function uses `select_random_year` to get a random year and `select_random_month` to get
    a random month for that year. It returns both values as a tuple.
  """
  year = select_random_year(years)
  month = select_random_month(year)
  
  return year, month

def process_data(years, times_per_year=6):
  """
  Function to process the datasets across different years and months.
  Args:
        years (list): A list of years to process the data for.
        times_per_year (int): Number of times to process data for each year. 
                              Default is 6, meaning the script will randomly 
                              select and process data for 6 different months per year.
                              The bigger the number the longer it takes the script to run.
  This function processes the NYC Yellow Taxi dataset for a random selection of months within each year.
  Attempts to download the corresponding dataset and applies various data analysis and logs relevant information
  """
  for year in years:
    for _ in range(times_per_year):
      random_year, random_month = select_random_year_month(year)

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

process_data(years)
