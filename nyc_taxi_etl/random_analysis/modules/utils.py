import pandas as pd

from modules.logger import logger

def find_na(df):
  """
    Identifies and counts missing (NaN) values in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to check for missing values.

    Returns:
        pd.DataFrame: The DataFrame with missing values identified.
    
    This function identifies missing values in the DataFrame and may optionally return 
    the DataFrame or perform further actions depending on its implementation.
    """
  na_count = df.isnull().sum().sum()
  if na_count > 0:
    logger.info(f"Found {na_count} NaN")
    # logger.info("Droping NaN's")
    # df = df.dropna()
  else:
    logger.info("No NaN or null values found")
  return df

def wrong_ratecode(df):
  """
    Identifies rows with incorrect rate codes in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to check for incorrect rate codes.

    Returns:
        int: The count of rows with incorrect rate codes.
    
    This function inspects the rate code column in the DataFrame and identifies any rows
    with incorrect or unexpected rate codes. It returns the count of such rows.
  """
  for column in df.columns:
    if 'rate' in column.lower():
      df[column] = pd.to_numeric(df[column], errors='coerce')
      ratecodes_count = (df[column] > 6).sum()

      if ratecodes_count > 0:
        logger.info(f"Found {ratecodes_count} rows with wrong ratecodeID")
      else:
        logger.info("RatecodeID is OK")

def negative_amount(df):
  """
    Identifies rows with negative values in the `total_amount` column of the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to check for negative values in the `total_amount` column.

    Returns:
        int: The count of rows with negative `total_amount` values.
    
    This function checks the `total_amount` column in the DataFrame for negative values
    and returns the count of such rows.
  """
  for column in df.columns:
    if 'total' in column.lower():
      negative_count = (df[column] < 0).sum()

      if negative_count > 0:
        logger.info(f"Found {negative_count} rows with negative total_amount values.")
      else:
        logger.info("No negative total_amount values found.")

def different_year(df, relevant_year):
  """
    Identifies rows where the year in the DataFrame does not match the expected year.

    Args:
        df (pd.DataFrame): The DataFrame to check for year mismatches.
        relevant_year (int): The expected year to match against.

    Returns:
        int: The count of rows with years different from the expected year.
    
    This function checks if the year in each row of the DataFrame matches the provided
    `relevant_year`. It returns the count of rows where the year does not match.
  """
  for column in df.columns:
    if 'pickup' in column.lower():
      df.loc[:,column] = pd.to_datetime(df.loc[:,column], errors='coerce')
      print(df[column].dtype)
      not_relevant_year = (df[column].dt.year != relevant_year).sum()

      if not_relevant_year > 0:
        logger.info(f"In Pick-Up column: Found {not_relevant_year} rows with different year.")
      else:
        logger.info("No different year found in pick-up")

    elif 'dropoff' in column.lower():
      df.loc[:,column] = pd.to_datetime(df.loc[:,column], errors='coerce')

      not_relevant_year = (df[column].dt.year != relevant_year).sum()

      if not_relevant_year > 0:
        logger.info(f"In drop-off column: Found {not_relevant_year} rows with different year.")
      else:
        logger.info("No different year found in drop-off")

def different_month(df, relevant_month):
  """
    Identifies rows where the month in the DataFrame does not match the expected month.

    Args:
        df (pd.DataFrame): The DataFrame to check for month mismatches.
        relevant_month (int): The expected month to match against.

    Returns:
        int: The count of rows with months different from the expected month.
    
    This function checks if the month in each row of the DataFrame matches the provided
    `relevant_month`. It returns the count of rows where the month does not match.
  """
  for column in df.columns:
    if 'pickup' in column.lower():
      df.loc[:,column] = pd.to_datetime(df.loc[:,column], errors='coerce')

      not_relevant_month = (df[column].dt.month != relevant_month).sum()

      if not_relevant_month > 0:
        logger.info(f"In column pick-up: Found {not_relevant_month} rows with different month")
      else:
        logger.info("No different month found in pick-up column")

    elif 'dropoff' in column.lower():
      df.loc[:,column] = pd.to_datetime(df.loc[:,column], errors='coerce')

      not_relevant_month = (df[column].dt.month != relevant_month).sum()

      if not_relevant_month > 0:
        logger.info(f"In column drop-off: Found {not_relevant_month} rows with different month")
      else:
        logger.info("No different month found in drop-off")

def find_outliers(df, column):
  """
    Identifies outliers in a specified column of the DataFrame using the IQR (Interquartile Range) method.

    Args:
        df (pd.DataFrame): The DataFrame to check for outliers.
        column (list): A list of column names in which to find outliers.

    Returns:
        pd.DataFrame: The DataFrame containing only the rows with outliers in the specified column(s).

    The function calculates the IQR for each column specified in `column` and identifies outliers 
    based on the IQR method. Outliers are defined as values that fall below the lower bound or above 
    the upper bound, which are determined by the IQR.
  """
  for column in df.columns:
    if 'total' in column.lower():
      Q1 = df[column].quantile(0.25)
      Q3 = df[column].quantile(0.75)
      IQR = Q3 - Q1

      lower_bound = Q1 - 1.5 * IQR
      upper_bound = Q3 + 1.5 * IQR

      outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
      
      logger.info(f"Outliers en {column}: {outliers.shape[0]} filas encontradas.")
    elif 'trip_distance' in column.lower():
      Q1 = df[column].quantile(0.25)
      Q3 = df[column].quantile(0.75)
      IQR = Q3 - Q1

      lower_bound = Q1 - 1.5 * IQR
      upper_bound = Q3 + 1.5 * IQR

      outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
      logger.info(f"Outliers en {column}: {outliers.shape[0]} filas encontradas.")
