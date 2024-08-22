def find_na(df):
  na_count = df.isnull().sum().sum()
  if na_count > 0:
    print(f"Found {na_count} NaN")
    print("Droping NaN's")
    df = df.dropna()
  else:
    print("No NaN or null values found")
  return df
# def drop_na(df):

def wrong_ratecode(df):
  for column in df.columns:
    # df[column] = pd.to_numeric(df[column], errors='coerce')
    
    # df[column] = df[column].fillna(0)

    if 'rate' in column.lower():
      ratecodes_count = (df[column] > 6).sum()

      if ratecodes_count > 0:
        print(f"Found {ratecodes_count} rows with wrong ratecodeID")
      else:
        print("RatecodeID is OK, polilla")

def negative_amount(df):
  for column in df.columns:
    if 'total' in column.lower():
      negative_count = (df[column] < 0).sum()

      if negative_count > 0:
        print(f"Found {negative_count} rows with negative total_amount values.")
      else:
        print("No negative total_amount values found.")

def different_year(df, relevant_year):
  for column in df.columns:
    if 'pickup' in column.lower():
      # df[column] = pd.to_datetime(df[column],errors='coerce')
      print(df[column].dtype)
      not_relevant_year = (df[column].dt.year != relevant_year).sum()

      if not_relevant_year > 0:
        print(f"In Pick-Up column: Found {not_relevant_year} rows with different year.")
      else:
        print("No different year found in pick-up")

    elif 'dropoff' in column.lower():
      # df[column] = pd.to_datetime(df[column],errors='coerce')

      not_relevant_year = (df[column].dt.year != relevant_year).sum()

      if not_relevant_year > 0:
        print(f"In drop-off column: Found {not_relevant_year} rows with different year.")
      else:
        print("No different year found in drop-off")

def different_month(df, relevant_month):
  for column in df.columns:
    if 'pickup' in column.lower():
      # df[column] = pd.to_datetime(df[column],errors='coerce')

      not_relevant_month = (df[column].dt.month != relevant_month).sum()

      if not_relevant_month > 0:
        print(f"In column pick-up: Found {not_relevant_month} rows with different month")
      else:
        print("No different month found in pick-up column")

    elif 'dropoff' in column.lower():
      # df[column] = pd.to_datetime(df[column],errors='coerce')

      not_relevant_month = (df[column].dt.month != relevant_month).sum()

      if not_relevant_month > 0:
        print(f"In column drop-off: Found {not_relevant_month} rows with different month")
      else:
        print("No different month found in drop-off")

def find_outliers(df, column):
  Q1 = df[column].quantile(0.25)
  Q3 = df[column].quantile(0.75)
  IQR = Q3 - Q1
  
  lower_bound = Q1 - 1.7 * IQR
  upper_bound = Q3 + 1.7 * IQR

  outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
    
  return outliers