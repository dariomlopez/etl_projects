import os
import logging

log_dir = './nyc_taxi_etl/random_analysis/logs'
log_file = os.path.join(log_dir,'random_analysis.txt')

os.makedirs(log_dir, exist_ok=True)

if os.path.exists(log_file):
  print("Log removed")
  os.remove(log_file)

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(levelname)s - %(message)s',
  datefmt='%d-%m-%Y %I:%M:%S',
  handlers=[
    logging.FileHandler(log_file),
    logging.StreamHandler()
  ]
)

logger = logging.getLogger()