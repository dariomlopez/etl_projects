import os
import logging

log_dir = './nyc_taxi_etl/random_analysis/logs'
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %I:%M:%S',
    handlers=[
        logging.FileHandler(os.path.join(log_dir,'random_analysis.txt')),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger()