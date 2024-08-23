# etl_projects
In my current job, I work on ETL (Extract, Transform, Load) projects using Python, PostgreSQL, DBeaver, and Jenkins. This repository is where I emulate and practice similar projects, honing my skills and exploring new ETL processes.

# Data analysis
Since I don't have prior knowledge of the data contained in the NYC Yellow Taxi datasets I work with, I perform data analysis concurrently with the ETL process. This ensures that I don't store incorrect or invalid data in the database. The goal is to maintain data integrity and quality throughout the ETL pipeline.

##### Folder structure

nyc_taxi_etl/
│
├── README.md                # Project description and objectives
├── main.ipynb               # Notebook with detailed data analysis process
├── main.py                  # Main script to execute the ETL
├── requirements.txt         # List of project dependencies
│
├── modules/                 # Python modules containing ETL logic and utilities
│   ├── etl.py               # Module for ETL process logic
│   └── utils.py             # Utility functions used in ETL and analysis
│
├── random_analysis/         # Folder for analysis of additional datasets
│   ├── random_analysis.py   # Script for analyzing additional datasets
│   ├── README.md            # Description of the random_analysis folder
│   ├── modules/             # Python modules for random analysis
│   │   ├── utils.py         # Utility functions for random analysis
│   │   └── logger.py        # Logger configuration
│   └── logs/                # Folder to store execution log files
│       └── random_analysis.txt  # Log of the ETL process