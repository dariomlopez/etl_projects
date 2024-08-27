# NYC Taxi ETL Project

## Overview

The NYC Taxi ETL (Extract, Transform, Load) project is designed to process and analyze New York City taxi trip data. This project includes data extraction from public sources, transformation of data to ensure quality and consistency, and loading the cleaned data for further analysis.

## Project Structure

```plaintext
nyc_taxi_etl/
│
├── README.md               # Project description and objectives
├── main.ipynb              # Jupyter notebook with detailed data analysis process
├── main.py                 # Main script to execute the ETL process
├── requirements.txt        # List of project dependencies
│
├── modules/                # Python modules containing ETL logic and utilities
│   ├── etl.py              # Module for ETL process logic
│   └── utils.py            # Utility functions used in ETL and analysis
│
├── random_analysis/        # Folder for analysis of additional datasets
│   ├── random_analysis.py  # Script for analyzing additional datasets
│   ├── README.md           # Description of the random_analysis folder
│   ├── modules/
│   │   ├── utils.py        # Functions specific to random analysis
│   │   └── logger.py       # Logger creation and configuration
│   └── logs/               # Folder to store execution log files
│       └── random_analysis.txt # Log of the random analysis process
```
## Installation
### Clone the repository

```
git clone https://github.com/dariomlopez/etl_projects/tree/main/nyc_taxi_etl
cd nyc_taxi_etl
```
### Option 1: Create a virtual environment 
```
python -m venv venv
source venv/bin/activate
```
Then install the dependencies: 
```
pip install -r requirements.txt
```
### Option 2: Use Docker 
If you prefer using Docker, ensure you have Docker installed on your machine. Then, build and run the Docker container:
#### 1. Build Docker image 
```
docker build -t nyc_taxi_etl .
```
#### 2. Run the Docker container
```
docker run --rm -it nyc_taxi_etl
```

## Usage
### Run the main.py ETL process
If using a virtual environment or Docker, execute the main script to perform the ETL operations:
```
python main.py
```
### Random Analysis
For additional dataset analysis, navigate to the random_analysis folder and run:
```python random_analysis.py```
