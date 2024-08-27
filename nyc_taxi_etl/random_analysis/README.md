# Random Analysis Directory

The `random_analysis` directory contains scriptsused for exploring and analyzing random datasets datasets that are evaluated for similar issues as the original dataset analised in main.ipynb.

## Contents
- **`random_analysis.py`**: 
  - Python script for analyzing random NYC Yellow Taxi datasets programmatically.
  - This script is designed to automate certain analysis tasks and is useful for batch processing or applying consistent analysis methods across multiple datasets.
  - It can be modified to include additional analysis functions or criteria specific to the datasets being examined.

## Purpose

The purpose of this directory is to facilitate the examination of NYC Yellow Taxi datasets to ensure that similar data quality issues or anomalies are identified and addressed. By analyzing random or additional datasets, we can validate our data processing and analysis methodologies and ensure consistency across different data sources.

## Conclusions

### Data Quality Observations

After extensive analysis, it has been observed that:

1. **Null or NaN Values**: Are present across all datasets, regardless of the year. This suggests a systemic issue with missing data in the NYC Taxi dataset.

2. **Increased Data Issues Post-2020/2021**: Datasets from 2020/2021 onwards show a marked increase in problems such as:
   - Incorrect rate codes
   - Negative values in `total_amount`
   - Mismatched years and months
   - A higher number of outliers
   **Persistent Data Issues Without dropna()**: When dropna() is not used, these problems are present across all datasets. This indicates that the data issues are not isolated to certain years but can occur throughout the dataset if missing values are not properly handled.

Given these findings, additional cleaning steps are recommended for datasets from 2020 onwards. This includes stricter validation rules and possibly more conservative filtering of anomalous data points.