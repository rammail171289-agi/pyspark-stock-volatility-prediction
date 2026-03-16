# pyspark-stock-volatility-prediction
# Goal: Predict next trading day's expected price range using 1-month volatility.

# Stock Volatility Prediction using PySpark

## Overview

This project predicts the next trading day's expected price range using 1-month historical volatility.

The model calculates daily returns and uses standard deviation to estimate expected price movement.

## Architecture

Raw Data → PySpark Cleaning → Return Calculation → Volatility → Predicted Trading Range

## Technologies

- PySpark
- Apache Spark
- Azure Data Lake
- Azure Databricks

## Dataset

Historical stock data for Adani Green.

## Steps

1 Load raw CSV data  
2 Clean and standardize columns  
3 Calculate daily returns  
4 Compute volatility (standard deviation of returns)  
5 Predict expected price movement  

## Formula

Expected Move

```
Expected Move = Last Closing Price × Volatility
```

Predicted Range

```
Lower Price = Last Close − Expected Move
Upper Price = Last Close + Expected Move
```

## Example Output

Last Trading Date: 2026-03-13  
Last Closing Price: 861.35  

Predicted Range

Lower Price: 844.74  
Upper Price: 877.96

## Future Improvements

- Add visualization
- Automate pipeline using Airflow
- Store results in Delta Lake
