# Data Engineering Nanodegree Capstone Project: Stock Market Anaysis ETL
## Scoping the Project
### Project Overview
The purpose of this project is build data models (analytical tables) in preparation for data analysis and building machine learning models for Stock Market prediction. This project will utlized skills learning in Udacity Data Engineering Nanodegree, including storing data to S3, building data warehouse using Redshift and ETL pipelines using Airflow. The project is developed in AWS ClouldFormation EC2 instance. 

### Steps taken in the project
* step 1: Upload historic raw data to S3 buket 
        a. Download stock market and stock news data from kaggle (see datasets source) to EC2 machine. Need Kaggle API installed in order to download from EC2 environment  
        b. Create DAG `stock_historic_etl_dag.py` to implement the following tasks:
            i. Save historic raw data in s3://stock.etl/raw-historic-data/.
            ii. Load raw data from S3 and make staging tables and save to Redshift cluster.

### Purpose of the final data model
The final data model will be used to predict next day's stock market (rise or drop) using previous's one month stock price and most recent world's news.

## Datasets Source
- Stock news data 2008-2016: https://www.kaggle.com/aaron7sun/stocknews
- Stock Market Data 2010-2017: https://www.kaggle.com/borismarjanovic/price-volume-data-for-all-us-stocks-etfs
- Information about public companies that are being traded in US Stock market: https://www.kaggle.com/vaghefi/robinhood
- Current One month stock pricing data: pulled from Yahoo Finance
- Most Recent top 25 world's news data: from Reddit https://www.reddit.com/r/worldnews/top/

## Setup:
- Python Package: airflow, s3fs, pandas, yfinance, pyarrow
- API: Kaggle API 
      - install kaggle api https://github.com/Kaggle/kaggle-api
      - Add `kaggle` path to system variable `PATH=$PATH:/path/to/file`
- Development enviroment: AWS Cloudformation EC2 instance: https://aws.amazon.com/blogs/big-data/build-a-concurrent-data-orchestration-pipeline-using-amazon-emr-and-apache-livy/
