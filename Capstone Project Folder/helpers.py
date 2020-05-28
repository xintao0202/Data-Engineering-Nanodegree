import boto3
import airflow.hooks.S3_hook
import pandas as pd
import yfinance as yf
import pyarrow
import zipfile
import io
import os
import numpy as np
import s3fs
import json
import datetime
import praw
import commands

s3 = boto3.resource('s3')

def upload_file_to_S3(filename, key, bucket_name):
    s3.Bucket(bucket_name).upload_file(filename, key)

def upload_file_to_S3_with_hook(filename, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook('my_S3_conn')
    hook.load_file(filename, key, bucket_name, replace=True)

def df_to_S3(df, bucket_name, key):
    s3_path="s3://{}/{}".format(bucket_name, key)
    df.to_parquet(s3_path,compression='gzip')

def save_stock_s3(symbol,start,end,bucket_name):
    df=yf.download(symbol,start,end,progress=False)
    key=symbol+'-from'+start+'to'+end
    df_to_S3(df, bucket_name, key)

def convert_zip_to_dfs(bucket_name, key):
    s3_resource = boto3.resource('s3')
    zip_obj = s3_resource.Object(bucket_name=bucket_name, key=key)
    buffer = io.BytesIO(zip_obj.get()["Body"].read())
    z = zipfile.ZipFile(buffer)

    # https://stackoverflow.com/questions/44575251/reading-multiple-files-contained-in-a-zip-file-with-pandas
    dfs = {os.path.splitext(text_file.filename)[0]: pd.read_csv(z.open(text_file.filename))
           for text_file in z.infolist()
           if text_file.filename.endswith('.csv')}
    return dfs

def convert_stock_from_zip(bucket_name, key):
    s3_resource = boto3.resource('s3')
    zip_obj = s3_resource.Object(bucket_name=bucket_name, key=key)
    buffer = io.BytesIO(zip_obj.get()["Body"].read())
    z = zipfile.ZipFile(buffer)
    stock_dict_historic={}

    for file_info in z.infolist():
        if "Stocks" in file_info.filename and "txt" in file_info.filename:
            stock_name=file_info.filename.split("/")[1]
            stock_name=stock_name.split(".")[0]
            # some files has no data, need to exclude
            if stock_name !="Stocks" and file_info.file_size >0:
                df=pd.read_csv(z.open(file_info.filename), sep=',')
                stock_dict_historic.update({stock_name: df})
    return stock_dict_historic

def get_bbands(df, ndays):
    dm = df[['Close']].rolling(ndays).mean()
    ds = df[['Close']].rolling(ndays).std()
    df['upperBB'] = dm + 2 * ds
    df['lowerBB'] = dm - 2 * ds
    return df

# Simple Moving Average
def get_SMA(df, ndays):
    df['SMA']=df[['Close']].rolling(ndays).mean()
    return df

# Expontential Moving Average
def get_EMA(df, ndays):
    df['EMA'] = df[['Close']].ewm( span = ndays, min_periods = ndays - 1).mean()
    return df

# Rate of Change
def get_ROC(df, ndays):
    dn = df[['Close']].diff(ndays)
    dd = df[['Close']].shift(ndays)
    df['ROC'] = dn/dd
    return df

def data_transformation(company_df, news_df, stock_df, stock_symbol):
    # First Keep useful information from stock price tables (Date, close and volume), forward fill missing values. 
    stock_price_df=stock_df.copy()
    stock_price_df.drop(['Open','High','Low', 'OpenInt'], axis=1, inplace=True)
    stock_price_df.fillna(method='ffill')
    stock_price_df.set_index('Date', inplace=True)
    
    
    # Adding daily return (%), stock_rise (1, 0 if drop), technical indicators 
    stock_price_df['daily_return']=stock_price_df[['Close']]/stock_price_df[['Close']].shift(1)-1
    stock_price_df=get_bbands(stock_price_df, 10)
    stock_price_df=get_SMA(stock_price_df, 10)
    stock_price_df=get_EMA(stock_price_df, 10)
    stock_price_df=get_ROC(stock_price_df, 1)
    stock_price_df['stock_rise']=np.where(stock_price_df['daily_return']>0, 1, 0)
    
    
    # Adding company info
    company_info_df=company_df.copy()
    company_info_df.drop(['bloomberg_unique','margin_initial_ratio','maintenance_ratio','day_trade_ratio', 'list_date', 'default_collar_fraction', 'open', 'high', 'low', 'volume', 'average_volume_2_weeks', 'average_volume', 'high_52_weeks', 'dividend_yield', 'float', 'low_52_weeks', 'market_cap', 'pb_ratio', 'pe_ratio', 'shares_outstanding', 'description'], axis=1, inplace=True)
    row=company_info_df.loc[company_info_df['symbol'] == stock_symbol]
    if(len(row)>0):
        stock_price_df['simple_name']=row['simple_name'].values[0]
        stock_price_df['name']=row['name'].values[0]
        stock_price_df['country']=row['country'].values[0]
        stock_price_df['headquarters_city']=row['headquarters_state'].values[0]
        stock_price_df['sector']=row['sector'].values[0]
        stock_price_df['industry']=row['industry'].values[0]

    # Adding news data
    news_df_copy=news_df.copy()
    news_df_copy.drop(['Label'], axis=1, inplace=True)
    news_df_copy.set_index('Date', inplace=True)
    df_merge=pd.merge(stock_price_df, news_df_copy, left_index=True, right_index=True)
    
    return df_merge

def save_to_s3(df, stock, folder):
    df_copy=df.copy()
    df_copy.reset_index(inplace=True)
    s3 = boto3.resource('s3')
    json_data=df_copy.to_json(orient='records')
    key="{}/{}.json".format(folder, stock)
    s3object = s3.Object('stock.etl', key)
    s3object.put(Body=(bytes(json.dumps(json_data).encode('UTF-8'))))


def load_historic_process_save_s3(key_company, key_news, key_stock):
    
    # get company info
    df_companyinfo_list= convert_zip_to_dfs("stock.etl", key_company)
    df_companyinfo_key=list(df_companyinfo_list.keys())[0]
    df_company_info=df_companyinfo_list.get(df_companyinfo_key)

    # get news info
    df_news_list= convert_zip_to_dfs("stock.etl", key_news)
    df_news_historic=df_news_list.get('Combined_News_DJIA')
    
    # get stock data
    stock_dict_historic=convert_stock_from_zip(bucket_name="stock.etl", key=key_stock)
    
    # process and save
    
    for key, value in stock_dict_historic.items():
        stock_name=key.upper()
        df=data_transformation(df_company_info, df_news_historic, value, stock_name)
        save_to_s3(df, stock=stock_name, folder="historic.combine")

def download_current_stocks_to_df(stocks, ndays):
    end=datetime.datetime.now().strftime("%Y-%m-%d")
    start=(datetime.datetime.now()-datetime.timedelta(days=ndays)).strftime("%Y-%m-%d")
    stock_dict={}
    for symbol in stocks:
        df_symbol=yf.download(symbol,start,end,progress=False)
        stock_dict.update({symbol:df_symbol})
    return stock_dict

def get_24hr_news():
    reddit = praw.Reddit(client_id='FbxkAEhvb7pxbg', \
                     client_secret='zyl8WnU9X5rr7y5_EeGUVj994kA', \
                     user_agent='topnewsscraper', \
                     username='xintao0202', \
                     password='zangma53@198422')
    subreddit = reddit.subreddit('worldnews')
    top_subreddit = subreddit.top("day")
    topics_dict = { "title":[], "score":[], "created": []}
    
    for submission in top_subreddit:
        topics_dict["title"].append(submission.title)
        topics_dict["score"].append(submission.score)
    #     topics_dict["id"].append(submission.id)
    #     topics_dict["url"].append(submission.url)
    #     topics_dict["comms_num"].append(submission.num_comments)
        topics_dict["created"].append(datetime.datetime.fromtimestamp(submission.created))
    #     topics_dict["body"].append(submission.selftext)
    
    topics_df = pd.DataFrame(topics_dict)
    return topics_df

def transformation_save_stock(stock_df, stock_symbol):
    # First Keep useful information from stock price tables (Date, close and volume), forward fill missing values. 
    stock_price_df=stock_df.copy()
    stock_price_df.drop(['Open','High','Low', 'Close'], axis=1, inplace=True)
    stock_price_df.fillna(method='ffill')
    stock_price_df.rename(columns={'Adj Close':'Close'}, inplace=True)
    
    
    # Adding daily return (%), stock_rise (1, 0 if drop), technical indicators 
    stock_price_df['daily_return']=stock_price_df[['Close']]/stock_price_df[['Close']].shift(1)-1
    stock_price_df=get_bbands(stock_price_df, 10)
    stock_price_df=get_SMA(stock_price_df, 10)
    stock_price_df=get_EMA(stock_price_df, 10)
    stock_price_df=get_ROC(stock_price_df, 1)
    stock_price_df['stock_rise']=np.where(stock_price_df['daily_return']>0, 1, 0)
    
    # save stock data
    today_date=datetime.datetime.now().strftime("%Y.%m.%d")
    save_to_s3(stock_price_df, stock_symbol, "current/stocks/{}".format(today_date))
    
def transformation_save_news(news_df):      
   # process news data
    news_df_copy=news_df.copy()
    news_df_copy['Rank']=news_df_copy['score'].rank(method='dense', ascending=False).astype(int)
    news_df_copy.columns=['News', 'Score', 'Date', 'Rank']
    news_df_copy=news_df_copy[['Date', 'Rank', 'Score', 'News']] 
    
    # save news data
    save_to_s3(news_df_copy, "24hrNews" "current/news/{}".format(today_date))

def current_stocks_etl(list_of_stocks, ndays):
    stocks_dict=download_current_stocks_to_df(list_of_stocks, ndays)
    for key, value in stocks_dict.items():
        stock_name=key.upper()
        transformation_save_stock(value, stock_name)
    
def current_news_etl():
    topics_df=get_24hr_news()
    transformation_save_news(topics_df)

def data_quality_check(folder, n_files):
    output=os.system("aws s3 ls s3://stock.etl/{}/ --recursive | wc -l".format(folder))
    if (output!=n_files):
        raise ValueError('Data quality check failed: file number not match')
    status, output = commands.getstatusoutput("aws s3api list-objects-v2 --bucket stock.etl --prefix {} --output text --query 'sort_by(Contents,&Size)[0].Size'".format(folder))
    if (int(output[0]))<=0:
        raise ValueError('Data quality check failed: contains empty file')
