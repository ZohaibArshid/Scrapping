#from binance import AsyncClient, DepthCacheManager,BinanceSocketManager
from unicodedata import decimal
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
import binance
import numpy as np
import pandas as pd
import sqlite3
import time
import datetime
import os

root_dir = os.path.dirname(os.path.abspath(__file__))


def clean_and_interpolate(df):
    start_date = df.index[0]
    end_date = df.index[-1]
    idx = pd.date_range(start_date, end_date, freq = "1min")
    df = df[~df.index.duplicated(keep='first')]
    df = df.reindex(idx, fill_value = np.nan)
    df.fillna(method="ffill",inplace=True)
    df = df[~df.index.duplicated(keep='first')]
    return df



def fetch_per_min_data(start,min_to_fetch):
    api_key = os.environ.get('BINANCE_KEY') #"AdhKYlShCSMCTowXb08ALlY5BGFd4zzizNDw9XxikYAKF0h5sr1JccrFQaRsD6xa"
    api_secret = os.environ.get('BINANCE_SECRET') #"ruruiViJQ7PBnhiVmyoqown7UvkQ0C1YOeEkGlThsOGEsRCoaC8OY9XZNCN1mccT"
    try:
        client = binance.Client(api_key, api_secret)
        klines = client.get_historical_klines("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE,start)
        df = pd.DataFrame(klines)
        df.columns = ['Open_Time', 'Open', 'High', 'Low', 'Close', 'Volume','Close_Time', 'Quote_Asset_Volume', 
                        'Number_of_Trades', 'TB_Base_Volume', 'TB_Quote_Volume', 'Ignore']


        df.drop(['Close_Time','Quote_Asset_Volume', 'TB_Base_Volume', 'TB_Quote_Volume', 'Ignore'],inplace=True,axis=1)
        df['Date'] = pd.to_datetime(df['Open_Time']/1000, unit='s')    
        df['Open_Time'] = pd.to_datetime(df['Open_Time']/1000, unit='s')
        df.set_index('Open_Time',inplace=True)


        df["Open"] = df.Open.astype(float).round(decimals = 2)
        df["High"] = df.High.astype(float).round(decimals = 2)
        df["Low"] = df.Low.astype(float).round(decimals = 2)
        df["Close"] = df.Close.astype(float).round(decimals = 2)
        df = df[['Date','Open','Close','High','Low','Volume','Number_of_Trades']]    

        client.close_connection()
        #df = clean_and_interpolate(df)
        fetch_data_min = pd.to_datetime(df['Date']).dt.minute.values[0]
        print('to fetched',min_to_fetch)
        print('fetched',fetch_data_min)
        if fetch_data_min == min_to_fetch:
            sqliteConnection = sqlite3.connect(os.path.join(root_dir, 'db/binance.db'))
            cursor = sqliteConnection.cursor()
            #print("Successfully Connected to SQLite")
            df.to_sql('Binance', sqliteConnection, if_exists='append', index=False)
            print("Record inserted into Binance table ")
            cursor.close()
            sqliteConnection.close()
        else:
            fetch_per_min_data(start,min_to_fetch)
    except Exception as e:
            print(f'Exception msg: {e}')


def fetch_missing_data():
    api_key = os.environ.get('BINANCE_KEY')
    api_secret = os.environ.get('BINANCE_SECRET') 
    try:
        cnx = sqlite3.connect(os.path.join(root_dir, 'db/binance.db'))
        df = pd.read_sql_query("SELECT * FROM Binance", cnx)
        cnx.close()
        last_date = df.tail(1).Date
        last_date = last_date.values[0]
        next_time = pd.to_datetime(last_date) + datetime.timedelta(minutes=1)
        start = str(next_time)
        end = str(datetime.datetime.utcnow())
        
        client = Client(api_key, api_secret)
        klines = client.get_historical_klines("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE,start,end)
        df = pd.DataFrame(klines)
        df.columns = ['Open_Time', 'Open', 'High', 'Low', 'Close', 'Volume','Close_Time', 'Quote_Asset_Volume', 
                        'Number_of_Trades', 'TB_Base_Volume', 'TB_Quote_Volume', 'Ignore']
        df.drop(['Close_Time','Quote_Asset_Volume', 'TB_Base_Volume', 'TB_Quote_Volume', 'Ignore'],inplace=True,axis=1)
        df['Date'] = pd.to_datetime(df['Open_Time']/1000, unit='s')    
        df['Open_Time'] = pd.to_datetime(df['Open_Time']/1000, unit='s')
        df.set_index('Open_Time',inplace=True)

        df["Open"] = df.Open.astype(float).round(decimals = 2)
        df["High"] = df.High.astype(float).round(decimals = 2)
        df["Low"] = df.Low.astype(float).round(decimals = 2)
        df["Close"] = df.Close.astype(float).round(decimals = 2)
        df = df[['Date','Open','Close','High','Low','Volume','Number_of_Trades']]    


        client.close_connection()
        df = clean_and_interpolate(df)
        sqliteConnection = sqlite3.connect(os.path.join(root_dir, 'db/binance.db'))
        cursor = sqliteConnection.cursor()
        df.to_sql('Binance', sqliteConnection, if_exists='append', index=False)
        print("Missing Record inserted")
        cursor.close()
        sqliteConnection.close()
    except Exception as e:
            print(f'Exception msg: {e}')
            
            
    
fetch_missing_data()
current = datetime.datetime.now().minute
while True:
    if datetime.datetime.now().minute != current:
        current = datetime.datetime.now().minute
        fetch_per_min_data(str(datetime.datetime.utcnow()),current)
        time.sleep(1)
