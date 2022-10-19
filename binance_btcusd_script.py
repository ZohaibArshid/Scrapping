from binance import AsyncClient, DepthCacheManager,BinanceSocketManager
import numpy as np
import pandas as pd

class Binance:
    def __init__(self,start_date,end_date,interval):
        self.start_date = start_date
        self.end_date = end_date
        self.interval = interval
        self.df = None

    def __interval_record(df):
        return [
        df[:1]['Date'].values[0],
        df[:1]['Open'].values[0],
        df[-1:]['Close'].values[0],
        #df[:1]['Close Time'].values,    
        df['High'].max(),
        df['Low'].min(),
        df['Volume'].astype(float).mean(),
        df['Number of Trades'].astype(float).mean()]

    def __clean_and_interpolate(df):
        start_date = df.index[0]
        end_date = df.index[-1]
        idx = pd.date_range(start_date, end_date, freq = "1min")
        df = df[~df.index.duplicated(keep='first')]
        df = df.reindex(idx, fill_value = np.nan)
        #df.interpolate(method ='linear', limit_direction ='forward', inplace = True)
        df.fillna(method="ffill",inplace=True)
        df = df[~df.index.duplicated(keep='first')]
        return df

    async def fetch_per_min_data(self):
        try:
#             day,hour,minute = self.time.split(':')
#             interval = (int(day)*24*60)+(int(hour)*60)+int(minute)

            client = await AsyncClient.create("AdhKYlShCSMCTowXb08ALlY5BGFd4zzizNDw9XxikYAKF0h5sr1JccrFQaRsD6xa",
                "ruruiViJQ7PBnhiVmyoqown7UvkQ0C1YOeEkGlThsOGEsRCoaC8OY9XZNCN1mccT")       
            lst = []
            async for kline in await client.get_historical_klines_generator("BTCUSDT", AsyncClient.KLINE_INTERVAL_1MINUTE,self.start_date,self.end_date):
                lst.append(kline)
            df = pd.DataFrame(lst)
            df.columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume','Close Time', 'Quote Asset Volume', 
                    'Number of Trades', 'TB Base Volume', 'TB Quote Volume', 'Ignore']
            df.drop(['Close Time','Quote Asset Volume', 'TB Base Volume', 'TB Quote Volume', 'Ignore'],inplace=True,axis=1)
            df['Date'] = pd.to_datetime(df['Open Time']/1000, unit='s')    
            df['Open Time'] = pd.to_datetime(df['Open Time']/1000, unit='s')
            #df['Close Time'] = pd.to_datetime(df['Close Time']/1000, unit='s')
            #print(df.head())

            #df2 = pd.DataFrame(df)
            df.set_index('Open Time',inplace=True)
            await client.close_connection()
            df = clean_and_interpolate(df)

            interval_records_list = []
            record_list = []
            new = pd.DataFrame()
            for index, row in df.iterrows():
                record_list.append(row)

                if(len(record_list) == self.interval):
                    new = pd.DataFrame(record_list)
                    interval_records_list.append(interval_record(new))
                    record_list = []

            new = pd.DataFrame(record_list)
            interval_records_list.append(interval_record(new))
            df = pd.DataFrame(interval_records_list)

            df.columns = ['Date','Open', 'Close', 'High', 'Low', 'Volume','Number of Trades']
            return df
        except Exception as e:
                print(f'Exception msg: {e}')

                

obj = Binance("1 Dec, 2017", "10 Dec, 2017",10) #minutes
await obj.fetch_per_min_data()

