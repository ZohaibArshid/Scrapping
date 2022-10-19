import snscrape.modules.twitter as sntwitter
import pandas as pd
import sqlite3
from datetime import timedelta
import time
import numpy as np
import datetime
import os

root_dir = os.path.dirname(os.path.abspath(__file__))

def scrap_tweets(start_time,end_time,start_min,end_min):
    #start_time, end_time = current_time()
    crypto_hashtags = ['cryptoworld', 'cryptocurrencies', '#btc', '#cryptoinvestor', '#invest', '#forexlifestyle', '#cryptotrading', '#cryptos', '#bitcoin', '#cryptocurrency']
    maxTweets = 100
    tweets_list1 = []
    try:
        for it in crypto_hashtags:
            for i,tweet in enumerate(sntwitter.TwitterSearchScraper(f'{it} since_time:{start_time} until_time:{end_time}').get_items()):
                if i%3==0:
                    pass
                    #print(i)
                if i>maxTweets:
                    break
                tweets_list1.append([tweet.date, tweet.id, tweet.content, tweet.user.username, tweet.url, tweet.user, tweet.outlinks, tweet.tcooutlinks, tweet.replyCount, tweet.retweetCount, tweet.likeCount, tweet.quoteCount, tweet.conversationId, tweet.lang, tweet.source, tweet.media, tweet.quotedTweet, tweet.mentionedUsers])


        tweets_df1 = pd.DataFrame(tweets_list1, columns=['Datetime', 'Tweet Id', 'Text', 'Username', 'Permalink', 'User', 'Outlinks', 'CountLinks', 'ReplyCount', 'RetweetCount', 'LikeCount', 'QuoteCount', 'ConversationId', 'Language', 'Source', 'Media', 'QuotedTweet', 'MentionedUsers'])
        tweets_df1['hashtag'] = tweets_df1.Text.str.findall(r'#.*?(?=\s|$)')
        tweets_df1['hastag_counts']=tweets_df1.apply(lambda x: len(x['hashtag']), axis = 1 )
        test_df = tweets_df1.copy()
        test_df = test_df.drop_duplicates(subset=['Tweet Id'])
        test_df['Datetime'] =pd.to_datetime(test_df.Datetime)
        test_df = test_df.sort_values(by='Datetime')

        test_df.drop(['Tweet Id','Username', 'Permalink','User','Outlinks','ConversationId', 'Source', 'Media', 'QuotedTweet', 'MentionedUsers'], inplace=True, axis=1)        
        test_df['CountLinks']= test_df['CountLinks'].astype('str')
        test_df['hashtag']= test_df['hashtag'].astype('str')
        print('Done....')

        test_df['Datetime']= pd.to_datetime(test_df['Datetime']) 
        test_df['Date'] = np.where((test_df['Datetime'].dt.minute.values[0] >= start_min) & (test_df['Datetime'].dt.minute.values[0] <= end_min),True,False)
        test_df = test_df[test_df.Date]
        test_df.drop('Date',axis=1,inplace=True)
        print(start_time)
        sqliteConnection = sqlite3.connect(os.path.join(root_dir, 'db/tweets.db'))
        cursor = sqliteConnection.cursor()
        #print("Successfully Connected to SQLite")
        test_df.to_sql('Twitter', sqliteConnection, if_exists='append', index=False)
        print("Record inserted successfully into Twiter_Tweets table ")
        cursor.close()
        sqliteConnection.close()
    except Exception as e:
            print(f'Exception msg: {e}')
#------------------------------Function End---------------------------#
    
start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=11)
end_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
df = scrap_tweets(int(round(start_time.timestamp())),int(round(end_time.timestamp())),start_time.minute,end_time.minute)
next_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=11)

while True:
    if datetime.datetime.utcnow().minute == next_time.minute:
        # function_call
        scrap_tweets(int(round(start_time.timestamp())),int(round(end_time.timestamp())),start_time.minute,end_time.minute)
        #print(start_time,'Send Start Time')
        #print(end_time,'Send End Time')
        start_time = end_time
        end_time = next_time - datetime.timedelta(minutes=1)
        next_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=11)
        time.sleep(1)    
    
