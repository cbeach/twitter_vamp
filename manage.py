import json, time, datetime, sys, commands, pickle, config, pika, bz2, tweepy
import subprocess
from core.fetcher import * 
from core.archiver import * 

consumer_key = config.consumer_key
consumer_secret = config.consumer_secret
access_token = config.access_token
access_token_secret = config.access_token_secret

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, parser=RawJsonParser())

print "logged in"

while(True):
    archiver_sp = subprocess.Popen(['python', 'core/archiver.py', 'data/raw'])
#    parser_sp = subprocess.Popen(['python', 'core/parse.py'])
    try:
        watcher = StreamListener()
        streamer = tweepy.Stream(auth=auth, listener=watcher)
        streamer.sample()
    except Exception as e:
        print("Error Encountered: %s" % e)
        time.sleep(30)  
