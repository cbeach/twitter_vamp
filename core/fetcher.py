import json, time, datetime, sys, commands, pickle, config, pika, bz2, tweepy
from tweepy.parsers import *
from sets import Set


class StreamListener(tweepy.StreamListener):
    def __init__(self, *args, **kwargs):
        super(StreamListener, self).__init__(*args, **kwargs)
        self.buff = ''

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.RABBITMQ_HOST))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=config.RAW_EXCHANGE, type='fanout')

    def on_data(self, data):
        try:
            self.channel.basic_publish(exchange=config.RAW_EXCHANGE, routing_key='raw', body=data)
            return None
        except Exception, e:
            # Catch any unicode errors while printing to console
            # and just ignore them to avoid breaking application.
            print "Exception: %s" % e
            pass

class RawJsonParser(Parser):
    def parse(self, method, payload):
            return payload

def instantiate_stream():
    l = StreamListener()
    streamer = tweepy.Stream(auth=auth, listener=l)
    streamer.sample()
    return 

if __name__ == "__main__":

    query_rate = 145 #queries / hr

    consumer_key= config.consumer_key
    consumer_secret=config.consumer_secret
    access_token=config.access_token
    access_token_secret=config.access_token_secret

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, parser=RawJsonParser())

    print "logged in as %s" % api.me()

    while(True):
        try:
            instantiate_stream()
        except Exception as e:
            print("Error Encountered: %s" % e)
            time.sleep(30)  
            
