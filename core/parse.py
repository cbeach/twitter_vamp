import pika, pickle, json, redis, config
import twitter_subscriber as rts

class parser:
    def __init__(self, source_name=None, publish_host='localhost'):
        """
        Open the channel and connection to rabbitmq for publishing.
        Then create the Twitter feed for getting the raw tweets from
        the API
        """
        self.redis_server = redis.Redis(config.REDIS_SERVER)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=publish_host))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='direct.text',type='direct')
        self.channel.exchange_declare(exchange='direct.mentions',type='direct')
        self.channel.exchange_declare(exchange='direct.hashtags',type='direct')
        self.channel.exchange_declare(exchange='direct.urls',type='direct')
        self.channel.exchange_declare(exchange='direct.user',type='direct')
        self.channel.exchange_declare(exchange='direct.place',type='direct')
        self.channel.exchange_declare(exchange='direct.delete',type='direct')
        self.channel.exchange_declare(exchange='direct.uhmr', type='direct')


        self.tweet_source = rts.twitter_feed(self.parse_tweet, source_name = source_name, exchange='direct.raw', routing_key = 'raw', exchange_type = 'fanout')
        self.tweet_source.start_feed()

    def parse_tweet(self, ch, message, properties, body):
        subscribed = self.redis_server.get('active_feeds') 
        for sub in subscribed:
            func = getattr(self, sub)
            func(body)
    
    def text(self,tweet):
        if u'text' in body:
            message = {'id':tweet.get(id), 'text':tweet.get(text)}
            self.channel.basic_publish(exchange='direct.text', routing_key='parse.text', body=json.dumps(message))
        
    def mentions(self,tweet):
        if u'entities' in body:
            if u'user_mentions' in body.get('entities'):
                message = {'id':tweet.get(id), 'mentions':tweet.get(mentions)}
                self.channel.basic_publish(exchange='direct.mentions', routing_key='parse.mentions', body=json.dumps(message))
        
    def hashtags(self, tweet):
        if u'entities' in body:
            if u'hashtags' in body.get('entities'):
                message = {'id':tweet.get(id), 'hashtags':tweet.get(hashtags)}
                self.channel.basic_publish(exchange='direct.hashtags', routing_key='parse.hashtags', body=json.dumps(message))
        
    def urls(self, tweet):
        if u'entities' in body:
            if u'urls' in body.get('entities'):
                message = {'id':tweet.get(id), 'urls':tweet.get(urls)}
                self.channel.basic_publish(exchange='direct.urls', routing_key='parse.urls', body=json.dumps(message))
        
    def user(self, tweet):
        if u'user' in body:
            message = {'id':tweet.get(id), 'user':tweet.get(user)}
            self.channel.basic_publish(exchange='direct.user', routing_key='parse.user', body=json.dumps(message))
        
    def place(self, tweet):
        if u'place' in body:
            message = {'id':tweet.get(id), 'place':tweet.get(place)}
            self.channel.basic_publish(exchange='direct.place', routing_key='parse.place', body=json.dumps(message))
        
    def delete(self, tweet):
        if u'delete' in body:
            self.channel.basic_publish(exchange='direct.delete', routing_key='parse.delete', body=json.dumps(delete))

    def social_info(self, tweet):

        if u'user' in body and u'entities' in body:
            message = {'id':tweet.get(id), 'user':tweet.get(user), 'entities':tweet.get(entities),}
            self.channel.basic_publish(exchange='direct.uhmr', routing_key='parse.uhmr', body=json.dumps(message))

if __name__ == "__main__":
    p = parser()
