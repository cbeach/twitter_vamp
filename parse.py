import raw_twitter_subscriber as rts
import pika, pickle

class parser:
    def __init__(self, source_type, source_name=None):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='direct.text',type='direct')
        self.channel.exchange_declare(exchange='direct.mentions',type='direct')
        self.channel.exchange_declare(exchange='direct.hashtags',type='direct')
        self.channel.exchange_declare(exchange='direct.urls',type='direct')
        self.channel.exchange_declare(exchange='direct.user',type='direct')
        self.channel.exchange_declare(exchange='direct.place',type='direct')
        self.channel.exchange_declare(exchange='direct.delete',type='direct')
        
        self.tweet_source = rts.twitter_feed(source_type, self.parse_tweet, source_name = source_name, exchange='direct.raw', routing_key = 'raw')
        self.tweet_source.start_feed()

    def parse_tweet(self, ch, message, properties, body):
        print('hello')
        if u'text' in body:
            self.publish_text(body.get('id'),body.get('text'))

        if u'entities' in body:
            if u'user_mentions' in body.get('entities'):
                self.publish_mentions(body.get('id'),body.get('entities').get('user_mentions'))
            if u'hashtags' in body.get('entities'):
                self.publish_hashtags(body.get('id'),body.get('entities').get('hashtags'))
            if u'urls' in body.get('entities'):
                self.publish_urls(body.get('id'),body.get('entities').get('urls'))
        
        if u'user' in body:
            self.publish_user(body.get('id'), body.get('user'))        
        
        if u'place' in body:
            self.publish_place(body.get('id'),body.get('place'))
        if u'delete' in body:
            self.publish_delete(body)
        #implement delete
    
    def publish_text(self,id,text):
        message = {'id':id, 'text':text}
        self.channel.basic_publish(exchange='direct.text', routing_key='parse.text', body=pickle.dumps(message))
        
    def publish_mentions(self,id, mentions):
        message = {'id':id, 'mentions':mentions}
        self.channel.basic_publish(exchange='direct.mentions', routing_key='parse.mentions', body=pickle.dumps(message))
        
    def publish_hashtags(self,id, hashtags):
        message = {'id':id, 'hashtags':hashtags}
        self.channel.basic_publish(exchange='direct.hashtags', routing_key='parse.hashtags', body=pickle.dumps(message))
        
    def publish_urls(self,id, urls):
        message = {'id':id, 'urls':urls}
        self.channel.basic_publish(exchange='direct.urls', routing_key='parse.urls', body=pickle.dumps(message))
        
    def publish_user(self,id,user):
        message = {'id':id, 'users':user}
        self.channel.basic_publish(exchange='direct.user', routing_key='parse.user', body=pickle.dumps(message))
        
    def publish_place(self,id,place):
        message = {'id':id, 'place':place}
        self.channel.basic_publish(exchange='direct.place', routing_key='parse.place', body=pickle.dumps(message))
        
    def publish_delete(self,delete):
        self.channel.basic_publish(exchange='direct.delete', routing_key='parse.delete', body=pickle.dumps(delete))
        

if __name__=='__main__':
    p = parser(3)
     
