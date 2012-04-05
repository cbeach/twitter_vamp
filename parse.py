import raw_twitter_subscriber as rts

class parser:
    def __init__(self, source_type, source_name):
        self.tweet_source = rts.twitter_feed(source_type, source_name, self.parse_tweet)
        self.tweet_source.start_feed()

    def parse_tweet(ch = None, message = None, properties = None,body = None):
        if body.get('text') != None:
            self.publish_text(body.get('id'),body.get('text'))

        if body.get('entities') != None:
            if body.get('entities').get('user_mentions') != None:
                self.publish_mentions(body.get('id'),body.get('entities').get('user_mentions'))
            if body.get('entities').get('hashtags') != None:
                self.publish_hashtags(body.get('id'),body.get('entities').get('hashtags'))
            if body.get('entities').get('urls') != None:
                self.publish_urls(body.get('id'),body.get('entities').get('urls'))
        
        if body.get('user') != None:
            self.publish_user(body.get('id'), body.get('user'))        
        
        if body.get('place') != None:
            self.publish_place(body.get('id'),body.get('place'))

        #implement delete
    
    def publish_text(self,text_id,text):
        pass
    def publish_mentions(self,id, entities):
        pass
    def publish_hashtags(self,id, entities):
        pass
    def publish_urls(self,id, entities):
        pass
    def publish_user(self,id,user):
        pass
    def publish_place(self,id,place):
        pass
    def publish_delete(self,id,delete):
        pass

if __name__=='__main__':
    p = parser(2,'sample.1000.json')
     
