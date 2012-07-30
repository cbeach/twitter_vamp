import twitter_subscriber as ts
import json, pickle, time, sys


class user_list:

    def __init__(self):
        self.users = {} 
        self.tweet_count = 0 
        
        self.tweet_source = ts.twitter_feed(self.receive, exchange='direct.uhmr', routing_key = 'parse.uhmr')
        self.tweet_source.start_feed()

    def receive(self, ch, properties, message, body):
        if u'user' in body and u'entities' in body:
            user_name = body.get('user').get('name')
            #if the user name is pressent, increment values
            if user_name in self.users.keys():
                self.users[user_name]['count'] += 1
                for tag in body[u'entities'][u'hashtags']:
                    if u'text' in tag:
                        if tag[u'text'] in self.users[user_name][u'hashtags']:
                            self.users[user_name][u'hashtags'][tag[u'text']] += 1
                        else:
                            self.users[user_name][u'hashtags'][tag[u'text']] = 1    
                for mention in body[u'entities'][u'user_mentions']:
                    if u'screen_name' in mention:
                        if mention[u'screen_name'] in self.users[user_name][u'user_mentions']:
                            self.users[user_name][u'user_names'][mention[u'screen_name']] += 1
                        else:
                            self.users[user_name][u'user_names'][mention[u'screen_name']] = 1
            else:
                self.users[user_name] = {'count':1, u'user_names':{}, u'user_mentions':{}, u'hashtags':{}}    
                
        self.tweet_count += 1
        print(self.tweet_count)
        if self.tweet_count == 100000:
            self.write_to_file()
        
    def write_to_file(self):
    
        self.user_list = open("./raw/user_list",'w')
        pickle.dump(self.users, self.user_list)
        self.user_list.close()

u = user_list()


