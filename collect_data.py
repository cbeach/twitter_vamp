import pycurl, json, time, sys, commands, pickle, MySQLdb
from sets import Set

STREAM_URL = 'https://stream.twitter.com/1/statuses/sample.json'


USER = "avatar_223"
PASS = "Omicron2"

class Client:
    type_set = Set()
    tweet_count = 0
    f = open('speed_test', 'a')
    p = open('pickled', 'w')
    tweets = []
    searching = ["follow_request_sent", "notifications", "following"]
    
    def __init__(self):
        
        
        #Open a connection to the twitter streaming api
        self.buffer = ""
        self.twitter_con = pycurl.Curl()
        self.twitter_con.setopt(pycurl.USERPWD, "%s:%s" % (USER, PASS))
        self.twitter_con.setopt(pycurl.URL, STREAM_URL)
        self.twitter_con.setopt(pycurl.WRITEFUNCTION, self.on_receive)
        self.twitter_con.perform()
    
    def db_init(self):
        #Initialize the connection to the database
        try:
            self.db_con = MySQLdb.connect('localhost', 'twitter_test', 'twittertest', 'twitter_test')
            self.db_cursor = self.db_con.cursor()
            self.create_tables()
        except _mysql.Error, e:
            print('Error %d: %s', (e.args[0], e.args[1]))
            sys.exit()
   
    def create_tables(self):
        self.db_cursor.execute("CREATE TABLE IF NOT EXISTS \
        tv_tweets(id LONG PRIMARY KEY, \
        created_at INT, \
        VARCHAR(150) text, \
        VARCHAR(512) source, \
        BIT favorited,\
        BIT truncated, \
        BIT retweeted, \
        INT retweet_count,/
        INT in_reply_to_user_id, \
        LONG in_reply_to_status_id, \
        VARCHAR(512) in_reply_to_screen_name, \
        LONG in_reply_to_user_id_str, \
        LONG in_reply_to_id_str, \
        LONG id_str, \
        INT geo_type, \
        FLOAT lat, \
        FLOAT long, \
        INT user_id, \
        FOREIGN KEY (user_id) REFERENCES user(user_id), \
        LONG place, \
        FOREIGN KEY (place_id) REFERENCES place(id),
        INT hashtag_id, \
        FOREIGN KEY (hashtag_id) REFERENCES hashtags(tweet_id), \
        INT mention_id, \
        FOREIGN KEY (mention_id) REGERENCES mentions(tweet_id), \
        INT url_id, \
        FOREIGN KEY (url_id) REFERENCES urls(tweet_id), \
        )")

    def on_receive(self, data):
        self.buffer += data

        if data.endswith("\r\n") and self.buffer.strip():
            content = json.loads(self.buffer)
            self.buffer = ""
            
            if self.tweet_count < 1000:
                self.tweets.append(content)
                print(self.tweet_count)
            else:
                print("Done!")
                self.p.write(pickle.dumps(self.tweets))
                return 0
            self.tweet_count += 1
    def print_user_attrib(self,data, attrib):
        try:
            if data["user"] != None:
                if data["user"][attrib] != None:
                    print(type(data["user"][attrib]), attrib)
                    return True

        except TypeError:
            return False
        except KeyError:
            return False

    def get_tweets(self):
        return self.tweet_count[:]

    def close_culr(self):
        self.conn.close()

    def is_iterable(self,obj):
        try:
            it = iter(obj)
        except TypeError:
            return False
        return True

    def search_object(self, obj, base_string):
        types_and_keys = Set()
        if self.is_iterable(obj):
            try:
                for i in obj.keys():
                    if self.is_iterable(obj) == True:
                        types_and_keys.add(self.search_object(obj[i], base_string+"->"+i))
                    else:
                        return base_string+"->"+str(type((obj[i])),i)+"\n"
            except AttributeError:
                return base_string+"->"+str(type(obj))
        return types_and_keys




if __name__ == "__main__":
    client = Client()

