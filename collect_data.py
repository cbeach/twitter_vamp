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
        self.db_init()
        self.create_tables()
        
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
        except MySQLdb.Error, e:
            print('Error %d: %s', (e.args[0], e.args[1]))
            sys.exit()
        
    def create_tables(self):
        self.db_cursor.execute("CREATE TABLE IF NOT EXISTS \
        tv_tweets(tweet_id BIGINT PRIMARY KEY, created_at INT, text VARCHAR(150), \
        source VARCHAR(512), favorited BIT, truncated BIT, retweeted BIT, \
        retweet_count INT, in_reply_to_user_id INT, in_reply_to_status_id LONG, \
        in_reply_to_screen_name VARCHAR(512), in_reply_to_user_id_str LONG, \
        in_reply_to_id_str LONG, id_str LONG, geo_type INT, lat FLOAT, lon FLOAT, \
        user_id INT, FOREIGN KEY (user_id) REFERENCES tv_users(user_id), \
        place_id BIGINT, FOREIGN KEY (place_id) REFERENCES tv_places(place_id) )")

#properties in user_flags bit field--------------------
#profile_use_background_image BIT, \
#contributors_enabled BIT, \
#verified BIT, \
#is_translator BIT, \
#geo_enabled BIT, \
#protected BIT, \
#default_profile BIT, \
#profile_background_title BIT, \
#show_all _inline_media BIT, \
#default_profile_image BIT, \


        self.db_cursor.execute("CREATE TABLE IF NOT EXISTS \
tv_users(user_id BIGINT PRIMARY KEY, \
user_flags BIT(10), \
favorites_count INT, \
friends_count INT, \
listed_count INT, \
utc_offset INT, \
statuses_count INT, \
followers_count INT, \
profile_image_url_https VARCHAR(512), \
profile_image_url VARCHAR(512), \
profile_background_image_url_https VARCHAR(512), \
profile_background_image_url VARCHAR(512), \
profile_sidebar_fill_color INT, \
profile_text_color INT, \
profile_link_color INT, \
profile_background_color INT, \
id_str INT, \
created_at INT, \
time_zone INT, \
profile_sidebar_border_color INT, \
screen_name VARCHAR(512), \
url VARCHAR(512), \
description VARCHAR(512), \
lang VARCHAR(24), \
place INT, \
FOREIGN KEY (place) REFERENCES tv_places(place_id) \
)")  
        
        self.db_cursor.execute("CREATE TABLE IF NOT EXISTS \
tv_places(place_id BIGINT PRIMARY KEY, \
name VARCHAR(32), \
url VARCHAR(512), \
country VARCHAR(32), \
place_type VARCHAR(32), \
country_code CHAR(2), \
full_name VARCHAR(70), \
bounding_box VARBINARY(128), \
attributes VARBINARY(1024)\
)"      )


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

