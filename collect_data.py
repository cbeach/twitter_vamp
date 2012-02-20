import pycurl, json, time, datetime, sys, commands, pickle, MySQLdb
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

    user_flags =     ["profile_use_background_image", "contributors_enabled", \
                      "verified", "is_translator", "geo_enabled","protected", \
                      "default_profile", "profile_background_title", \
                      "show_all_inline_media", "default_profile_image"]
    tweet_flags =    ["favorited", "truncated", "retweeted"]

    user_columns =   ["id", "user_flags", "favourites_count", "friends_count", \
                      "listed_count", "utc_offset", "statuses_count", "followers_count", \
                      "profile_image_url_https", "profile_image_url", "profile_background_image_url_https", \
                      "profile_background_image_url", "profile_sidebar_fill_color", "profile_text_color", \
                      "profile_link_color", "profile_background_color", "id_str", "created_at", "time_zone", \
                      "profile_sidebar_border_color", "screen_name", "url", "description", "lang", "place"]
    tweet_columns =  ["id", "created_at", "text", "source", "tweet_flags", "retweet_count", "in_reply_to_user_id", \
                      "in_reply_to_status_id", "in_reply_to_screen_name", \
                      "in_reply_to_user_id_str", "in_reply_to_id_str", "id_str", \
                      "geo_type", "lat", "lon", "user_id", "place_id"]

    place_columns =  ["id", "name", "url", "country", "place_type", "country_code", "full_name", \
                      "bounding_box","attriubutes"]        
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
        tv_tweets(tweet_id BIGINT PRIMARY KEY, created_at INT, text VARCHAR(256), \
        source VARCHAR(512), tweet_flags INT, \
        retweet_count INT, in_reply_to_user_id INT, in_reply_to_status_id LONG, \
        in_reply_to_screen_name VARCHAR(512), in_reply_to_user_id_str LONG, \
        in_reply_to_id_str LONG, id_str LONG, geo_type VARCHAR(64), lat FLOAT, lon FLOAT, \
        user_id INT, FOREIGN KEY (user_id) REFERENCES tv_users(user_id), \
        place_id BIGINT, FOREIGN KEY (place_id) REFERENCES tv_places(place_id) )")

        self.db_cursor.execute("CREATE TABLE IF NOT EXISTS \
        tv_users(user_id BIGINT PRIMARY KEY, user_flags BIT(10), favorites_count INT, \
        friends_count INT, listed_count INT, utc_offset INT, statuses_count INT, \
        followers_count INT, profile_image_url_https VARCHAR(512), \
        profile_image_url VARCHAR(512), profile_background_image_url_https VARCHAR(512), \
        profile_background_image_url VARCHAR(512), profile_sidebar_fill_color INT, \
        profile_text_color INT, profile_link_color INT, profile_background_color INT, \
        id_str INT, created_at INT, time_zone VARCHAR(32), profile_sidebar_border_color INT, \
        screen_name VARCHAR(512), url VARCHAR(512), description VARCHAR(512), \
        lang VARCHAR(24), place BIGINT, FOREIGN KEY (place) REFERENCES tv_places(place_id) \
        )")  
        
        self.db_cursor.execute("CREATE TABLE IF NOT EXISTS \
        tv_places(place_id BIGINT PRIMARY KEY, name VARCHAR(32), url VARCHAR(512), \
        country VARCHAR(32), place_type VARCHAR(32), country_code CHAR(2), \
        full_name VARCHAR(70), bounding_box VARBINARY(128), attributes VARBINARY(1024))")

    #TODO: implement insert function
    def db_insert(self, content):
        
        tweet_insert = "INSERT INTO tv_tweets(tweet_id, created_at, text, source, \
        tweet_flags, retweet_count, in_reply_to_user_id, \
        in_reply_to_status_id, in_reply_to_screen_name, in_reply_to_user_id_str, \
        in_reply_to_id_str, id_str, geo_type, lat, lon, user_id, place_id) VALUES("

        user_insert = "INSERT INTO tv_users(user_id, user_flags, favorites_count, \
        friends_count, listed_count, utc_offset, statuses_count, followers_count, \
        profile_image_url_https, profile_image_url, profile_background_image_url_https, \
        profile_background_image_url, profile_sidebar_fill_color, profile_text_color, \
        profile_link_color, profile_background_color, id_str, created_at, time_zone, \
        profile_sidebar_border_color, screen_name, url, description, lang, place) \
        VALUES("

        place_insert = "INSERT INTO tv_places(place_id, name, url, country, place_type, \
        country_code, full_name, bounding_box, attributes) VALUES( "
        
        tweet_data = self.format_tweet(content)
        user_data = self.format_user(content)
        place_data = self.format_place(content)

        if user_data != None: 
            for i in user_data:
                try:
                    user_insert += "'" + str(i) + "'" + ","
                except UnicodeEncodeError:
                    print('Error %d: %s'% (e.args[0], e.args[1]))
                    user_insert += "'NULL',"
            user_insert = user_insert.rstrip(' ,')
            user_insert += ');'
            try: 
                self.db_cursor.execute(user_insert)
            except MySQLdb.IntegrityError as e:
                pass
            except MySQLdb.OperationalError as e:
                print('Error %d: %s'% (e.args[0], e.args[1]))
            except MySQLdb.ProgrammingError as e:
                print('Error %d: %s'% (e.args[0], e.args[1]))
        if tweet_data != None: 
            count = 0
            for i in tweet_data:
                try:
                    tweet_insert += "'" + str(i) + "'" + ","
                except UnicodeEncodeError:
                    print(tweet_insert)
                    print(i)
                    tweet_insert += "'NULL',"
            tweet_insert = tweet_insert.rstrip(' ,') + ');'
            try: 
                self.db_cursor.execute(tweet_insert)
            except MySQLdb.IntegrityError as e:
                pass
            except MySQLdb.OperationalError as e:
                print('Error %d: %s'% (e.args[0], e.args[1]))
            except MySQLdb.ProgrammingError as e:
                print('Error %d: %s'% (e.args[0], e.args[1]))
        if place_data != None:
            for i in place_data:
                try:
                    place_insert += "'" + str(i) + "'" + ","
                except UnicodeEncodeError:
                    print("\n\n\n")
                    print(place_insert)
                    print(i)
                    place_insert += "'NULL',"
            place_insert = place_insert.rstrip(' ,') + ');'
            try: 
                self.db_cursor.execute(place_insert)
            except MySQLdb.IntegrityError as e:
                pass
            except MySQLdb.OperationalError as e:
                print('Error %d: %s'% (e.args[0], e.args[1]))
            except MySQLdb.ProgrammingError as e:
                print('Error %d: %s'% (e.args[0], e.args[1]))
    #----------------------------------------------------
    #
    #This is going to be SOOOO ugly :(
    #
    #----------------------------------------------------
    def format_tweet(self,content):

        bit_field = 0
        values = []

        for i in self.tweet_flags:
            try:
                if content.get(i) != None:
                    bit_field += self.__to_bool(content.get(i))
                bit_field << 1
            except TypeError:
                bit_field << 1 

        for i in self.tweet_columns:
            try:
                if i == "tweet_flags":
                    values.append(bit_field)
                elif i == "created_at":
                    if content.get(i) != None:
                        values.append(time.mktime(time.strptime(content.get("created_at"), '%a %b %d %H:%M:%S +0000 %Y')))
                    else:
                        values.append(time.time())
                elif i == "text" and content.get(i) != None:
                    values.append(MySQLdb.escape_string(content.get(i).encode('utf-8')))
                elif i == "user_id" and content.get("user") != None:
                    values.append(content.get("user").get("id"))
                elif i == "source":
                    if content.get("source") != None:
                        try:
                            values.append(MySQLdb.escape_string(content.get(i).encode('utf-8')))
                        except (ValueError, AttributeError):
                            values.append("NULL")
                    else:
                        values.append(-1)
                elif i == "in_reply_to_user_id":
                    try:
                        values.append(int(content.get("place").get(i),16))
                    except (ValueError, AttributeError,TypeError):
                        values.append(-1)
                elif i == "place_id":
                    if content.get("place") != None:
                        try:
                            values.append(int(content.get("place").get("id"),16))
                        except (ValueError, AttributeError):
                            values.append(-1)
                    else:
                        values.append(-1)
                elif i == "geo" and content.get("geo") != None:
                    values.append(content.get("geo").get("type"))

                elif i == "lat":
                    if content.get("geo") != None:
                        values.append(content.get("geo").get("coordinates")[0])
                    else: 
                        values.append(1000)
                elif i == "lon":
                    if content.get("geo") != None:
                        values.append(content.get("geo").get("coordinates")[1])
                    else: 
                        values.append(1000)
                elif content.get(i) == None:
                    values.append("NULL")

                else:
                    values.append(content.get(i))
            except TypeError as e:
                print "OH NOOOOS"
                print i
                print e
        return values
#TODO: Add a lookup for the place and user foreign keys ------------       
 
    def format_place(self, content):
        
        values = []
        for i in self.place_columns:
            try: 
                if content.get("place") == None:
                    values.append("NULL")
                elif content.get("place").get(i) == None:
                    values.append("NULL")
                elif i == "bounding_box":
                    values.append(content.get("place").get(i).get("coordinates"))
                elif i == "name" or i == "full_name":
                    try:
                        values.append(MySQLdb.escape_string(content.get("place").get(i).encode('utf-8')))
                    except (ValueError, AttributeError):
                        values.append("NULL")
                elif i == "id": 
                    if content.get("place") != None:
                        try:
                            values.append(int(content.get("place").get("id"),16))
                        except (ValueError, AttributeError):
                            values.append(-1)
                    else:
                        values.append(-1)
                else:
                    values.append( content.get("place").get(i))
            except TypeError as e:
                print "OH NOOOOS"
                print i
                print e
        return values


    def format_user(self, content):
        
        bit_field = 0
        values = []

        if content.get("user") == None:
            return None

        for i in self.user_flags:
            try:
                if content.get(i) != None:
                    bit_field += self.__to_bool(content.get(i))
                bit_field << 1
            except TypeError:
                bitfield << 1 
        
        for i in self.user_columns:
            try:
                if i == "user_flags":
                    values.append(bit_field)
                elif i == "created_at":
                    values.append(time.mktime(time.strptime(content.get("created_at"), '%a %b %d %H:%M:%S +0000 %Y')))
                elif i == "profile_background_color":
                    try:
                        values.append(int(content.get("user").get(i),16))
                    except (ValueError, AttributeError):
                        values.append(-1)
                elif i == "profile_text_color":
                    try:
                        values.append(int(content.get("user").get(i),16))
                    except (ValueError, AttributeError):
                        values.append(-1)
                elif i == "profile_sidebar_fill_color":
                    try:
                        values.append(int(content.get("user").get(i),16))
                    except (ValueError, AttributeError):
                        values.append(-1)
                elif i == "profile_sidebar_border_color":
                    try:
                        values.append(int(content.get("user").get(i),16))
                    except (ValueError, AttributeError):
                        values.append(-1)
                elif i == "description" and content.get("user").get(i) != None:
                    values.append(MySQLdb.escape_string(content.get("user").get(i).encode('utf-8')))
                elif i == "profile_link_color":
                    try:
                        values.append(int(content.get("user").get(i),16))
                    except (ValueError, AttributeError):
                        values.append(-1)
                elif i == "place":
                    if content.get("place") != None:
                        try:
                            values.append(int(content.get("place").get("id"),16))
                        except (ValueError, AttributeError):
                            values.append(-1)
                    else:
                        values.append(-1)
                elif i == "utc_offset" and content.get("user").get(i) == None:
                    values.append(-1)
                elif content.get("user").get(i) == None:
                    values.append("NULL")
                else:
                    values.append(content.get("user").get(i))
            except TypeError as e:
                print("OH NOOOOS")
                print(e)
                print(i)
        return values
#TODO: Add a lookup for the place foreign key ------------       
    



    def on_receive(self, data):
        self.buffer += data

        if data.endswith("\r\n") and self.buffer.strip():
            content = json.loads(self.buffer)
            self.buffer = ""
            
            if self.tweet_count < 10000:
                self.db_insert(content)
            else:
                print("Done!")
                self.p.write(pickle.dumps(self.tweets))
                return 0
            self.tweet_count += 1
    
    def __to_bool(self, string):
        if string == 'True':
            return 1
        elif string == 'False':
            return 0



if __name__ == "__main__":
    client = Client()

