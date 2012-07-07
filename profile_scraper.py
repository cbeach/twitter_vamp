import tweepy, time, config, json, redis, bz2

query_rate = 145 #queries / hr

consumer_key= config.consumer_key
consumer_secret=config.consumer_secret
access_token=config.access_token
access_token_secret=config.access_token_secret

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

print "logged in as %s" % api.me().name


redis_server = redis.Redis("localhost") 
tracked_user = redis_server.lpop("followed_users")
cur = tweepy.Cursor(api.followers,id=tracked_user)

followers_collected = 0

user_list = set()

#query the redis db for users.
#if there's a user, start pulling users.
#if there isn't sleep for 15 minutes
#

while(True):
    

    if tracked_user != None:
        try:
            for user in cur.items():
                user_list.add((user.screen_name, user.id))
                followers_collected += 1
                print("%s\t\t\t %s \t\t\t %s" % (tracked_user, user.screen_name, followers_collected))
                if followers_collected % 500 == 0:
                    f = bz2.BZ2File("follower_lists/%s.json.bz2" % (tracked_user), 'w')
                    temp = [i for i in user_list]
                    json.dump(temp, f) 
                if followers_collected % 100 == 0:
                    time.sleep(3600/query_rate) #3600=seconds in an hour
        except Exception, e:
            print("Encountered Error: %s" % e)
            time.sleep(30)
            continue
    if len(redis_server.lrange("followed_users",0,-1)) < 1:
        time.sleep(900)  #15 minutes in seconds 
        continue
        
    tracked_user = redis_server.lpop("followed_users")
    followers_collected = 0









