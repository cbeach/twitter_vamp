import tweepy, time, config, json

query_rate = 150 #queries / hr

# == OAuth Authentication ==
#
# This mode of authentication is the new preferred way
# of authenticating with Twitter.

# The consumer keys can be found on your application's Details
# page located at https://dev.twitter.com/apps (under "OAuth settings")
consumer_key= config.consumer_key
consumer_secret=config.consumer_secret

# The access tokens can be found on your applications's Details
# page located at https://dev.twitter.com/apps (located 
# under "Your access token")
access_token=config.access_token
access_token_secret=config.access_token_secret

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

# If the authentication was successful, you should
# see the name of the account print out
print "logged in as %s" % api.me().name

# If the application settings are set for "Read and Write" then
# this line should tweet out the message to your account's 
# timeline. The "Read and Write" setting is on https://dev.twitter.com/apps
#api.update_status('Tweepy works ^_^')

tracked_user = "Nike"


cur = tweepy.Cursor(api.followers,id=tracked_user)

followers_collected = 0

user_list = set()

for user in cur.items():
    user_list.add((user.screen_name, user.id))
    followers_collected += 1
    time.sleep(3600/query_rate) #3600=seconds in an hour
    if followers_collected % 10000 == 0:
        f = open("follower_lists/followers-%s-%s" % (tracked_user, time.strftime('%Y-%m-%d-%H-%M')), 'w')
        temp = [i for i in user_list]
        json.dump(temp, f) 
        

        
    print((user.screen_name, user.id))









