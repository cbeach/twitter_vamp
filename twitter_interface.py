import pycurl, json

class Twitter:
    
    stream_url = 'https://stream.twitter.com/1/statuses/sample.json'
    distribute_tweet=None 
    tweet_count = 0

    def __init__(self, user='Avatar_223', password='Omicron2', callback = None):
        
        #Open a connection to the twitter streaming api
        self.distribute_tweet = callback
        self.buffer = ""
        self.twitter_con = pycurl.Curl()
        self.twitter_con.setopt(pycurl.USERPWD, "%s:%s" % (user, password))
        self.twitter_con.setopt(pycurl.URL, self.stream_url)
        self.twitter_con.setopt(pycurl.WRITEFUNCTION, self.on_receive)
        self.twitter_con.perform()


    def set_callback(self, mcp_callback):
        self.callback = mcp_callback

    def on_receive(self, data):
        self.buffer += data

        if data.endswith("\r\n") and self.buffer.strip():
            content = json.loads(self.buffer)
            self.buffer = ""
            print("calling the callback")
            self.distribute_tweet(content)
            self.tweet_count += 1


