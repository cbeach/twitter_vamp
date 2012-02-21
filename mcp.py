import database_interface as di;
import twitter_interface as ti;

class Twitter_MCP:
    
    def __init__(self):
        self.db = di.database_interface()
        self.twit = ti.Twitter(callback=self.receive_tweet)
        self.twit.set_callback(receive_tweet)
        self.bot = None

    def receive_tweet(self, tweet):
        print(tweet)
        #self.db.store_tweet(tweet)
        
if __name__ == "__main__":
    twitter_vamp = Twitter_MCP()
