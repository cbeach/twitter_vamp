import database_interface as di;
import twitter_interface as ti;

"""Master Control Program

This module take commands from the irc bot, and shuffles twitter data from
the live stream to the database interface.

It can do live swapping of interfaces so that down time is kept at an 
absolute minimum
"""

class Twitter_MCP:
    
    def __init__(self):
        self.db = di.database_interface()
        self.twit = ti.Twitter(callback=self.receive_tweet)
        self.twit.set_callback(receive_tweet)
        self.bot = None

    def receive_tweet(self, tweet):
        """Callback function that's given to the twitter stream interface

        When ever a complete tweet is received this function is called and sends the
        tweet to the database interface.
        """
        print(tweet)
        self.db.store_tweet(tweet)
        
if __name__ == "__main__":
    twitter_vamp = Twitter_MCP()
