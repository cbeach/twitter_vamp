import bz2, time, json, sys, config
import twitter_subscriber as rts

#
#   Archive file contains 100,000 tweets 
#

class Archiver:
    """
        This class saves a number of tweets and saves them in the format listed below

        Archive name format: yyyy-mm-dd-hh-mm.json.bz2
    """
    tweet_count = 0
    archive = None

    tweets_per_archive = 100000

    def __init__(self,archive_dir='', name = ''):
        
        self.tweet_source = rts.twitter_feed(self.archive_tweet, source_name = name, exchange='direct.raw', routing_key = 'raw',exchange_type = 'fanout')

        self.archive_dir = archive_dir
        self.new_archive() 
        self.tweet_source.start_feed()
    def __del__(self):
        self.archive.close()

    def new_archive(self):
        """ 
            Open a new file in the specified location with the correct format
        """
        if self.archive != None:
            self.archive.close()
        name = self.archive_dir + time.strftime('/%Y-%m-%d-%H-%M') + '.json.bz2'
        self.archive = bz2.BZ2File(name, 'w')
        print("Creating new archive: " + name)

    def archive_tweet(self, ch, message, properties, body):
        self.archive.write(json.dumps(body))
        self.tweet_count += 1
        if self.tweet_count == self.tweets_per_archive:
            self.tweet_count = 0
            self.new_archive()


if __name__ == "__main__":
    l = Archiver(archive_dir=sys.argv[1])

