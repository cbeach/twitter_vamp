import bz2, time, json, sys, config
import twitter_subscriber as rts

#
#   Archive name format: yyyy-mm-dd-hh-mm.json.bz2
#   Archive file contains 100,000 tweets 
#

class Librarian:
    tweet_count = 0

    def __init__(self,archive_dir='',source_type = 3, name = ''):
        
        self.tweet_source = rts.twitter_feed(source_type, self.archive_tweet, source_name = name, exchange='direct.raw', routing_key = 'raw')

        self.archive_dir = archive_dir
        self.archive = bz2.BZ2File(archive_dir + time.strftime('%Y-%m-%d-%H-%M-%S') + '.json.bz2','w')
        
        self.tweet_source.start_feed()
    def __del__(self):
        self.archive.close()

    def new_archive(self):
        self.archive.close()
        self.archive = bz2.BZ2File(self.archive_dir + time.strftime('%Y-%m-%d-%H-%M-%S') + '.json.bz2','w')

    def archive_tweet(self, ch, message, properties, body):
        self.archive.write(json.dumps(body))
        self.tweet_count += 1
        print(self.tweet_count)
        if self.tweet_count == 100000:
            self.tweet_count = 0
            self.new_archive()


if __name__ == "__main__":
    l = Librarian(archive_dir=sys.argv[1])

