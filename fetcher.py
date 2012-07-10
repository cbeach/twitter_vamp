import pycurl, json, time, datetime, sys, commands, pickle, MySQLdb, config, pika, bz2
from sets import Set


class Fetcher:
    buff = ''

    def __init__(self, file_name=None):
        
            
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.RABBITMQ_HOST))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=config.RAW_EXCHANGE, type='fanout')

        if file_name == None:
        
            self.buff = ''
            self.twitter_con = pycurl.Curl()
            self.twitter_con.setopt(pycurl.USERPWD, "%s:%s" % (config.USER, config.PASS))
            self.twitter_con.setopt(pycurl.URL, config.STREAM_URL)
            self.twitter_con.setopt(pycurl.WRITEFUNCTION, self.on_receive)
            self.twitter_con.perform()
        else:
            tweet_file = bz2.BZ2File(file_name, 'r')
            while(True):
                data = tweet_file.readline()
                self.on_receive(data)
    
    def on_receive(self, data):
        self.buff += data
        if data.endswith("\r\n") and self.buff.strip():
            self.channel.basic_publish(exchange=config.RAW_EXCHANGE, routing_key='raw', body=self.buff)
            self.buff = ''
        return None
def instantiate_stream():
    if len(sys.argv) > 1 and sys.argv[1] == "file":
        f = Fetcher("./raw/2012-03-05.json.bz2")
    else:
        f = Fetcher()

if __name__ == "__main__":
    while(True):
        try:
            instantiate_stream()
        except:
            print("Error Encountered")
            time.sleep(30)  
