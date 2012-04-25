import pycurl, json, time, datetime, sys, commands, pickle, MySQLdb, config, pika
from sets import Set




class Fetcher:
    buff = ''

    def __init__(self):
        
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.RABBITMQ_HOST))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=config.RAW_EXCHANGE, type='direct')
        
        self.buff = ''
        self.twitter_con = pycurl.Curl()
        self.twitter_con.setopt(pycurl.USERPWD, "%s:%s" % (config.USER, config.PASS))
        self.twitter_con.setopt(pycurl.URL, config.STREAM_URL)
        self.twitter_con.setopt(pycurl.WRITEFUNCTION, self.on_receive)
        self.twitter_con.perform()

        

    def on_receive(self, data):
        self.buff += data
        if data.endswith("\r\n") and self.buff.strip():
            self.channel.basic_publish(exchange=config.RAW_EXCHANGE, routing_key='raw', body=self.buff)
            self.buff = ''
        return None
if __name__ == "__main__":
    f = Fetcher()
