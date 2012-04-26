import json, pika, sys

class raw_feed:
    def readline(self):
        return raw_input() + '\n'

class twitter_feed:
    count = 0
    def __init__(self, source_type, callback, source_name='', host_name='localhost', exchange='', routing_key='',exchange_type='direct'):
        self.callback = callback
        self.source_type = source_type
        if source_type == 1:
            self.input_obj = raw_feed()
        elif source_type == 2:
            self.input_obj = open(source_name)
        elif source_type == 3:
            self.exchange = exchange
            self.routing_key = routing_key

            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host_name))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange, type = exchange_type)
            self.queue = self.channel.queue_declare(exclusive=True)
            self.channel.queue_bind(exchange=self.exchange, queue=self.queue.method.queue, routing_key=routing_key)
            self.channel.basic_consume(self.get_tweet,self.queue.method.queue)

    def get_tweet(self, ch, method, properties, body):
        """
        Get a single tweet from the specified source.
        1 is from raw_input
        2 if from a file
        3 is from an AMPQ work Queue
        """
        buffer = ''
        content = None
        if self.source_type == 1 or self.source_type == 2:
            for i in range(256):
                try:
                    if self.input_obj == None:
                        return None
                    buffer += self.input_obj.readline()
                except EOFError:
                    raise EOFError('End of File Reached')
                if buffer.endswith("\r\n") and buffer.strip():
                    try:
                        content = json.loads(buffer)
                        self.callback(ch,method,properties,content)
                        buffer = ''
                        break
                    except ValueError:
                        buffer = ''
                        break
        elif self.source_type == 3:
            try:
                body = body.strip()
                content = json.loads(body)
                self.callback(ch, method, properties, content)
                ch.basic_ack(delivery_tag = method.delivery_tag)
                buffer = ''
            except ValueError as e:
                print("ValueError: "+ str(e))
                buffer = ''

        return content

    def start_feed(self):
        if self.source_type == 1 or self.source_type == 2:
            while(True):
                try:
                    self.get_tweet(None,None,None,None)
                except EOFError:
                    break
        elif self.source_type == 3:
            self.channel.start_consuming()

def print_tweet(body):
    print(type(body))

if __name__ == "__main__":
    r = twitter_feed(3,None,print_tweet)
    r.start_feed()
