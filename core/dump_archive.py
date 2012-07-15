import os, json, pika, bz2, config

archives = os.listdir('data/raw')
archives = sorted(archives)

with open('data/sorted_archives') as p:
    processed = p.readlines()

connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.RABBITMQ_HOST))
channel = connection.channel()
channel.exchange_declare(exchange='lang_in', type='direct')

buff = ''
with open('data/sorted_archive', 'a') as p:
    for path in archives:
        if path in processed:
            print 'Processing %s' % path
            continue
        try:
            with bz2.BZ2File(os.path.join('data/raw/',path),'r') as f:
                for data in f:
                    buff += data
                    if data.endswith("\r\n") and buff.strip():
                        channel.basic_publish(exchange='lang_in', routing_key='lang_in', body=buff)
                        buff = ''
        except EOFError:
            print 'error!'
            pass

        p.write(path+'\n')


