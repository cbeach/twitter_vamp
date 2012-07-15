import pika, json, bz2, sys, os, random, atexit
from core.twitter_subscriber import *
from nlp.language_list import *

archive_list = {}
for key, value in lang_list.items():
    archive_list[key] = bz2.BZ2File(os.path.join('data/sorted_by_lang', value + '.json.bz2'), 'w')

def archive(ch, method, properties, content):
    try:
        lang = content.pop('lang')
    except KeyError:
        return    
    archive_list[lang].write(json.dumps(content)) 
    archive.count += 1
    print archive.count


def close_archives():
    print 'closing files!'
    for key in archive_list.keys():
        archive_list[key].close()
        
archive.count = 0
atexit.register(close_archives)

feed = twitter_feed(archive, exchange='direct.lang', routing_key='lang')
feed.start_feed()






