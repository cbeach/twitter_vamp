import redis, json, bz2, sys, os, random, atexit, config
from core.twitter_subscriber import *
from nlp.language_list import *

archive_list = {}
redis_server = redis.StrictRedis('localhost')

for key, value in lang_list.items():
    archive_list[key] = bz2.BZ2File(os.path.join('data/sorted_by_lang', value + '.json.bz2'), 'w')

def archive(content):
    try:
        content = json.loads(content)
        lang = content.pop('lang')
    except KeyError:
        return    
    except AttributeError:
        return
    except TypeError:
        return 
    archive_list[lang].write(json.dumps(content)) 

def close_archives():
    print 'closing files!'
    for key in archive_list.keys():
        archive_list[key].close()
        
archive.count = 0
atexit.register(close_archives)

while(True):
    archive(redis_server.lpop('lang_detected'))



