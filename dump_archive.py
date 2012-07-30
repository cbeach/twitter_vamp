import os, json, redis, bz2, config, time, gc, logging


#logging.basicConfig(filename='./logs/archive_replay.log')
#log = logging.getLogger(__name__)

redis_server = redis.StrictRedis(config.REDIS_SERVER)

archives = os.listdir('data/raw')
archives = sorted(archives)


def wait_for_room():
    while redis_server.llen('lang') > 10000:
        time.sleep(.5)
def dump_data(f, position=None):
    if position is not None:
        f.seek(position)
    for data in f:
        dump_data.buff += data
        if data.endswith("\r\n") and dump_data.buff.strip():
            redis_server.rpush('lang', dump_data.buff)
#            redis_server.rpush('entities', dump_data.buff)
            dump_data.buff = ''
        if redis_server.llen('lang') > 50000:
            wait_for_room()

dump_data.buff = ''

try:
    with open('data/sorted_archives') as p:
        processed = p.readlines()
except IOError:
    processed = []

with open('data/sorted_archives', 'a') as p:
    for path in archives:
        if path in processed:
            print 'Skipping %s' % i
            continue
        print 'Processing %s' % path
        try:
            f = bz2.BZ2File(os.path.join('data/raw/',path),'r')
            dump_data(f)
        except EOFError as e:
            pass
        except IOError:
            pass
        except MemoryError as e:
            print 'MemoryError'
            position = f.tell()
            del f
            print 'Attempting to recover...'
            gc.collect()
            time.sleep(5)
            print 'Reopening file.'
            f = bz2.BZ2File(os.path.join('data/raw/',path),'r')
            print 'Continuing to dump data.'
            dump_data(f, position)
            
        f.close()
        p.write(path+'\n')


