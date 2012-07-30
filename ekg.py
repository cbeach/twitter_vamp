import pika, redis, config
import twitter_subscriber as rts


class EKG:
    
    def __init__(self):
        self.redis = redis.Redis(config.REDIS_SERVER)
        self.live_feeds = []
        signal.signal(signal.SIGALRM, self.check_pulse)
        signal.setitimer(signal.ITIMER_REAL, 2, 2)

    def check_pulse(self, signum, _):
        ekg = redis.lrange('EKG', 0, -1) 
        redis.delete('active_feeds')
        for i in ekg:
            redis.sadd('active_feeds', i)

            

        


