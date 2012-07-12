import json, sys, codecs
from twitter_subscriber import twitter_feed
from language_list import lang_list


class lang_stats:
    tweets_analyzed = 0
    lang_totals = {}
    lang_sample = {}

    def __init__(self):
        self.tf = twitter_feed(self.process, exchange='direct.lang',routing_key='lang')
        self.tf.start_feed()

    def process(self, ch, method, properties, body):
        self.tweets_analyzed += 1
        if body.get(u'lang') in self.lang_totals:
            self.lang_totals[body.get('lang')] += 1
            if len(self.lang_sample[body.get('lang')]) < 100:
                self.lang_sample[body.get('lang')].append(body.get('text'))
        else:
            self.lang_totals[body.get('lang')] = 1
            self.lang_sample[body.get('lang')] = [body.get('text')]
        sample_complete = True
        trouble_languages = 0
        print chr(27) + "[2J"
        print(self.tweets_analyzed)
        print(len(self.lang_sample))
        sample_complete = True
        for i in self.lang_sample.items():
            if len(i[1]) < 100:
                sample_complete = False
                print("%s: %d"%(i[0], len(i[1])))
                trouble_languages += 1
                sample_complete = False

        if sample_complete == True:
            print('done')
            c = codecs.open('lang_breakdown','w','utf-8')
            s = codecs.open('lang_sampel','w','utf-8')
            for i in self.lang_totals.items():
                c.write('%s, %d \n\tpercent: %f\n'%(lang_list[i[0]], i[1], 100*(float(i[1])/float(self.tweets_analyzed))) )
            for i in self.lang_sample.keys():
                for j in self.lang_sample[i]:
                    s.write(u'%s, %s\n'%(lang_list[i], j))
            sys.exit()

l = lang_stats()

    
