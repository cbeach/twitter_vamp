import raw_twitter_subscriber as rts

class lexicon_generator:
    def __init__(self):
        self.feed = twitter_feed

    def clean_tweet(content): #english only... sort of.
        """
        Do text processing on token.  If any of the characters in the token lie outside of the 
        lower 128 of ascii get rid of it.
        also remove all of the hashtags usernames and url's
        """
        non = [chr(i) for i in range(128) if i < 65 or i > 90 and i < 97 or i > 122 and i != 39]
        words = {}

        if(content.get('text') != None and content.get('user') != None and content.get('user').get('lang') == 'en'):
            tweet = content.get('text')
            words = tweet.split()
            for j in range(len(words)):
                if words[j].startswith(u'@') == False and words[j].startswith(u'#') == False and words[j].startswith(u'http') == False:
                    past_char = ''
                    run_count = 0
                    for k in words[j]:
                        if ord(k) < 65 or ord(k) > 90 and ord(k) < 97 or ord(k) > 122 and ord(k) != 39:
                            add_word = False
                            break
                        if( k == past_char):
                            run_count += 1
                        elif(run_count > 2):
                            words[j] = words[j].replace(past_char*run_count, past_char*1)
                        past_char = k
                    for k in non:
                        words[j] = words[j].replace(k,'')
                    words[j] = words[j].lower()
        return words

    def generate_self.lexicon(source,n,file_obj=None,starvation=.00000025):
        """
        Generate a self.lexicon of common words from twitter.

        """

        loop = True
        tweet_count = 0
        self.lexicon = {}
        current = 0
        run_count = 0
        
        while(loop == True):
            add_word = True
            try:
                content = self.feed.get_tweet(source, file_obj=file_obj)
            except EOFError:
                return self.lexicon
            if content == None:
                continue
            else:
                tweet_count += 1
                words = clean_tweet(content)                
            for i in range(len(words)):
                if(self.lexicon.get(words[i]) != None and add_word == True):
                    self.lexicon.get(words[i])[0] += 1
                    self.lexicon.get(words[i])[1] += tweet_count
                elif add_word == True:
                    self.lexicon[words[i]] = [1.0,tweet_count]
            if(tweet_count%10000 == 0):
                for i in self.lexicon.keys():
                    self.lexicon[i][0] -= starvation * (tweet_count - self.lexicon[i][1])
                    if(self.lexicon[i][0] < 0):
                        self.lexicon.pop(i)
                past = current
                current = len(self.lexicon)
                print("%d:   %d    %d"%(tweet_count, current, current - past))

                
            buffer = ""
            if(n > 0 and tweet_count > n):
                loop = False
        return self.lexicon
