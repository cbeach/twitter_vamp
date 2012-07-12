import raw_twitter_subscriber as rts

class entities:
    def __init__(self, source_type, source_name):
        self.feed = rts(source_type, source_name) 


    def generate_umtu(source,n,file_obj=None,starvation=.00000025):
        """
        Generate all of the usernames that have been seen, all of the people that they mention,
        the hashtags that have been used and the urls that have been mentiond

        """

        loop = True
        tweet_count = 0

        users = {}
        mentions = {}
        hash_tags = {}
        urls = {}

        while(loop == True):
            add_word = True
            try:
                content = get_tweet(source, file_obj=file_obj)
            except EOFError:
                return lexicon
            if content == None:
                continue
            else:
                tweet_count += 1
            #get the user's information
            if content.get('user') != None and content.get('user').get('name') != None:
                if users.get(content.get('user').get('name')) == None:
                    users[content.get('user').get('name')] = [content.get('user').get('id'), 1]
                else:   
                    users[content.get('user').get('name')][1] +=  1


            #get all of the entities out of the tweet
            if content.get('entities') != None:
                if content.get('entities').get('user_mentions') != None:
                    for i in content.get('entities').get('user_mentions'):
                        if mentions.get(i.get('screen_name')) == None:
                            mentions[i.get('screen_name')] = 1
                        else:   
                            mentions[i.get('screen_name')] +=  1
                
                if content.get('entities').get('hashtags') != None:
                    for i in content.get('entities').get('hashtags'):
                        if hash_tags.get(i.get('text')) == None:
                            hash_tags[i.get('text')] = 1
                        else:   
                            hash_tags[i.get('text')] +=  1
                    
                if content.get('entities').get('urls') != None:
                    for i in content.get('entities').get('urls'):
                        if urls.get(i.get('url')) == None:
                            urls[i.get('url')] = 1
                        else:   
                            urls[i.get('url')] +=  1
            if tweet_count % 10000 == 0:
                print('%d\t%d\t%d\t%d'%(len(users),len(mentions),len(hash_tags),len(urls)))
            buffer = ""
            if(n > 0 and tweet_count > n):
                loop = False
        return (users, mentions, hash_tags, urls)
