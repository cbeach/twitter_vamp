contributors:                           None

#favorited:                              bool
#truncated:                              bool
#retweeted:                              bool

#in_reply_to_user_id:                    int
#retweet_count:                          int

#id:                                     long
#in_reply_to_status_id:                  long

#created_at:                             unicode$int
#text:                                   unicode$varchar
#in_reply_to_screen_name:                unicode$varchar
#in_reply_to_user_id_str:                unicode$long
#source:                                 unicode$varchar
#in_reply_to_status_id_str:              unicode$long
#id_str:                                 unicode$long


geo:                                    dict
    type:                               unicode
    coordinates:                        list:float

coordinates:                            dict
    type:                               unicode
    coordinates:                        list

entities:                               dict
    user_mentions:                      list
    hashtags:                           list
    urls:                               list

place:                                  dict
    name:                               unicode$foreign key
    url:                                unicode$None            #redundent, can be constructed from country code
    country:                            unicode$foreign key
    place_type:                         unicode$int             #looks like it's things like city, admin, etc
    country_code:                       unicode$unicode
    full_name:                          unicode$foreign key     #probably redundant
    id:                                 unicode$int/long        #it looks like it's a long hexidecimal
    bounding_box:                       dict
        type                            unicode:int index
        coordinates                     list:list:float
    attributes:                         dict:None

user: dict
    follow_request_sent:                None
    following:                          None
    notifications:                      None
    profile_use_background_image:       bool$bit
    contributors_enabled:               bool$bit
    verified:                           bool$bit
    is_translator:                      bool$bit
    geo_enabled:                        bool$bit
    protected:                          bool$bit
    default_profile:                    bool$bit
    profile_background_tile:            bool$bit
    show_all_inline_media:              bool$bit
    default_profile_image:              bool$bit
    id:                                 int$int
    favourites_count:                   int$short
    friends_count:                      int$short
    listed_count:                       int$short
    utc_offset:                         int$byte
    statuses_count:                     int$short
    followers_count:                    int$short
    profile_image_url_https:            unicode$unicode         #take out the common prefix
    profile_image_url:                  unicode$unicode         #take out the common prefix
    profile_background_image_url:       unicode$unicode         #take out common prefix
    profile_background_image_url_https: unicode$unicode         #take out common prefix
    profile_sidebar_fill_color:         unicode$int
    profile_text_color:                 unicode$int
    location:                           unicode$foreign key
    description:                        unicode$unicode
    profile_link_color:                 unicode$int
    profile_background_color:           unicode$int
    id_str:                             unicode$int             #convert from string
    screen_name:                        unicode$unicode
    lang:                               unicode$foreign key
    name:                               unicode$unicode
    url:                                unicode
    created_at:                         unicode$int
    time_zone:                          unicode$int
    profile_sidebar_border_color:       unicode$int


