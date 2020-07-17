"""Some helper functions



@Author: Sebastian Duque Mesa

"""

import numpy as np
import pandas as pd

def parse_tweet(tweet: dict) ->  pd.DataFrame:
    '''Parse tweet's data and store to pandas dataframe'''

    # Parse tweet's root level info
    tweet_id_str = tweet['id_str']
    tweet_created_at = tweet['created_at']

    # Tweet's user data
    tweet_username = tweet['user']['screen_name']
    tweet_user_id = tweet['user']['id_str']
    tweet_user_location = tweet['user']['location']

    # Tweet's geo data
    if 'coordinates' in tweet:
        tweet_coords = tweet['coordinates']['coordinates']      # Tweet coordinates in [long,lat]
    elif 'place' in tweet:
        tweet_bounding_box = tweet['place']['bounding_box']['coordinates']      # Tweet's location bounding box
        tweet_coords = bbox_centroid(tweet_bounding_box)                  # Tweet's centroid
    else:
        tweet_coords = None


    # If tweet is truncated and has extended_tweet entity
    if tweet['truncated'] and 'extended_tweet' in tweet:
        # parse tweet entities from extended_tweet

        tweet_text = tweet['extended_tweet']['full_text']

        # parse tweet's hashtags text into list if present, if no hashtags set to None
        tweet_hashtags = [ hashtag['text'] \
                           for hashtag in tweet['extended_tweet']['hashtags'] ] \
                         if tweet['extended_tweet']['hashtags'] else None

        # parse user mentions in tweet
        tweet_user_mentions = [ {'username': user_mention['screen_name'],'id':user_mention['id_str']} \
                                for user_mention in tweet['extended_tweet']['user_mentions'] ] \
                              if tweet['extended_tweet']['user_mentions'] else None
    else:
        # keep tweet entities from root level

        tweet_text = tweet['text']

        # parse tweet's hashtags text into list if present, if no hashtags set to None
        tweet_hashtags = [ hashtag['text'] \
                           for hashtag in tweet['entities']['hashtags'] ] \
                         if tweet['entities']['hashtags'] else None

        # parse user mentions in tweet
        tweet_user_mentions = [ {'username': user_mention['screen_name'],'id':user_mention['id_str']} \
                                for user_mention in tweet['entities']['user_mentions'] ] \
                              if tweet['entities']['user_mentions'] else None


    # If the tweet is a reply
    if tweet['in_reply_to_status_id_str']:
        tweet_in_reply_to_status_id_str = tweet['in_reply_to_status_id_str']    # original Tweet’s ID
        tweet_in_reply_to_user_id = tweet['in_reply_to_user_id']                # original Tweet’s author ID
        tweet_in_reply_to_username = tweet['in_reply_to_screen_name']           # the screen name of the original Tweet’s author

    if 'retweeted_status' in tweet and 'extended_tweet' in tweet['retweeted_status']:
        tweet_text.append(tweet['retweeted_status']['extended_tweet']['full_text'])

    if 'quoted_status' in tweet and 'extended_tweet' in tweet['quoted_status']:
        tweet_text.append(tweet['quoted_status']['extended_tweet']['full_text'])

    tweet_text = max(tweet_text, key=len)


    print(tweet_text)

def bbox_centroid(bounding_box: list) -> list:
    '''Calculates the centroid of a bounding box'''

    # list of unique bbox longitudes
    longs = np.unique( [point[0] for point in bounding_box])
    # list of unique bbox latitudes
    lats =  np.unique( [point[1] for point in bounding_box])
    
    # Centroid is the average of longs and lats
    centroid_long = np.sum(longs)/2
    centroid_lat = np.sum(lats)/2

    return [centroid_long,centroid_lat]