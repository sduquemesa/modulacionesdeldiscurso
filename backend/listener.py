"""Defines the listener class passed to tweepy


@Author: Sebastian Duque Mesa

"""

from tweepy.streaming import StreamListener
from tweepy.api import API
import json
import time
import sys
import pandas as pd
from sqlalchemy import create_engine

class Listener(StreamListener):
    """ 
    A listener that handles tweets received from the stream.
    """
    def __init__(self, api = None):
        self.api = api or API(wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        self.reconnection_attemps = 0
        self.collected_tweets = 0

        # create a engine to the database
        # self.engine = create_engine('sqlite:///app/tweets.sqlite')
        # switch to the following definition if run this code locally
        self.engine = create_engine('sqlite:///tweets.sqlite')

    def on_status(self, status):
        """Called when a new status arrives
        Recieves a Status object. 
        Tweet data is in Status._json which is already a dict.

        """

        self.reconnection_attemps = 0       # restart reconnection attemps counter when there is incoming data
        self.collected_tweets += 1

        tweet = status._json

        # Find and keep the longest tweet text
        tweet_text_list = [tweet['text']]

        if 'extended_tweet' in tweet:
            tweet_text_list.append(tweet['extended_tweet']['full_text'])
        if 'retweeted_status' in tweet and 'extended_tweet' in tweet['retweeted_status']:
            tweet_text_list.append(tweet['retweeted_status']['extended_tweet']['full_text'])
        if 'quoted_status' in tweet and 'extended_tweet' in tweet['quoted_status']:
            tweet_text_list.append(tweet['quoted_status']['extended_tweet']['full_text'])

        tweet_text = max(tweet_text_list, key=len)

        print(tweet_text)

        return True

    def on_error(self, status):
        """Called when a non-200 status code is returned
        Handles rate limit, increses reconnection attempt time every time is unsuccessful.
        """
        
        if status == 420:
            self.reconnection_attemps += 1
            sys.stdout.write('Error 420: Enhance Your Calm, the app has been rate limited\n')
            time.sleep(60*self.reconnection_attemps)
            return True
        else:
            sys.stdout.write('Error {}\n'.format(status))
            return False
