"""Defines the listener class passed to tweepy


@Author: Sebastian Duque Mesa

"""

from tweepy.streaming import StreamListener
import json
import time
import sys
import pandas as pd
from sqlalchemy import create_engine

class Listener(StreamListener):
    """ 
    A listener that handles tweets received from the stream.
    """
    def __init__(self, api):
        self.api = api
        self.reconnection_attemps = 0
        self.collected_tweets = 0

    def on_data(self, data):

        self.reconnection_attemps = 0       # restart reconnection attemps counter when there is incoming data
        self.collected_tweets += 1

        tweet = json.loads(data)
        print(self.collected_tweets,'\t',tweet['created_at'], end='\r')

        return True

    def on_error(self, status):
        if status == 420:
            self.reconnection_attemps += 1
            sys.stdout.write('Error 420: Enhance Your Calm, the app has been rate limited\n')
            time.sleep(60*self.reconnection_attemps)
            return True
        else:
            sys.stdout.write('Error {}\n'.format(status))
            return False
