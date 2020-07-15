"""Starts stream collection from twitter

This script sets ups tweepy, loggin into twitter streaing API with the
keys provided in the .env file.

Run settings.py before using this script.

"""

from tweepy import OAuthHandler, Stream, StreamListener

import yaml
import time, os, sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database



####------------- PARAMETERS -------------####
keysfile_path = Path('.') / '.twitter_keys.yaml'
with open(keysfile_path) as file:
    api_keys = yaml.full_load(file)

CONSUMER_KEY = api_keys["CONSUMER_KEY"]
CONSUMER_SECRET = api_keys["CONSUMER_SECRET"]
ACCESS_TOKEN = api_keys["ACCESS_TOKEN"]
ACCESS_TOKEN_SECRET = api_keys["ACCESS_TOKEN_SECRET"]

# Search filter rule set to geo baunding box
MEDELLIN_BBOX = [-75.8032106603,5.9566942289,-75.2758668684,6.4913941464]

class Listener(StreamListener):
    """ 
    A listener that handles tweets received from the stream.
    """
    def __init__(self):
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

if __name__ == '__main__':

    # Instantiate the listener object
    listener = Listener()

    # Authentication
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    # create a engine to the database
    engine = create_engine("sqlite:///tweets.sqlite")
    # if the database does not exist
    if not database_exists(engine.url):
        # create a new database
        create_database(engine.url)


    sys.stdout.write('Starting stream...\n')
    stream = Stream(auth, listener)
    stream.filter(locations = MEDELLIN_BBOX)