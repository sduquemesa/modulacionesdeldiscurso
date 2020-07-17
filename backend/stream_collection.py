"""Starts stream collection from twitter

This script sets ups tweepy, loggin into twitter streaing API with the
keys provided in the .env file.

Run settings.py before using this script.

@Author: Sebastian Duque Mesa

"""

from tweepy import OAuthHandler, Stream, API

from listener import Listener

from urllib3.exceptions import ProtocolError

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

if __name__ == '__main__':

    # Authentication
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    # Instantiate twitter API with auth info
    api = API(auth, 
        # retry 3 times with 5 seconds delay when getting these error codes
        # For more details see 
        # https://dev.twitter.com/docs/error-codes-responses  
        retry_count=3,retry_delay=5,retry_errors=set([401, 404, 500, 503]), 
        # monitor remaining calls and block until replenished  
        wait_on_rate_limit=True,
        wait_on_rate_limit_notify=True 
    )

    # create a engine to the database
    engine = create_engine("sqlite:///tweets.sqlite")
    # if the database does not exist
    if not database_exists(engine.url):
        # create a new database
        create_database(engine.url)

    # Instantiate the listener object
    listener = Listener(api)

    sys.stdout.write('Starting stream...\n')

    # Instantiate the Stream object
    stream = Stream(auth, listener)

    # begin the stream
    while True:
        # maintian connection unless interrupted
        try:
            stream.filter(locations = MEDELLIN_BBOX)
        # reconnect automantically if error arise
        # due to unstable network connection
        except(ProtocolError, AttributeError):
            continue