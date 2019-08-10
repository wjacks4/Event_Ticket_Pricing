"""
FIGURE OUT HOW TO PICKLE FILES INTO S3

PURPOSE - TEST
"""

import json
from dateutil import parser
import time
import os
import subprocess
import urllib
import urllib.request
import pandas as pd
import unidecode
from unidecode import unidecode
import requests
import urllib
from urllib import parse
import sys
import base64
import numpy as np
import re
import pymysql
# import MySQLdb
import base64
import datetime
from datetime import datetime
import easydict
from collections import defaultdict
import pickle
import pprint
from pprint import pprint

import urllib
import pandas as pd
import numpy as np
import json
import requests

import boto3

"""
GET ARTIST LIST FROM MYSQL DB
"""


def data_fetch_pymysql():
    connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                 user='tickets_user',
                                 password='tickets_pass',
                                 db='tickets_db')

    artists_df = pd.read_sql('SELECT * FROM ARTISTS_WITH_EVENTS order by current_followers desc', con=connection)
    return artists_df

#data_fetch_pymysql()


"""
GET ARTIST LIST FROM DYNAMODB
"""

def data_fetch_dynamo():
    """
    DEFINE DYNAMODB DATABASES
    """
    dynamodb = boto3.resource('dynamodb')
    dynamoTable = dynamodb.Table('Artist_Table')

    artists_df= pd.DataFrame(dynamoTable.scan()['Items'])
    print(artists_df)

    return artists_df

# data_fetch_dynamo()


"""
PULL BACK DYNAMO DB DATA
"""

base_url = ('https://api.seatgeek.com/2/')
client_id_str = ('MTM4MTIyMDZ8MTU1NDQ3MTkxMy43Ng')
client_secret_str = ('c49766eaad2bc8bc33810d112d141ca9a09b0a78b1be52c459eb19c5fd3527a5')


def seatgeek_events():

    artists_df = data_fetch_pymysql().head(1)['artist']

    """CURRENT DATE ASSIGNMENT"""
    current_date = datetime.now()

    """INCREMENTING VARIABLE"""
    i = 1

    """BEGIN BUILDING DATAFRAME FROM JSON"""
    event_df = pd.DataFrame()

    """LOOP THRU ARTISTS"""
    # for artist in artists:
    for artist in artists_df:

        """DEFINE PERFORMER SLUG VARIABLE"""
        performer_slug_stg = artist.replace("&", "")
        performer_slug = (performer_slug_stg.replace("  ", " ")).replace(" ", "-")

        # print(performer_slug)


        try:
            url = 'https://api.seatgeek.com/2/events?format=json'
            payload = {'per_page': 1,
                       'performers.slug': performer_slug,
                       'client_id': client_id_str,
                       }
            r = requests.get(url, params=payload, verify=True)

            print(r.url)

            json_obj = json.loads(r.text)

            # pprint(json_obj)

            event_list = json_obj['events']

            for event in event_list:
                summ_data = event
                venue_data = summ_data['venue']
                venue_data = summ_data.get('venue')

                price_data = summ_data['stats']

                event_dict = {'name': [summ_data['title']], 'id': [summ_data['id']],
                              'datetime_utc': [summ_data['datetime_utc']], 'venue': [venue_data['name']], 'capacity':[venue_data['capacity']]
                              'city': [venue_data['city']], 'state': [venue_data['state']],
                              'avg_price': [price_data['average_price']], 'median_price': [price_data['median_price']],
                              'lowest_price': [price_data['lowest_price']],
                              'highest_price': [price_data['highest_price']],
                              'no_listing': [price_data['listing_count']], 'create_ts': [current_date]}
                print(event_dict)

                test_df = pd.DataFrame.from_dict(event_dict)


        except IndexError as e:

            print('NO RELATED SEATGEEK EVENTS')



seatgeek_events()
