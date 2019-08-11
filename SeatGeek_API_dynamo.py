"""
PUT DATA FROM SEATGEEK API INTO DYNAMODB
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
GET DATA FROM SEATGEEK API, SEND TO DYNAMODB
"""


def seatgeek_events():

    artists_df = data_fetch_pymysql()['artist']

    """CURRENT DATE ASSIGNMENT"""
    current_date = datetime.now()

    """
    AND THE DYNAMODB WAY TO STORE DATA
    """
    dynamodb = boto3.resource('dynamodb')
    dynamoTable = dynamodb.Table('Event_Table')

    """LOOP THRU ARTISTS"""
    for artist in artists_df:

        """DEFINE PERFORMER SLUG VARIABLE"""
        performer_slug_stg = artist.replace("&", "")
        performer_slug = (performer_slug_stg.replace("  ", " ")).replace(" ", "-")

        # print(performer_slug)

        try:
            url = 'https://api.seatgeek.com/2/events?format=json'
            payload = {'per_page': 20,
                       'performers.slug': performer_slug,
                       'client_id': client_id_str,
                       }
            r = requests.get(url, params=payload, verify=True)

            # print(r.url)

            json_obj = json.loads(r.text)

            # pprint(json_obj)

            event_list = json_obj['events']

            for event in event_list:

                venue_dict = event['venue']
                price_dict = event['stats']

                event_key = artist + venue_dict['name'] + venue_dict['city'] + venue_dict['state'] + event['datetime_utc']

                # static_data = {'name': event['title'], 'artist': artist, 'city': venue_dict['city'], 'date_UTC': event['datetime_utc'], 'state': venue_dict['state'],
                #               'venue': venue_dict['name'], 'capacity': venue_dict['capacity']}
                #
                # price_data = [{'create_ts': current_date, 'lowest_price': price_dict['lowest_price'], 'highest_price': price_dict['highest_price'],
                #                'med_price': price_dict['median_price'], 'listing_count': price_dict['listing_count']}]
                #
                # event_data = [{'event_id': event_key, 'name': event['title'], 'artist': artist, 'city': venue_dict['city'], 'date_UTC': event['datetime_utc'], 'state': venue_dict['state'],
                #               'venue': venue_dict['name'], 'capacity': venue_dict['capacity'], 'create_ts': current_date, 'lowest_price': price_dict['lowest_price'], 'highest_price': price_dict['highest_price'],
                #                'med_price': price_dict['median_price'], 'avg_price': price_dict['average_price'], 'listing_count': price_dict['listing_count']}]

                dynamoTable.put_item(

                    Item = {
                        'Event_ID': event_key, 'name': event['title'], 'artist': artist, 'city': venue_dict['city'],
                        'date_UTC': str(event['datetime_utc']), 'state': venue_dict['state'],
                        'venue': venue_dict['name'], 'capacity': venue_dict['capacity'], 'create_ts': str(current_date),
                        'lowest_price': price_dict['lowest_price'], 'highest_price': price_dict['highest_price'],
                        'med_price': price_dict['median_price'], 'avg_price': price_dict['average_price'],
                        'listing_count': price_dict['listing_count']
                    }
                )


        except IndexError as e:

            print('NO RELATED SEATGEEK EVENTS')


seatgeek_events()


def dynamo_pull():

    event_json = (dynamoTable.get_item(
        Key={
            'Event_ID': event
        }
    ))['Item']

    # print(event_json)

    print(event_json['Event_ID'])
    print(event_json['Event_name'])
    print(event_json['Event_data'])
    print(event_json['Ticket_prices'])

# dynamo_pull()
