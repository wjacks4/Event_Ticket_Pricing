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
GET ARTIST LIST FROM DYNAMODB
"""

def data_fetch_dynamo():
    """
    DEFINE DYNAMODB DATABASES
    """
    dynamodb = boto3.resource('dynamodb')
    dynamoTable = dynamodb.Table('Artist_Table')

    artists_df= pd.DataFrame(dynamoTable.scan()['Items']).sort_values(by=['artist'])
    print(artists_df)
    print(artists_df.head(10))
    return artists_df

data_fetch_dynamo()


def Data_Fetch_pymysql():
    # Fetch_QL = 'SELECT * FROM ARTISTS_ONLY;'

    # USING pymysql#
    connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                 user='tickets_user',
                                 password='tickets_pass',
                                 db='tickets_db')

    Fetch_QL = 'SELECT * FROM Artists_expanded;'
    cursor = connection.cursor()
    Artists_DF = (pd.read_sql('SELECT * FROM ARTISTS_WITH_EVENTS order by current_followers desc', con=connection)).sort_values['artist']
    print(Artist_DF.head(10))
    return Artists_DF


Data_Fetch_pymysql()


"""
DEFINE GLOBAL ACCESS STRINGS
"""

Stubhub_Key_1 = b'VOU4xvGfhGO9qpVxGo3SABeebnpTmAJw'
Stubhub_Secret_1 = b'RR2tFwHG7Pinv4ik'


Cat_Key_Secret_1 = (Stubhub_Key_1 + b":" + Stubhub_Secret_1)
Cat_Key_encode_1 = base64.standard_b64encode(Cat_Key_Secret_1)

print(Cat_Key_encode_1)


Stubhub_Key_2 = b'd9fWHtQvs34cAebdfAzDTCOf6DLn9Nm7'
Stubhub_Secret_2 = b'11UA5vKSQuZzjb4m'

Cat_Key_Secret_2 = (Stubhub_Key_2 + b":" + Stubhub_Secret_2)
Cat_Key_encode_2 = base64.standard_b64encode(Cat_Key_Secret_2)

print(Cat_Key_encode_2)


Stubhub_Key_3 = b'odHqRlEjZuudOptPDEcf1ojiauRstJ9C'
Stubhub_Secret_3 = b'H978jHMGfzFmtGuv'

Cat_Key_Secret_3 = (Stubhub_Key_3 + b":" + Stubhub_Secret_3)
Cat_Key_encode_3 = base64.standard_b64encode(Cat_Key_Secret_3)

print(Cat_Key_encode_3)

Stubhub_Key_4 = b'j0eN22SApF63czY3zcjv7wX5SED96FRF'
Stubhub_Secret_4 = b'9ugyJJ7pGAGouRJA'

Cat_Key_Secret_4 = (Stubhub_Key_4 + b":" + Stubhub_Secret_4)
Cat_Key_encode_4 = base64.standard_b64encode(Cat_Key_Secret_4)

print(Cat_Key_encode_4)

Stubhub_Key_5 = b'VhDtFC2UE8oQtBpYLmhWhz931FRPfjsn'
Stubhub_Secret_5 = b'y2QjurJH2nmcKNt4'

Cat_Key_Secret_5 = (Stubhub_Key_5 + b":" + Stubhub_Secret_5)
Cat_Key_encode_5 = base64.standard_b64encode(Cat_Key_Secret_5)

print(Cat_Key_encode_5)


"""
DEFINE ACCESS TOKEN FUNCTIONS
"""

def Get_Access_Token_1():
    # -------DEFINE URL BUILDING BLOCKS------#
    base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
    query_params = 'grant_type=client_credentials'

    # -----BUILD URL FOR REQUEST-----#
    request_url = (base_url + "?" + query_params)

    # -------ADD ON ADDITIONAL DATA TO URL REQUEST-----#
    payload = {"username": "wjacks4@g.clemson.edu", "password": "Hester3123"}
    headers = {"Authorization": "Basic Vk9VNHh2R2ZoR085cXBWeEdvM1NBQmVlYm5wVG1BSnc6UlIydEZ3SEc3UGludjRpaw==",
               "Content-Type": "application/json"}

    req = requests.post(request_url, data=json.dumps(payload), headers=headers)
    json_obj = req.json()
    token = json_obj['access_token']

    print(token)
    return (token)


# Get_Access_Token_1()

def Get_Access_Token_2():
    # -------DEFINE URL BUILDING BLOCKS------#
    base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
    query_params = 'grant_type=client_credentials'

    # -----BUILD URL FOR REQUEST-----#
    request_url = (base_url + "?" + query_params)

    # -------ADD ON ADDITIONAL DATA TO URL REQUEST-----#
    payload = {"username": "hiltonsounds@gmail.com", "password": "Hester3123"}
    headers = {"Authorization": "Basic ZDlmV0h0UXZzMzRjQWViZGZBekRUQ09mNkRMbjlObTc6MTFVQTV2S1NRdVp6amI0bQ==",
               "Content-Type": "application/json"}

    req = requests.post(request_url, data=json.dumps(payload), headers=headers)
    json_obj = req.json()
    token = json_obj['access_token']

    print(token)
    return (token)


# Get_Access_Token_2()

def Get_Access_Token_3():
    # -------DEFINE URL BUILDING BLOCKS------#
    base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
    query_params = 'grant_type=client_credentials'

    # -----BUILD URL FOR REQUEST-----#
    request_url = (base_url + "?" + query_params)

    # -------ADD ON ADDITIONAL DATA TO URL REQUEST-----#
    payload = {"username": "edenk@g.clemson.edu", "password": "Hester3123"}
    headers = {"Authorization": "Basic ZDlmV0h0UXZzMzRjQWViZGZBekRUQ09mNkRMbjlObTc6MTFVQTV2S1NRdVp6amI0bQ==",
               "Content-Type": "application/json"}

    req = requests.post(request_url, data=json.dumps(payload), headers=headers)
    json_obj = req.json()
    token = json_obj['access_token']

    print(token)
    return (token)


# Get_Access_Token_3()


def Get_Access_Token_4():
    # -------DEFINE URL BUILDING BLOCKS------#
    base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
    query_params = 'grant_type=client_credentials'

    # -----BUILD URL FOR REQUEST-----#
    request_url = (base_url + "?" + query_params)

    # -------ADD ON ADDITIONAL DATA TO URL REQUEST-----#
    payload = {"username": "butteredtoast66@gmail.com", "password": "Hester3123"}
    headers = {"Authorization": "Basic ajBlTjIyU0FwRjYzY3pZM3pjanY3d1g1U0VEOTZGUkY6OXVneUpKN3BHQUdvdVJKQQ==",
               "Content-Type": "application/json"}

    req = requests.post(request_url, data=json.dumps(payload), headers=headers)
    json_obj = req.json()
    token = json_obj['access_token']

    print(token)
    return (token)


# Get_Access_Token_4()


def Get_Access_Token_5():
    # -------DEFINE URL BUILDING BLOCKS------#
    base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
    query_params = 'grant_type=client_credentials'

    # -----BUILD URL FOR REQUEST-----#
    request_url = (base_url + "?" + query_params)

    # -------ADD ON ADDITIONAL DATA TO URL REQUEST-----#
    payload = {"username": "sunglassman3123@gmail.com", "password": "Hester3123"}
    headers = {"Authorization": "Basic VmhEdEZDMlVFOG9RdEJwWUxtaFdoejkzMUZSUGZqc246eTJRanVySkgybm1jS050NA==",
               "Content-Type": "application/json"}

    req = requests.post(request_url, data=json.dumps(payload), headers=headers)
    json_obj = req.json()
    print(json_obj)
    token = json_obj['access_token']

    # print(token)
    return (token)


# Get_Access_Token_5()



"""
GET DATA FROM SEATGEEK API, SEND TO DYNAMODB
"""

def stubhub_events():

    artists_df = data_fetch_dynamo().head(5)['artist']

    """CURRENT DATE ASSIGNMENT"""
    current_date = datetime.now()

    """
    URL BUILDING BLOCKS
    """
    base_url = 'https://api.stubhub.com/sellers/search/events/v3'

    """
    AND THE DYNAMODB WAY TO STORE DATA
    """
    dynamodb = boto3.resource('dynamodb')
    dynamoTable = dynamodb.Table('Stubhub_Event_Table')

    """
    COUNTER INITIALIZE
    """
    i = 1

    """LOOP THRU ARTISTS"""
    for artist in artists_df:

        """
        ENCODE QUERY PARAMETERS
        """
        artist_encode = artist.replace(" ", "%20")
        query_params = ("q=" + artist_encode + "&" + "rows=100")
        artist_url = (base_url + "?" + query_params)

        print(artist_url)

        if i <= 5:

            try:

                Auth_Header = ("Bearer " + Get_Access_Token_5())
                headers = {"Authorization": Auth_Header, "Accept": "application/json"}
                req = requests.get(artist_url, headers=headers)
                json_obj = req.json()

                pprint(json_obj)

                event_list = json_obj['events']

                for event in event_list:

                    if 'PARKING' not in event['name'] and 'Festival' not in event['name']:

                        venue_dict = event['venue']
                        price_dict = event['ticketInfo']

                        print(price_dict['totalTickets'])
                        print(price_dict['totalListings'])

                        event_key = artist + venue_dict['name'] + venue_dict['city'] + venue_dict['state'] + str(event['eventDateUTC']).replace("T", " ")

                        dynamoTable.put_item(

                            Item = {
                                'Event_ID': event_key,
                                'name': event['name'],
                                'artist': artist,
                                'city': venue_dict['city'],
                                'date_UTC': str(event['eventDateUTC']).replace("T", " "),
                                'state': venue_dict['state'],
                                'venue': venue_dict['name'],
                                'create_ts': str(current_date),
                                'lowest_price': int(price_dict['minListPrice']),
                                'highest_price': int(price_dict['maxListPrice']),
                                'ticket_count': int(price_dict['totalTickets']),
                                'listing_count': int(price_dict['totalListings'])
                            }
                        )


            except IndexError as e:

                print('NO RELATED SEATGEEK EVENTS')

            # except KeyError as Overload:

            #     print(KeyError)
            #     print('exceeded quota for stubhub API')


# stubhub_events()


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
