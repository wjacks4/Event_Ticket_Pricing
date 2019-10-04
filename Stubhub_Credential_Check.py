"""
STUBHUB API DATA PULL
"""

# import mysql
# from mysql.connector import Error
# import psycopg2 as p
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
# import mysql-python
import pymysql
# import MySQLdb
import base64
import datetime
import boto3
from datetime import datetime

import boto3

"""PRINT TO LOG FOR MONITORING PURPOSES"""
current_date = datetime.now()
print('THIS PROGRAM RAN AT ' + str(current_date))

"""GLOBAL VARAIBLES FOR API ACCESS"""
base_url = 'https://api.stubhub.com/sellers/search/events/v3'


def data_fetch_pymysql():
    connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                 user='tickets_user',
                                 password='tickets_pass',
                                 db='tickets_db')
    cursor = connection.cursor()
    artists_df = pd.read_sql('SELECT * FROM ARTISTS_WITH_EVENTS order by event_count desc, current_followers desc', con=connection)
    return artists_df

class keys:
    """

    STREAMLINE RETREIVAL OF STUBHUB ACCESS TOKENS BY PASSING THIS CLASS EACH ACCOUNT'S
    - KEY
    - SECRET
    - USERNAME
    - PASSWORD

    """

    def __init__(self, key, secret, username, password):
        self.key_encode = (base64.standard_b64encode(key + b":" + secret)).decode("utf-8")

        base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
        query_params = 'grant_type=client_credentials'
        request_url = (base_url + "?" + query_params)
        header_auth = ('Basic ' + (base64.standard_b64encode(key + b":" + secret)).decode("utf-8"))

        payload = {"username": username, "password": password}
        headers = {"Authorization": header_auth, "Content-Type": "application/json"}

        req = requests.post(request_url, data=json.dumps(payload), headers=headers)
        json_obj = req.json()
        self.token = json_obj['access_token']


"""INITIALIZE EACH OF THE 5 INSTANCES OF THE CLASS 'KEYS'"""
# token1 = keys(b'zz5xHP3Miax2zeo9fnKivFSPGmWsLiSv', b'G4j3RRmpBxo8jM7s', 'wjacks4@g.clemson.edu', 'Hester3123')
token1 = keys(b'knI4wisTkeBR4txGgGzUiHvpgAHPfWp8', b'Y37FpPHhIiHJdrWL', 'pluug3123@gmail.com', 'Hester3123')
token2 = keys(b'mwrKyXKBADj7gqY2jqmjAkXFpMgr0u5p', b'GF96v7mWwUDY5fnV', 'hiltonsounds@gmail.com', 'Hester3123')
token3 = keys(b'hf0bANqvcOAJxqhoAccKEI9ulv2oovef', b'aOOlKPrTckv6iJPU', 'edenk@g.clemson.edu', 'Hester3123')
token4 = keys(b'Q53rXMFZn9FfQuxNJhYJAPhbxFTDpH59', b'pQSLJvFEuk2AoHqG', 'butteredtoast66@gmail.com', 'Hester3123')
token5 = keys(b'uyoddTC6PL6ZIGaMkirj64bFRvLbMoDY', b'Ok4sujJFfhvYIT7W', 'sunglassman3123@gmail.com', 'Hester3123')


def stubhub_event_pull(temp_df, artist_in, artist_url, token_in):
    
    try:
        auth_header = ("Bearer " + token_in.token)
        headers = {"Authorization": auth_header, "Accept": "application/json"}
        
        req = requests.get(artist_url, headers=headers)
        print(req.content)
        
        json_obj = req.json()
        event_list = json_obj['events']
        # print(event_list)

        for event in event_list:
            event_name = event['name']

            if 'PARKING' not in event_name:
                event_id = str(event['id'])
                event_venue = event['venue']['name']
                event_city = event['venue']['city']
                event_state = event['venue']['state']
                event_date_str = (event['eventDateUTC']).replace("T", " ")
                event_date_cut = event_date_str[:19]
                event_date_UTC = datetime.strptime(event_date_cut, '%Y-%m-%d %H:%M:%S')
                lowest_price = event['ticketInfo']['minListPrice']
                highest_price = event['ticketInfo']['maxListPrice']
                ticket_count = event['ticketInfo']['totalTickets']
                listing_count = event['ticketInfo']['totalListings']

                event_key = (
                        event_name + event_venue + event_city + event_state + str(event_date_UTC) + str(
                    current_date))
                print(event_key)

                """S3 NEW DATA CREATION"""
                event_array = pd.DataFrame([[artist_in, '', event_name, event_id, event_venue, event_city,
                                             event_state, event_date_UTC, lowest_price, highest_price,
                                             ticket_count, listing_count, current_date]],
                                           columns=['artist', 'artist_id', 'name', 'ID', 'venue', 'city',
                                                    'state', 'date_UTC', 'lowest_price', 'highest_price',
                                                    'ticket_count', 'listing_count', 'create_ts'])

                try:
                    
                    temp_df = temp_df.append(event_array, ignore_index=True, sort=True)
                    
                except AttributeError as No_Data:
                    
                    print(No_Data)
                    print('The request to Stubhub failed, probably because youve exceeded your quota')

            return temp_df

    except KeyError as Overload:
        print(KeyError)
        print('exceeded quota for stubhub API')


def pull_caller(inner_func):


    """GET ARTISTS DF FROM MYSQL"""
    artists_df = data_fetch_pymysql().head(5)['artist']

    """INITIALIZE INCREMENTING VARIABLE"""
    i = 1
    
    temp_df = pd.DataFrame()

    for artist in artists_df:

        artist_encode = artist.replace(" ", "%20")
        query_params = ("q=" + artist_encode + "&" + "rows=100")
        artist_url = (base_url + "?" + query_params)

        if i <= 1:
            
            temp_df = stubhub_event_pull(temp_df, artist, artist_url, token1)
            
            
        elif 1 < i <= 2:
            
            temp_df = stubhub_event_pull(temp_df, artist, artist_url, token2)
        
        elif 2 < i <= 3:
            
            temp_df = stubhub_event_pull(temp_df, artist, artist_url, token3)
            
        elif 3 < i <= 4:
            
            temp_df = stubhub_event_pull(temp_df, artist, artist_url, token4)
            
        else:
            
            temp_df = stubhub_event_pull(temp_df, artist, artist_url, token5)
            
        i = i + 1


"""CALL MAIN FUNCTION"""
pull_caller(stubhub_event_pull)


"""PRINT TO LOG FOR MONITORING PURPOSES"""
current_date = datetime.now()
print('THIS PROGRAM FINISHED AT ' + str(current_date))
