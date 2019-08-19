# -----------------------------------------------------#
# -----------EVENTBRITE API DATA PULL------------------#
# -----------------------------------------------------#
# -----------PURPOSE - FOR EACH ARTIST ON A MAJOR------#
# ---------------------SPOTIFY PLAYLIST, SEARCH FOR----#
# ---------------------THEIR EVENTS ON EVENTBRITE------#
# ---------------------AND INSERT ALL RELEVANT DATA----#
# ---------------------INTO AN AWS RDB TABLE-----------#
# -----------------------------------------------------#
# ----------LAST UPDATED ON 5/9/2019-------------------#
# -----------------------------------------------------#

# !/usr/bin/env python3

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
import boto3
import base64
import datetime
from datetime import datetime
import fuzzywuzzy
from fuzzywuzzy import fuzz

current_Date = datetime.now()
print('THIS PROGRAM RAN AT ' + str(current_Date))

# ------------------------------------------------------------------#
# ---------------EVENTBRITE API AUTHORIZATION DATA------------------#
# ------------------------------------------------------------------#
API_key = "QBBZEWV5XWAAFECR3D"
API_secret = "7NG5DUZEJBCIGLFJWZRTQ3R7SE3UXUDCA4DFD7U3MFC57UQF45"
OAuth_token = "ZG7IKNHFJFFYSXDN4R5K"
Anon_OAuth_token = "SWIBI6XDBCO2UP5AOA7Y"
base_string = "https://www.eventbriteapi.com/v3/events/search/?token=ZG7IKNHFJFFYSXDN4R5K&"


def data_fetch_pymysql():
    connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                 user='tickets_user',
                                 password='tickets_pass',
                                 db='tickets_db')

    Fetch_QL = 'SELECT * FROM Artists_expanded;'
    cursor = connection.cursor()
    Artists_DF = pd.read_sql('SELECT * FROM ARTISTS_WITH_EVENTS order by current_followers desc', con=connection)
    return Artists_DF


# data_fetch_pymysql()


"""
AND THE DYNAMODB WAY TO STORE DATA
"""
dynamodb = boto3.resource('dynamodb')
dynamotable = dynamodb.Table('EventBrite_Event_Table')


def eventbrite_artist_search(df):
    """
    MAIN API FUNCTION
    """


    artist_df = df.head(250)
    # Artist_df = df.head(5)

    current_date = datetime.now()

    for artist_dat in artist_df.iterrows():

        spotify_artist = artist_dat[1]['artist']
        spotify_artist_id = artist_dat[1]['artist_id']

        artist_encode = (spotify_artist.replace("&", " ")).replace(" ", "%20")

        artist_url = (base_string + "expand=ticket_availability,external_ticketing,venue&" + "q=" + artist_encode)

        try:
            # ---------GET RAW RESPONSE FROM URL, DECODE IT TO JSON----------#
            rawdat = urllib.request.urlopen(artist_url)
            encoded_dat = rawdat.read().decode('utf-8', errors='ignore')
            json_dat = json.loads(encoded_dat)

            # ---------------BEGIN BUILDING EVENTS DATAFRAME FROM JSON----------------#
            # -----CREATE BLANK DATAFRAME FOR APPENDING, ISOLATE EVENTS FROM JSON-----#
            event_df = pd.DataFrame()
            events = json_dat['events']

            # -----------LOOP THROUGH EVENTS IN EVENT LIST---------------#
            for event in events:

                """
                TRY EXTRACTING EVENT DATA EXCEPT WHEN NOT EXISTS
                """
                try:

                    """
                    CLEAN EVENT DATA
                    """
                    event_name = ((event['name']['text']).replace('"', '')).encode('utf-8')
                    name_decode = unidecode(str(event_name, encoding="utf-8")).replace('"', '')

                    """
                    FUZZY MATCHING TO AVOID UNNECESSARY DATA
                    """
                    Spotify_name = spotify_artist
                    EventBrite_name = event_name

                    fuzz_partial = fuzz.partial_ratio(Spotify_name.lower(), EventBrite_name.lower())
                    fuzz_ratio = fuzz.ratio(Spotify_name.lower(), EventBrite_name.lower())

                    # print(Spotify_name)

                    # ----------ONLY CONTINUE EXTRACTING EVENT DATA IF FUZZY PARTIAL SCORE > .75...IDK--------#

                    if (fuzz_ratio + fuzz_partial) > 150:
                        print(event_name)
                        print(fuzz_partial)
                        print(fuzz_ratio)

                        """
                        INDIVIDUAL VARIABLE EXTRACTION
                        """
                        event_id = event['id']
                        event_venue = event['venue']['name']
                        event_city = event['venue']['address']['city']
                        event_state = event['venue']['address']['region']
                        event_date_UTC = event['start']['utc']
                        lowest_price = event['ticket_availability']['minimum_ticket_price']['major_value']
                        highest_price = event['ticket_availability']['maximum_ticket_price']['major_value']
                        capacity = event['venue']['capacity']
                        sold_out_indicator = event['ticket_availability']['is_sold_out']
                        shareable = event['shareable']
                        available_elsewhere = event['is_externally_ticketed']


                        event_key = ( name_decode + str(event_id) + event_venue + event_city + event_state + str(event_date_UTC) + str(current_date))
                        
                        
                        event_array = pd.DataFrame([[spotify_artist, spotify_artist_id, event_name, event_id,
                                                     event_venue, event_city, event_state, event_date_UTC, lowest_price,
                                                     highest_price, capacity, sold_out_indicator, shareable,
                                                     available_elsewhere]],
                                                   columns=['artist', 'artist_id', 'name', 'ID', 'venue', 'city',
                                                            'state', 'date_UTC', 'lowest_price', 'highest_price',
                                                            'capacity', 'sold_out_indicator', 'shareable',
                                                            'available_elsewhere'])

                        insert_tuple = (
                        spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state,
                        event_date_UTC, lowest_price, highest_price, capacity, sold_out_indicator, shareable,
                        available_elsewhere, current_date)

#                         print(insert_tuple)
                        event_QL = 'INSERT INTO `EVENTBRITE_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `capacity`, `sold_out`, `shareable`, `available_elsewhere`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

#                         print(event_QL)
                        connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                                     user='tickets_user', password='tickets_pass', db='tickets_db')
                        cursor = connection.cursor()

                        result = cursor.execute(event_QL, insert_tuple)
                        connection.commit()

                        """
                        DYNAMO WAY TO DO IT
                        """
                        event_key = ( name_decode + str(event_id) + event_venue + event_city + event_state + str(event_date_UTC) + str(current_date))
                        print(event_key)
                        
                        dynamotable.put_item(

                            Item={
                                'Event_ID': event_key, 'name': event_name, 'artist': spotify_artist, 'city': event_city,
                                'date_UTC': str(event_date_UTC), 'state': event_state,
                                'venue': event_venue, 'capacity': capacity, 'create_ts': str(current_date),
                                'lowest_price': lowest_price, 'highest_price': highest_price,
                                'sold_out': sold_out_indicator,
                                'shareable': shareable, 'available_elsewhere': available_elsewhere
                            }
                        )

                except TypeError as no_data:

                    error = 'One of the fields was missing'

            # print(event_df)

        except urllib.error.HTTPError:

            # print(spotify_artist)
            # print(artist_url)

            print('Bad Request')


eventbrite_artist_search(data_fetch_pymysql())

