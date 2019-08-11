"""
TICKETMASTER API DATA PULL
"""

#!/usr/bin/env python3

#import mysql
#from mysql.connector import Error
#import psycopg2 as p
import json
import pprint
from pprint import pprint
from dateutil import parser
import time
import boto3
import os
import subprocess
import urllib
import urllib.request
import pandas as pd
from decimal import Decimal
import unidecode
from unidecode import unidecode
import requests
import urllib
from urllib import parse
import sys
import base64
import numpy as np
#import mysql-python
import pymysql
import base64
import datetime
from datetime import datetime
import pytz


"""
GLOBAL API STRING DATA
"""
event_search_url = ('https://app.ticketmaster.com/discovery/v2/events.json?&apikey=83sdXVyv4k3NnuGCIvk5nAHE3NSWddg7&size=10&keyword=')
event_base_url = ('https://app.ticketmaster.com/discovery/v2/events.json?apikey=')
data_type = ('.json?')
api_key1 = ('83sdXVyv4k3NnuGCIvk5nAHE3NSWddg7')
api_key2 = ('2C4llrNfIrGgEZxAft1QuJ5bpbS3SdpF')

api_keys = ['83sdXVyv4k3NnuGCIvk5nAHE3NSWddg7', '2C4llrNfIrGgEZxAft1QuJ5bpbS3SdpF']


def data_fetch_pymysql():

    connection = pymysql.connect (host = 'ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                  user = 'tickets_user',
                                  password = 'tickets_pass',
                                  db = 'tickets_db')
    
    Fetch_QL = 'SELECT * FROM Artists_expanded;'
    cursor = connection.cursor()
    artists_df = pd.read_sql('SELECT * FROM ARTISTS_WITH_EVENTS order by current_followers desc', con = connection)

    return artists_df

data_fetch_pymysql()


"""
SEE HOW MANY REQUESTS YOU HAVE LEFT
"""
try:
    test_url1 = ('https://app.ticketmaster.com/discovery/v2/events.json?&apikey=83sdXVyv4k3NnuGCIvk5nAHE3NSWddg7&size=10&keyword=Da+Baby')
    test_url2 = ('https://app.ticketmaster.com/discovery/v2/events.json?&apikey=2C4llrNfIrGgEZxAft1QuJ5bpbS3SdpF&size=10&keyword=Da+Baby')
    request1 = urllib.request.urlopen(test_url1).info()
    request2 = urllib.request.urlopen(test_url2).info()

except urllib.error.HTTPError as Overload:

    print(Overload)
    print('Too many requests')


def event_pull (df):

    """
    MAIN FUNCTION - PULL API DATA FROM TICKETMASTER AND STORE IN SQL AND NOSQL
    """

    """
    SQL CONNECTION
    """
    connection=pymysql.connect(host = 'ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user = 'tickets_user', password = 'tickets_pass', db = 'tickets_db')
    cursor=connection.cursor()

    """
    NOSQL CONNECTION
    """
    dynamodb = boto3.resource('dynamodb')
    dynamotable = dynamodb.Table('TicketMaster_Event_Table')

    current_date = datetime.now()

    """
    BUILD STATE TRANSLATION FILE
    """

    us_state_abbrev = {
        'Alabama': 'AL',
        'Alaska': 'AK',
        'Arizona': 'AZ',
        'Arkansas': 'AR',
        'California': 'CA',
        'Colorado': 'CO',
        'Connecticut': 'CT',
        'Delaware': 'DE',
        'Florida': 'FL',
        'Georgia': 'GA',
        'Hawaii': 'HI',
        'Idaho': 'ID',
        'Illinois': 'IL',
        'Indiana': 'IN',
        'Iowa': 'IA',
        'Kansas': 'KS',
        'Kentucky': 'KY',
        'Louisiana': 'LA',
        'Maine': 'ME',
        'Maryland': 'MD',
        'Massachusetts': 'MA',
        'Michigan': 'MI',
        'Minnesota': 'MN',
        'Mississippi': 'MS',
        'Missouri': 'MO',
        'Montana': 'MT',
        'Nebraska': 'NE',
        'Nevada': 'NV',
        'New Hampshire': 'NH',
        'New Jersey': 'NJ',
        'New Mexico': 'NM',
        'New York': 'NY',
        'North Carolina': 'NC',
        'North Dakota': 'ND',
        'Ohio': 'OH',
        'Oklahoma': 'OK',
        'Oregon': 'OR',
        'Pennsylvania': 'PA',
        'Rhode Island': 'RI',
        'South Carolina': 'SC',
        'South Dakota': 'SD',
        'Tennessee': 'TN',
        'Texas': 'TX',
        'Utah': 'UT',
        'Vermont': 'VT',
        'Virginia': 'VA',
        'Washington': 'WA',
        'West Virginia': 'WV',
        'Wisconsin': 'WI',
        'Wyoming': 'WY',
    }

    #print(us_state_abbrev)

    state_df = pd.DataFrame(list(us_state_abbrev.items()), columns=['State', 'Abbreviation'])

    """
    INITIALIZE INCREMENTING VARIABLE
    """
    i = 1

    event_ID_df = pd.DataFrame()

    #----------LOOP THROUGH ARTISTS IN COLUMN FROM INPUT DATAFRAME-----------#
    for artist_dat in df.iterrows():

        spotify_artist = artist_dat[1]['artist']
        spotify_artist_id = artist_dat[1]['artist_id']

        #---------------------BUILD URL ACCESS STRING---------------------#
        artist_encode = spotify_artist.encode('utf-8')
        artist_decode = unidecode(str(artist_encode, encoding = "utf-8"))
        artist_keyword = artist_decode.replace(" ", "+")

        if i <= 250:
        # if i <10:

            access_string = (event_base_url + api_key1 + '&size=10&keyword=' + artist_keyword)

            try:

                #--------------SUBMIT REQUEST TO URL, GET JSON RESPONSE-----------#
                raw_Dat = urllib.request.urlopen(access_string)
                encoded_Dat = raw_Dat.read().decode('utf-8', 'ignore')
                json_Dat = json.loads(encoded_Dat)


                try:

                    event_Dat = json_Dat['_embedded']['events']

                    # pprint(event_Dat)

                    #-------------EXTRACT EVENT ID FROM DATA IN EACH MEMBER OF EVENT OBJECT-----------#
                    for event in event_Dat:
                        name = event['name']
                        event_id = event['id']


                        #---------------------------------------------------------------#
                        #-----EXTRACT VARIABLES OF INTEREST FROM JSON OBJECTS-----------#
                        #-----------HANDLE EXCEPTIONS FOR MISSING VALUES----------------#
                        #---------------------------------------------------------------#
                        try:
                            event_name = event['name']
                            print(event_name)
                        except KeyError as noName:
                            event_name = ''

                        try:
                            event_venue = event['_embedded']['venues'][0]['name']
                        except KeyError as noVenue:
                            event_venue=' '

                        try:
                            event_city = event['_embedded']['venues'][0]['city']['name']
                        except KeyError as noCity:
                            event_city = ' '

                        try:
                            event_state = event['venues'][0]['state']['name']
                        # event_state_full = event['_embedded']['venues'][0]['state']['name']
                        # #print(event_state_full)
                        # event_state = (state_df.loc[state_df['State'] == event_state_full, 'Abbreviation']).tolist()
                        # #print(event_state)

                        except KeyError as noState:
                            event_state = ' '

                        try:
                            event_date_Local = event['dates']['start']['localDate']
                        except KeyError as noEventDate:
                            event_date_Local = ' '

                        try:
                            event_time_Local = event['dates']['start']['localTime']
                            event_datetime_Local = datetime.strptime((event_date_Local + " " + event_time_Local), '%Y-%m-%d %H:%M:%S')
                        except KeyError as noEventTime:
                            event_time_Local = ' '
                            event_datetime_Local = datetime.strptime(event_date_Local, '%Y-%m-%d')

                        try:
                            TZ_string = event['dates']['timezone']
                            event_TZ = pytz.timezone(TZ_string)
                            try:
                                local_dt = event_TZ.localize(event_datetime_Local, is_dst=None)
                                date_UTC = local_dt.astimezone(pytz.utc)
                            except ValueError as missing_datetime:
                                event_date = " "
                        except KeyError as noTZ:
                            event_TZ = '?'
                        # date_UTC = (event_datetime_UTC)

                        try:
                            event_sale_start = event['sales']['public']['startDateTime']
                        except KeyError as noSaleStart:
                            event_sale_start = ' '

                        try:
                            event_lowest_price = event['priceRanges'][0]['min']
                        except KeyError as noPriceDat:
                            event_lowest_price = ''

                        try:
                            event_highest_price = event['priceRanges'][0]['max']
                        except KeyError as noPriceDat:
                            event_highest_price = ''


                        event_profile=pd.DataFrame([[spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, date_UTC, event_sale_start, event_lowest_price, event_highest_price]],
                                      columns=['artist', 'artist_id', 'attraction_name', 'event_id', 'venue', 'city', 'state', 'date_UTC', 'sale_start_date', 'event_lowest_price', 'event_highest_price'])

                        insert_tuple = (spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, date_UTC, event_sale_start, event_lowest_price, event_highest_price, current_date)


                        #------------SQL TIME - SUBSTITUTE STRINGS INTO SQL QUERY FOR DB SUBMISSION-------------#
                        event_QL = 'INSERT INTO TICKETMASTER_EVENTS(artist, artist_id, name, id, venue, city, state, date_UTC, sale_start, lowest_price, highest_price, create_ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'

                        result = cursor.execute(event_QL, insert_tuple)
                        connection.commit()


                        """
                        DYNAMO WAY TO DO IT
                        """
                        record_key = (spotify_artist + event_name + event_venue + event_city + event_state + str(date_UTC) + str(current_date))

                        try:

                            dynamotable.put_item(

                                Item={
                                    'Record_ID':record_key,
                                    'name': event_name,
                                    'artist': spotify_artist,
                                    'city': event_city,
                                    'date_UTC': str(date_UTC),
                                    'state': event_state,
                                    'venue': event_venue,
                                    'create_ts': str(current_date),
                                    'lowest_price': int(event_lowest_price),
                                    'highest_price': int(event_highest_price)
                                }
                            )

                        except ValueError as NoPrice:

                            dynamotable.put_item(

                                Item={
                                    'Record_ID':record_key,
                                    'name': event_name,
                                    'artist': spotify_artist,
                                    'city': event_city,
                                    'date_UTC': str(date_UTC),
                                    'state': event_state,
                                    'venue': event_venue,
                                    'create_ts': str(current_date)
                                }
                            )

                except KeyError as NoEmbedded:

                    print('No Embedded Data')

                    time.sleep(1)

            except KeyError as No_Events:

                print('No Events for this Artist!')

            except urllib.error.HTTPError as Overload:

                print('Too Many requests, wait 2 seconds')
                time.sleep(2)

        else:

            try:

                access_string = (event_base_url + api_key2 + '&size=10&keyword=' + artist_keyword)

                #--------------SUBMIT REQUEST TO URL, GET JSON RESPONSE-----------#
                raw_Dat = urllib.request.urlopen(access_string)
                encoded_Dat = raw_Dat.read().decode('utf-8', 'ignore')
                json_Dat = json.loads(encoded_Dat)

                try:

                    event_Dat = json_Dat['_embedded']['events']

                    #-------------EXTRACT EVENT ID FROM DATA IN EACH MEMBER OF EVENT OBJECT-----------#
                    for event in event_Dat:
                        name = event['name']
                        event_id = event['id']


                        #---------------------------------------------------------------#
                        #-----EXTRACT VARIABLES OF INTEREST FROM JSON OBJECTS-----------#
                        #-----------HANDLE EXCEPTIONS FOR MISSING VALUES----------------#
                        #---------------------------------------------------------------#
                        try:
                            event_name = event['name']
                        except KeyError as noName:
                            event_name = ''

                        try:
                            event_venue = event['_embedded']['venues'][0]['name']
                        except KeyError as noVenue:
                            event_venue=' '

                        try:
                            event_city = event['_embedded']['venues'][0]['city']['name']
                        except KeyError as noCity:
                            event_city = ' '

                        try:
                            event_state = event['venues'][0]['state']['name']
                            # event_state_full = event['_embedded']['venues'][0]['state']['name']
                            # print(event_state_full)
                            # event_state = (state_df.loc[state_df['State'] == event_state_full, 'Abbreviation']).tolist()
                            # print(event_state)

                        except KeyError as noState:
                            event_state = ' '

                        try:
                            event_date_Local = event['dates']['start']['localDate']
                        except KeyError as noEventDate:
                            event_date_Local = ' '


                        try:
                            event_time_Local = event['dates']['start']['localTime']
                            event_datetime_Local = datetime.strptime((event_date_Local + " " + event_time_Local), '%Y-%m-%d %H:%M:%S')
                        except KeyError as noEventTime:
                            event_time_Local = ' '
                            event_datetime_Local = datetime.strptime(event_date_Local, '%Y-%m-%d')

                        try:
                            TZ_string = event['dates']['timezone']
                            event_TZ = pytz.timezone(TZ_string)
                            try:
                                local_dt = event_TZ.localize(event_datetime_Local, is_dst=None)
                                date_UTC = local_dt.astimezone(pytz.utc)
                            except ValueError as missing_datetime:
                                event_date = " "
                        except KeyError as noTZ:
                            event_TZ = '?'
                        #	date_UTC = (event_datetime_UTC)

                        #print('THE LOCAL EVENT DATETIME IS ' + str(local_dt) + ' AT TIME ZONE ' + str(TZ_string))
                        #print('THE UTC EVENT DATETIME IS ' + str(event_datetime_UTC))

                        try:
                            event_sale_start = event['sales']['public']['startDateTime']
                        except KeyError as noSaleStart:
                            event_sale_start = ' '

                        try:
                            event_lowest_price = event['priceRanges'][0]['min']
                        except KeyError as noPriceDat:
                            event_lowest_price = ''

                        try:
                            event_highest_price = event['priceRanges'][0]['max']
                        except KeyError as noPriceDat:
                            event_highest_price = ''


                        #-------CREATE A TEMPORARY DATAFRAME FOR EACH EVENT----------#
                        event_profile=pd.DataFrame([[spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, date_UTC, event_sale_start, event_lowest_price, event_highest_price]],
                                      columns=['artist', 'artist_id', 'attraction_name', 'event_id', 'venue', 'city', 'state', 'date_UTC', 'sale_start_date', 'event_lowest_price', 'event_highest_price'])

                        insert_tuple = (spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, date_UTC, event_sale_start, event_lowest_price, event_highest_price, current_date)

                        #print(insert_tuple)

                        #------------SQL TIME - SUBSTITUTE STRINGS INTO SQL QUERY FOR DB SUBMISSION-------------#
                        event_QL = 'INSERT INTO TICKETMASTER_EVENTS(artist, artist_id, name, id, venue, city, state, date_UTC, sale_start, lowest_price, highest_price, create_ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'

                        result  = cursor.execute(event_QL, insert_tuple)
                        connection.commit()

                        """
                        DYNAMO WAY TO DO IT
                        """

                        print(event_lowest_price)
                        print(event_highest_price)

                        record_key = (spotify_artist + event_name + event_venue + event_city + event_state + str(date_UTC)+ str(current_date))

                        try:

                            dynamotable.put_item(

                                Item={
                                    'Record_ID':record_key,
                                    'name': event_name,
                                    'artist': spotify_artist,
                                    'city': event_city,
                                    'date_UTC': str(date_UTC),
                                    'state': event_state,
                                    'venue': event_venue,
                                    'create_ts': str(current_date),
                                    'lowest_price': int(event_lowest_price), 'highest_price': int(event_highest_price)
                                }
                            )

                        except ValueError as NoPrice:

                            dynamotable.put_item(

                                Item={
                                    'Record_ID':record_key,
                                    'name': event_name,
                                    'artist': spotify_artist,
                                    'city': event_city,
                                    'date_UTC': str(date_UTC),
                                    'state': event_state,
                                    'venue': event_venue,
                                    'create_ts': str(current_date),
                                }
                            )

                        #----------WAIT TWO SECONDS BEFORE SUBMITTING NEXT QUERY TO AVOID OVERLOADING API-----------#
                        time.sleep(1)

                except KeyError as NoEmbedded:

                    print('No Embedded Data')

            except KeyError as No_Events:

                print('No Events for this Artist!')

            except urllib.error.HTTPError as Overload:

                print('Too Many requests, wait 2 seconds')
                time.sleep(2)

        #print(i)
        i = i+1

event_pull(data_fetch_pymysql().head(200))
