"""
EVENTBRITE API DATA PULL
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
from pprint import pprint
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

"""PRINT TO LOG FOR MONITORING PURPOSES"""
current_date = datetime.now()
print('THIS PROGRAM RAN AT ' + str(current_date))


"""GLOBAL API STRING DATA"""
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

    artists_df = pd.read_sql('SELECT * FROM ARTISTS_WITH_EVENTS order by event_count desc, current_followers desc', con=connection)
    return artists_df


# data_fetch_pymysql()


def eventbrite_event_pull():

    """
    MAIN API FUNCTION

        Get top 250 artists from the SQL table with relevant artists (they actually have upcoming events on stubhub)

        Pull pickled JSON file from S3, turn into Pandas DF

        Loop through these artists, making a request to the Eventbrite API for each encoded artist string

        Only keep records where the event name has an adequate fuzzy match score to the artist name

        Format items in API JSON response

        Insert into MYSQL, DynamoDB (NoSQL), and create local Pandas DF within loop

        Append local DF to pandas DF from S3, overwrite in s3

    """

    """GET ARTISTS DATAFRAME"""
    artists_df = data_fetch_pymysql().head(250)

    """CURRENT DATE ASSIGNMENT"""
    current_date = datetime.now()

    """DEFINE DYNAMODB ENDPOINT"""
    dynamodb = boto3.resource('dynamodb')
    dynamotable = dynamodb.Table('EventBrite_Event_Table')

    """PULL BACK ALL EVENTBRITE RECORDS FROM S3 BUCKET, FOR APPENDING LATER"""
    s3_client = boto3.client('s3')
    try:
        bucket = 'willjeventdata'
        key = 'eventbrite_events.pkl'
        response = s3_client.get_object(Bucket=bucket, Key=key)
        event_dict = (response['Body'].read())
        event_json = json.loads(event_dict.decode('utf8'))
        master_event_df = pd.DataFrame.from_dict(event_json)
        print('The S3 JSON list started with ' + str(len(master_event_df)) + ' records')
        temp_df = pd.DataFrame()

        for artist_dat in artists_df.iterrows():

            spotify_artist = artist_dat[1]['artist']
            spotify_artist_id = artist_dat[1]['artist_id']

            artist_encode = (spotify_artist.replace("&", " ")).replace(" ", "%20")

            artist_url = (base_string + "expand=ticket_availability,external_ticketing,venue&" + "q=" + artist_encode)

            try:

                rawdat = urllib.request.urlopen(artist_url)
                encoded_dat = rawdat.read().decode('utf-8', errors='ignore')
                json_dat = json.loads(encoded_dat)

                events = json_dat['events']

                for event in events:

                    try:

                        event_name = ((event['name']['text']).replace('"', '')).encode('utf-8')
                        name_decode = unidecode(str(event_name, encoding="utf-8")).replace('"', '')

                        Spotify_name = spotify_artist
                        EventBrite_name = event_name

                        fuzz_partial = fuzz.partial_ratio(Spotify_name.lower(), EventBrite_name.lower())
                        fuzz_ratio = fuzz.ratio(Spotify_name.lower(), EventBrite_name.lower())

                        if (fuzz_ratio + fuzz_partial) > 150:
                            # print(event_name)
                            # print(fuzz_partial)
                            # print(fuzz_ratio)

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

                            """MYSQL INSERTION"""
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
                            cursor.execute(event_QL, insert_tuple)
                            connection.commit()

                            """DYNAMODB INSERTION"""
                            event_key = ( name_decode + str(event_id) + event_venue + event_city + event_state + str(event_date_UTC) + str(current_date))
                            # print(event_key)

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

                            """S3 NEW DATA CREATION"""
                            event_array = pd.DataFrame([[spotify_artist, spotify_artist_id, event_name, event_id,
                                                         event_venue, event_city, event_state, event_date_UTC, lowest_price,
                                                         highest_price, capacity, sold_out_indicator, shareable,
                                                         available_elsewhere, current_date]],
                                                       columns=['artist', 'artist_id', 'name', 'event_id', 'venue', 'city',
                                                                'state', 'date_UTC', 'lowest_price', 'highest_price',
                                                                'capacity', 'sold_out', 'shareable',
                                                                'available_elsewhere', 'create_ts'])

                            temp_df = temp_df.append(event_array, ignore_index=True, sort=True)

                    except TypeError as no_data:

                        print('One of the fields was missing')

            except urllib.error.HTTPError:

                print('Bad Request')

        """APPEND LOCAL DF TO MASTER DF PULLED FROM S3"""
        master_event_df = master_event_df.append(temp_df, sort=True)
        print('The S3 JSON list now has ' + str(len(master_event_df)) + ' records')

        """S3 UPDATE"""
        s3_resource = boto3.resource('s3')
        new_event_json = master_event_df.to_json(orient='records')
        s3_resource.Object(bucket,key).put(Body=new_event_json)

    except s3_client.exceptions.NoSuchKey:

        print('THE S3 BUCKET SOMEHOW GOT DELETED...')


eventbrite_event_pull()


"""PRINT TO LOG FOR MONITORING PURPOSES"""
current_date = datetime.now()
print('THIS PROGRAM FINISHED AT ' + str(current_date))
