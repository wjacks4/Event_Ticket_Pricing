"""
SEATGEEK API DATA PULL
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
# import mysql-python
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

"""PRINT TO LOG FOR MONITORING PURPOSES"""
current_date = datetime.now()
print('THIS PROGRAM RAN AT ' + str(current_date))

"""GLOBALS API STRING DATA"""
base_url = ('https://api.seatgeek.com/2/')
client_id_str = ('MTM4MTIyMDZ8MTU1NDQ3MTkxMy43Ng')
client_secret_str = ('c49766eaad2bc8bc33810d112d141ca9a09b0a78b1be52c459eb19c5fd3527a5')


def data_fetch_pymysql():
    connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                 user='tickets_user',
                                 password='tickets_pass',
                                 db='tickets_db')

    artists_df = pd.read_sql('SELECT * FROM ARTISTS_WITH_EVENTS order by current_followers desc', con=connection)
    return artists_df


# data_fetch_pymysql()


"""GET EVENTS FROM SEATGEEK API, LOAD TO MYSQL DB"""


def seatgeek_events():

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
    artists_df = data_fetch_pymysql().head(5)['artist']

    """CURRENT DATE ASSIGNMENT"""
    current_date = datetime.now()

    """DEFINE DYNAMODB ENDPOINT"""
    dynamodb = boto3.resource('dynamodb')
    dynamoTable = dynamodb.Table('SeatGeek_Event_Table')

    """PULL BACK ALL SEATGEEK RECORDS FROM S3 BUCKET, FOR APPENDING LATER"""
    s3_client = boto3.client('s3')
    try:
        s3_client = boto3.client('s3')
        bucket = 'willjeventdata'
        key = 'seatgeek_events.pkl'
        response = s3_client.get_object(Bucket=bucket, Key=key)
        event_dict = (response['Body'].read())
        event_json = json.loads(event_dict.decode('utf8'))
        master_event_df = pd.DataFrame.from_dict(event_json)
        temp_df = pd.DataFrame()

        """INITIALIZE INCREMENTING VARIABLE"""
        i = 1

        # for artist in artists:
        for artist in artists_df:

            performer_slug_stg = artist.replace("&", "")
            performer_slug = (performer_slug_stg.replace("  ", " ")).replace(" ", "-")

            try:

                url = 'https://api.seatgeek.com/2/events?format=json'
                payload = {'per_page': 100,
                           'performers.slug': performer_slug,
                           'client_id': client_id_str,
                           }
                r = requests.get(url, params=payload, verify=True)
                json_obj = json.loads(r.text)
                event_list = json_obj['events']

                for event in event_list:
                    venue_data = event['venue']
                    price_data = event['stats']

                    try:
                        event_name = event['title']
                    # print(event_name)
                    except KeyError as noName:
                        event_name = ''

                    try:
                        event_id = event['id']
                    # print(event_id)
                    except KeyError as noID:
                        event_id = ''

                    try:
                        event_date_utc = event['datetime_utc']
                    # print(event_date_utc)
                    except KeyError as noDatetime:
                        event_date_utc = ''

                    try:
                        event_venue = venue_data['name']
                    # print(event_venue)
                    except KeyError as noVenue:
                        event_venue = ''

                    try:
                        event_capacity = venue_data['capacity']
                    # print(event_capacity)
                    except KeyError as noCapacity:
                        event_capacity = ''

                    try:
                        event_city = venue_data['city']
                    # print(event_city)
                    except KeyError as noCity:
                        event_city = ''

                    try:
                        event_state = venue_data['state']
                    # print(event_state)
                    except KeyError as noState:
                        event_state = ''

                    try:
                        avg_price = price_data['average_price']
                    # print(avg_price)
                    except KeyError as noAvg:
                        avg_price = ''

                    try:
                        med_price = price_data['median_price']
                    # print(med_price)
                    except KeyError as noMed:
                        med_price = ''

                    try:
                        lowest_price = price_data['lowest_price']
                    # print(lowest_price)
                    except KeyError as noLowest:
                        lowest_price = ''

                    try:
                        highest_price = price_data['highest_price']
                    # print(highest_price)
                    except KeyError as noHighest:
                        highest_price = ''

                    try:
                        no_listings = price_data['listing_count']
                    # print(no_listings)
                    except KeyError as noListingCount:
                        no_listings = ''

                    """MYSQL INSERTION"""
                    insert_tuple = (artist, event_name, event_id, event_venue, event_capacity, event_city, event_state,
                        event_date_utc, lowest_price, highest_price, avg_price, med_price, no_listings,
                        current_date)

                    event_QL = 'INSERT INTO `SEATGEEK_EVENTS` (`artist`, `name`, `id`, `venue`, `capacity`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `average_price`, `median_price`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

                    connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                                              user='tickets_user', password='tickets_pass', db='tickets_db')
                    cursor = connection.cursor()
                    cursor.execute(event_QL, insert_tuple)
                    connection.commit()

                    """DYNAMODB INSERTION"""
                    venue_dict = event['venue']
                    price_dict = event['stats']

                    event_key = (event_name + str(event_id) + event_venue + event_city + event_state + str(event_date_utc) + str(current_date))
                    # print(event_key)
                    dynamoTable.put_item(

                        Item={
                            'Event_ID': event_key, 'name': event['title'], 'artist': artist, 'city': venue_dict['city'],
                            'date_UTC': str(event['datetime_utc']), 'state': venue_dict['state'],
                            'venue': venue_dict['name'], 'capacity': venue_dict['capacity'], 'create_ts': str(current_date),
                            'lowest_price': price_dict['lowest_price'], 'highest_price': price_dict['highest_price'],
                            'med_price': price_dict['median_price'], 'avg_price': price_dict['average_price'],
                            'listing_count': price_dict['listing_count']
                        }
                    )

                    """S3 NEW DATA CREATION"""
                    event_array = pd.DataFrame([[artist, event_name, event_id, event_venue, event_capacity,
                                                 event_city, event_state, event_date_utc, lowest_price, highest_price,
                                                 avg_price, med_price, no_listings, current_date]],
                                               columns=['artist', 'name', 'ID', 'venue', 'capacity', 'city',
                                                        'state', 'date_UTC', 'lowest_price', 'highest_price', 'avg_price',
                                                        'med_price', 'listing_count', 'create_ts'])

                    temp_df = temp_df.append(event_array, ignore_index=True, sort=True)


            except IndexError as e:

                print('NO RELATED SEATGEEK EVENTS')

        """APPEND LOCAL DF TO MASTER DF PULLED FROM S3"""
        master_event_df = master_event_df.append(temp_df, sort=True)
        print('The S3 JSON list now has ' + str(len(master_event_df)) + ' records')

        """S3 UPDATE"""
        s3_resource = boto3.resource('s3')
        new_event_json = master_event_df.to_json(orient='records')
        s3_resource.Object(bucket,key).put(Body=new_event_json)

    except s3_client.exceptions.NoSuchKey:

        print('THE S3 BUCKET SOMEHOW GOT DELETED...')


seatgeek_events()


"""PRINT TO LOG FOR MONITORING PURPOSES"""
current_date = datetime.now()
print('THIS PROGRAM FINISHED AT ' + str(current_date))
