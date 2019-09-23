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


# data_fetch_pymysql()


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
token1 = keys(b'zz5xHP3Miax2zeo9fnKivFSPGmWsLiSv', b'G4j3RRmpBxo8jM7s', 'wjacks4@g.clemson.edu', 'Hester3123')
token2 = keys(b'mwrKyXKBADj7gqY2jqmjAkXFpMgr0u5p', b'GF96v7mWwUDY5fnV', 'hiltonsounds@gmail.com', 'Hester3123')
token3 = keys(b'hf0bANqvcOAJxqhoAccKEI9ulv2oovef', b'aOOlKPrTckv6iJPU', 'edenk@g.clemson.edu', 'Hester3123')
token4 = keys(b'Q53rXMFZn9FfQuxNJhYJAPhbxFTDpH59', b'pQSLJvFEuk2AoHqG', 'butteredtoast66@gmail.com', 'Hester3123')
token5 = keys(b'uyoddTC6PL6ZIGaMkirj64bFRvLbMoDY', b'Ok4sujJFfhvYIT7W', 'sunglassman3123@gmail.com', 'Hester3123')


def stubhub_event_pull():
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

    """DB CONNECTIONS"""
    connection = pymysql.connect('ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', 'tickets_user', 'tickets_pass',
                                 'tickets_db')
    cursor = connection.cursor()

    dynamodb = boto3.resource('dynamodb')
    dynamotable = dynamodb.Table('Stubhub_Event_Table')

    s3_client = boto3.client('s3')
    try:
        s3_client = boto3.client('s3')
        bucket = 'willjeventdata'
        key = 'stubhub_events.pkl'
        key_json = 'stubhub/stubhub_events.json'
        response = s3_client.get_object(Bucket=bucket, Key=key)
        event_dict = (response['Body'].read())
        event_json = json.loads(event_dict.decode('utf8'))
        master_event_df = pd.DataFrame.from_dict(event_json)
        print('The S3 JSON list started with ' + str(len(master_event_df)) + ' records')
        temp_df = pd.DataFrame()

        """GET ARTISTS DF FROM MYSQL"""
        artists_df = data_fetch_pymysql().head(250)['artist']

        """CURRENT DATE ASSIGNMENT"""
        current_date = datetime.now()

        """INITIALIZE INCREMENTING VARIABLE"""
        i = 1

        for artist in artists_df:

            artist_encode = artist.replace(" ", "%20")
            query_params = ("q=" + artist_encode + "&" + "rows=100")
            artist_url = (base_url + "?" + query_params)

            if i <= 50:

                print(i)
                try:
                    auth_header = ("Bearer " + token1.token)
                    headers = {"Authorization": auth_header, "Accept": "application/json"}
                    req = requests.get(artist_url, headers=headers)
                    json_obj = req.json()
                    event_list = json_obj['events']

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

                            """MYSQL INSERTION"""
                            insert_tuple = (
                                artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC,
                                lowest_price, highest_price, ticket_count, listing_count, current_date)

                            event_ql = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

                            cursor.execute(event_ql, insert_tuple)
                            connection.commit()

                            """DYNAMODB INSERTION"""
                            venue_dict = event['venue']
                            price_dict = event['ticketInfo']

                            event_key = (
                                    event_name + event_venue + event_city + event_state + str(event_date_UTC) + str(
                                current_date))
                            # print(event_key)

                            dynamotable.put_item(

                                Item={
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

                            """S3 NEW DATA CREATION"""
                            event_array = pd.DataFrame([[artist, '', event_name, event_id, event_venue, event_city,
                                                         event_state, event_date_UTC, lowest_price, highest_price,
                                                         ticket_count, listing_count, current_date]],
                                                       columns=['artist', 'artist_id', 'name', 'ID', 'venue', 'city',
                                                                'state', 'date_UTC', 'lowest_price', 'highest_price',
                                                                'ticket_count', 'listing_count', 'create_ts'])

                            temp_df = temp_df.append(event_array, ignore_index=True, sort=True)


                except KeyError as Overload:
                    print(KeyError)
                    print('exceeded quota for stubhub API')

            elif 50 < i <= 100:
                print(i)

                try:
                    auth_header = ("Bearer " + token2.token)
                    headers = {"Authorization": auth_header, "Accept": "application/json"}
                    req = requests.get(artist_url, headers=headers)
                    json_obj = req.json()

                    # print(json_obj)

                    event_list = json_obj['events']

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

                            """MYSQL INSERTION"""
                            insert_tuple = (
                                artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC,
                                lowest_price, highest_price, ticket_count, listing_count, current_date)

                            event_ql = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

                            cursor.execute(event_ql, insert_tuple)
                            connection.commit()

                            """DYNAMODB INSERTION"""
                            venue_dict = event['venue']
                            price_dict = event['ticketInfo']

                            event_key = (
                                    event_name + event_venue + event_city + event_state + str(event_date_UTC) + str(
                                current_date))
                            # print(event_key)

                            dynamotable.put_item(
                                Item={
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

                            """S3 NEW DATA CREATION"""
                            event_array = pd.DataFrame([[artist, '', event_name, event_id, event_venue, event_city,
                                                         event_state, event_date_UTC, lowest_price, highest_price,
                                                         ticket_count, listing_count, current_date]],
                                                       columns=['artist', 'artist_id', 'name', 'ID', 'venue', 'city',
                                                                'state', 'date_UTC', 'lowest_price', 'highest_price',
                                                                'ticket_count', 'listing_count', 'create_ts'])

                            temp_df = temp_df.append(event_array, ignore_index=True, sort=True)

                except KeyError as Overload:

                    print(KeyError)
                    print('exceeded quota for stubhub API')

            elif 100 < i <= 150:

                print(i)

                try:
                    auth_header = ("Bearer " + token3.token)
                    headers = {"Authorization": auth_header, "Accept": "application/json"}
                    req = requests.get(artist_url, headers=headers)
                    json_obj = req.json()

                    # print(json_obj)

                    event_list = json_obj['events']

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

                            """MYSQL INSERTION"""
                            insert_tuple = (
                                artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC,
                                lowest_price, highest_price, ticket_count, listing_count, current_date)

                            event_ql = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

                            cursor.execute(event_ql, insert_tuple)
                            connection.commit()

                            """DYNAMODB INSERTION"""
                            venue_dict = event['venue']
                            price_dict = event['ticketInfo']

                            event_key = (
                                    event_name + event_venue + event_city + event_state + str(event_date_UTC) + str(
                                current_date))
                            # print(event_key)

                            dynamotable.put_item(
                                Item={
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

                            """S3 NEW DATA CREATION"""
                            event_array = pd.DataFrame([[artist, '', event_name, event_id, event_venue, event_city,
                                                         event_state, event_date_UTC, lowest_price, highest_price,
                                                         ticket_count, listing_count, current_date]],
                                                       columns=['artist', 'artist_id', 'name', 'ID', 'venue', 'city',
                                                                'state', 'date_UTC', 'lowest_price', 'highest_price',
                                                                'ticket_count', 'listing_count', 'create_ts'])

                            temp_df = temp_df.append(event_array, ignore_index=True, sort=True)

                except KeyError as Overload:
                    print(KeyError)
                    print('exceeded quota for stubhub API')

            elif 150 < i <= 200:

                print(i)

                try:
                    auth_header = ("Bearer " + token4.token)
                    headers = {"Authorization": auth_header, "Accept": "application/json"}
                    req = requests.get(artist_url, headers=headers)
                    json_obj = req.json()

                    # print(json_obj)

                    event_list = json_obj['events']

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

                            """MYSQL INSERTION"""
                            insert_tuple = (
                                artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC,
                                lowest_price, highest_price, ticket_count, listing_count, current_date)

                            event_ql = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

                            cursor.execute(event_ql, insert_tuple)
                            connection.commit()

                            """DYNAMODB INSERTION"""
                            venue_dict = event['venue']
                            price_dict = event['ticketInfo']

                            event_key = (
                                    event_name + event_venue + event_city + event_state + str(event_date_UTC) + str(
                                current_date))
                            # print(event_key)

                            dynamotable.put_item(
                                Item={
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

                            """S3 NEW DATA CREATION"""
                            event_array = pd.DataFrame([[artist, '', event_name, event_id, event_venue, event_city,
                                                         event_state, event_date_UTC, lowest_price, highest_price,
                                                         ticket_count, listing_count, current_date]],
                                                       columns=['artist', 'artist_id', 'name', 'ID', 'venue', 'city',
                                                                'state', 'date_UTC', 'lowest_price', 'highest_price',
                                                                'ticket_count', 'listing_count', 'create_ts'])

                            temp_df = temp_df.append(event_array, ignore_index=True, sort=True)

                except KeyError as Overload:
                    print(KeyError)
                    print('exceeded quota for stubhub API')

            else:

                print(i)

                try:
                    auth_header = ("Bearer " + token5.token)
                    headers = {"Authorization": auth_header, "Accept": "application/json"}
                    req = requests.get(artist_url, headers=headers)
                    json_obj = req.json()

                    # print(json_obj)

                    event_list = json_obj['events']

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

                            """MYSQL INSERTION"""
                            insert_tuple = (
                                artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC,
                                lowest_price, highest_price, ticket_count, listing_count, current_date)

                            event_ql = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

                            cursor.execute(event_ql, insert_tuple)
                            connection.commit()

                            """DYNAMODB INSERTION"""
                            venue_dict = event['venue']
                            price_dict = event['ticketInfo']

                            event_key = (
                                    event_name + event_venue + event_city + event_state + str(event_date_UTC) + str(
                                current_date))
                            # print(event_key)

                            dynamotable.put_item(

                                Item={
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

                            """S3 NEW DATA CREATION"""
                            event_array = pd.DataFrame([[artist, '', event_name, event_id, event_venue, event_city,
                                                         event_state, event_date_UTC, lowest_price, highest_price,
                                                         ticket_count, listing_count, current_date]],
                                                       columns=['artist', 'artist_id', 'name', 'ID', 'venue', 'city',
                                                                'state', 'date_UTC', 'lowest_price', 'highest_price',
                                                                'ticket_count', 'listing_count', 'create_ts'])

                            temp_df = temp_df.append(event_array, ignore_index=True, sort=True)

                except KeyError as Overload:

                    print(KeyError)
                    print('exceeded quota for stubhub API')

            i = i + 1

        """APPEND LOCAL DF TO MASTER DF PULLED FROM S3"""
        master_event_df = master_event_df.append(temp_df, sort=True)
        print('The S3 JSON list now has ' + str(len(master_event_df)) + ' records')

        """S3 UPDATE"""
        s3_resource = boto3.resource('s3')
        new_event_json = master_event_df.to_json(orient='records')
        s3_resource.Object(bucket, key).put(Body=new_event_json)

        """S3 UPDATE .JSON"""
        json_reform = new_event_json.replace('[{', '{').replace(']}', '}').replace('},', '}\n')
        s3_resource.Object(bucket, key_json).put(Body=json_reform)


    except s3_client.exceptions.NoSuchKey:

        print('THE S3 BUCKET SOMEHOW GOT DELETED...')


stubhub_event_pull()

"""PRINT TO LOG FOR MONITORING PURPOSES"""
current_date = datetime.now()
print('THIS PROGRAM FINISHED AT ' + str(current_date))
