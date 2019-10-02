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


# data_fetch_pymysql()


def myconverter(o):
    if isinstance(o, datetime):
        return o.__str__()


def athena_drop():
    athena_client = boto3.client('athena')
    response = athena_client.start_query_execution(
        QueryString = ('drop table stubhub_events'),
        QueryExecutionContext ={'Database':'tickets_db'},
        ResultConfiguration={'OutputLocation':'s3://aws-athena-results-tickets-db/stubhub/'}
    )


def athena_create_main(main_columns):
    querystring = str(('create external table if not exists stubhub_events'
                       ' (' + main_columns + ') ROW FORMAT SERDE "org.openx.data.jsonserde.JsonSerDe" \
                     LOCATION "s3://willjeventdata/stubhub/main data/" TBLPROPERTIES ("has_encrypted_data"="false")')
                      )
    print(querystring)
    athena_client = boto3.client('athena')
    response = athena_client.start_query_execution(
        QueryString=('create external table if not exists stubhub_events'
                     ' (' + main_columns + ') ROW FORMAT SERDE "org.openx.data.jsonserde.JsonSerDe" LOCATION \
                     "s3://willjeventdata/stubhub/main data/" TBLPROPERTIES ("has_encrypted_data"="false")'
                     ),
        QueryExecutionContext={'Database': 'tickets_db'},
        ResultConfiguration={'OutputLocation': 's3://aws-athena-results-tickets-db/stubhub/'}
    )


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




def stubhub_event_pull(temp_df, artist_in, artist_url, cursor_in, connection_in, dynamotable_in, token_in):
    
    try:
        auth_header = ("Bearer " + token_in.token)
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
                    artist_in, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC,
                    lowest_price, highest_price, ticket_count, listing_count, current_date)

                event_ql = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

                cursor_in.execute(event_ql, insert_tuple)
                connection_in.commit()

                """DYNAMODB INSERTION"""
                venue_dict = event['venue']
                price_dict = event['ticketInfo']

                event_key = (
                        event_name + event_venue + event_city + event_state + str(event_date_UTC) + str(
                    current_date))
                # print(event_key)

                dynamotable_in.put_item(

                    Item={
                        'Event_ID': event_key,
                        'name': event['name'],
                        'artist': artist_in,
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
                event_array = pd.DataFrame([[artist_in, '', event_name, event_id, event_venue, event_city,
                                             event_state, event_date_UTC, lowest_price, highest_price,
                                             ticket_count, listing_count, current_date]],
                                           columns=['artist', 'artist_id', 'name', 'ID', 'venue', 'city',
                                                    'state', 'date_UTC', 'lowest_price', 'highest_price',
                                                    'ticket_count', 'listing_count', 'create_ts'])

                temp_df = temp_df.append(event_array, ignore_index=True, sort=True)

            return temp_df

    except KeyError as Overload:
        print(KeyError)
        print('exceeded quota for stubhub API')


def pull_caller(inner_func):
    
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
        key_temp = 'stubhub/temp data/stubhub_temp.pkl'
        key_json = 'stubhub/main data/stubhub_events.json'
        response = s3_client.get_object(Bucket=bucket, Key=key)
        event_dict = (response['Body'].read())
        event_json = json.loads(event_dict.decode('utf8'))
        # master_event_df = pd.DataFrame.from_dict(event_json)
        print('The S3 JSON list started with ' + str(len(event_json)) + ' records')
        # master_event_df = pd.DataFrame()
        temp_df = pd.DataFrame()

        """GET ARTISTS DF FROM MYSQL"""
        artists_df = data_fetch_pymysql().head(250)['artist']


        """INITIALIZE INCREMENTING VARIABLE"""
        i = 1

        for artist in artists_df:

            artist_encode = artist.replace(" ", "%20")
            query_params = ("q=" + artist_encode + "&" + "rows=100")
            artist_url = (base_url + "?" + query_params)

            if i <= 50:
                
                temp_df = stubhub_event_pull(temp_df, artist, artist_url, cursor, connection, dynamotable, token1)
                
            elif 50 < i <= 100:
                
                temp_df = stubhub_event_pull(temp_df, artist, artist_url, cursor, connection, dynamotable, token2)
            
            elif 100 < i <= 150:
                
                temp_df = stubhub_event_pull(temp_df, artist, artist_url, cursor, connection, dynamotable, token3)
                
            elif 150 < i <= 200:
                
                temp_df = stubhub_event_pull(temp_df, artist, artist_url, cursor, connection, dynamotable, token4)
                
            else:
                
                temp_df = stubhub_event_pull(temp_df, artist, artist_url, cursor, connection, dynamotable, token5)
                
            i = i + 1
            
    except s3_client.exceptions.NoSuchKey:

        print('THE S3 BUCKET SOMEHOW GOT DELETED...')
        
        
    """DICT APPEND METHOD"""
    """S3 RESOURCE"""
    s3_resource = boto3.resource('s3')

    """MAKE DICT FROM TEMP DATAFRAME"""
    temp_dict = temp_df.to_dict('records')

    """MERGE TEMP DICT AND MASTER DICT"""
    appended_dict = event_json + temp_dict
    print('The S3 JSON list now has ' + str(len(appended_dict)) + ' records')

    """S3 FROM TEMP DICT"""
    temp_dict_stg = json.dumps(temp_dict, default=myconverter)
    # s3_resource.Object(bucket, key_temp).put(Body=temp_dict_stg)
    s3_resource.Object(bucket, key_temp).put(Body=temp_dict_stg)
    print('successfully stored the ' + str(len(temp_dict)) + ' records of new data')

    """S3 PKL FROM APPENDED DICT"""
    appended_dict_stg = json.dumps(appended_dict, default=myconverter)
    # s3_resource.Object(bucket, key).put(Body=appended_dict_stg)
    s3_resource.Object(bucket, key).put(Body=appended_dict_stg)
    print('successfully overwrote the PKL file which now has ' + str(len(appended_dict)) + ' records')

    """S3 JSON FROM APPENDED DICT"""
    appended_json = appended_dict_stg.replace('[{', '{').replace(']}', '}').replace('},', '}\n')
    # s3_resource.Object(bucket,key_json).put(Body=appended_json)
    s3_resource.Object(bucket, key_json).put(Body=appended_json)
    print('successfully overwrote main JSON file which now has ' + str(len(appended_dict)) + ' records')


"""CALL MAIN FUNCTION"""
pull_caller(stubhub_event_pull)


"""PRINT TO LOG FOR MONITORING PURPOSES"""
current_date = datetime.now()
print('THIS PROGRAM FINISHED AT ' + str(current_date))
