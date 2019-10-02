"""
TICKETMASTER API DATA PULL
"""

#!/usr/bin/env python3

#import mysql
#from mysql.connector import Error
#import psycopg2 as p
import json
import time
import boto3
import urllib.request
import pandas as pd
import unidecode
from unidecode import unidecode
import urllib
import pymysql
import datetime
from datetime import datetime
from fuzzywuzzy import fuzz

"""PRINT TO LOG FOR MONITORING PURPOSES"""
current_date = datetime.now()
print('THIS PROGRAM RAN AT ' + str(current_date))


"""GLOBAL API STRING DATA"""
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
    artists_df = pd.read_sql('SELECT * FROM ARTISTS_WITH_EVENTS order by event_count desc, current_followers desc', con = connection)
    # artists_df = pd.read_sql('SELECT * FROM ARTISTS_WITH_EVENTS WHERE ARTIST = "Tedeschi Trucks Band" order by event_count desc, current_followers desc', con = connection)
    return artists_df


# data_fetch_pymysql()


def request_limit_check():
    try:
        test_url1 = ('https://app.ticketmaster.com/discovery/v2/events.json?&apikey=83sdXVyv4k3NnuGCIvk5nAHE3NSWddg7&size=10&keyword=Da+Baby')
        test_url2 = ('https://app.ticketmaster.com/discovery/v2/events.json?&apikey=2C4llrNfIrGgEZxAft1QuJ5bpbS3SdpF&size=10&keyword=Da+Baby')
        request1 = urllib.request.urlopen(test_url1).info()
        request2 = urllib.request.urlopen(test_url2).info()

    except urllib.error.HTTPError as Overload:

        print(Overload)
        print('Too many requests')


# request_limit_check()


def myconverter(o):
    if isinstance(o, datetime):
        return o.__str__()


def athena_drop():
    athena_client = boto3.client('athena')
    response = athena_client.start_query_execution(
        QueryString = ('drop table ticketmaster_events'),
        QueryExecutionContext ={'Database':'tickets_db'},
        ResultConfiguration={'OutputLocation':'s3://aws-athena-results-tickets-db/ticketmaster/'}
    )


def athena_create_temp(main_columns):
    querystring = str(('create external table if not exists ticketmaster_tmp'
                       ' (' + main_columns + ') ROW FORMAT SERDE "org.openx.data.jsonserde.JsonSerDe" \
                     LOCATION "s3://willjeventdata/ticketmaster/temp data/" TBLPROPERTIES ("has_encrypted_data"="false")')
                      )
    # print(querystring)
    athena_client = boto3.client('athena')
    response = athena_client.start_query_execution(
        QueryString=('create external table if not exists ticketmaster_events'
                     ' (' + main_columns + ') ROW FORMAT SERDE "org.openx.data.jsonserde.JsonSerDe" LOCATION \
                     "s3://willjeventdata/ticketmaster/temp data/" TBLPROPERTIES ("has_encrypted_data"="false")'
                     ),
        QueryExecutionContext={'Database': 'tickets_db'},
        ResultConfiguration={'OutputLocation': 's3://aws-athena-results-tickets-db/ticketmaster/'}
    )


def athena_create_main(main_columns):
    querystring = str(('create external table if not exists ticketmaster_events'
                       ' (' + main_columns + ') ROW FORMAT SERDE "org.openx.data.jsonserde.JsonSerDe" \
                     LOCATION "s3://willjeventdata/ticketmaster/main data/" TBLPROPERTIES ("has_encrypted_data"="false")')
                      )
    # print(querystring)
    athena_client = boto3.client('athena')
    response = athena_client.start_query_execution(
        QueryString=('create external table if not exists ticketmaster_events'
                     ' (' + main_columns + ') ROW FORMAT SERDE "org.openx.data.jsonserde.JsonSerDe" LOCATION \
                     "s3://willjeventdata/ticketmaster/main data/" TBLPROPERTIES ("has_encrypted_data"="false")'
                     ),
        QueryExecutionContext={'Database': 'tickets_db'},
        ResultConfiguration={'OutputLocation': 's3://aws-athena-results-tickets-db/ticketmaster/'}
    )


def ticketmaster_event_pull():

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
    artists_df = data_fetch_pymysql().head(5)

    """CURRENT DATE ASSIGNMENT"""
    current_date = datetime.now()

    """SQL CONNECTION"""
    connection=pymysql.connect(host = 'ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user = 'tickets_user', password = 'tickets_pass', db = 'tickets_db')
    cursor=connection.cursor()

    """NOSQL CONNECTION"""
    dynamodb = boto3.resource('dynamodb')
    dynamotable = dynamodb.Table('TicketMaster_Event_Table')

    """PULL BACK ALL EVENTBRITE RECORDS FROM S3 BUCKET, FOR APPENDING LATER"""
    s3_client = boto3.client('s3')
    try:
        bucket = 'willjeventdata'
        key = 'ticketmaster_events.pkl'
        key_temp = 'ticketmaster/temp data/ticketmaster_events.pkl'
        key_json = 'ticketmaster/main data/ticketmaster_events.json'
        response = s3_client.get_object(Bucket=bucket, Key=key)
        event_dict = (response['Body'].read())
        event_json = json.loads(event_dict.decode('utf8'))
        # master_event_df = pd.DataFrame.from_dict(event_json)
        print('The S3 JSON list started with ' + str(len(event_json))+ ' records')
        temp_df = pd.DataFrame()

        """INITIALIZE INCREMENTING VARIABLE"""
        i = 1

        for artist_dat in artists_df.iterrows():

            spotify_artist = artist_dat[1]['artist']
            spotify_artist_id = artist_dat[1]['artist_id']

            artist_encode = spotify_artist.encode('utf-8')
            artist_decode = unidecode(str(artist_encode, encoding = "utf-8"))
            artist_keyword = artist_decode.replace(" ", "+")

            if i <= 250:
            # if i <10:

                access_string = (event_base_url + api_key1 + '&size=25&keyword=' + artist_keyword)

                try:
                    raw_Dat = urllib.request.urlopen(access_string)
                    encoded_Dat = raw_Dat.read().decode('utf-8', 'ignore')
                    json_Dat = json.loads(encoded_Dat)

                    try:
                        event_Dat = json_Dat['_embedded']['events']
                        # pprint(event_Dat)

                        for event in event_Dat:
                            name = event['name']
                            event_id = event['id']

                            try:
                                event_name = event['name']
                                # print(event_name)
                            except KeyError as noName:
                                event_name = 'NA'

                            spotify_name = spotify_artist
                            ticketmaster_name = event_name

                            fuzz_partial = fuzz.partial_ratio(spotify_name.lower(), ticketmaster_name.lower())
                            fuzz_ratio = fuzz.ratio(spotify_name.lower(), ticketmaster_name.lower())

                            if (fuzz_ratio + fuzz_partial) > 150:

                                try:
                                    event_venue = event['_embedded']['venues'][0]['name']
                                except KeyError as noVenue:
                                    event_venue = 'NA'

                                try:
                                    event_city = event['_embedded']['venues'][0]['city']['name']
                                except KeyError as noCity:
                                    event_city = 'NA'

                                try:
                                    event_state = event['venues'][0]['state']['name']
                                except KeyError as noState:
                                    event_state = 'NA'

                                try:
                                    date_UTC = event['dates']['start']['dateTime']
                                except KeyError as noEventTime:
                                    date_UTC = 'NA'

                                try:
                                    event_sale_start = event['sales']['public']['startDateTime']
                                except KeyError as noSaleStart:
                                    event_sale_start = 'NA'

                                try:
                                    event_lowest_price = event['priceRanges'][0]['min']
                                except KeyError as noPriceDat:
                                    event_lowest_price = ''

                                try:
                                    event_highest_price = event['priceRanges'][0]['max']
                                except KeyError as noPriceDat:
                                    event_highest_price = ''

                                """MYSQL INSERTION"""
                                insert_tuple = (spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, date_UTC, event_sale_start, event_lowest_price, event_highest_price, current_date)

                                event_QL = 'INSERT INTO TICKETMASTER_EVENTS(artist, artist_id, name, id, venue, city, state, date_UTC, sale_start, lowest_price, highest_price, create_ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'

                                cursor.execute(event_QL, insert_tuple)
                                connection.commit()

                                """DYNAMO WAY TO DO IT"""
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
                                                'create_ts': str(current_date)
                                            }
                                        )

                                    except ValueError as MissingData:
                                        print('Too much missing data')

                                """S3 NEW DATA CREATION"""
                                event_array=pd.DataFrame([[spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, date_UTC, event_sale_start, event_lowest_price, event_highest_price, current_date]],
                                              columns=['artist', 'artist_id', 'name', 'event_id', 'venue', 'city', 'state', 'date_UTC', 'sale_start_date', 'lowest_price', 'highest_price', 'create_ts'])

                                temp_df = temp_df.append(event_array, ignore_index=True, sort=True)

                    except KeyError as NoEmbedded:
                        print('No Embedded Data')
                        time.sleep(1)

                except KeyError as No_Events:
                    print('No Events for this Artist!')

                except urllib.error.HTTPError as Overload:
                    print('Too Many requests, wait 2 seconds')
                    time.sleep(2)

            else:
                access_string = (event_base_url + api_key2 + '&size=10&keyword=' + artist_keyword)

                try:
                    raw_Dat = urllib.request.urlopen(access_string)
                    encoded_Dat = raw_Dat.read().decode('utf-8', 'ignore')
                    json_Dat = json.loads(encoded_Dat)

                    try:
                        event_Dat = json_Dat['_embedded']['events']

                        # pprint(event_Dat)

                        # -------------EXTRACT EVENT ID FROM DATA IN EACH MEMBER OF EVENT OBJECT-----------#
                        for event in event_Dat:
                            name = event['name']
                            event_id = event['id']

                            try:
                                event_name = event['name']
                            except KeyError as noName:
                                event_name = 'NA'

                            try:
                                event_venue = event['_embedded']['venues'][0]['name']
                            except KeyError as noVenue:
                                event_venue = 'NA'

                            try:
                                event_city = event['_embedded']['venues'][0]['city']['name']
                            except KeyError as noCity:
                                event_city = 'NA'

                            try:
                                event_state = event['venues'][0]['state']['name']
                            except KeyError as noState:
                                event_state = 'NA'

                            try:
                                date_UTC = event['dates']['start']['dateTime']
                            except KeyError as noEventTime:
                                date_UTC = 'NA'

                            try:
                                event_sale_start = event['sales']['public']['startDateTime']
                            except KeyError as noSaleStart:
                                event_sale_start = 'NA'

                            try:
                                event_lowest_price = event['priceRanges'][0]['min']
                            except KeyError as noPriceDat:
                                event_lowest_price = ''

                            try:
                                event_highest_price = event['priceRanges'][0]['max']
                            except KeyError as noPriceDat:
                                event_highest_price = ''

                            """MYSQL INSERTION"""
                            insert_tuple = (
                            spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city,
                            event_state, date_UTC, event_sale_start, event_lowest_price, event_highest_price,
                            current_date)

                            event_QL = 'INSERT INTO TICKETMASTER_EVENTS(artist, artist_id, name, id, venue, city, state, date_UTC, sale_start, lowest_price, highest_price, create_ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'

                            cursor.execute(event_QL, insert_tuple)
                            connection.commit()

                            """DYNAMO WAY TO DO IT"""
                            record_key = (spotify_artist + event_name + event_venue + event_city + event_state + str(
                                date_UTC) + str(current_date))

                            try:
                                dynamotable.put_item(
                                    Item={
                                        'Record_ID': record_key,
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
                                try:
                                    dynamotable.put_item(

                                        Item={
                                            'Record_ID': record_key,
                                            'name': event_name,
                                            'artist': spotify_artist,
                                            'city': event_city,
                                            'date_UTC': str(date_UTC),
                                            'state': event_state,
                                            'venue': event_venue,
                                            'create_ts': str(current_date)
                                        }
                                    )

                                except ValueError as MissingData:
                                    print('Too much missing data')

                            """S3 NEW DATA CREATION"""
                            event_array = pd.DataFrame([[spotify_artist, spotify_artist_id, event_name, event_id,
                                                         event_venue, event_city, event_state, date_UTC,
                                                         event_sale_start, event_lowest_price, event_highest_price,
                                                         current_date]],
                                                       columns=['artist', 'artist_id', 'name', 'id',
                                                                'venue', 'city', 'state', 'date_UTC', 'sale_start',
                                                                'lowest_price', 'highest_price',
                                                                'create_ts'])

                            temp_df = temp_df.append(event_array, ignore_index=True, sort=True)

                    except KeyError as NoEmbedded:
                        print('No Embedded Data')
                        time.sleep(1)

                except KeyError as No_Events:
                    print('No Events for this Artist!')

                except urllib.error.HTTPError as Overload:
                    print('Too Many requests, wait 2 seconds')
                    time.sleep(2)

            """INCREMENT COUNTING VARIABLE"""
            i = i+1

        """APPEND LOCAL DF TO MASTER DF PULLED FROM S3"""
        # master_event_df = master_event_df.append(temp_df, sort=True)

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

        """ATHENA CREATE DROP AND CREATE MAIN TABLE"""
        columns_string = str(temp_df.columns.values).replace("['", "`").replace(" '", " `").replace("']", '` string').replace("' ", "` string, ").replace("'\n", "` string, ").replace("`date_UTC` string", "`date_UTC` timestamp").replace("`create_ts` string", "`create_ts` timestamp")
        athena_drop()
        time.sleep(15)
        athena_create_main(columns_string)

    except s3_client.exceptions.NoSuchKey:
        print('THE S3 BUCKET SOMEHOW GOT DELETED...')


ticketmaster_event_pull()


"""PRINT TO LOG FOR MONITORING PURPOSES"""
current_date = datetime.now()
print('THIS PROGRAM FINISHED AT ' + str(current_date))
