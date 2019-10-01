"""
SEATGEEK API DATA PULL
"""

import pymysql
import datetime
from datetime import datetime
import pandas as pd
import json
import requests
import boto3
import time

"""PRINT TO LOG FOR MONITORING PURPOSES"""
current_date = datetime.now()
print('THIS PROGRAM RAN AT ' + str(current_date))

"""GLOBALS API STRING DATA"""
base_url = ('https://api.seatgeek.com/2/')
client_id_str = ('MTM4MTIyMDZ8MTU1NDQ3MTkxMy43Ng')
client_secret_str = ('c49766eaad2bc8bc33810d112d141ca9a09b0a78b1be52c459eb19c5fd3527a5')


def xstr(s):
    return '' if s is None else s


def myconverter(o):
    if isinstance(o, datetime):
        return o.__str__()

def data_fetch_pymysql():
    connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                 user='tickets_user',
                                 password='tickets_pass',
                                 db='tickets_db')

    artists_df = pd.read_sql('SELECT * FROM ARTISTS_WITH_EVENTS order by event_count desc, current_followers desc', con=connection)
    return artists_df


# data_fetch_pymysql()

def athena_drop():
    athena_client = boto3.client('athena')
    response = athena_client.start_query_execution(
<<<<<<< HEAD
        QueryString = ('drop table seatgeek_events'),
        QueryExecutionContext ={'Database':'tickets_db'},
        ResultConfiguration={'OutputLocation':'s3://aws-athena-results-tickets-db/seatgeek/'}
=======
    QueryString = ('drop table seatgeek_events'),
    QueryExecutionContext ={'Database':'tickets_db'},
    ResultConfiguration={'OutputLocation':'s3://aws-athena-results-tickets-db/seatgeek/'}
>>>>>>> 9c49e611334ed370fb801c90a81b837ae255bac2
    )


def athena_create_temp(main_columns):
    querystring = str(('create external table if not exists seatgeek_tmp' 
                     ' (' + main_columns + ') ROW FORMAT SERDE "org.openx.data.jsonserde.JsonSerDe" \
                     LOCATION "s3://willjeventdata/seatgeek/temp data/" TBLPROPERTIES ("has_encrypted_data"="false")')
                      )
    print(querystring)
    athena_client = boto3.client('athena')
    response = athena_client.start_query_execution(
        QueryString=('create external table if not exists seatgeek_tmp'
                     ' (' + main_columns + ') ROW FORMAT SERDE "org.openx.data.jsonserde.JsonSerDe" LOCATION \
                     "s3://willjeventdata/seatgeek/temp data/" TBLPROPERTIES ("has_encrypted_data"="false")'
                     ),
        QueryExecutionContext={'Database': 'tickets_db'},
        ResultConfiguration={'OutputLocation': 's3://aws-athena-results-tickets-db/seatgeek/'}
    )


def athena_create_main(main_columns):
    querystring = str(('create external table if not exists seatgeek_events'
                       ' (' + main_columns + ') ROW FORMAT SERDE "org.openx.data.jsonserde.JsonSerDe" \
                     LOCATION "s3://willjeventdata/seatgeek/main data/" TBLPROPERTIES ("has_encrypted_data"="false")')
                      )
    print(querystring)
    athena_client = boto3.client('athena')
    response = athena_client.start_query_execution(
        QueryString=('create external table if not exists seatgeek_events'
                     ' (' + main_columns + ') ROW FORMAT SERDE "org.openx.data.jsonserde.JsonSerDe" LOCATION \
                     "s3://willjeventdata/seatgeek/main data/" TBLPROPERTIES ("has_encrypted_data"="false")'
                     ),
        QueryExecutionContext={'Database': 'tickets_db'},
        ResultConfiguration={'OutputLocation': 's3://aws-athena-results-tickets-db/seatgeek/'}
    )


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
    artists_df = data_fetch_pymysql().head(3)['artist']

    """CURRENT DATE ASSIGNMENT"""
    current_date = datetime.now()

    """DEFINE DYNAMODB ENDPOINT"""
    dynamodb = boto3.resource('dynamodb')
    dynamoTable = dynamodb.Table('SeatGeek_Event_Table')

    """PULL BACK ALL SEATGEEK RECORDS FROM S3 BUCKET, FOR APPENDING LATER"""
    s3_client = boto3.client('s3')
    try:
        # s3_client = boto3.client('s3')
        bucket = 'willjeventdata'
        key = 'seatgeek_events.pkl'
        test_key = 'seatgeek_events_test.pkl'
        # key_temp = 'seatgeek_event/temp data/seatgeek_temp.pkl'
        test_key_temp = 'seatgeek_event/temp data/seatgeek_test.pkl'
        # key_json = 'seatgeek/main data/seatgeek_events.json'
        test_key_json = 'seatgeek/main data/seatgeek_test.json'
        response = s3_client.get_object(Bucket=bucket, Key=key)
        event_dict = (response['Body'].read())
        event_json = json.loads(event_dict.decode('utf8'))
        # master_event_df = pd.DataFrame.from_dict(event_json)
        print('The S3 JSON list started with ' + str(len(event_json)) + ' records')
        temp_df = pd.DataFrame()

        """DICT APPEND STAGING"""
        event_dict_decode = event_dict.decode('utf-8')
        event_dict_dict = json.loads(event_dict_decode)

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
                        event_name = xstr(event['title'])
                    # print(event_name)
                    except KeyError as noName:
                        event_name = ''

                    try:
                        event_id = xstr(event['id'])
                    # print(event_id)
                    except KeyError as noID:
                        event_id = ''

                    try:
                        event_date_utc = xstr(event['datetime_utc']).replace('T', ' ')
                        event_date_format = datetime.strptime((xstr(event['datetime_utc']).replace('T', ' ')[:10]), '%Y-%m-%d')
                    # print(event_date_utc)
                    except KeyError as noDatetime:
                        event_date_utc = ''

                    try:
                        event_venue = xstr(venue_data['name'])
                    # print(event_venue)
                    except KeyError as noVenue:
                        event_venue = ''

                    try:
                        event_capacity = xstr(venue_data['capacity'])
                    # print(event_capacity)
                    except KeyError as noCapacity:
                        event_capacity = ''

                    try:
                        event_city = xstr(venue_data['city'])
                    # print(event_city)
                    except KeyError as noCity:
                        event_city = ''

                    try:
                        event_state = xstr(venue_data['state'])
                    # print(event_state)
                    except KeyError as noState:
                        event_state = ''

                    try:
                        avg_price = xstr(price_data['average_price'])
                    # print(avg_price)
                    except KeyError as noAvg:
                        avg_price = ''

                    try:
                        med_price = xstr(price_data['median_price'])
                    # print(med_price)
                    except KeyError as noMed:
                        med_price = ''

                    try:
                        lowest_price = xstr(price_data['lowest_price'])
                    # print(lowest_price)
                    except KeyError as noLowest:
                        lowest_price = ''

                    try:
                        highest_price = xstr(price_data['highest_price'])
                    # print(highest_price)
                    except KeyError as noHighest:
                        highest_price = ''

                    try:
                        no_listings = xstr(price_data['listing_count'])
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
                            'median_price': price_dict['median_price'], 'average_price': price_dict['average_price'],
                            'listing_count': price_dict['listing_count']
                        }
                    )

                    """S3 NEW DATA CREATION"""
                    event_array = pd.DataFrame([[artist, event_name, event_id, event_venue, event_capacity,
                                                 event_city, event_state, event_date_format, lowest_price, highest_price,
                                                 avg_price, med_price, no_listings, current_date]],
                                               columns=['artist', 'name', 'id', 'venue', 'capacity', 'city',
                                                        'state', 'date_UTC', 'lowest_price', 'highest_price', 'average_price',
                                                        'median_price', 'listing_count', 'create_ts'])

                    temp_df = temp_df.append(event_array, ignore_index=True, sort=True)

<<<<<<< HEAD
=======

>>>>>>> 9c49e611334ed370fb801c90a81b837ae255bac2
            except IndexError as e:

                print('NO RELATED SEATGEEK EVENTS')

        """DICT APPEND METHOD"""
        s3_resource = boto3.resource('s3')
        
        """MAKE DICT FROM TEMP DATAFRAME"""
        temp_dict = temp_df.to_dict('records')

        """MERGE TEMP DICT AND MASTER DICT"""
        appended_dict = event_json + temp_dict
        
        """S3 FROM TEMP DICT"""
        temp_dict_stg = json.dumps(temp_dict, default = myconverter)
        # s3_resource.Object(bucket, key_temp).put(Body=temp_dict_stg)
        s3_resource.Object(bucket, test_key_temp).put(Body=temp_dict_stg)
        print('successfully stored the ' + str(len(temp_dict)) + ' records of new data')

        """S3 PKL FROM APPENDED DICT"""
        appended_dict_stg = json.dumps(appended_dict, default = myconverter)
        # s3_resource.Object(bucket, key).put(Body=appended_dict_stg) 
        s3_resource.Object(bucket, test_key).put(Body=appended_dict_stg)
        print('successfully overwrote the PKL file which now has ' + str(len(appended_dict_stg)) + ' records')

        """S3 JSON FROM APPENDED DICT"""
        appended_json = appended_dict_stg.replace('[{', '{').replace(']}', '}').replace('},', '}\n')
        # s3_resource.Object(bucket,key_json).put(Body=appended_json)
        s3_resource.Object(bucket,test_key_json).put(Body=appended_json)
        print('successfully overwrote main JSON file which now has ' + str(len(appended_dict_stg)) + ' records')

        """ATHENA CREATE MAIN TABLE"""
        columns_string = str(temp_df.columns.values).replace("['", "`").replace(" '", " `").replace("']", '` string').replace("' ", "` string, ").replace("'\n", "` string, ").replace("`date_UTC` string", "`date_UTC` timestamp").replace("`create_ts` string", "`create_ts` timestamp")
        print(columns_string)
        athena_drop()
        time.sleep(15)
        athena_create_main(columns_string)

    except s3_client.exceptions.NoSuchKey:

        print('THE S3 BUCKET SOMEHOW GOT DELETED...')


seatgeek_events()


"""PRINT TO LOG FOR MONITORING PURPOSES"""
current_date = datetime.now()
print('THIS PROGRAM FINISHED AT ' + str(current_date))
