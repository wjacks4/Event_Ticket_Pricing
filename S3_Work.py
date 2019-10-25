"""
FIGURE OUT HOW TO PICKLE FILES INTO S3

PURPOSE - TEST
"""

import json
import time
import urllib
import urllib.request
import pandas as pd
import unidecode
import requests
import urllib
import base64
import numpy as np
import re
import pymysql
import datetime
import pickle
import pprint
import boto3
import gzip

"""
GET ARTIST LIST FROM MYSQL DB
"""


def data_fetch_pymysql():
    connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                 user='tickets_user',
                                 password='tickets_pass',
                                 db='tickets_db')

    artists_df = pd.read_sql('SELECT * FROM ARTISTS_WITH_EVENTS order by current_followers desc', con=connection)
    return artists_df


# data_fetch_pymysql()


"""
TEST LOCAL PICKLING
"""


def test_local():
    events_dict = dict()

    event_json = data_fetch_pymysql().to_json(orient='records')

    print(event_json)

    with open('C:/Users/wjack/Documents/test3.pickle', 'wb') as handle:
        pickle.dump(event_json, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open('C:/Users/wjack/Documents/test3.pickle', 'rb') as handle:
        test_import = pickle.load(handle)


#test_local()


"""
TEST S3 PICKLING
"""


def test_s3():

    event_json = data_fetch_pymysql().to_json(orient='records')

    print(event_json)

    s3_resource = boto3.resource('s3')

    bucket='willjeventdata'
    key='test.pkl'
    s3_resource.Object(bucket,key).put(Body=event_json)

#test_s3()



"""
TEST PULLING DATA FROM EXISTING MYSQL INTO PANDAS, TURNING INTO JSON, THEN PICKLING IN S3
"""

def create_eventbrite_s3():

    connection = connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                              user='tickets_user', password='tickets_pass', db='tickets_db')

    eventbrite_df = pd.read_sql('SELECT * FROM EVENTBRITE_EVENTS', con=connection)

    print((eventbrite_df).head(20))

    eventbrite_json = eventbrite_df.to_json(orient='records')

    s3_resource = boto3.resource('s3')

    bucket='willjeventdata'
    key='eventbrite_events.pkl'
    s3_resource.Object(bucket,key).put(Body=eventbrite_json)

# create_eventbrite_s3()

def create_stubhub_s3():

    connection = connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                              user='tickets_user', password='tickets_pass', db='tickets_db')

    stubhub_df = pd.read_sql('SELECT * FROM STUBHUB_EVENTS LIMIT 1000', con=connection)

    print((stubhub_df).head(20))

    stubhub_json = stubhub_df.to_json(orient='records')

    s3_resource = boto3.resource('s3')

    bucket='willjeventdata'
    key='stubhub_events.pkl'
    s3_resource.Object(bucket,key).put(Body=stubhub_json)

# create_stubhub_s3()

def create_ticketmaster_s3():

    connection = connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                              user='tickets_user', password='tickets_pass', db='tickets_db')

    ticketmaster_df = pd.read_sql('SELECT * FROM TICKETMASTER_EVENTS WHERE date_UTC != "0000-00-00 00:00:00" and date_UTC != "" and date_UTC is not null', con=connection)
    print('df pulled locally')
    ticketmaster_json = ticketmaster_df.to_json(orient='records')
    print('df has been turned to json')
    ticketmaster_json_stg = ticketmaster_json.replace('[{', '{').replace(']}', '}').replace('},', '}\n')
    print('json reformatted')
    s3_resource = boto3.resource('s3')

    bucket='willjeventdata'
    key='ticketmaster_events.pkl'
    key_json = 'ticketmaster/main data/ticketmaster_events.json'


    s3_resource.Object(bucket,key).put(Body=ticketmaster_json)
    s3_resource.Object(bucket,key_json).put(Body=ticketmaster_json_stg)
    print('json loaded into s3')



# create_ticketmaster_s3()
        
        
def create_seatgeek_s3():

    connection = connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                              user='tickets_user', password='tickets_pass', db='tickets_db')

    seatgeek_df = pd.read_sql('SELECT * FROM SEATGEEK_EVENTS', con=connection)

    print((seatgeek_df).head(20))

    seatgeek_json = seatgeek_df.to_json(orient='records')

    s3_resource = boto3.resource('s3')

    bucket='willjeventdata'
    key='seatgeek_events.pkl'
    s3_resource.Object(bucket,key).put(Body=seatgeek_json)

# create_seatgeek_s3()




"""
TEST PULLING BACK PICKLED DATA
"""

def pickle_pull():
    
    
    s3_client = boto3.client('s3')
    bucket = 'willjeventdata'
    key = 'eventbrite_events.pkl'
    response = s3_client.get_object(Bucket=bucket, Key=key)
    event_dict = (response['Body'].read())
    event_json = json.loads(event_dict.decode('utf8'))
    master_event_df = pd.DataFrame.from_dict(event_json)
    print('The S3 JSON list now has ' + str(len(master_event_df)) + ' records')
    
    print(master_event_df.head(10))

# pickle_pull()
    
    
    
"""
TEST TURNING ATHENA TABLE INTO JSON IN S3
"""

session = boto3.Session()
client = session.client('athena', region_name = params["region"])

params = {
    'region': 'us-west-2',
    'database': 'tickets_db',
    'bucket': 'willjeventdata',
    'path': '/stubhub/staged data/daily_lowest',
    'query': ("CREATE TABLE TEST WITH ( format = 'JSON', external_location = 's3://willjeventdata/stubhub/staged data/daily_lowest/') AS SELECT * FROM stubhub_daily_lowest LIMIT 100")
    }

def athena_query(client, params):
    
    response = client.start_query_execution(
            QueryString=params['query'],
            QueryExecutionContext={
                'Database': params['database']
            }
            ,
            ResultConfiguration={
                'OutputLocation': 's3://' + params['bucket'] + params['path']
            }
    )
    return response


def athena_to_s3(session, params):
    client = session.client('athena', region_name = params["region"])
    execution = athena_query(client, params)
    execution_id = execution["QueryExecutionId"]
    state = 'RUNNING'
    
    while (state in ['RUNNING']):
        response = client.get_query_execution(QueryExecutionId = execution_id)
        
        if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            
            if state =='FAILED':
                return False
            elif state=='SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                filename = re.findall('.*./(.*)', s3_path)[0]
                return filename
            
        time.sleep(1)
        
        
def cleanup(session, params):
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(params['bucket'])
    for item in my_bucket.objects.filter(Prefix=params['path']):
        item.delete()

# cleanup(session, params)

# athena_query(client, params)




