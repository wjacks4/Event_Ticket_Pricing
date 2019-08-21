"""
FIGURE OUT HOW TO PICKLE FILES INTO S3

PURPOSE - TEST
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
import re
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

    eventbrite_df = pd.read_sql('SELECT * FROM EVENTBRITE_EVENTS LIMIT 1000', con=connection)

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

    ticketmaster_df = pd.read_sql('SELECT * FROM TICKETMASTER_EVENTS LIMIT 1000', con=connection)

    print((ticketmaster_df).head(20))

    ticketmaster_json = ticketmaster_df.to_json(orient='records')

    s3_resource = boto3.resource('s3')

    bucket='willjeventdata'
    key='ticketmaster_events.pkl'
    s3_resource.Object(bucket,key).put(Body=ticketmaster_json)

# create_ticketmaster_s3()

def create_seatgeek_s3():

    connection = connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                              user='tickets_user', password='tickets_pass', db='tickets_db')

    seatgeek_df = pd.read_sql('SELECT * FROM SEATGEEK_EVENTS LIMIT 1000', con=connection)

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
    key = 'seatgeek_events.pkl'
    response = s3_client.get_object(Bucket=bucket, Key=key)
    event_dict = (response['Body'].read())
    event_json = json.loads(event_dict.decode('utf8'))
    master_event_df = pd.DataFrame.from_dict(event_json)
    print('The S3 JSON list now has ' + str(len(master_event_df)) + ' records')

pickle_pull()







