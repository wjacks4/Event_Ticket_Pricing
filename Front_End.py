"""
START DOING....ANALYSIS???
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

"""IMPORT PICKLE FROM S3, SAVE AS LARGE JSON OBJ"""


# def tm_pull():
#     s3_client = boto3.client('s3')
#     bucket = 'willjeventdata'
#     key = 'ticketmaster_events.pkl'
#     response = s3_client.get_object(Bucket=bucket, Key=key)
#     event_dict = (response['Body'].read())
#     event_json = json.loads(event_dict.decode('utf8'))
#     master_event_df = pd.DataFrame.from_dict(event_json)
#     return master_event_df
#
# tm_pull()

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

class event_df:

    def __init__(self, bucket, key):
        response = s3_client.get_object(Bucket=bucket, Key=key)
        event_dict = response['Body'].read()
        event_json = json.loads(event_dict.decode('utf8'))
        self.master_event_df = pd.DataFrame.from_dict(event_json)


# tm_df = event_df('willjeventdata', 'ticketmaster_events.pkl')
# print(tm_df.master_event_df.head(10))
# sg_df = event_df('willjeventdata', 'seatgeek_events.pkl')
# print(sg_df.master_event_df.head(10))
# sh_df = event_df('willjeventdata', 'stubhub_events.pkl')
# print(sh_df.master_event_df.head(10))
# eb_df = event_df('willjeventdata', 'eventbrite_events.pkl')
# print(eb_df.master_event_df.head(10))


# print(tm_df.master_event_df.columns)
print(sg_df.master_event_df.columns)
columns_string = str(sg_df.master_event_df.columns.values).replace('[', '').replace(']', '').replace("' ", "',")
print(columns_string)

# print(sh_df.master_event_df.columns)
# print(eb_df.master_event_df.columns)

# tm_df.columns


#test_tm = tm_df.master_event_df.head(10)
#test_sg = sg_df.master_event_df.head(10)
#test_sh = sh_df.master_event_df.head(10)
#test_eb = eb_df.master_event_df.head(10)

#test_append = test_tm.append([test_sg, test_sh, test_eb])
#print(test_append.columns)

#test_tm.columns=['artist', 'artist_id', 'name', 'city', 'create_ts', 'date_UTC', 'highest_price', 'event_id', 'lowest_price', 'highest_price']







