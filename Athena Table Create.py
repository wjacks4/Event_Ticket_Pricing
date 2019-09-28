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

class s3_table_creator:

    def __init__(self, source):
        self.bucket_pkl_string = str(source + '_events.pkl')
        self.new_json_string = str(source + '_events.json')
        self.source_str = str(source)
        self.table = str(source+'_events2')
    
    def pickle_pull(self):
        s3_client = boto3.client('s3')
        bucket = 'willjeventdata'
        key = self.bucket_pkl_string
        response = s3_client.get_object(Bucket=bucket, Key=key)
        print('got response')
        event_dict = (response['Body'].read())
        print('read response to dict')
        event_json = json.loads(event_dict.decode('utf8'))
        print('decoded response')
        master_event_df = (pd.DataFrame.from_dict(event_json)).head(10)
        print('got df')
        test_df = master_event_df.head(10)
        columns_string = str(test_df.columns.values).replace("['", "`").replace(" '", " `").replace("']", '` string').replace("' ", "` string, ").replace("'\n", "` string, ").replace("`date_UTC` string", "`date_UTC` timestamp").replace("`create_ts` string", "`create_ts` timestamp")
        print(columns_string)
        return(columns_string)
    
    def json_put(self, input_df):
        bucket='willjeventdata'
        new_event_json = input_df.to_json(orient='records')
        print(new_event_json)
    
        json_reform = new_event_json.replace('[{', '{').replace(']}', '}').replace('},', '}\n')
        print(json_reform)
    
        s3_resource = boto3.resource('s3')
        key2 = self.new_json_string
        s3_resource.Object(bucket, key2).put(Body=json_reform)


    def athena_drop(self):
        athena_client = boto3.client('athena')
        response = athena_client.start_query_execution(
        QueryString = ('drop table ' + self.table),
        QueryExecutionContext ={'Database':'tickets_db'},
        ResultConfiguration={'OutputLocation':'s3://aws-athena-results-tickets-db/stubhub/'})
        
        
    def athena_create(self, main_columns):
        querystring = str(('create external table if not exists ' + self.table +
                ' (' + main_columns + ') ROW FORMAT SERDE "org.openx.data.jsonserde.JsonSerDe" LOCATION "s3://willjeventdata/'
                + self.source_str + '/" TBLPROPERTIES ("has_encrypted_data"="false")'))
        print(querystring)
        athena_client = boto3.client('athena')
        response = athena_client.start_query_execution(
        QueryString = ('create external table if not exists ' + self.table + 
                ' (' + main_columns + ') ROW FORMAT SERDE "org.openx.data.jsonserde.JsonSerDe" LOCATION "s3://willjeventdata/'
                + self.source_str + '/" TBLPROPERTIES ("has_encrypted_data"="false")'),
        QueryExecutionContext ={'Database':'tickets_db'},
        ResultConfiguration={'OutputLocation':'s3://aws-athena-results-tickets-db/stubhub/'})


    def athena_daily_max_create(self, day_columns):
        athena_client = boto3.client('athena')
        # response = athena_client.start_query_execution()
        
        
        
    


#seatgeek_translate = pkl_to_json('seatgeek')
#seatgeek_translate.json_put(seatgeek_translate.pickle_pull())

#ticketmaster_translate = pkl_to_json('ticketmaster')
#ticketmaster_translate.json_put(ticketmaster_translate.pickle_pull())

#stubhub_translate = pkl_to_json('stubhub')
#stubhub_translate.json_put(stubhub_translate.pickle_pull())

#eventbrite_translate = pkl_to_json('eventbrite')
#eventbrite_translate.json_put(eventbrite_translate.pickle_pull()
        

stubhub_athena = s3_table_creator('stubhub')
# stubhub_athena.pickle_pull()
# stubhub_athena.athena_drop()
stubhub_athena.athena_create(stubhub_athena.pickle_pull())






