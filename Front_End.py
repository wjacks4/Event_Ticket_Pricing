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


"""IMPORT PICKLE FROM S3, SAVE AS LARGE JSON OBJ"""


def pickle_pull():
    s3_client = boto3.client('s3')
    bucket = 'willjeventdata'
    key = 'seatgeek_events.pkl'
    response = s3_client.get_object(Bucket=bucket, Key=key)
    event_dict = (response['Body'].read())
    event_json = json.loads(event_dict.decode('utf8'))
    master_event_df = pd.DataFrame.from_dict(event_json)
    print('The S3 JSON list now has ' + str(len(master_event_df)) + ' records')

    new_event_json = master_event_df.to_json(orient='records')
    s3_resource = boto3.resource('s3')
    key2 = 'seatgeek_events2.json'
    s3_resource.Object(bucket, key2).put(Body=new_event_json)


pickle_pull()


