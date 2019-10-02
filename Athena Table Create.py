"""
START DOING....ANALYSIS???
"""
import pandas as pd
import numpy as np
import json
import requests
import boto3

"""IMPORT PICKLE FROM S3, SAVE AS LARGE JSON OBJ"""

class athena_table_creator:

    def __init__(self, source):
        self.bucket_pkl_string = str(source + '_events.pkl')
        self.new_json_string = str(source + '_events.json')
        self.source_str = str(source)
        self.table = str(source+'_events2')

    def athena_seatgeek_clean(self, day_columns):
        athena_client = boto3.client('athena')
        response = athena_client.start_query_execution(
            QueryString=('create table seatgeek_cleaned as select distinct name'
                         ', artist, city'
                         ', state'
                         ', venue'
                         ', date_utc'
                         ', capacity'
                         ', coalesce((case when LENGTH(lowest_price) < 1 then "0" else lowest_price end), "0") as lowest_price'
                         ', median_price'
                         ', highest_price'
                         ', create_ts'
                         ' from seatgeek_events;'
            ),
            QueryExecutionContext={'Database': 'tickets_db'},
            ResultConfiguration={'OutputLocation': 's3://aws-athena-results-tickets-db/seatgeek/'}
        )
        
        
        




