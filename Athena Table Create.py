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
        self.events_table = str(source+'_events')
        self.daily_lowest = str(source + '_daily_lowest')

    def athena_seatgeek_clean(self):
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
            
    def athena_drop_main(self):
        athena_client = boto3.client('athena')
        response = athena_client.start_query_execution(
            QueryString = ('drop table ' + self.daily_lowest),
            QueryExecutionContext ={'Database':'tickets_db'},
            ResultConfiguration={'OutputLocation':'s3://aws-athena-results-tickets-db/' + self.source_str + '/'}
        )            

    def athena_daily_lowest(self):
        qstring = ('create table ' + self.daily_lowest + ' as select distinct artist,'
               'name, '
               'venue, ' 
               'city, '
               'state, '
               'date_UTC, '
               'max(lowest_price) as lowest_price_day, create_date, '
               'from ('
               'select *, date(create_ts) as create_date '
               'from ' + self.events_table + ' order by create_ts desc'
               ') '
               'group by artist, name, venue, city, state, date_UTC, create_date;'
               )
        print(qstring)
        athena_client = boto3.client('athena')
        response = athena_client.start_query_execution(
            QueryString = ('create table ' + self.daily_lowest + ' as select distinct artist,'
                       'name, '
                       'venue, ' 
                       'city, '
                       'state, '
                       'date_UTC, '
                       'max(lowest_price) as lowest_price_day, create_date '
                       'from ('
                       'select *, date(create_ts) as create_date '
                       'from ' + self.events_table + ' order by create_ts desc'
                       ') '
                       'group by artist, name, venue, city, state, date_UTC, create_date;'
            ),
            QueryExecutionContext={'Database':'tickets_db'},
            ResultConfiguration={'OutputLocation': 's3://aws-athena-results-tickets-tb/' + self.source_str + '/'}
        )
                           
   
seatgeek_athena = athena_table_creator('seatgeek')

seatgeek_athena.athena_drop_main()
seatgeek_athena.athena_daily_lowest()

       
       