'''
SEATGEEK API DATA PULL

PURPOSE - TEST
'''

#!/usr/bin/env python3

#import mysql
#from mysql.connector import Error
#import psycopg2 as p
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
#import mysql-python
import pymysql
#import MySQLdb
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

base_url = ('https://api.seatgeek.com/2/')

client_id_str = ('MTM4MTIyMDZ8MTU1NDQ3MTkxMy43Ng')

client_secret_str = ('c49766eaad2bc8bc33810d112d141ca9a09b0a78b1be52c459eb19c5fd3527a5')



#----------------------------------------------------------------------#
#--------------------------DYNAMODB SETUP------------------------------#
#----------------------------------------------------------------------#

dynamodb = boto3.resource('dynamodb')
dynamoTable = dynamodb.Table('Event_Table')


#----------------------------------------------------------------------#
#---------------------GET ARTIST LIST FROM MYSQL DB--------------------#
#----------------------------------------------------------------------#

def Data_Fetch_pymysql():

    #Fetch_QL = 'SELECT * FROM ARTISTS_ONLY;'
    
    #USING pymysql#
    connection = pymysql.connect (host = 'ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                  user = 'tickets_user',
                                  password = 'tickets_pass',
                                  db = 'tickets_db')
    
    Fetch_QL = 'SELECT * FROM Artists_expanded;'
    cursor = connection.cursor()
    Artists_DF = pd.read_sql('SELECT * FROM ARTISTS_WITH_EVENTS order by current_followers desc', con = connection)  
    return Artists_DF

Data_Fetch_pymysql()


#----------------------------------------------------------------------#
#--------------------------DEFINE EVENT DICT---------------------------#
#----------------------------------------------------------------------#


#events_dict = dict()

def dynamo_append():


	with open('C:/Users/wjack/Documents/test2.pickle', 'rb') as handle:
		events_dict = pickle.load(handle)

	#event = events_dict['AtreyuDowntown Las Vegas Events CenterLas VegasNV2019-10-18T10:30:00']

	#json_test = json.loads(event)

	#print(json_test[0])


	dynamoTable = dynamodb.Table('Event_Table')

	for event in events_dict:

		event_dat = json.loads(events_dict[event])[0]
		#pprint(event_dat)
		
		artist = event_dat['artist']
		city = event_dat['city']
		date_UTC = event_dat['date_UTC']
		name = event_dat['name']
		state = event_dat['state']
		venue = event_dat['venue']
		
		event_data = {'name':name, 'artist':artist, 'city':city, 'date_UTC':date_UTC, 'state':state, 'venue':venue}
		#pprint(event_data)
		
		create_ts = event_dat['create_ts']
		highest_price = event_dat['highest_price']
		listing_count = event_dat['listing_count']
		lowest_price = event_dat['lowest_price']
		med_price = event_dat['med_price']
		
		price_data = [{'create_ts':create_ts, 'lowest_price':lowest_price, 'highest_price':highest_price, 'med_price':med_price, 'listing_count':listing_count}]
		#price_data2 = [{'create_ts':1234, 'lowest_price':124, 'highest_price':124, 'med_price':124, 'listing_count':124}]
		#price_data = {'create_ts':create_ts}
		
		#dynamoTable.put_item(
			
		#	Item = {
		#		'Event_ID':event,
		#		'Event_name':name,
		#		'Event_data':event_data,
		#		'Ticket_prices':price_data
		#	}
		#)

		print(event)
		#dynamoTable.update_item(
		#
		#	Key={
		#		'Event_ID':event,
		#	},
		#	UpdateExpression="SET Event_name = :r",
		#	ExpressionAttributeValues={
		#		':r': "WHAT THE FUCK"
		#	}
		#)
		
		dynamoTable.update_item(
		
			Key={
				'Event_ID':event
			},
			UpdateExpression= "SET Ticket_prices = list_append(Ticket_prices, :vals)",
			ExpressionAttributeValues={
				':vals': price_data
			}
		)
			
		
#dynamo()



def dynamo_digest():

	dynamoTable = dynamodb.Table('Event_Table')
	
	with open('C:/Users/wjack/Documents/test2.pickle', 'rb') as handle:
		events_dict = pickle.load(handle)	
	
	
	
	for event in events_dict:
	
		print(event)
		
		event_json = (dynamoTable.get_item(
			Key={
				'Event_ID':event
			}
		))['Item']
		
		#print(event_json)
		
		print(event_json['Event_ID'])
		print(event_json['Event_name'])
		print(event_json['Event_data'])
		print(event_json['Ticket_prices'])
		
#dynamo_digest()


"""
TEST 
"""


def seatgeek_events():
	global dict
	artists_df = data_fetch_pymysql().head(1)['artist']
	# Artists_df = Data_Fetch_pymysql().head(200)

	"""CURRENT DATE ASSIGNMENT"""
	current_date = datetime.now()

	"""INCREMENTING VARIABLE"""
	i = 1

	"""BEGIN BUILDING DATAFRAME FROM JSON"""
	event_df = pd.DataFrame()

	"""LOOP THRU ARTISTS"""
	# for artist in artists:
	for artist in artists_df:

		"""DEFINE PERFORMER SLUG VARIABLE"""
		performer_slug_stg = artist.replace("&", "")
		performer_slug = (performer_slug_stg.replace("  ", " ")).replace(" ", "-")

		"""TRY CONNECTION TO SEATGEEK API"""

		try:

			url = 'https://api.seatgeek.com/2/events?format=json'
			payload = {'per_page': 1,
					   'performers.slug': performer_slug,
					   'client_id': client_id_str,
					   }
			r = requests.get(url, params=payload, verify=True)

			print(r.url)

			json_obj = json.loads(r.text)

			# print(json_obj)

			event_list = json_obj['events']

			for event in event_list:
				summ_data = event
				venue_data = summ_data['venue']
				price_data = summ_data['stats']

				event_dict = {'name': [summ_data['title']], 'id': [summ_data['id']],
							  'datetime_utc': [summ_data['datetime_utc']], 'venue': [venue_data['name']],
							  'city': [venue_data['city']], 'state': [venue_data['state']],
							  'avg_price': [price_data['average_price']], 'median_price': [price_data['median_price']],
							  'lowest_price': [price_data['lowest_price']],
							  'highest_price': [price_data['highest_price']],
							  'no_listing': [price_data['listing_count']], 'create_ts': [current_date]}
				print(event_dict)

				test_df = pd.DataFrame.from_dict(event_dict)

				test_json = test_df.to_json(orient='records')

				s3_resource = boto3.resource('s3')

				bucket = 'willjeventdata'
				key = 'SeatGeek_Events.pkl'
				s3_resource.Object(bucket, key).put(Body=test_json)

				"""

                try:
                    event_name = event['title']
                    # print(event_name)
                except KeyError as noName:
                    event_name = ''

                try:
                    # event_id = json_obj['events'][0]['id']
                    event_id = summ_data['id']
                    print(event_id)
                except KeyError as noID:
                    event_id = ''

                try:
                    event_date_utc = json_obj['events'][0]['datetime_utc']
                    # print(event_date_utc)
                except KeyError as noDatetime:
                    event_date_UTC = ''

                try:
                    # event_venue = json_obj['events'][0]['venue']['name']
                    event_venue = venue_data['name']
                    print(event_venue)
                except KeyError as noVenue:
                    event_venue = ''

                try:
                    event_city = json_obj['events'][0]['venue']['city']
                # print(event_city)
                except KeyError as noCity:
                    event_city = ''

                try:
                    event_state = json_obj['events'][0]['venue']['state']
                # print(event_state)
                except KeyError as noState:
                    event_state = ''

                event_stats = json_obj['events'][0]['stats']
                # print(event_stats)

                try:
                    avg_price = json_obj['events'][0]['stats']['average_price']
                # print(avg_price)
                except KeyError as noAvg:
                    avg_price = ''

                try:
                    med_price = json_obj['events'][0]['stats']['median_price']
                # print(med_price)
                except KeyError as noMed:
                    med_price = ''

                try:
                    lowest_price = json_obj['events'][0]['stats']['lowest_price']
                # print(lowest_price)
                except KeyError as noLowest:
                    lowest_price = ''

                try:
                    highest_price = json_obj['events'][0]['stats']['highest_price']
                # print(highest_price)
                except KeyError as noHighest:
                    highest_price = ''

                try:
                    no_listings = json_obj['events'][0]['stats']['listing_count']
                # print(no_listings)
                except KeyError as noListingCount:
                    no_listings = ''



                event_key = spotify_artist + event_venue + event_city + event_state + event_date_UTC

                event_array = pd.DataFrame([['TEST', spotify_artist, spotify_artist_id, event_name, event_id, event_venue,
                                             event_city, event_state, event_date_UTC, lowest_price, highest_price,
                                             avg_price, med_price, no_listings, current_Date]],
                                           columns=['Event_ID', 'artist', 'artist_id', 'name', 'ID', 'venue', 'city',
                                                    'state', 'date_UTC', 'lowest_price', 'highest_price', 'avg_price',
                                                    'med_price', 'listing_count', 'create_ts'])

                event_json = event_array.to_json(orient='records')

                list2 = (spotify_artist, spotify_artist_id, current_Date)

                if event_key in events_dict:

                    prev_event_list = events_dict[event_key]
                    appended_list = prev_event_list.append(event_json)
                    events_dict[event_key] = appended_list

                # -------------THE PANDAS WAY TO DO IT----------------#
                # prev_event_df = events_dict[event_key]
                # appended_df = prev_event_df.append(event_array)
                # events_dict[event_key] = appended_df

                else:

                    events_dict[event_key] = event_json

                # -----------THE PANDAS WAY TO DO IT------------#
                # events_dict[event_key] = event_array

                print(events_dict)

                # -------APPEND EACH EVENT TO MASTER DATAFRAME...NOT SURE IF I STILL NEED THIS------#
                event_df = event_df.append(event_array)
            """

		except IndexError as e:

			print('NO RELATED SEATGEEK EVENTS')

	"""
    print(event_df)

    with open('C:/Users/wjack/Documents/test2.pickle', 'wb') as handle:
        pickle.dump(events_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

    """

# seatgeek_events()

