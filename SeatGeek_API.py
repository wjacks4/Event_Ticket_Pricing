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

def data_fetch_pymysql():
    connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
                                 user='tickets_user',
                                 password='tickets_pass',
                                 db='tickets_db')

    artists_df = pd.read_sql('SELECT * FROM ARTISTS_WITH_EVENTS order by current_followers desc', con=connection)
    return artists_df


# data_fetch_pymysql()


"""
GET EVENTS FROM SEATGEEK API, LOAD TO MYSQL DB
"""


def seatgeek_events():
	global dict
	artists_df = data_fetch_pymysql().head(200)['artist']

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
				venue_data = event['venue']
				price_data = event['stats']

				try:
					event_name = event['title']
					# print(event_name)
				except KeyError as noName:
					event_name = ''

				try:
					event_id = event['id']
					# print(event_id)
				except KeyError as noID:
					event_id = ''

				try:
					event_date_utc = event['datetime_utc']
					# print(event_date_utc)
				except KeyError as noDatetime:
					event_date_utc = ''

				try:
					event_venue = venue_data['name']
					# print(event_venue)
				except KeyError as noVenue:
					event_venue = ''

				try:
					event_capacity = venue_data['capacity']
					# print(event_capacity)
				except KeyError as noCapacity:
					event_capacity = ''

				try:
					event_city = venue_data['city']
					# print(event_city)
				except KeyError as noCity:
					event_city = ''

				try:
					event_state = venue_data['state']
					# print(event_state)
				except KeyError as noState:
					event_state = ''

				try:
					avg_price = price_data['average_price']
					# print(avg_price)
				except KeyError as noAvg:
					avg_price = ''

				try:
					med_price = price_data['median_price']
					# print(med_price)
				except KeyError as noMed:
					med_price = ''

				try:
					lowest_price = price_data['lowest_price']
					# print(lowest_price)
				except KeyError as noLowest:
					lowest_price = ''

				try:
					highest_price = price_data['highest_price']
					# print(highest_price)
				except KeyError as noHighest:
					highest_price = ''

				try:
					no_listings = price_data['listing_count']
				# print(no_listings)
				except KeyError as noListingCount:
					no_listings = ''

				"""
				event_array = pd.DataFrame([[artist, event_name, event_id, event_venue,
											 event_city, event_state, event_date_utc, lowest_price, highest_price,
											 avg_price, med_price, no_listings, current_date]],
										   columns=['artist', 'name', 'ID', 'venue', 'city',
													'state', 'date_UTC', 'lowest_price', 'highest_price', 'avg_price',
													'med_price', 'listing_count', 'create_ts'])
				"""

				insert_tuple = (artist, event_name, event_id, event_venue, event_capacity, event_city, event_state,
				event_date_utc, lowest_price, highest_price, avg_price, med_price, no_listings, current_date)

				event_QL = 'INSERT INTO `SEATGEEK_EVENTS` (`artist`, `name`, `id`, `venue`, `capacity`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `average_price`, `median_price`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

				# ----------CONNECT TO DB AND SUBMIT SQL QUERY------------#
				connection = connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
														  user='tickets_user', password='tickets_pass', db='tickets_db')
				cursor = connection.cursor()

				result = cursor.execute(event_QL, insert_tuple)
				connection.commit()

		except IndexError as e:

			print('NO RELATED SEATGEEK EVENTS')

seatgeek_events()

