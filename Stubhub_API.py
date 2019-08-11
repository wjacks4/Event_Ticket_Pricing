"""

STUBHUB API DATA PULL


"""

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


import boto3


current_Date = datetime.now()
print('THIS PROGRAM RAN AT ' + str(current_Date))

"""
DEFINE STUBHUB ACCESS KEYS / CODES
"""

Stubhub_Key_1 = b'VOU4xvGfhGO9qpVxGo3SABeebnpTmAJw'
Stubhub_Secret_1 = b'RR2tFwHG7Pinv4ik'


Cat_Key_Secret_1 = (Stubhub_Key_1 + b":" + Stubhub_Secret_1)
Cat_Key_encode_1 = base64.standard_b64encode(Cat_Key_Secret_1)

print(Cat_Key_encode_1)


Stubhub_Key_2 = b'd9fWHtQvs34cAebdfAzDTCOf6DLn9Nm7'
Stubhub_Secret_2 = b'11UA5vKSQuZzjb4m'

Cat_Key_Secret_2 = (Stubhub_Key_2 + b":" + Stubhub_Secret_2)
Cat_Key_encode_2 = base64.standard_b64encode(Cat_Key_Secret_2)

print(Cat_Key_encode_2)


Stubhub_Key_3 = b'odHqRlEjZuudOptPDEcf1ojiauRstJ9C'
Stubhub_Secret_3 = b'H978jHMGfzFmtGuv'

Cat_Key_Secret_3 = (Stubhub_Key_3 + b":" + Stubhub_Secret_3)
Cat_Key_encode_3 = base64.standard_b64encode(Cat_Key_Secret_3)

print(Cat_Key_encode_3)

Stubhub_Key_4 = b'j0eN22SApF63czY3zcjv7wX5SED96FRF'
Stubhub_Secret_4 = b'9ugyJJ7pGAGouRJA'

Cat_Key_Secret_4 = (Stubhub_Key_4 + b":" + Stubhub_Secret_4)
Cat_Key_encode_4 = base64.standard_b64encode(Cat_Key_Secret_4)

print(Cat_Key_encode_4)

Stubhub_Key_5 = b'VhDtFC2UE8oQtBpYLmhWhz931FRPfjsn'
Stubhub_Secret_5 = b'y2QjurJH2nmcKNt4'

Cat_Key_Secret_5 = (Stubhub_Key_5 + b":" + Stubhub_Secret_5)
Cat_Key_encode_5 = base64.standard_b64encode(Cat_Key_Secret_5)

print(Cat_Key_encode_5)


"""
GET ARTIST LIST FROM SYNTHESIZED STUBHUB / SPOTIFY TABLE THAT TARGETS ARTISTS W/ EVENTS
"""


def data_fetch_pymysql():

	connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com',
										user='tickets_user',
										password='tickets_pass',
										db='tickets_db')
	cursor = connection.cursor()
	artists_df = pd.read_sql('SELECT * FROM ARTISTS_WITH_EVENTS order by current_followers desc', con=connection)
	return artists_df


data_fetch_pymysql()


def get_access_token_1():

	base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
	query_params = 'grant_type=client_credentials'

	request_url = (base_url + "?" + query_params)

	payload = {"username": "wjacks4@g.clemson.edu", "password": "Hester3123"}
	headers = {"Authorization": "Basic Vk9VNHh2R2ZoR085cXBWeEdvM1NBQmVlYm5wVG1BSnc6UlIydEZ3SEc3UGludjRpaw==",
												"Content-Type": "application/json"}

	req = requests.post(request_url, data=json.dumps(payload), headers=headers)
	json_obj = req.json()
	token = json_obj['access_token']

	print(token)
	return token


def get_access_token_2():

	base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
	query_params = 'grant_type=client_credentials'

	request_url = (base_url + "?" + query_params)

	payload = {"username": "hiltonsounds@gmail.com", "password": "Hester3123"}
	headers = {"Authorization": "Basic ZDlmV0h0UXZzMzRjQWViZGZBekRUQ09mNkRMbjlObTc6MTFVQTV2S1NRdVp6amI0bQ==",
												"Content-Type": "application/json"}

	req = requests.post(request_url, data=json.dumps(payload), headers=headers)
	json_obj = req.json()
	token = json_obj['access_token']

	print(token)
	return token


def get_access_token_3():

	base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
	query_params = 'grant_type=client_credentials'

	request_url = (base_url + "?" + query_params)

	payload = {"username":"edenk@g.clemson.edu", "password":"Hester3123"}
	headers = {"Authorization": "Basic ZDlmV0h0UXZzMzRjQWViZGZBekRUQ09mNkRMbjlObTc6MTFVQTV2S1NRdVp6amI0bQ==",
												"Content-Type": "application/json"}

	req = requests.post(request_url, data=json.dumps(payload), headers=headers)
	json_obj = req.json()
	token = json_obj['access_token']

	print(token)
	return token


def get_access_token_4():

	base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
	query_params = 'grant_type=client_credentials'

	request_url = (base_url + "?" + query_params)

	payload = {"username": "butteredtoast66@gmail.com", "password": "Hester3123"}
	headers = {"Authorization": "Basic ajBlTjIyU0FwRjYzY3pZM3pjanY3d1g1U0VEOTZGUkY6OXVneUpKN3BHQUdvdVJKQQ==",
												"Content-Type": "application/json"}

	req = requests.post(request_url, data=json.dumps(payload), headers=headers)
	json_obj = req.json()
	token = json_obj['access_token']

	print(token)
	return token


def get_access_token_5():

	base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
	query_params = 'grant_type=client_credentials'

	request_url = (base_url + "?" + query_params)

	payload = {"username": "sunglassman3123@gmail.com", "password": "Hester3123"}
	headers = {"Authorization": "Basic VmhEdEZDMlVFOG9RdEJwWUxtaFdoejkzMUZSUGZqc246eTJRanVySkgybm1jS050NA==",
												"Content-Type": "application/json"}

	req = requests.post(request_url, data=json.dumps(payload), headers=headers)
	json_obj = req.json()
	print(json_obj)
	token = json_obj['access_token']

	print(token)
	return token


def stubhub_event_pull():

	connection = pymysql.connect('ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', 'tickets_user', 'tickets_pass', 'tickets_db')
	cursor = connection.cursor()

	artists_df = data_fetch_pymysql().head(1)['artist']

	base_url = 'https://api.stubhub.com/sellers/search/events/v3'

	current_date = datetime.now()
	
	i = 1

	"""
	AND THE DYNAMODB WAY TO STORE DATA
	"""
	dynamodb = boto3.resource('dynamodb')
	dynamotable = dynamodb.Table('Stubhub_Event_Table')

	"""
	LOOP THRU ARTISTS
	"""
	for artist in artists_df:

		artist_encode = artist.replace(" ", "%20")

		query_params = ("q=" + artist_encode + "&" + "rows=100")		

		artist_url = (base_url + "?" + query_params)
		
		if i <= 40:
		
			print(i)
			try:
			
				auth_header = ("Bearer " + get_access_token_1())
				headers = {"Authorization": auth_header, "Accept": "application/json"}
				req = requests.get(artist_url, headers=headers)
				json_obj = req.json()

				# print(json_obj)
							
				event_list = json_obj['events']

				for event in event_list:
					
					event_name = event['name']
					
					if 'PARKING' not in event_name:

						"""
						MYSQL WAY
						"""
						print(event_name)
						event_id = str(event['id'])
						event_venue = event['venue']['name']
						event_city = event['venue']['city']
						event_state = event['venue']['state']
						event_date_str = (event['eventDateUTC']).replace("T", " ")
						event_date_cut = event_date_str[:19]
						event_date_UTC = datetime.strptime(event_date_cut, '%Y-%m-%d %H:%M:%S')
						lowest_price = event['ticketInfo']['minListPrice']
						highest_price = event['ticketInfo']['maxListPrice']
						ticket_count = event['ticketInfo']['totalTickets']
						listing_count = event['ticketInfo']['totalListings']
						
						event_array = pd.DataFrame([[artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count]],
													columns=['artist', 'artist_id', 'name', 'ID', 'venue', 'city', 'state', 'date_UTC', 'lowest_price', 'highest_price', 'ticket_count', 'listing_count'])

						insert_tuple = (artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count, current_date)
						
						event_ql = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

						result = cursor.execute(event_ql, insert_tuple)
						connection.commit()


						"""
						DYNAMODB WAY
						"""
						venue_dict = event['venue']
						price_dict = event['ticketInfo']

						event_key = artist + venue_dict['name'] + venue_dict['city'] + venue_dict['state'] + str(
							event['eventDateUTC']).replace("T", " ")

						dynamotable.put_item(

							Item={
								'Event_ID': event_key,
								'name': event['name'],
								'artist': artist,
								'city': venue_dict['city'],
								'date_UTC': str(event['eventDateUTC']).replace("T", " "),
								'state': venue_dict['state'],
								'venue': venue_dict['name'],
								'create_ts': str(current_date),
								'lowest_price': int(price_dict['minListPrice']),
								'highest_price': int(price_dict['maxListPrice']),
								'ticket_count': int(price_dict['totalTickets']),
								'listing_count': int(price_dict['totalListings'])
							}
						)

			except KeyError as Overload:
			
				print(KeyError)
				print('exceeded quota for stubhub API')

		elif 40 < i <= 80:

			print(i)

			try:
		
				auth_header = ("Bearer " + get_access_token_2())
				headers = {"Authorization": auth_header, "Accept": "application/json"}
				req = requests.get(artist_url, headers=headers)
				json_obj = req.json()

				# print(json_obj)
							
				event_list = json_obj['events']

				for event in event_list:
					
					event_name = event['name']
					
					if 'PARKING' not in event_name:
						print(event_name)
						event_id = str(event['id'])
						event_venue= event['venue']['name']
						event_city = event['venue']['city']
						event_state = event['venue']['state']
						event_date_str = (event['eventDateUTC']).replace("T", " ")
						event_date_cut= event_date_str[:19]
						event_date_UTC = datetime.strptime(event_date_cut, '%Y-%m-%d %H:%M:%S')
						lowest_price = event['ticketInfo']['minListPrice']
						highest_price = event['ticketInfo']['maxListPrice']
						ticket_count = event['ticketInfo']['totalTickets']
						listing_count = event['ticketInfo']['totalListings']
						
						event_array = pd.DataFrame([[artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count]],
										columns=['artist', 'artist_id', 'name', 'ID', 'venue', 'city', 'state', 'date_UTC', 'lowest_price', 'highest_price', 'ticket_count', 'listing_count'])

						insert_tuple = (artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count, current_Date)
						
						event_ql = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

						result = cursor.execute(event_ql, insert_tuple)
						connection.commit()


						"""
						DYNAMODB WAY
						"""
						venue_dict = event['venue']
						price_dict = event['ticketInfo']

						event_key = artist + venue_dict['name'] + venue_dict['city'] + venue_dict['state'] + str(
							event['eventDateUTC']).replace("T", " ")

						dynamotable.put_item(

							Item={
								'Event_ID': event_key,
								'name': event['name'],
								'artist': artist,
								'city': venue_dict['city'],
								'date_UTC': str(event['eventDateUTC']).replace("T", " "),
								'state': venue_dict['state'],
								'venue': venue_dict['name'],
								'create_ts': str(current_date),
								'lowest_price': int(price_dict['minListPrice']),
								'highest_price': int(price_dict['maxListPrice']),
								'ticket_count': int(price_dict['totalTickets']),
								'listing_count': int(price_dict['totalListings'])
							}
						)
					
			except KeyError as Overload:
		
				print(KeyError)
				print('exceeded quota for stubhub API')	

		elif 80 < i <= 120:
		
			print(i)

			try:
		
				auth_header = ("Bearer " + get_access_token_3())
				headers = {"Authorization": auth_header, "Accept": "application/json"}
				req = requests.get(artist_url, headers=headers)
				json_obj = req.json()

				# print(json_obj)
							
				event_list = json_obj['events']

				for event in event_list:
					
					event_name = event['name']
					
					if 'PARKING' not in event_name:
						print(event_name)
						event_id = str(event['id'])
						event_venue= event['venue']['name']
						event_city = event['venue']['city']
						event_state = event['venue']['state']
						event_date_str = (event['eventDateUTC']).replace("T", " ")
						event_date_cut= event_date_str[:19]
						event_date_UTC = datetime.strptime(event_date_cut, '%Y-%m-%d %H:%M:%S')
						lowest_price = event['ticketInfo']['minListPrice']
						highest_price = event['ticketInfo']['maxListPrice']
						ticket_count = event['ticketInfo']['totalTickets']
						listing_count = event['ticketInfo']['totalListings']
						
						event_array = pd.DataFrame([[artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count]],
										columns=['artist', 'artist_id', 'name', 'ID', 'venue', 'city', 'state', 'date_UTC', 'lowest_price', 'highest_price', 'ticket_count', 'listing_count'])

						insert_tuple = (artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count, current_Date)
						
						event_ql = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

						result = cursor.execute(event_ql, insert_tuple)
						connection.commit()


						"""
						DYNAMODB WAY
						"""
						venue_dict = event['venue']
						price_dict = event['ticketInfo']

						print(price_dict['totalTickets'])
						print(price_dict['totalListings'])

						event_key = artist + venue_dict['name'] + venue_dict['city'] + venue_dict['state'] + str(
							event['eventDateUTC']).replace("T", " ")

						dynamotable.put_item(

							Item={
								'Event_ID': event_key,
								'name': event['name'],
								'artist': artist,
								'city': venue_dict['city'],
								'date_UTC': str(event['eventDateUTC']).replace("T", " "),
								'state': venue_dict['state'],
								'venue': venue_dict['name'],
								'create_ts': str(current_date),
								'lowest_price': int(price_dict['minListPrice']),
								'highest_price': int(price_dict['maxListPrice']),
								'ticket_count': int(price_dict['totalTickets']),
								'listing_count': int(price_dict['totalListings'])
							}
						)

			except KeyError as Overload:
		
				print(KeyError)
				print('exceeded quota for stubhub API')	

		elif 120 < i <= 160:
		
			print(i)

			try:
		
				auth_header = ("Bearer " + get_access_token_4())
				headers = {"Authorization": auth_header, "Accept": "application/json"}
				req = requests.get(artist_url, headers=headers)
				json_obj = req.json()

				# print(json_obj)
							
				event_list = json_obj['events']

				for event in event_list:
					
					event_name = event['name']
					if 'PARKING' not in event_name:
						print(event_name)
						event_id = str(event['id'])
						event_venue= event['venue']['name']
						event_city = event['venue']['city']
						event_state = event['venue']['state']
						event_date_str = (event['eventDateUTC']).replace("T", " ")
						event_date_cut= event_date_str[:19]
						event_date_UTC = datetime.strptime(event_date_cut, '%Y-%m-%d %H:%M:%S')
						lowest_price = event['ticketInfo']['minListPrice']
						highest_price = event['ticketInfo']['maxListPrice']
						ticket_count = event['ticketInfo']['totalTickets']
						listing_count = event['ticketInfo']['totalListings']
						
						event_array = pd.DataFrame([[artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count]],
										columns=['artist', 'artist_id', 'name', 'ID', 'venue', 'city', 'state', 'date_UTC', 'lowest_price', 'highest_price', 'ticket_count', 'listing_count'])

						insert_tuple = (artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count, current_Date)
						
						event_ql = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

						result = cursor.execute(event_ql, insert_tuple)
						connection.commit()



						"""
						DYNAMODB WAY
						"""
						venue_dict = event['venue']
						price_dict = event['ticketInfo']

						event_key = artist + venue_dict['name'] + venue_dict['city'] + venue_dict['state'] + str(
							event['eventDateUTC']).replace("T", " ")

						dynamotable.put_item(

							Item={
								'Event_ID': event_key,
								'name': event['name'],
								'artist': artist,
								'city': venue_dict['city'],
								'date_UTC': str(event['eventDateUTC']).replace("T", " "),
								'state': venue_dict['state'],
								'venue': venue_dict['name'],
								'create_ts': str(current_date),
								'lowest_price': int(price_dict['minListPrice']),
								'highest_price': int(price_dict['maxListPrice']),
								'ticket_count': int(price_dict['totalTickets']),
								'listing_count': int(price_dict['totalListings'])
							}
						)

			except KeyError as Overload:
		
				print(KeyError)
				print('exceeded quota for stubhub API')	

		else:
		
			print(i)

			try:
		
				auth_header = ("Bearer " + get_access_token_5())
				headers = {"Authorization": auth_header, "Accept": "application/json"}
				req = requests.get(artist_url, headers=headers)
				json_obj = req.json()

				# print(json_obj)
							
				event_list = json_obj['events']

				for event in event_list:
					
					event_name = event['name']
					if 'PARKING' not in event_name:
						print(event_name)
						event_id = str(event['id'])
						event_venue= event['venue']['name']
						event_city = event['venue']['city']
						event_state = event['venue']['state']
						event_date_str = (event['eventDateUTC']).replace("T", " ")
						event_date_cut= event_date_str[:19]
						event_date_UTC = datetime.strptime(event_date_cut, '%Y-%m-%d %H:%M:%S')
						lowest_price = event['ticketInfo']['minListPrice']
						highest_price = event['ticketInfo']['maxListPrice']
						ticket_count = event['ticketInfo']['totalTickets']
						listing_count = event['ticketInfo']['totalListings']
						
						event_array = pd.DataFrame([[artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count]],
										columns=['artist', 'artist_id', 'name', 'ID', 'venue', 'city', 'state', 'date_UTC', 'lowest_price', 'highest_price', 'ticket_count', 'listing_count'])

						insert_tuple = (artist, '', event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count, current_Date)
						
						event_ql = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

						result = cursor.execute(event_ql, insert_tuple)
						connection.commit()


						"""
						DYNAMODB WAY
						"""
						venue_dict = event['venue']
						price_dict = event['ticketInfo']

						event_key = artist + venue_dict['name'] + venue_dict['city'] + venue_dict['state'] + str(
							event['eventDateUTC']).replace("T", " ")

						dynamotable.put_item(

							Item={
								'Event_ID': event_key,
								'name': event['name'],
								'artist': artist,
								'city': venue_dict['city'],
								'date_UTC': str(event['eventDateUTC']).replace("T", " "),
								'state': venue_dict['state'],
								'venue': venue_dict['name'],
								'create_ts': str(current_date),
								'lowest_price': int(price_dict['minListPrice']),
								'highest_price': int(price_dict['maxListPrice']),
								'ticket_count': int(price_dict['totalTickets']),
								'listing_count': int(price_dict['totalListings'])
							}
						)

			except KeyError as Overload:

				print(KeyError)
				print('exceeded quota for stubhub API')

		i = i+1


stubhub_event_pull()
				
				
				
				
				



