#-----------------------------------------------------#
#-----------SEATGEEK API DATA PULL--------------------#
#-----------------------------------------------------#
#-----------PURPOSE - FOR EACH ARTIST ON A MAJOR------#
#---------------------SPOTIFY PLAYLIST, SEARCH FOR----#
#---------------------THEIR EVENTS ON STUBHUB---------#
#---------------------AND INSERT ALL RELEVANT DATA----#
#---------------------INTO AN AWS RDB TABLE-----------#
#-----------------------------------------------------#
#----------LAST UPDATED ON 5/9/2019-------------------#
#-----------------------------------------------------#

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

def dynamo():


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
			
		
dynamo()
	
def SeatGeek_Events():
    
	Artists_df = Data_Fetch_pymysql().head(20)
	#Artists_df = Data_Fetch_pymysql().head(200)
	
	#------------------GET ARTIST LIST FROM DF----------------#
	artists = Artists_df['artist']

	#-----------GET CURRENT DATETIME FOR TIMESTAMP ADD------------#
	current_Date = datetime.now()
	
	i = 1

	#---------------BEGIN BUILDING EVENTS DATAFRAME FROM JSON----------------#
	#-----CREATE BLANK DATAFRAME FOR APPENDING, ISOLATE EVENTS FROM JSON-----#
	event_df = pd.DataFrame()

	#--------------------LOOP THRU ARTISTS--------------------#
	#for artist in artists:	
	for artist_dat in Artists_df.iterrows():
        
		#-----------EXTRACT ARTIST FROM THE ROW------------------#
		spotify_artist = artist_dat[1]['artist']
		spotify_artist_id = artist_dat[1]['artist_id']
	
		performer_slug=spotify_artist
		
		try:
		
			url = 'https://api.seatgeek.com/2/events?format=json'
			payload = {'per_page' : 1,
					   'performers.slug':performer_slug,
					   'client_id':  client_id_str,
					  }
			r = requests.get(url, params=payload,verify=True)

			print(r.url)

			json_obj = json.loads(r.text)
			
			#print(json_obj)
			
			try:
				event_name = json_obj['events'][0]['title']
				#print(event_name)
			except KeyError as noName:
				event_name = ''
					
			try:
				event_id = json_obj['events'][0]['id']
				#print(event_id)
			except KeyError as noID:
				event_id = ''
				
			try:
				event_date_UTC = json_obj['events'][0]['datetime_utc']
				#print(event_date_utc)
			except KeyError as noDatetime:
				event_date_UTC = ''	
			
			try:
				event_venue = json_obj['events'][0]['venue']['name']
				#print(event_venue)
			except KeyError as noVenue:
				event_venue = ''		

			try:
				event_city = json_obj['events'][0]['venue']['city']
				#print(event_city)
			except KeyError as noCity:
				event_city = ''		


			try:
				event_state = json_obj['events'][0]['venue']['state']
				#print(event_state)
			except KeyError as noState:
				event_state = ''

			event_stats=json_obj['events'][0]['stats']
			#print(event_stats)
			
			try:
				avg_price = json_obj['events'][0]['stats']['average_price']
				#print(avg_price)
			except KeyError as noAvg:
				avg_price = ''

			try:
				med_price = json_obj['events'][0]['stats']['median_price']
				#print(med_price)
			except KeyError as noMed:
				med_price = ''
			
			try:
				lowest_price = json_obj['events'][0]['stats']['lowest_price']
				#print(lowest_price)
			except KeyError as noLowest:
				lowest_price = ''
			
			try:
				highest_price = json_obj['events'][0]['stats']['highest_price']
				#print(highest_price)
			except KeyError as noHighest:
				highest_price = ''	
			
			try:
				no_listings = json_obj['events'][0]['stats']['listing_count']
				#print(no_listings)
			except KeyError as noListingCount:
				no_listings = ''	
				
			
			event_key = spotify_artist + event_venue + event_city + event_state + event_date_UTC
			
			event_array = pd.DataFrame([['TEST', spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, avg_price, med_price, no_listings, current_Date]], 
									   columns =['Event_ID', 'artist', 'artist_id', 'name', 'ID', 'venue', 'city', 'state', 'date_UTC', 'lowest_price', 'highest_price', 'avg_price', 'med_price', 'listing_count', 'create_ts'])
			
			
		
			event_json = event_array.to_json(orient = 'records')
		
			list2 = (spotify_artist, spotify_artist_id, current_Date)

			if event_key in events_dict:
				
				prev_event_list = events_dict[event_key]
				appended_list = prev_event_list.append(event_json)
				events_dict[event_key]=appended_list
				
				#-------------THE PANDAS WAY TO DO IT----------------#
				#prev_event_df = events_dict[event_key]
				#appended_df = prev_event_df.append(event_array)
				#events_dict[event_key] = appended_df
				
			else:
				
				events_dict[event_key] = event_json
			
				#-----------THE PANDAS WAY TO DO IT------------#
				#events_dict[event_key] = event_array
		
			print(events_dict)
		
			#-------APPEND EACH EVENT TO MASTER DATAFRAME...NOT SURE IF I STILL NEED THIS------#
			event_df = event_df.append(event_array)

		
		except IndexError as e:
		
			print('NO RELATED SEATGEEK EVENTS')
			
	print(event_df)
	
	with open('C:/Users/wjack/Documents/test2.pickle', 'wb') as handle:
		pickle.dump(events_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
	

#SeatGeek_Events()


