import mysql
from mysql.connector import Error
import psycopg2 as p
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

test_db = pd.read_csv("C:/Users/whjac/Downloads/data science/Ticket Flipping/Data/regional-global-daily-latest.csv")

#print(list(test_db.columns.values))

#print(test_db['Artist'])

base_url = ('https://app.ticketmaster.com/discovery/v2/events.json?&apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3&size=10&keyword=')

sample=test_db.head(10)

#print(sample)


	
def EVENT_IDs (df):

	artists = df['Artist']

	event_df = pd.DataFrame()
	
	for artist in artists:
	
		try: 
			
			artist_encode = artist.encode('utf-8')
			artist_decode = unidecode(str(artist_encode, encoding = "utf-8"))			
			artist_keyword = artist_decode.replace(" ", "+")		
			access_string = (base_url + artist_keyword)
			#print(access_string)
			
			
			raw_Dat = urllib.request.urlopen(access_string)			
			encoded_Dat = raw_Dat.read().decode('utf-8', 'ignore')			
			json_Dat = json.loads(encoded_Dat)			
			event_Dat = json_Dat['_embedded']['events']		
			
			for event in event_Dat:
				name = event['name']
				id = event['id']
				print((name).encode('utf8'))
				print((id).encode('utf8'))
				
				
				each_event = pd.DataFrame([[name, id]], columns=['attraction_name', 'ID'])
			
				event_df = event_df.append(each_event)
			
			#print(each_event)
				
			time.sleep(.25)
		
		except KeyError as Oshit:
		
			print(Oshit)
			
			
	print(event_df)
	
	return event_df
			

#EVENT_IDs(sample)

sample_event_url = ('https://app.ticketmaster.com/discovery/v2/events/1AKZA_YGkd7zQGw.json?apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3')







def EVENT_DETAILS():

	event_base_url = ('https://app.ticketmaster.com/discovery/v2/events/')
	
	api_key = ('.json?apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3')
	
	IDs = EVENT_IDs(sample)['ID']
	
	for event_ID in IDs:
	
		event_url = (event_base_url + event_ID + api_key)
		
		raw_Data=urllib.request.urlopen(event_url)

		#print(rawData.read())
			
		encoded_Dat = raw_Data.read().decode('utf-8', 'ignore')			
		json_Dat = json.loads(encoded_Dat)
		event_venue = json_Dat['_embedded']['venues']

		print(event_venue)

		for item in event_venue:
			city = item['city']
			print(city)
			
			
EVENT_DETAILS()


	
	

