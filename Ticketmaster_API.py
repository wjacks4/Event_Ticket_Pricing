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

test_db = pd.read_csv("C:/Users/whjac/Desktop/Ticket Flipping/Event_Ticket_Pricing/Data/test.csv")

#print(list(test_db.columns.values))

#print(test_db['Artist'])

base_url = ('https://app.ticketmaster.com/discovery/v2/events.json?&apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3&size=10&keyword=')

sample=test_db.head(1)

#print(sample)


	
def EVENT_IDs (df):

	artists = df['Artist']

	event_ID_df = pd.DataFrame()
	
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
				
				
				each_event_ID = pd.DataFrame([[name, id]], columns=['attraction_name', 'ID'])
			
				event_ID_df = event_ID_df.append(each_event_ID)
			
			#print(each_event)
				
			time.sleep(.25)
		
		except KeyError as Oshit:
		
			print(Oshit)
			
			
	print(event_ID_df)
	
	return event_ID_df
			

#EVENT_IDs(sample)

sample_event_url = ('https://app.ticketmaster.com/discovery/v2/events/1AKZA_YGkd7zQGw.json?apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3')







def EVENT_DETAILS():

	event_base_url = ('https://app.ticketmaster.com/discovery/v2/events/')
	
	api_key = ('.json?apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3')
	
	IDs = EVENT_IDs(sample)['ID']
	
	event_df = pd.DataFrame()
	
	for event_ID in IDs:
	
		event_url = (event_base_url + event_ID + api_key)

		print(event_url)
		
		raw_Data=urllib.request.urlopen(event_url)

		#print(rawData.read())
			
		encoded_Dat = raw_Data.read().decode('utf-8', 'ignore')			
		json_Dat = json.loads(encoded_Dat)
		event_venue = json_Dat['_embedded']['venues'][0]['name']
		event_city = json_Dat['_embedded']['venues'][0]['city']['name']
		event_dates= json_Dat['dates']
		event_sales = json_Dat['sales']
		event_name = json_Dat['name']
		
		try: 
			event_prices = json_Dat['priceRanges']
			print(event_prices[0]['min'])
			
			TEST=pd.DataFrame([[event_name, event_venue, event_city, event_dates['start']['localDate'], event_sales['public']['startDateTime'], event_prices[0]['min'] ]], 
							columns=['attraction_name', 'venue', 'city', 'event_date', 'sale_start_date', 'lowest_face_val_price'])			
			
		except KeyError as No_Price_Data:
			
			print('No Price Data Available')
			
			TEST=pd.DataFrame([[event_name, event_venue, event_city, event_dates['start']['localDate'], event_sales['public']['startDateTime'], '' ]], 
						columns=['attraction_name', 'venue', 'city', 'event_date', 'sale_start_date', 'lowest_face_val_price'])			

			

		#for item in event_venue:
		#	city = item['city']['name']
		#	venue = item['name']
		
			#print(city)
			#print(venue)
			
		#	each_event = pd.DataFrame([[event_name, venue, city]], columns=['attraction_name', 'venue', 'city'])
			
		event_df = event_df.append(TEST)
			
		time.sleep(.25)
			
			
		
			
		
			
		
	event_df.to_csv('C:/Users/whjac/Desktop/Ticket Flipping/Event_Ticket_Pricing/Data/Ticketmaster_event_list.csv', index=False)
	
	print(event_df)
	
	return event_df
			
			
EVENT_DETAILS()


	
	

