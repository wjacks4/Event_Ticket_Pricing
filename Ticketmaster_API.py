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
import MySQLdb
import sqlalchemy

def Data_Fetch():

	test_db = pd.read_csv("C:/Users/whjac/Desktop/Ticket Flipping/Event_Ticket_Pricing/Data/test.csv")

	Fetch_QL = 'SELECT * FROM ARTISTS_ONLY;'

	connection=MySQLdb.connect('ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', 'tickets_user', 'tickets_pass', 'tickets_db')
	cursor=connection.cursor()

	cursor.execute(Fetch_QL)
	Artists_List = cursor.fetchall()
	
	Artists_DF = pd.read_sql('SELECT * FROM ARTISTS_ONLY_EXPANDED', con = connection)
	
	#print(Artists_DF)
	#Artists_DF.to_csv('C:/Users/whjac/Desktop/Ticket Flipping/Event_Ticket_Pricing/Data/Arist_Data.csv', index = False, encoding = 'utf-8')
		
	return Artists_DF
	

Data_Fetch()



base_url = ('https://app.ticketmaster.com/discovery/v2/events.json?&apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3&size=10&keyword=')


#----------------------CHECK HEADER----------------#

#test_url = ('https://app.ticketmaster.com/discovery/v2/events.json?&apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3&size=10&keyword=Da+Baby')
#request = urllib.request.urlopen(test_url).info()
#print(request)




Test = Data_Fetch().head(3)

print(Test)
	
def EVENT_IDs (df):


	artists = Test['artist']
	#artists = df['Artist']
	#artists = Artists_DF['artist']

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
				#print((name).encode('utf8'))
				#print((id).encode('utf8'))
				
				
				each_event_ID = pd.DataFrame([[name, id]], columns=['attraction_name', 'ID'])
			
				event_ID_df = event_ID_df.append(each_event_ID)
			
			#print(each_event)
				
			time.sleep(5)
		
		except KeyError as Oshit:
		
			print(Oshit)
			
			
	print(event_ID_df)
	
	return event_ID_df
			

EVENT_IDs(Test)

sample_event_url = ('https://app.ticketmaster.com/discovery/v2/events/1AKZA_YGkd7zQGw.json?apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3')







def EVENT_DETAILS():

	event_base_url = ('https://app.ticketmaster.com/discovery/v2/events/')
	
	api_key = ('.json?apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3')
	
	IDs = EVENT_IDs(Test)['ID']
	
	event_df = pd.DataFrame()
	
	for event_ID in IDs:
	
		event_url = (event_base_url + event_ID + api_key)

		print(event_url)
		
		raw_Data=urllib.request.urlopen(event_url)

		#print(rawData.read())
		
		encoded_Dat = raw_Data.read().decode('utf-8', 'ignore')			
		json_Dat = json.loads(encoded_Dat)
		
		
		#---------------------------------------------------------------#
		#-----------HANDLE EXCEPTIONS FOR MISSING VALUES----------------#
		#---------------------------------------------------------------#
		try: 
			event_venue = json_Dat['_embedded']['venues'][0]['name']
		except KeyError as noVenue:
			event_venue=' '
			
		try:
			event_city = json_Dat['_embedded']['venues'][0]['city']['name']
		except KeyError as noCity:
			event_city = ' '
			
		try:
			event_dates= json_Dat['dates']
		except KeyError as noDate:
			event_dates = ''
			
		try:
			event_sales = json_Dat['sales']
		except KeyError as noSales:
			event_sales = ''
			
		try:
			event_name = json_Dat['name']
		except KeyError as noName:
			event_name = ''
		
		try:
			event_start_date = json_Dat['dates']['start']['localDate']
		
		except KeyError as noStartDate:
			event_start_date = ' '
		
		
		try: 
			event_sale_start = json_Dat['sales']['public']['startDateTime']
		except KeyError as noSaleStart:
			event_sale_start = ' '
			


		try: 
		
			event_lowest_price = json_Dat['priceRanges'][0]['min']

			print(json_Dat['priceRanges'][0]['min'])
			
			event_profile=pd.DataFrame([[event_name, event_venue, event_city, event_start_date, event_sale_start, event_lowest_price ]], 
							columns=['attraction_name', 'venue', 'city', 'event_date', 'sale_start_date', 'lowest_face_val_price'])	



			TestQL = "INSERT INTO TEST_Table(event_name, event_venue, city, event_date, sale_start, lowest_price) VALUES ('%s', '%s', '%s', '%s', '%s', '%s');" %(event_name, event_venue, event_city, event_start_date, event_sale_start, event_lowest_price)

			print(TestQL)

			connection=MySQLdb.connect('ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', 'tickets_user', 'tickets_pass', 'tickets_db')
			cursor=connection.cursor()

			cursor.execute(TestQL)
			#data=cursor.fetchall()
			connection.commit()	
							
			
		except KeyError as No_Price_Data:
			
			print('No Price Data Available')
			
			event_profile=pd.DataFrame([[event_name, event_venue, event_city, event_start_date, event_sale_start, '' ]], 
						columns=['attraction_name', 'venue', 'city', 'event_date', 'sale_start_date', 'lowest_face_val_price'])			

			
			
		event_df = event_df.append(event_profile)
			
		time.sleep(5)
			
	event_df.to_csv('C:/Users/whjac/Desktop/Ticket Flipping/Event_Ticket_Pricing/Data/Ticketmaster_event_list.csv', index=False)
	
	print(event_df)
	
	return event_df
						
EVENT_DETAILS()


	
	

