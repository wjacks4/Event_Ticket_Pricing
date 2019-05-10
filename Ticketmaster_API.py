#-----------------------------------------------------#
#-----------TICKETMASTER API DATA PULL----------------#
#-----------------------------------------------------#
#-----------PURPOSE - FOR EACH ARTIST ON A MAJOR------#
#---------------------SPOTIFY PLAYLIST, SEARCH FOR----#
#---------------------THEIR EVENTS ON TICKETMASTER----#
#---------------------AND INSERT ALL RELEVANT DATA----#
#---------------------INTO AN AWS RDB TABLE-----------#
#-----------------------------------------------------#
#----------LAST UPDATED ON 5/9/2019------------------#
#-----------------------------------------------------#

#!/usr/bin/env python3

#import mysql
#from mysql.connector import Error
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
import requests
import urllib
from urllib import parse
import sys
import base64
import numpy as np
#import mysql-python
import pymysql
import base64
import datetime
from datetime import datetime
import pytz


#--------------------------------------------------------------------#
#---------TICKETMASTER API QUERY AUTORIZATION / QUERY DATA-----------#
#--------------------------------------------------------------------#
event_search_url = ('https://app.ticketmaster.com/discovery/v2/events.json?&apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3&size=10&keyword=')
event_base_url = ('https://app.ticketmaster.com/discovery/v2/events/')
data_type = ('.json?')
api_key = ('.json?apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3')
size = '20'




#----------------------------------------------------------------------#
#---------------------GET ARTIST LIST FROM MYSQL DB--------------------#
#----------------------------------------------------------------------#
def Data_Fetch_pymysql():

	#test_db = pd.read_csv("C:/Users/wjack/Desktop/Event_Ticket_Pricing/Event_Ticket_Pricing/Data/test.csv")

	Fetch_QL = 'SELECT * FROM ARTISTS_ONLY;'

    #USING pymysql#
	connection = pymysql.connect (host = 'ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user = 'tickets_user', password = 'tickets_pass', db = 'tickets_db')
    			
	cursor=connection.cursor()

	cursor.execute(Fetch_QL)
	Artists_List = cursor.fetchall()
	
	Artists_DF = pd.read_sql('SELECT * FROM ARTISTS_ONLY_EXPANDED', con = connection)
	
	return Artists_DF



#--------------------------------------------------------------------------------------------#
#------------------------------------CHECK HEADER--------------------------------------------#
#--------THIS NEEDS TO BE RUN WHENVER YOU WANT TO CHECK HOW MANY REQUESTS YOU HAVE LEFT------#
#--------------------------------------------------------------------------------------------#

test_url = ('https://app.ticketmaster.com/discovery/v2/events.json?&apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3&size=10&keyword=Da+Baby')
request = urllib.request.urlopen(test_url).info()
print(request)




#--------------------------------------------------------------------------------------------------------------#
#------CREATE A LIST OF EVENT IDS FROM A KEYWORD SEARCH USING EVERY ARTIST FROM MAJOR SPOTIFY PLAYLISTS--------#
#--------------------------------------------------------------------------------------------------------------#
def EVENT_IDs (df):


	#-----------------CREATE BLANK DATAFRAME FOR APPENDING-------------------#
	event_ID_df = pd.DataFrame()


	#----------LOOP THROUGH ARTISTS IN COLUMN FROM INPUT DATAFRAME-----------#
	for artist_dat in df.iterrows():
	
		spotify_artist = artist_dat[1]['artist']
		spotify_artist_id = artist_dat[1]['artist_id']

		#--------------TRY PULLING EVENT IDs, EXCEPT WHEN NO EVENTS APPEAR FOR AN ARTIST NAME-----------#

		try: 

			#---------------------BUILD URL ACCESS STRING---------------------#
			artist_encode = spotify_artist.encode('utf-8')
			artist_decode = unidecode(str(artist_encode, encoding = "utf-8"))			
			artist_keyword = artist_decode.replace(" ", "+")		
			access_string = (event_search_url + artist_keyword)

			#--------------SUBMIT REQUEST TO URL, GET JSON RESPONSE-----------#
			raw_Dat = urllib.request.urlopen(access_string)			
			encoded_Dat = raw_Dat.read().decode('utf-8', 'ignore')			
			json_Dat = json.loads(encoded_Dat)

			#----------ISOLATE EVENT OBJECT FROM JSON RESPONSE----------------#
			event_Dat = json_Dat['_embedded']['events']		

			#-------------EXTRACT EVENT ID FROM DATA IN EACH MEMBER OF EVENT OBJECT-----------#
			for event in event_Dat:
				name = event['name']
				event_id = event['id']

				#-------------CREATE TEMPORARY DATAFRAME FOR EACH EVENT ID--------------------#
				each_event_ID = pd.DataFrame([[spotify_artist, spotify_artist_id, name, event_id]], columns=['artist_name', 'artist_id', 'attraction_name', 'event_id'])

				#----------------APPEND TEMPORARY DATAFRAME ONTO MASTER DF--------------------#
				event_ID_df = event_ID_df.append(each_event_ID)
			
            #----------WAIT TWO SECONDS BEFORE SUBMITTING NEXT QUERY TO AVOID OVERLOADING API-----------#
			time.sleep(2)
		
        #----------THROW EXCEPTION WHEN NO EVENTS EXIST FOR AN ARTIST-----------#
		except KeyError as No_Events:
		
			print('No Events for this Artist!')
			
		except urllib.error.HTTPError as Overload:
		
			print('Too Many requests, wait 2 seconds')
			time.sleep(2)

	#----------RETURN THE ID DATAFRAME FOR USE WITH MAIN FUNCTION--------------#
	return event_ID_df
	

#--------SAMPLE EVENT URL FOR TESTING PURPOSES----------#
sample_event_url = ('https://app.ticketmaster.com/discovery/v2/events/1AKZA_YGkd7zQGw.json?apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3')



#--------------------------------------------------------#
#----MASTER FUNCTION - CALL DATA PULL F'N AND ID F'N-----#
#----THEN LOOP THROUGH EVENT IDS AND PULL DATA FOR EACH--#
#--------------------------------------------------------#

def EVENT_DETAILS():


	#---------SELECT A SMALL SUBSET OF THE ARTIST DATAFRAME----------#
	Test = Data_Fetch_pymysql().head(67)

	#----------CONNECT TO DB AND SUBMIT SQL QUERY------------#
	connection=pymysql.connect(host = 'ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user = 'tickets_user', password = 'tickets_pass', db = 'tickets_db')
	cursor=connection.cursor()

	#--------FEED THE RESULT OF THE DATA FETCH FUNCTION INTO THE EVENT_ID FUNCTION------------#
	IDs_df = EVENT_IDs(Test)

	#--------------CREATE EMPTY EVENT DATAFRAME TO APPEND DATA ON TO LATER----------------#
	event_df = pd.DataFrame()

	#-----------GET CURRENT DATETIME FOR TIMESTAMP ADD------------#
	current_Date = datetime.now()
	#current_Date = 'TEST'

	#------EXTRACT INFORMATION FOR EACH EVENT, USING EVENT IDs GENERATED EARLIER------#
	for IDs_dat in IDs_df.iterrows():
        
		event_id = IDs_dat[1]['event_id']
		spotify_artist = IDs_dat[1]['artist_name']
		spotify_artist_id = IDs_dat[1]['artist_id']

		#--------------BUILD URL FOR EACH SPECIFIC QUERY---------------#
		event_base_url = ('https://app.ticketmaster.com/discovery/v2/events/')
		api_key = ('.json?apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3')
		event_url = (event_base_url + event_id + api_key)
        	
        	
		try:
			#---------GET RAW RESPONSE FROM URL, DECODE IT TO JSON----------#
			raw_Data=urllib.request.urlopen(event_url)
			encoded_Dat = raw_Data.read().decode('utf-8', 'ignore')			
			json_Dat = json.loads(encoded_Dat)


			#---------------------------------------------------------------#
			#-----EXTRACT VARIABLES OF INTEREST FROM JSON OBJECTS-----------#
			#-----------HANDLE EXCEPTIONS FOR MISSING VALUES----------------#
			#---------------------------------------------------------------#
			try:
				event_name = json_Dat['name']
			except KeyError as noName:
				event_name = ''

			try: 
				event_venue = json_Dat['_embedded']['venues'][0]['name']
			except KeyError as noVenue:
				event_venue=' '
				
			try:
				event_city = json_Dat['_embedded']['venues'][0]['city']['name']
			except KeyError as noCity:
				event_city = ' '

			try:
				event_state = json_Dat['_embedded']['venues'][0]['state']['name']
			except KeyError as noState:
				event_state = ' '

			try:
				event_date_Local = json_Dat['dates']['start']['localDate']
			except KeyError as noEventDate:
				event_date_Local = ' '

			try:
				event_time_Local = json_Dat['dates']['start']['localTime']
			except KeyError as noEventTime:
				event_time_Local = ' '
				
			event_datetime_Local = (event_date_Local + " " + event_time_Local)

			try:
				TZ_string = json_Dat['dates']['timezone']
				event_TZ = pytz.timezone(TZ_string)
				try:
				
					basic_event_Date = datetime.strptime (event_datetime_Local, "%Y-%m-%d %H:%M:%S")
					local_dt = event_TZ.localize(basic_event_Date, is_dst=None)
					event_date = local_dt.astimezone(pytz.utc)
				except ValueError as missing_datetime:
					event_date = " "
			except KeyError as noTZ:
				event_TZ = '?'
				event_date = (event_datetime_Local)


			try: 
				event_sale_start = json_Dat['sales']['public']['startDateTime']
			except KeyError as noSaleStart:
				event_sale_start = ' '

			try:
				event_lowest_price = json_Dat['priceRanges'][0]['min']
			except KeyError as noPriceDat:
				event_lowest_price = ''
				
			try:
				event_highest_price = json_Dat['priceRanges'][0]['max']
			except KeyError as noPriceDat:
				event_highest_price = ''


			#-------CREATE A TEMPORARY DATAFRAME FOR EACH EVENT----------#
			event_profile=pd.DataFrame([[spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date, TZ_string, event_sale_start, event_lowest_price, event_highest_price]], 
						  columns=['artist', 'artist_id', 'attraction_name', 'event_id', 'venue', 'city', 'state', 'event_date', 'event_TZ', 'sale_start_date', 'event_lowest_price', 'event_highest_price'])	

			insert_tuple = (spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date, TZ_string, event_sale_start, event_lowest_price, event_highest_price, current_Date)

			#------------SQL TIME - SUBSTITUTE STRINGS INTO SQL QUERY FOR DB SUBMISSION-------------#
			event_QL = 'INSERT INTO TICKETMASTER_EVENTS(artist, artist_id, name, id, venue, city, state, date, time_zone, sale_start, lowest_price, highest_price, create_ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);' 

			result  = cursor.execute(event_QL, insert_tuple)
			connection.commit()	
									
			#-------APPEND EACH EVENT TO MASTER DATAFRAME...NOT SURE IF I STILL NEED THIS------#
			#event_df = event_df.append(event_profile)

			#---WAIT TWO SECONDS TO AVOID OVERLOADING API-----#
			time.sleep(2)
		
		except urllib.error.HTTPError as Overload:
		
			print('Too many requests, wait two seconds')
			
			time.sleep(2)
	
	return event_df
						
EVENT_DETAILS()


	
	

