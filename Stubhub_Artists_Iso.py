#-----------------------------------------------------#
#-----------STUBHUB API DATA PULL---------------------#
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


current_Date = datetime.now()
print('THIS PROGRAM RAN AT ' + str(current_Date))

#-------------------------------------------------------------#
#---------------WE GOT THE MF STUBHUB API BOIII---------------#
#-------------------------------------------------------------#


Stubhub_Key = b'knI4wisTkeBR4txGgGzUiHvpgAHPfWp8'
Stubhub_Secret = b'jkmiGIeQAl3NlHUe'

Cat_Key_Secret = (Stubhub_Key + b":" + Stubhub_Secret)
Cat_Key_encode = base64.standard_b64encode(Cat_Key_Secret)

print(Cat_Key_encode)


#----------------------------------------------------------------------#
#---------------------GET ARTIST LIST FROM MYSQL DB--------------------#
#----------------------------------------------------------------------#

def Data_Fetch_pymysql():

	#Fetch_QL = 'SELECT * FROM ARTISTS_ONLY;'
    
	#USING pymysql#
	connection = pymysql.connect (host = 'ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user = 'tickets_user', password = 'tickets_pass',db = 'tickets_db')
	
	Fetch_QL = 'SELECT * FROM Artists_expanded;'
	cursor = connection.cursor()
	Artists_DF = pd.read_sql('SELECT * FROM Artists_trimmed_ranked WHERE current_followers >= 20000 and current_followers <= 500000 order by current_followers desc', con = connection)  
	return Artists_DF

Data_Fetch_pymysql()


def Get_Access_Token():

	#-------DEFINE URL BUILDING BLOCKS------#
	base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
	query_params = 'grant_type=client_credentials'
	
	#-----BUILD URL FOR REQUEST-----#
	request_url = (base_url + "?" + query_params)
	
	#-------ADD ON ADDITIONAL DATA TO URL REQUEST-----#
	payload = {"username":"pluug3123@gmail.com", "password":"Hester3123"}
	headers = {"Authorization": "Basic Vk9VNHh2R2ZoR085cXBWeEdvM1NBQmVlYm5wVG1BSnc6UlIydEZ3SEc3UGludjRpaw==", "Content-Type": "application/json"}
	
	req = requests.post(request_url, data=json.dumps(payload), headers=headers)
	json_obj = req.json()
	token = json_obj['access_token']
	
	print(token)
	return (token)
	
Get_Access_Token()







def ARTISTS_WITH_EVENTS():

	#--------DEFINE THE SQL DB CONNECTION (MYSQLDB)-------#
	connection=pymysql.connect('ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', 'tickets_user', 'tickets_pass', 'tickets_db')
	cursor=connection.cursor()

	#---------SELECT A SMALL SUBSET OF THE ARTIST DATAFRAME----------#
	Artists_df = Data_Fetch_pymysql().head(400)
	#Artists_df = Data_Fetch_pymysql().head(20)

	#---------DEFINE URL BUILDING BLOCKS-------#
	base_url = 'https://api.stubhub.com/sellers/search/events/v3'

	#------------------GET ARTIST LIST FROM DF----------------#
	artists = Artists_df['artist']

	#-----------GET CURRENT DATETIME FOR TIMESTAMP ADD------------#
	current_Date = datetime.now()

	#------------DROP AND RECREATE THE RELEVANT ARTISTS TABLE-------------#
	drop_QL = 'DROP TABLE ARTISTS_WITH_EVENTS;'                       
	result  = cursor.execute(drop_QL)
	connection.commit()
	
	create_QL = 'CREATE TABLE ARTISTS_WITH_EVENTS AS SELECT * FROM Artists_Targeted limit 1;'                       
	result  = cursor.execute(create_QL)
	connection.commit()			
	
	clear_QL = 'DELETE FROM ARTISTS_WITH_EVENTS;'                       
	result  = cursor.execute(clear_QL)
	connection.commit()	


	#--------------------LOOP THRU ARTISTS--------------------#
	#for artist in artists:	
	for artist_dat in Artists_df.iterrows():

		#-----------EXTRACT ARTIST FROM THE ROW------------------#
		spotify_artist = artist_dat[1]['artist']
		spotify_artist_id = artist_dat[1]['artist_id']
		spotify_artist_popularity = artist_dat[1]['popularity']
		spotify_artist_followers = artist_dat[1]['current_followers']

		#---------ENCODE ARTIST NAMES IN HTML SYNTAX-----------#
		artist_encode = spotify_artist.replace(" ", "%20")
			
		#---------------------QUERY PARAMS---------------------#
		query_params = ("q=" + artist_encode + "&" + "rows=100")		
			
		#---------BUILD THE URL TO REQUEST DATA FROM-----------#
		artist_url = (base_url + "?" + query_params)
			
		#print(artist_url)
		#--------------ADD HEADERS & MAKE REQUEST----------------#

		try:
		
			Auth_Header = ("Bearer " + Get_Access_Token())
			headers = {"Authorization": Auth_Header, "Accept": "application/json"}
			req = requests.get(artist_url, headers=headers)
			json_obj = req.json()
			
			event_list = json_obj['events']
			event_count = len(event_list)
			
			#print(len(event_list))
			
			if event_count > 10:
			
				insert_tuple = (spotify_artist, spotify_artist_id, spotify_artist_popularity, spotify_artist_followers)
				
				insert_QL = 'INSERT INTO `ARTISTS_WITH_EVENTS` (`artist`, `artist_id`, `popularity`, `current_followers`) VALUES (%s, %s, %s, %s)'

				result  = cursor.execute(insert_QL, insert_tuple)
				connection.commit()	
			
		except KeyError as Overload:
		
			print(KeyError)
			#print('exceeded quota for stubhub API')			
				
ARTISTS_WITH_EVENTS()