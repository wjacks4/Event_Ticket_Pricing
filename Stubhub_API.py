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


import boto3


current_Date = datetime.now()
print('THIS PROGRAM RAN AT ' + str(current_Date))

#-------------------------------------------------------------#
#---------------WE GOT THE MF STUBHUB API BOIII---------------#
#-------------------------------------------------------------#

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



def Get_Access_Token_1():

	#-------DEFINE URL BUILDING BLOCKS------#
	base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
	query_params = 'grant_type=client_credentials'
	
	#-----BUILD URL FOR REQUEST-----#
	request_url = (base_url + "?" + query_params)
	
	#-------ADD ON ADDITIONAL DATA TO URL REQUEST-----#
	payload = {"username":"wjacks4@g.clemson.edu", "password":"Hester3123"}
	headers = {"Authorization": "Basic Vk9VNHh2R2ZoR085cXBWeEdvM1NBQmVlYm5wVG1BSnc6UlIydEZ3SEc3UGludjRpaw==", "Content-Type": "application/json"}
	
	req = requests.post(request_url, data=json.dumps(payload), headers=headers)
	json_obj = req.json()
	token = json_obj['access_token']
	
	print(token)
	return (token)
	
#Get_Access_Token_1()

def Get_Access_Token_2():

	#-------DEFINE URL BUILDING BLOCKS------#
	base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
	query_params = 'grant_type=client_credentials'
	
	#-----BUILD URL FOR REQUEST-----#
	request_url = (base_url + "?" + query_params)
	
	#-------ADD ON ADDITIONAL DATA TO URL REQUEST-----#
	payload = {"username":"hiltonsounds@gmail.com", "password":"Hester3123"}
	headers = {"Authorization": "Basic ZDlmV0h0UXZzMzRjQWViZGZBekRUQ09mNkRMbjlObTc6MTFVQTV2S1NRdVp6amI0bQ==", "Content-Type": "application/json"}
	
	req = requests.post(request_url, data=json.dumps(payload), headers=headers)
	json_obj = req.json()
	token = json_obj['access_token']
	
	print(token)
	return (token)
	
#Get_Access_Token_2()

def Get_Access_Token_3():

	#-------DEFINE URL BUILDING BLOCKS------#
	base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
	query_params = 'grant_type=client_credentials'
	
	#-----BUILD URL FOR REQUEST-----#
	request_url = (base_url + "?" + query_params)
	
	#-------ADD ON ADDITIONAL DATA TO URL REQUEST-----#
	payload = {"username":"edenk@g.clemson.edu", "password":"Hester3123"}
	headers = {"Authorization": "Basic ZDlmV0h0UXZzMzRjQWViZGZBekRUQ09mNkRMbjlObTc6MTFVQTV2S1NRdVp6amI0bQ==", "Content-Type": "application/json"}
	
	req = requests.post(request_url, data=json.dumps(payload), headers=headers)
	json_obj = req.json()
	token = json_obj['access_token']
	
	print(token)
	return (token)
	
#Get_Access_Token_3()



def Get_Access_Token_4():

	#-------DEFINE URL BUILDING BLOCKS------#
	base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
	query_params = 'grant_type=client_credentials'
	
	#-----BUILD URL FOR REQUEST-----#
	request_url = (base_url + "?" + query_params)
	
	#-------ADD ON ADDITIONAL DATA TO URL REQUEST-----#
	payload = {"username":"butteredtoast66@gmail.com", "password":"Hester3123"}
	headers = {"Authorization": "Basic ajBlTjIyU0FwRjYzY3pZM3pjanY3d1g1U0VEOTZGUkY6OXVneUpKN3BHQUdvdVJKQQ==", "Content-Type": "application/json"}
	
	req = requests.post(request_url, data=json.dumps(payload), headers=headers)
	json_obj = req.json()
	token = json_obj['access_token']
	
	print(token)
	return (token)
	
#Get_Access_Token_4()


def Get_Access_Token_5():

	#-------DEFINE URL BUILDING BLOCKS------#
	base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
	query_params = 'grant_type=client_credentials'
	
	#-----BUILD URL FOR REQUEST-----#
	request_url = (base_url + "?" + query_params)
	
	#-------ADD ON ADDITIONAL DATA TO URL REQUEST-----#
	payload = {"username":"sunglassman3123@gmail.com", "password":"Hester3123"}
	headers = {"Authorization": "Basic VmhEdEZDMlVFOG9RdEJwWUxtaFdoejkzMUZSUGZqc246eTJRanVySkgybm1jS050NA==", "Content-Type": "application/json"}
	
	req = requests.post(request_url, data=json.dumps(payload), headers=headers)
	json_obj = req.json()
	print(json_obj)
	token = json_obj['access_token']
	
	print(token)
	return (token)
	
#Get_Access_Token_5()
			

	
def STUBHUB_EVENT_PULL():
    
	#--------DEFINE THE SQL DB CONNECTION (MYSQLDB)-------#
	connection=pymysql.connect('ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', 'tickets_user', 'tickets_pass', 'tickets_db')
	cursor=connection.cursor()

	#---------SELECT A SMALL SUBSET OF THE ARTIST DATAFRAME----------#
	Artists_df = Data_Fetch_pymysql().head(200)
	#Artists_df = Data_Fetch_pymysql().head(20)

	#---------DEFINE URL BUILDING BLOCKS-------#
	base_url = 'https://api.stubhub.com/sellers/search/events/v3'

	#------------------GET ARTIST LIST FROM DF----------------#
	artists = Artists_df['artist']

	#-----------GET CURRENT DATETIME FOR TIMESTAMP ADD------------#
	current_Date = datetime.now()
	
	i = 1

	#--------------------LOOP THRU ARTISTS--------------------#
	#for artist in artists:	
	for artist_dat in Artists_df.iterrows():
        
		#-----------EXTRACT ARTIST FROM THE ROW------------------#
		spotify_artist = artist_dat[1]['artist']
		spotify_artist_id = artist_dat[1]['artist_id']

		#---------ENCODE ARTIST NAMES IN HTML SYNTAX-----------#
		artist_encode = spotify_artist.replace(" ", "%20")
			
		#---------------------QUERY PARAMS---------------------#
		query_params = ("q=" + artist_encode + "&" + "rows=100")		
			
		#---------BUILD THE URL TO REQUEST DATA FROM-----------#
		artist_url = (base_url + "?" + query_params)
			
		#print(artist_url)
		#--------------ADD HEADERS & MAKE REQUEST----------------#
		
		if i <=40:
#		if i <=4:
		
			print(i)
			try:
			
				Auth_Header = ("Bearer " + Get_Access_Token_1())
				headers = {"Authorization": Auth_Header, "Accept": "application/json"}
				req = requests.get(artist_url, headers=headers)
				json_obj = req.json()

				#print(json_obj)
							
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
						
						event_array = pd.DataFrame([[spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count]], 
									  columns =['artist', 'artist_id', 'name', 'ID', 'venue', 'city', 'state', 'date_UTC', 'lowest_price', 'highest_price', 'ticket_count', 'listing_count'])

						insert_tuple = (spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count, current_Date)
						
						event_QL = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'                       

						result  = cursor.execute(event_QL, insert_tuple)
						connection.commit()

			except KeyError as Overload:
			
				print(KeyError)
				print('exceeded quota for stubhub API')
				
				
		elif i > 40 and i <= 80:
#		elif i > 4 and i <=8:
		
			print(i)

			try:
		
				Auth_Header = ("Bearer " + Get_Access_Token_2())
				headers = {"Authorization": Auth_Header, "Accept": "application/json"}
				req = requests.get(artist_url, headers=headers)
				json_obj = req.json()

				#print(json_obj)
							
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
						
						event_array = pd.DataFrame([[spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count]], 
									  columns =['artist', 'artist_id', 'name', 'ID', 'venue', 'city', 'state', 'date_UTC', 'lowest_price', 'highest_price', 'ticket_count', 'listing_count'])

						insert_tuple = (spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count, current_Date)
						
						event_QL = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'                       

						result  = cursor.execute(event_QL, insert_tuple)
						connection.commit()
					
			except KeyError as Overload:
		
				print(KeyError)
				print('exceeded quota for stubhub API')	

			
		elif i > 80 and i <= 120:
#		elif i > 8 and i <=12:
		
			print(i)

			try:
		
				Auth_Header = ("Bearer " + Get_Access_Token_3())
				headers = {"Authorization": Auth_Header, "Accept": "application/json"}
				req = requests.get(artist_url, headers=headers)
				json_obj = req.json()

				#print(json_obj)
							
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
						
						event_array = pd.DataFrame([[spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count]], 
									  columns =['artist', 'artist_id', 'name', 'ID', 'venue', 'city', 'state', 'date_UTC', 'lowest_price', 'highest_price', 'ticket_count', 'listing_count'])

						insert_tuple = (spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count, current_Date)
						
						event_QL = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'                       

						result  = cursor.execute(event_QL, insert_tuple)
						connection.commit()

			except KeyError as Overload:
		
				print(KeyError)
				print('exceeded quota for stubhub API')	

			
		elif i > 120 and i<=160:
#		elif i > 12 and i <=16:
		
		
			print(i)


			try:
		
				Auth_Header = ("Bearer " + Get_Access_Token_4())
				headers = {"Authorization": Auth_Header, "Accept": "application/json"}
				req = requests.get(artist_url, headers=headers)
				json_obj = req.json()

				#print(json_obj)
							
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
						
						event_array = pd.DataFrame([[spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count]], 
									  columns =['artist', 'artist_id', 'name', 'ID', 'venue', 'city', 'state', 'date_UTC', 'lowest_price', 'highest_price', 'ticket_count', 'listing_count'])

						insert_tuple = (spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count, current_Date)
						
						event_QL = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'                       

						result  = cursor.execute(event_QL, insert_tuple)
						connection.commit()

			except KeyError as Overload:
		
				print(KeyError)
				print('exceeded quota for stubhub API')	

		else:
		
			print(i)

			try:
		
				Auth_Header = ("Bearer " + Get_Access_Token_5())
				headers = {"Authorization": Auth_Header, "Accept": "application/json"}
				req = requests.get(artist_url, headers=headers)
				json_obj = req.json()

				#print(json_obj)
							
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
						
						event_array = pd.DataFrame([[spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count]], 
									  columns =['artist', 'artist_id', 'name', 'ID', 'venue', 'city', 'state', 'date_UTC', 'lowest_price', 'highest_price', 'ticket_count', 'listing_count'])

						insert_tuple = (spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count, current_Date)
						
						event_QL = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'                       

						result  = cursor.execute(event_QL, insert_tuple)
						connection.commit()

			except KeyError as Overload:

				print(KeyError)
				print('exceeded quota for stubhub API')	


		i = i+1
               
STUBHUB_EVENT_PULL()




				
				
				
				
				
				
				
				



