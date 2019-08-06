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
import fuzzywuzzy
from fuzzywuzzy import fuzz

import urllib
import pandas as pd
import numpy as np
import json
import requests

import boto3

def test():

	'''
	blabla
	:return:
	'''


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
#--------------------------DEFINE EVENT CLASS--------------------------#
#----------------------------------------------------------------------#


class event:

	#-----------GET CURRENT DATETIME FOR TIMESTAMP ADD------------#
	current_Date = datetime.now()
	
	#----------------------------------------------------------------------#
	#--------------------------DYNAMODB SETUP------------------------------#
	#----------------------------------------------------------------------#

	dynamodb = boto3.resource('dynamodb')
	dynamoTable = dynamodb.Table('Event_Table')



	def __init__(self, artist):
		self.performer_slug = artist
		self.current_Date = datetime.now()
		self.dynamoTable = boto3.resource('dynamodb').Table('Event_Table')
		
	
	def seatgeek_api(self, artist):
	
	
		#----------------------------------------------------------------------#
		#--------------------------GLOBAL STUFF--------------------------------#
		#----------------------------------------------------------------------#
		base_url = ('https://api.seatgeek.com/2/')
		client_id_str = ('MTM4MTIyMDZ8MTU1NDQ3MTkxMy43Ng')
		client_secret_str = ('c49766eaad2bc8bc33810d112d141ca9a09b0a78b1be52c459eb19c5fd3527a5')
	
	
		'''
		THE MEAT
		'''
		
		try:
		
			url = 'https://api.seatgeek.com/2/events?format=json'
			payload = {'per_page' : 1,
					   'performers.slug':artist,
					   'client_id':  client_id_str,
					  }
			r = requests.get(url, params=payload,verify=True)

			print(r.url)

			json_obj = json.loads(r.text)



			# TODO: consider this dict.get(key[, value])

			try:
				event_name = json_obj['events'][0]['title']
				print(event_name)
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
	
			event_key = artist + event_venue + event_city + event_state + event_date_UTC
			
			#--------------------------------------------------#
			#----------THE DYNAMODB WAY TO DO IT---------------#
			#--------------------------------------------------#
			
			New_ticket_prices = {'create_ts':self.current_Date, 'lowest_price':lowest_price, 'highest_price':highest_price, 'med_price':med_price, 'listing_count':no_listings}



			event_json = (self.dynamoTable.get_item(
				Key={
					'Event_ID':event_key
				}
			))['Item']
			
			Existing_ticket_prices = event_json['Ticket_prices']
			
			print(Existing_ticket_prices)
			print(New_ticket_prices)
			
			
			#-------------------------------------------------------#
			#----------THE (HACKY) MYSQL WAY TO DO IT---------------#
			#-------------------------------------------------------#	

			
			


		except IndexError as e:
		
			print('NO RELATED SEATGEEK EVENTS')

	
	def ticketmaster_api(self, artist):
	
		#--------------------------------------------------------------------#
		#---------TICKETMASTER API QUERY AUTORIZATION / QUERY DATA-----------#
		#--------------------------------------------------------------------#
		event_search_url = ('https://app.ticketmaster.com/discovery/v2/events.json?&apikey=83sdXVyv4k3NnuGCIvk5nAHE3NSWddg7&size=10&keyword=')
		event_base_url = ('https://app.ticketmaster.com/discovery/v2/events.json?apikey=')
		data_type = ('.json?')
		api_key1 = ('83sdXVyv4k3NnuGCIvk5nAHE3NSWddg7')
		api_key2 = ('2C4llrNfIrGgEZxAft1QuJ5bpbS3SdpF')
	
		#--------------TRY PULLING EVENT IDs, EXCEPT WHEN NO EVENTS APPEAR FOR AN ARTIST NAME-----------#
		try: 

			#---------------------BUILD URL ACCESS STRING---------------------#
			artist_encode = artist.encode('utf-8')
			artist_decode = unidecode(str(artist_encode, encoding = "utf-8"))			
			artist_keyword = artist_decode.replace(" ", "+")
			
			access_string = (event_base_url + api_key1 + '&size=10&keyword=' + artist_keyword)

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
				
				#---------------------------------------------------------------#
				#-----EXTRACT VARIABLES OF INTEREST FROM JSON OBJECTS-----------#
				#-----------HANDLE EXCEPTIONS FOR MISSING VALUES----------------#
				#---------------------------------------------------------------#
				try:
					event_name = event['name']
					print(event_name)
				except KeyError as noName:
					event_name = ''
					
		#----------THROW EXCEPTION WHEN NO EVENTS EXIST FOR AN ARTIST-----------#
		except KeyError as No_Events:
		
			print('No Events for this Artist!')
			
		except urllib.error.HTTPError as Overload:
		
			print('Too Many requests, wait 2 seconds')
			time.sleep(2)
				
			
	def eventbrite_api(self, artist):		
	
		#------------------------------------------------------------------#
		#---------------EVENTBRITE API AUTHORIZATION DATA------------------#
		#------------------------------------------------------------------#
		API_key = "QBBZEWV5XWAAFECR3D"
		API_secret = "7NG5DUZEJBCIGLFJWZRTQ3R7SE3UXUDCA4DFD7U3MFC57UQF45"
		OAuth_token = "ZG7IKNHFJFFYSXDN4R5K"
		Anon_OAuth_token = "SWIBI6XDBCO2UP5AOA7Y"
		base_string = "https://www.eventbriteapi.com/v3/events/search/?token=ZG7IKNHFJFFYSXDN4R5K&"
	
		#---------ENCODE ARTIST NAMES IN HTML SYNTAX-----------#
		artist_encode = (artist.replace("&", " ")).replace(" ", "%20")
		
		#---------BUILD THE URL TO REQUEST DATA FROM-----------#
		artist_url = (base_string + "expand=ticket_availability,external_ticketing,venue&" + "q=" + artist_encode)

		try:
			#---------GET RAW RESPONSE FROM URL, DECODE IT TO JSON----------#
			rawdat = urllib.request.urlopen(artist_url)
			encoded_dat = rawdat.read().decode('utf-8', errors='ignore')
			json_dat = json.loads(encoded_dat)
			
			#---------------BEGIN BUILDING EVENTS DATAFRAME FROM JSON----------------#
			#-----CREATE BLANK DATAFRAME FOR APPENDING, ISOLATE EVENTS FROM JSON-----#
			event_df = pd.DataFrame()
			events = json_dat['events']
			
			#-----------LOOP THROUGH EVENTS IN EVENT LIST---------------#
			for event in events:
			
				#----------TRY TO EXTRACT DATA FROM 'EVENTS' OBJECT IN JSON...EXCEPT WHEN NO DATA--------#
				
				try:
				
					#--------FIRST, EXTRACT EVENT NAME AND ELIMINATE SQL ESCAPE CHARACTERS-------#
					event_name = ((event['name']['text']).replace('"', '')).encode('utf-8')
					name_decode = unidecode(str(event_name, encoding="utf-8")).replace('"', '')
					
					#------AVOID PULLING BACK TOO MANY EVENTS BY FUZZY MATCHING SPOTIFY NAME TO EVENTBRITE NAMES------#
					Spotify_name = artist
					EventBrite_name = event_name
					
					#----------TEST OUT FUZZYWUZZY FUNCTION-----------------#
					fuzz_partial = fuzz.partial_ratio(Spotify_name.lower(), EventBrite_name.lower())
					fuzz_ratio = fuzz.ratio(Spotify_name.lower(), EventBrite_name.lower())
					
					print(Spotify_name)
					
					#----------ONLY CONTINUE EXTRACTING EVENT DATA IF FUZZY PARTIAL SCORE > .75...IDK--------#
					
					if (fuzz_ratio + fuzz_partial) > 150:
						
						print(event_name)
						print(fuzz_partial)
						print(fuzz_ratio)


				#------------SINCE INSTANCES OF NO-DATA SEEMS RARE IN EVENTBITE, JUST SKIP RECORD ENTIRELY----------#
				except TypeError as no_data:
					
					print('One of the fields was missing')					

		except urllib.error.HTTPError:
			
			print(artist)
			print(artist_url)
			print('Bad Request')
				
			
	def stubhub_api():
	
		#-------------------------------------------------------------#
		#---------------------GLOBAL STUFF----------------------------#
		#-------------------------------------------------------------#

		#---------DEFINE URL BUILDING BLOCKS-------#
		base_url = 'https://api.stubhub.com/sellers/search/events/v3'


		#----------------------------------------------------------#
		#-------------STUBHUB KEYS FOR EACH ACCOUNT----------------#
		#----------------------------------------------------------#
		
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

		#----------------------------------------------------------#
		#----GET ACCCESS TOKENS TO STUBHUB API FOR EACH ACCOUNT----#
		#----------------------------------------------------------#
		
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
	
		#----------------------------------------------------#
		#------------THE MEAT - MAKE THE REQUEST-------------#
		#----------------------------------------------------#
	
		#---------ENCODE ARTIST NAMES IN HTML SYNTAX-----------#
		artist_encode = self.artist.replace(" ", "%20")
			
		#---------------------QUERY PARAMS---------------------#
		query_params = ("q=" + artist_encode + "&" + "rows=100")		
			
		#---------BUILD THE URL TO REQUEST DATA FROM-----------#
		artist_url = (base_url + "?" + query_params)
			
		#print(artist_url)
		#--------------ADD HEADERS & MAKE REQUEST----------------#
		
		try:
		
			Auth_Header = ("Bearer " + Get_Access_Token_1())
			headers = {"Authorization": Auth_Header, "Accept": "application/json"}
			req = requests.get(artist_url, headers=headers)
			json_obj = req.json()
	
			event_list = json_obj['events']

			for event in event_list:
				
				event_name = event['name']
				
				if 'PARKING' not in event_name:
					print(event_name)
					
		
		except KeyError as Overload:

			print(KeyError)
			print('exceeded quota for stubhub API')	






#-----------------------------------------------------------------------------------------------#
#-----------------LOOP THROUGH ARTISTS, CALLING THE EVENT CLASS FOR EACH ARTIST-----------------#
#-----------------------------------------------------------------------------------------------#

def DAL():

	Artists_DF = Data_Fetch_pymysql()

	for artist in Artists_DF.head(5)['artist']:
	
		test = event(artist)
		
		#print(test.performer_slug)
		#print(test.current_Date)
		
		test.seatgeek_api(artist)
		#test.ticketmaster_api(artist)
		#test.eventbrite_api(artist)
		#test.stubhub_api(artist)
		
			
DAL()
