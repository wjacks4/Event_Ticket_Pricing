#-----------------------------------------------------#
#-----------TICKETMASTER API DATA PULL----------------#
#-----------------------------------------------------#
#-----------PURPOSE - FOR EACH ARTIST ON A MAJOR------#
#---------------------SPOTIFY PLAYLIST, SEARCH FOR----#
#---------------------THEIR EVENTS ON TICKETMASTER----#
#---------------------AND INSERT ALL RELEVANT DATA----#
#---------------------INTO AN AWS RDB TABLE-----------#
#-----------------------------------------------------#
#----------LAST UPDATED ON 4/28/2019------------------#
#-----------------------------------------------------#

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
	
	#print(Artists_DF)
	#Artists_DF.to_csv('C:/Users/whjac/Desktop/Ticket Flipping/Event_Ticket_Pricing/Data/Arist_Data.csv', index = False, encoding = 'utf-8')
		
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


	#----------ISOLATE ARTIST COLUMN FROM INPUT DATASET-------#
	
	artists = df['artist']

	#-----------------CREATE BLANK DATAFRAME FOR APPENDING-------------------#
	event_ID_df = pd.DataFrame()
	
	
	#----------LOOP THROUGH ARTISTS IN COLUMN FROM INPUT DATAFRAME-----------#
	for artist in artists:
	
	
	#--------------TRY PULLING EVENT IDs, EXCEPT WHEN NO EVENTS APPEAR FOR AN ARTIST NAME-----------#

		try: 

			#---------------------BUILD URL ACCESS STRING---------------------#
			artist_encode = artist.encode('utf-8')
			artist_decode = unidecode(str(artist_encode, encoding = "utf-8"))			
			artist_keyword = artist_decode.replace(" ", "+")		
			access_string = (event_search_url + artist_keyword)
			print(access_string)
			

			#--------------SUBMIT REQUEST TO URL, GET JSON RESPONSE-----------#
			raw_Dat = urllib.request.urlopen(access_string)			
			encoded_Dat = raw_Dat.read().decode('utf-8', 'ignore')			
			json_Dat = json.loads(encoded_Dat)
			
			#----------ISOLATE EVENT OBJECT FROM JSON RESPONSE----------------#
			event_Dat = json_Dat['_embedded']['events']		
			
			#-------------EXTRACT EVENT ID FROM DATA IN EACH MEMBER OF EVENT OBJECT-----------#
			for event in event_Dat:
				name = event['name']
				id = event['id']
				
				#-------------CREATE TEMPORARY DATAFRAME FOR EACH EVENT ID--------------------#
				each_event_ID = pd.DataFrame([[name, id]], columns=['attraction_name', 'ID'])
			
				#----------------APPEND TEMPORARY DATAFRAME ONTO MASTER DF--------------------#
				event_ID_df = event_ID_df.append(each_event_ID)
			
			#----------WAIT TWO SECONDS BEFORE SUBMITTING NEXT QUERY TO AVOID OVERLOADING API-----------#
			time.sleep(5)
		
		#----------THROW EXCEPTION WHEN NO EVENTS EXIST FOR AN ARTIST-----------#
		except KeyError as No_Events:
		
			print('No Events for this Artist!')
			
	#print(event_ID_df)

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
    Test = Data_Fetch_pymysql().head(2)
    print(Test)

	#----------CONNECT TO DB AND SUBMIT SQL QUERY------------#
    connection=pymysql.connect(host = 'ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user = 'tickets_user', password = 'tickets_pass', db = 'tickets_db')
    cursor=connection.cursor()
	
    #--------FEED THE RESULT OF THE DATA FETCH FUNCTION INTO THE EVENT_ID FUNCTION------------#
    IDs = EVENT_IDs(Test)['ID']
	
    #--------------CREATE EMPTY EVENT DATAFRAME TO APPEND DATA ON TO LATER----------------#
    event_df = pd.DataFrame()
	
    #-----------GET CURRENT DATETIME FOR TIMESTAMP ADD------------#
    current_Date = datetime.now()
    #current_Date = 'TEST'
    
    #------EXTRACT INFORMATION FOR EACH EVENT, USING EVENT IDs GENERATED EARLIER------#
    for event_id in IDs:

        #--------------BUILD URL FOR EACH SPECIFIC QUERY---------------#
        event_base_url = ('https://app.ticketmaster.com/discovery/v2/events/')
        api_key = ('.json?apikey=OrCBYA46Xdvtl7RFfU88egw4L8HDPRW3')
        event_url = (event_base_url + event_id + api_key)
        print(event_url)
        	
        	
        #---------GET RAW RESPONSE FROM URL, DECODE IT TO JSON----------#
        raw_Data=urllib.request.urlopen(event_url)
        encoded_Dat = raw_Data.read().decode('utf-8', 'ignore')			
        json_Dat = json.loads(encoded_Dat)
                
        print(json_Dat)
		
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

        try:
            event_TZ = json_Dat['dates']['timezone']
        except KeyError as noTZ:
            event_TZ = ' '

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
        event_profile=pd.DataFrame([[event_name, event_id, event_venue, event_city, event_state, event_date_Local, event_time_Local, event_TZ, event_sale_start, event_lowest_price, event_highest_price]], 
						columns=['attraction_name', 'event_id', 'venue', 'city', 'state', 'event_date', 'event_time', 'event_TZ', 'sale_start_date', 'event_lowest_price', 'event_highest_price'])	

        
        insert_tuple = (event_name, event_id, event_venue, event_city, event_state, event_date_Local, event_time_Local, event_TZ, event_sale_start, event_lowest_price, event_highest_price, current_Date)

        print(insert_tuple)

		#------------SQL TIME - SUBSTITUTE STRINGS INTO SQL QUERY FOR DB SUBMISSION-------------#
        event_QL = 'INSERT INTO TICKETMASTER_TABLE(name, id, vanue, city, state, date_local, time_local, time_zone, sale_start_date, lowest_price, highest_price, create_ts) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s");' 

        print(event_QL)

        cursor.execute(event_QL)
        connection.commit()	
								
		#-------APPEND EACH EVENT TO MASTER DATAFRAME...NOT SURE IF I STILL NEED THIS------#
        event_df = event_df.append(event_profile)
		
		#---WAIT TWO SECONDS TO AVOID OVERLOADING API-----#
        time.sleep(5)
			
	#---------EXPORT AGGREGATE EVENT DATAFRAME TO CSV---------#
	#event_df.to_csv('C:/Users/whjac/Desktop/Ticket Flipping/Event_Ticket_Pricing/Data/Ticketmaster_event_list.csv', index=False)
	#print(event_df)
	
    return event_df
						
EVENT_DETAILS()


	
	

