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
#import MySQLdb
import base64
import datetime
from datetime import datetime

#-------------------------------------------------------------#
#---------------WE GOT THE MF STUBHUB API BOIII---------------#
#-------------------------------------------------------------#

Stubhub_Key = b'VOU4xvGfhGO9qpVxGo3SABeebnpTmAJw'

Stubhub_Secret = b'RR2tFwHG7Pinv4ik'

Cat_Key_Secret = (Stubhub_Key + b":" + Stubhub_Secret)
print(Cat_Key_Secret)

Cat_Key_encode = base64.standard_b64encode(Cat_Key_Secret)
print(Cat_Key_encode)




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
    Artists_DF = pd.read_sql('SELECT * FROM Artists_expanded', con = connection)  
    print(Artists_DF)
    return Artists_DF

Data_Fetch_pymysql()



def Get_Access_Token():

	#-------DEFINE URL BUILDING BLOCKS------#
	base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
	query_params = 'grant_type=client_credentials'
	
	#-----BUILD URL FOR REQUEST-----#
	request_url = (base_url + "?" + query_params)
	print(request_url)
	
	#-------ADD ON ADDITIONAL DATA TO URL REQUEST-----#
	payload = {"username":"wjacks4@g.clemson.edu", "password":"Hester3123"}
	headers = {"Authorization": "Basic Vk9VNHh2R2ZoR085cXBWeEdvM1NBQmVlYm5wVG1BSnc6UlIydEZ3SEc3UGludjRpaw==", "Content-Type": "application/json"}
	
	req = requests.post(request_url, data=json.dumps(payload), headers=headers)
	json_obj = req.json()
	print(json_obj)
	token = json_obj['access_token']
	print(token)
	return (token)
	
#Get_Access_Token()

	
def Get_Event_IDs_pymysql():
    
    #--------DEFINE THE SQL DB CONNECTION (MYSQLDB)-------#
    connection=pymysql.connect('ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', 'tickets_user', 'tickets_pass', 'tickets_db')
    cursor=connection.cursor()

    #---------SELECT A SMALL SUBSET OF THE ARTIST DATAFRAME----------#
    Test = Data_Fetch_pymysql().head(2)

    #---------DEFINE URL BUILDING BLOCKS-------#
    base_url = 'https://api.stubhub.com/sellers/search/events/v3'
	
    #------------------GET ARTIST LIST FROM DF----------------#
    artists = Test['artist']
    
    #-----------GET CURRENT DATETIME FOR TIMESTAMP ADD------------#
    current_Date = datetime.now()
    #current_Date = 'TEST'
	
    #--------------------LOOP THRU ARTISTS--------------------#
    #for artist in artists:	
    for artist_dat in Test.iterrows():
        
        print(artist_dat)
        
        #-----------EXTRACT ARTIST FROM THE ROW------------------#
        spotify_artist = artist_dat[1]['artist']
        spotify_artist_id = artist_dat[1]['artist_id']
	
        #---------ENCODE ARTIST NAMES IN HTML SYNTAX-----------#
        artist_encode = spotify_artist.replace(" ", "%20")
    		
        #---------------------QUERY PARAMS---------------------#
        query_params = ("q=" + artist_encode + "&" + "rows=100")		
    		
        #---------BUILD THE URL TO REQUEST DATA FROM-----------#
        artist_url = (base_url + "?" + query_params)
        print(artist_url)
    		
        #--------------ADD HEADERS & MAKE REQUEST----------------#
        Auth_Header = ("Bearer " + Get_Access_Token())
        headers = {"Authorization": Auth_Header, "Accept": "application/json"}
        req = requests.get(artist_url, headers=headers)
        json_obj = req.json()
    		
        #print(json_obj)
        
        event_list = json_obj['events']
        
        for event in event_list:
            
            event_name = event['name']
            event_id = str(event['id'])
            event_venue= event['venue']['name']
            event_city = event['venue']['city']
            event_state = event['venue']['state']
            event_date_str = (event['eventDateUTC']).replace("T", " ")
            event_date_cut= event_date_str[:19]
            event_date_UTC = datetime.strptime(event_date_cut, '%Y-%m-%d %H:%M:%S')
            #event_date_UTC = 'TEST'
            print(event_date_UTC)
            lowest_price = event['ticketInfo']['minListPrice']
            highest_price = event['ticketInfo']['maxListPrice']
            ticket_count = event['ticketInfo']['totalTickets']
            listing_count = event['ticketInfo']['totalListings']
            
            event_array = pd.DataFrame([[spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count]], 
                          columns =['artist', 'artist_id', 'name', 'ID', 'venue', 'city', 'state', 'date_UTC', 'lowest_price', 'highest_price', 'ticket_count', 'listing_count'])
    
            insert_tuple = (spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, ticket_count, listing_count, current_Date)
    
            print(insert_tuple)
            
            event_QL = 'INSERT INTO `STUBHUB_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `ticket_count`, `listing_count`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'                       

            print(event_QL)
            result  = cursor.execute(event_QL, insert_tuple)
            connection.commit()
            
            print ("Record inserted successfully into python_users table")                           
                
Get_Event_IDs_pymysql()



