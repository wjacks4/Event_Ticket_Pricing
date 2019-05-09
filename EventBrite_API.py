#-----------------------------------------------------#
#-----------EVENTBRITE API DATA PULL------------------#
#-----------------------------------------------------#
#-----------PURPOSE - FOR EACH ARTIST ON A MAJOR------#
#---------------------SPOTIFY PLAYLIST, SEARCH FOR----#
#---------------------THEIR EVENTS ON EVENTBRITE------#
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
import fuzzywuzzy
from fuzzywuzzy import fuzz



#------------------------------------------------------------------#
#---------------EVENTBRITE API AUTHORIZATION DATA------------------#
#------------------------------------------------------------------#
API_key = "QBBZEWV5XWAAFECR3D"
API_secret = "7NG5DUZEJBCIGLFJWZRTQ3R7SE3UXUDCA4DFD7U3MFC57UQF45"
OAuth_token = "ZG7IKNHFJFFYSXDN4R5K"
Anon_OAuth_token = "SWIBI6XDBCO2UP5AOA7Y"
base_string = "https://www.eventbriteapi.com/v3/events/search/?token=ZG7IKNHFJFFYSXDN4R5K&"



#----------------------------------------------------------------------#
#---------------------GET ARTIST LIST FROM MYSQL DB--------------------#
#----------------------------------------------------------------------#
def Data_Fetch():

	Fetch_QL = 'SELECT * FROM ARTISTS_ONLY;'

	connection = pymysql.connect (host = 'ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user = 'tickets_user', password = 'tickets_pass', db = 'tickets_db')
	cursor=connection.cursor()

	cursor.execute(Fetch_QL)
	Artists_List = cursor.fetchall()
	
	Artists_DF = pd.read_sql('SELECT * FROM ARTISTS_ONLY_EXPANDED', con = connection)
		
	return Artists_DF
	

Data_Fetch()





#-----------------------------------------------------------------------------#
#-----------CODE FROM STACKOVERFLOW DEFINING SIMPLE FUZZY FUNCTION------------#
#-----------------------------------------------------------------------------#
def levenshtein_ratio_and_distance(s, t, ratio_calc = False):
    """ levenshtein_ratio_and_distance:
        Calculates levenshtein distance between two strings.
        If ratio_calc = True, the function computes the
        levenshtein distance ratio of similarity between two strings
        For all i and j, distance[i,j] will contain the Levenshtein
        distance between the first i characters of s and the
        first j characters of t
    """
    # Initialize matrix of zeros
    rows = len(s)+1
    cols = len(t)+1
    distance = np.zeros((rows,cols),dtype = int)

    # Populate matrix of zeros with the indeces of each character of both strings
    for i in range(1, rows):
        for k in range(1,cols):
            distance[i][0] = i
            distance[0][k] = k

    # Iterate over the matrix to compute the cost of deletions,insertions and/or substitutions    
    for col in range(1, cols):
        for row in range(1, rows):
            if s[row-1] == t[col-1]:
                cost = 0 # If the characters are the same in the two strings in a given position [i,j] then the cost is 0
            else:
                # In order to align the results with those of the Python Levenshtein package, if we choose to calculate the ratio
                # the cost of a substitution is 2. If we calculate just distance, then the cost of a substitution is 1.
                if ratio_calc == True:
                    cost = 2
                else:
                    cost = 1
            distance[row][col] = min(distance[row-1][col] + 1,      # Cost of deletions
                                 distance[row][col-1] + 1,          # Cost of insertions
                                 distance[row-1][col-1] + cost)     # Cost of substitutions
    if ratio_calc == True:
        # Computation of the Levenshtein Distance Ratio
        Ratio = ((len(s)+len(t)) - distance[row][col]) / (len(s)+len(t))
        return Ratio
    else:
        # print(distance) # Uncomment if you want to see the matrix showing how the algorithm computes the cost of deletions,
        # insertions and/or substitutions
        # This is the minimum number of edits needed to convert string a to string b
        return "The strings are {} edits away".format(distance[row][col])



#-----------------------------------------------------------------------------#
#----------------------MAIN EVENT SEARCH FUNCTION-----------------------------#
#-----------------------------------------------------------------------------#

def EventBrite_Artist_Search(df):
    #---------SELECT A SMALL SUBSET OF THE ARTIST DATAFRAME----------#
	sample = df.head(67)
    #artists = sample['artist']
	
    #-----------GET CURRENT DATETIME FOR TIMESTAMP ADD------------#
	current_Date = datetime.now()
    #current_Date = 'TEST'
	
	#--------------------LOOP THRU ARTISTS--------------------#
	for artist_dat in sample.iterrows():
        
		spotify_artist = artist_dat[1]['artist']
		spotify_artist_id = artist_dat[1]['artist_id']
		
		#---------ENCODE ARTIST NAMES IN HTML SYNTAX-----------#
		artist_encode = spotify_artist.replace(" ", "%20")
		
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
					Spotify_name = spotify_artist
					EventBrite_name = event_name
					
					#----------TEST OUT FUZZYWUZZY FUNCTION-----------------#
					fuzz_partial = fuzz.partial_ratio(Spotify_name.lower(), EventBrite_name.lower())
					
					#----------ONLY CONTINUE EXTRACTING EVENT DATA IF FUZZY PARTIAL SCORE > .75...IDK--------#
					
					if fuzz_partial > 75:

						#-----------INDIVIDUAL VARIABLE EXTRACTION------------#
						event_id = event['id']
						event_venue = event['venue']['name']
						event_city = event['venue']['address']['city']
						event_state = event['venue']['address']['region']
						event_date_UTC = event['start']['utc']
						lowest_price = event['ticket_availability']['minimum_ticket_price']['major_value']
						highest_price = event['ticket_availability']['maximum_ticket_price']['major_value']
						capacity = event['venue']['capacity']
						sold_out_indicator = event['ticket_availability']['is_sold_out']
						shareable = event['shareable']
						available_elsewhere = event['is_externally_ticketed']				

						event_array = pd.DataFrame([[spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, capacity, sold_out_indicator, shareable, available_elsewhere]], 
												   columns =['artist', 'artist_id', 'name', 'ID', 'venue', 'city', 'state', 'date_UTC', 'lowest_price', 'highest_price', 'capacity', 'sold_out_indicator', 'shareable', 'available_elsewhere'])
						

						insert_tuple = (spotify_artist, spotify_artist_id, event_name, event_id, event_venue, event_city, event_state, event_date_UTC, lowest_price, highest_price, capacity, sold_out_indicator, shareable, available_elsewhere, current_Date)
									
						event_QL = 'INSERT INTO `EVENTBRITE_EVENTS` (`artist`, `artist_id`, `name`, `id`, `venue`, `city`, `state`, `date_UTC`, `lowest_price`, `highest_price`, `capacity`, `sold_out`, `shareable`, `available_elsewhere`, `create_ts`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'                       


						#----------CONNECT TO DB AND SUBMIT SQL QUERY------------#
						connection=	connection = pymysql.connect (host = 'ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user = 'tickets_user', password = 'tickets_pass', db = 'tickets_db')
						cursor=connection.cursor()
						
						result  = cursor.execute(event_QL, insert_tuple)	
						connection.commit()					
								

						#-------APPEND EACH EVENT TO MASTER DATAFRAME...NOT SURE IF I STILL NEED THIS------#
						event_df = event_df.append(event_array)
						
				
				#------------SINCE INSTANCES OF NO-DATA SEEMS RARE IN EVENTBITE, JUST SKIP RECORD ENTIRELY----------#
				except TypeError as no_data:
				
					print ('One of the fields was missing')
					
		except urllib.error.HTTPError:
			
			print(spotify_artist)
			print(artist_url)
		
			print('Bad Request')
	
		
#---------------------------------------------------#
#---------------CALL MAIN FUNCTION------------------#	
#---------------------------------------------------#	
EventBrite_Artist_Search(Data_Fetch())

