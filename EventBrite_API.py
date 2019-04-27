# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 15:00:58 2019

@author: bswxj01
"""

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
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.client import Spotify
import requests
import urllib
from urllib import parse
import sys
import base64
import numpy as np
import fuzzywuzzy
from fuzzywuzzy import fuzz


API_key = "QBBZEWV5XWAAFECR3D"
API_secret = "7NG5DUZEJBCIGLFJWZRTQ3R7SE3UXUDCA4DFD7U3MFC57UQF45"
OAuth_token = "ZG7IKNHFJFFYSXDN4R5K"
Anon_OAuth_token = "SWIBI6XDBCO2UP5AOA7Y"

base_string = "https://www.eventbriteapi.com/v3/events/search/?token=ZG7IKNHFJFFYSXDN4R5K&"


test_db = pd.read_csv("C:/Users/whjac/Desktop/Ticket Flipping/Event_Ticket_Pricing/Data/test.csv")
sample = test_db.head(3)

print(sample)

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



def EventBrite_Artist_Search(df):

	sample = df.head(3)
	
	artists = sample['Artist']
	
	
	
	for artist in artists:
		
		artist_encode = artist.replace(" ", "%20")
		artist_url = (base_string + "expand=ticket_availability,external_ticketing,venue&" + "q=" + artist_encode)
		
		print(artist_url)
		rawdat = urllib.request.urlopen(artist_url)
		encoded_dat = rawdat.read().decode('utf-8', errors='ignore')
		
		print(rawdat)
		
		json_dat = json.loads(encoded_dat)
		events = json_dat['events']
		
		event_df = pd.DataFrame()
			
		attr_list = ['name', 'id', 'start', 'end', 'capacity', 'listed', 'shareable', 'venue_id']
			
		for event in events:
		
			try:
				name = event['name']['text']
				
				#-----FUZZY MATCHING TESTING----#
				
				Spotify_name = artist
				EventBrite_name = name
				
				#-----------TEST OUT FUZZY LEVENSHEN FUNCTION-----------#
			
				#Distance = levenshtein_ratio_and_distance(Spotify_name, EventBrite_name)
				#print(Distance)
				
				#Ratio = levenshtein_ratio_and_distance(Spotify_name, EventBrite_name,ratio_calc = True)
				#print(Ratio)
				

				#----------TEST OUT FUZZYWUZZY FUNCTION-----------------#
				
				fuzz_partial = fuzz.partial_ratio(Spotify_name.lower(), EventBrite_name.lower())
				print(fuzz_partial)
				
				if fuzz_partial > 75:
				
					print('Partial Fuzzy score is greater than 75!!')
				
					id = event['id']
					start = event['start']['utc']
					end = event['end']['utc']
					capacity = event['capacity']
					listed = event['listed']
					shareable = event['shareable']
					venue_id = event['venue_id']
					
					venue_state = event['venue']['address']['region']
					venue_city = event['venue']['address']['city']
					
					minimum_price = event['ticket_availability']['minimum_ticket_price']['major_value']
					maximum_price = event['ticket_availability']['maximum_ticket_price']['major_value']
					sold_out_indicator = event['ticket_availability']['is_sold_out']
					available_elsewhere = event['is_externally_ticketed']
				
					event_profile=pd.DataFrame([[name, id, start, end, capacity, listed, shareable, venue_id, venue_state, venue_city, minimum_price, maximum_price, sold_out_indicator, available_elsewhere]], 
						columns=['event_name', 'event_id', 'event_start', 'event_end', 'event_capacity', 'listed', 'shareable', 'venue_id', 'venue_state', 'venue_city', 'minimum_price', 'maximum_price', 'sold_out_indicator', 'available_elsewhere'])	
					
					event_df = event_df.append(event_profile)
					
			except TypeError as no_data:
			
				print ('One of the fields was missing')
			
			
		print(artist)

		
		#-----------------------------------------#
		#----------EXPORT THE PANDAS DFs----------#
		#-----------------------------------------#
		
		event_df.to_csv('C:/Users/whjac/Desktop/Ticket Flipping/Event_Ticket_Pricing/Data/EventBrite_Sample_Fuzzy.csv', index = False, encoding = 'utf-8')
		
		event_df_cleaned.to_csv('C:/Users/whjac/Desktop/Ticket Flipping/Event_Ticket_Pricing/Data/EventBrite_Sample_Cleaned.csv', index = False, encoding = 'utf-8')
		
		

		
		
		
EventBrite_Artist_Search(test_db)

