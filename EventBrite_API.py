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


API_key = "QBBZEWV5XWAAFECR3D"

API_secret = "7NG5DUZEJBCIGLFJWZRTQ3R7SE3UXUDCA4DFD7U3MFC57UQF45"

OAuth_token = "ZG7IKNHFJFFYSXDN4R5K"

Anon_OAuth_token = "SWIBI6XDBCO2UP5AOA7Y"


base_string = "https://www.eventbriteapi.com/v3/events/search/?token=ZG7IKNHFJFFYSXDN4R5K&"


test_db = pd.read_csv("C:/Users/whjac/Desktop/Ticket Flipping/Event_Ticket_Pricing/Data/test.csv")

sample = test_db.head(3)

print(sample)


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
				name = event['name']
				id = event['id']
				start = event['start']
				end = event['end']
				capacity = event['capacity']
				listed = event['listed']
				shareable = event['shareable']
				venue_id = event['venue_id']
				
				venue_state = event['venue']['address']['region']
				venue_city = event['venue']['address']['city']
				
				minimum_price = event['ticket_availability']['minimum_ticket_price']['major_value']
				maximum_price = event['ticket_availability']['maximum_ticket_price']['major_value']
				sold_out_indicator = event['ticket_availability']['is_sold_out']
				start_date_utc = event['ticket_availability']['start_sales_date']
				available_elsewhere = event['is_externally_ticketed']
			
				event_profile=pd.DataFrame([[name, id, start, end, capacity, listed, shareable, venue_id, venue_state, venue_city, minimum_price, maximum_price, sold_out_indicator, start_date_utc, available_elsewhere]], 
					columns=['event_name', 'event_id', 'event_start', 'event_end', 'event_capacity', 'listed', 'shareable', 'venue_id', 'venue_state', 'venue_city', 'minimum_price', 'maximum_price', 'sold_out_indicator', 'start_date_utc', 'available_elsewhere'])	
				
				event_df = event_df.append(event_profile)
					
			except TypeError as no_data:
			
				print ('One of the fields was missing')
			
			

		
			
		
		print(event_df)
		
		

		
		
		
EventBrite_Artist_Search(test_db)

