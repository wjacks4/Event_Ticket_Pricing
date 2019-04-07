# -*- coding: utf-8 -*-
"""
Created on Fri Apr  5 09:35:23 2019

@author: bswxj01
"""

import urllib
import pandas as pd
import numpy as np
import json
import requests


base_url = ('https://api.seatgeek.com/2/')

client_id_str = ('MTM4MTIyMDZ8MTU1NDQ3MTkxMy43Ng')

client_secret_str = ('c49766eaad2bc8bc33810d112d141ca9a09b0a78b1be52c459eb19c5fd3527a5')

event_data_sample = pd.read_csv("C:/Users/whjac/Desktop/Ticket Flipping/Event_Ticket_Pricing/Data/Ticketmaster_event_list.csv")

print(event_data_sample)



#USING THE STR SUBSTITUTION APPROACH#

def STR_URL_METHOD():

	url = (base_url + 'events?' + 'client_id=' + client_id_str + '&' + 'per_page=1' + '&' + 'performers.slug=DABABY' + '&' + 'venue.city=Denver')

	print(url)

	raw_Data=urllib.request.urlopen(url)
	encoded_Dat = raw_Data.read().decode('utf-8', 'ignore')			
	json_obj = json.loads(encoded_Dat)
	
	event_name = json_obj['events'][0]['title']
	
	print(event_name)

#STR_URL_METHOD()

#print('NEXT METHOD')






#USING THE PAYLOAD APPROACH#

def PAYLOAD_METHOD(performer_slug, city):

	print(performer_slug)
	print(city)

	url = 'https://api.seatgeek.com/2/events?format=json'
	payload = {'per_page' : 1,
			   'performers.slug':performer_slug,
			   'venue.city': city,
			   'client_id':  client_id_str,
			  }
	r = requests.get(url, params=payload,verify=True)

	print(r.url)

	json_obj = json.loads(r.text)
	
	event_name = json_obj['events'][0]['title']
	
	print(event_name)
	
	event_stats=json_obj['events'][0]['stats']
	
	print(event_stats)
	
	avg_price = json_obj['events'][0]['stats']['average_price']
	med_price = json_obj['events'][0]['stats']['median_price']
	lowest_price = json_obj['events'][0]['stats']['lowest_price']
	highest_price = json_obj['events'][0]['stats']['highest_price']
	no_listings = json_obj['events'][0]['stats']['listing_count']
	
	print(avg_price)
	print(med_price)
	print(lowest_price)
	print(highest_price)
	print(no_listings)
	
	
	
	
PAYLOAD_METHOD('DaBaby', 'Denver')








	
	
	
def SeatGeek_Events():
    
	artist=event_data_sample["attraction_name"]
	
	location=event_data_sample["city"]

	
    
	for event in event_data_sample.iterrows() :
	
		performer_slug=(event[1]['attraction_name'])
		city=(event[1]['city'])
		
		try:
		
			PAYLOAD_METHOD(performer_slug, city)
			
		except IndexError as e:
		
			print('NO RELATED SEATGEEK EVENTS')
			
			
SeatGeek_Events()


