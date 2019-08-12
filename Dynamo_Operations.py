'''
SEATGEEK API DATA PULL

PURPOSE - TEST
'''

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


import urllib
import pandas as pd
import numpy as np
import json
import requests

import boto3

base_url = ('https://api.seatgeek.com/2/')

client_id_str = ('MTM4MTIyMDZ8MTU1NDQ3MTkxMy43Ng')

client_secret_str = ('c49766eaad2bc8bc33810d112d141ca9a09b0a78b1be52c459eb19c5fd3527a5')



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
#--------------------------DEFINE EVENT DICT---------------------------#
#----------------------------------------------------------------------#


#events_dict = dict()

def dynamo_append():


	with open('C:/Users/wjack/Documents/test2.pickle', 'rb') as handle:
		events_dict = pickle.load(handle)

	#event = events_dict['AtreyuDowntown Las Vegas Events CenterLas VegasNV2019-10-18T10:30:00']

	#json_test = json.loads(event)

	#print(json_test[0])


	dynamoTable = dynamodb.Table('Event_Table')

	for event in events_dict:

		event_dat = json.loads(events_dict[event])[0]
		#pprint(event_dat)
		
		artist = event_dat['artist']
		city = event_dat['city']
		date_UTC = event_dat['date_UTC']
		name = event_dat['name']
		state = event_dat['state']
		venue = event_dat['venue']
		
		event_data = {'name':name, 'artist':artist, 'city':city, 'date_UTC':date_UTC, 'state':state, 'venue':venue}
		#pprint(event_data)
		
		create_ts = event_dat['create_ts']
		highest_price = event_dat['highest_price']
		listing_count = event_dat['listing_count']
		lowest_price = event_dat['lowest_price']
		med_price = event_dat['med_price']
		
		price_data = [{'create_ts':create_ts, 'lowest_price':lowest_price, 'highest_price':highest_price, 'med_price':med_price, 'listing_count':listing_count}]
		#price_data2 = [{'create_ts':1234, 'lowest_price':124, 'highest_price':124, 'med_price':124, 'listing_count':124}]
		#price_data = {'create_ts':create_ts}
		
		#dynamoTable.put_item(
			
		#	Item = {
		#		'Event_ID':event,
		#		'Event_name':name,
		#		'Event_data':event_data,
		#		'Ticket_prices':price_data
		#	}
		#)

		print(event)
		#dynamoTable.update_item(
		#
		#	Key={
		#		'Event_ID':event,
		#	},
		#	UpdateExpression="SET Event_name = :r",
		#	ExpressionAttributeValues={
		#		':r': "WHAT THE FUCK"
		#	}
		#)
		
		dynamoTable.update_item(
		
			Key={
				'Event_ID':event
			},
			UpdateExpression= "SET Ticket_prices = list_append(Ticket_prices, :vals)",
			ExpressionAttributeValues={
				':vals': price_data
			}
		)
			
		
#dynamo()



def dynamo_digest():

	dynamoTable = dynamodb.Table('Event_Table')
	
	with open('C:/Users/wjack/Documents/test2.pickle', 'rb') as handle:
		events_dict = pickle.load(handle)	
	
	
	
	for event in events_dict:
	
		print(event)
		
		event_json = (dynamoTable.get_item(
			Key={
				'Event_ID':event
			}
		))['Item']
		
		#print(event_json)
		
		print(event_json['Event_ID'])
		print(event_json['Event_name'])
		print(event_json['Event_data'])
		print(event_json['Ticket_prices'])
		
#dynamo_digest()
