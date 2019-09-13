"""
INSTAGRAM API PULL
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs
import time
import re
from urllib.request import urlopen
import json
from pandas.io.json import json_normalize
import pandas as pd, numpy as np
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
import pytz

#---------------------------------#
#----------GLOBAL DATA------------#
#---------------------------------#
chrome_options = Options()
chrome_options.add_argument("--headless")

#----------------------------------------------------------------------#
#---------------------GET ARTIST LIST FROM MYSQL DB--------------------#
#----------------------------------------------------------------------#
def Data_Fetch_pymysql():

	#test_db = pd.read_csv("C:/Users/wjack/Desktop/Event_Ticket_Pricing/Event_Ticket_Pricing/Data/test.csv")

	Fetch_QL = 'SELECT * FROM Artists_trimmed;'

    #USING pymysql#
	connection = pymysql.connect (host = 'ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user = 'tickets_user', password = 'tickets_pass', db = 'tickets_db')
    			
	cursor=connection.cursor()

	cursor.execute(Fetch_QL)
	Artists_List = cursor.fetchall()
	
	Artists_DF = pd.read_sql('SELECT * FROM Artists_trimmed_ranked', con = connection)
	
	#print(Artists_DF)
	return Artists_DF

#Data_Fetch_pymysql()


def Songkick_Pull():


	#---------SELECT A SMALL SUBSET OF THE ARTIST DATAFRAME----------#
	Artists_df = Data_Fetch_pymysql().head(500)
	

	#--------------CREATE EMPTY EVENT DATAFRAME TO APPEND DATA ON TO LATER----------------#
	insta_df = pd.DataFrame()

	problem_names = pd.DataFrame()

	for artist in Artists_df.iterrows():
	
		#-----------GET CURRENT DATETIME FOR TIMESTAMP ADD------------#
		current_Date = datetime.now()		

		artist_name = artist[1]['artist']
		compressed_name = artist[1]['artist'].replace(' ', '')
		print(compressed_name)
		
		browser = webdriver.Chrome('C:/Users/wjack/Desktop/chromedriver.exe', chrome_options=chrome_options)
		#browser = webdriver.Chrome('C:/Users/wjack/Desktop/chromedriver.exe')
		
		access_string = ('http://instagram.com/' + compressed_name + '/')
		browser.get(access_string)
		time.sleep(5)
		
		followers_element = browser.find_elements_by_xpath('//*[@id="react-root"]/section/main/div/header/section/ul/li[2]/a/span')
		
		verified_element = browser.find_elements_by_xpath('//*[@id="react-root"]/section/main/div/header/section/div[1]/span')
		
		try:
			
			verified = verified_element[0].get_attribute('title')
			
			try: 
			
				followers = followers_element[0].get_attribute('title')
				
				artist_array = pd.DataFrame([[artist_name, followers, current_Date]], 
							  columns =['artist', 'followers', 'create_ts'])
							  
				#-------APPEND EACH EVENT TO MASTER DATAFRAME...NOT SURE IF I STILL NEED THIS------#
				insta_df = insta_df.append(artist_array)	

			except IndexError as Name_Different:
			
				print('Instagram handle different than actual name...look through these')
				weird_artist = pd.DataFrame([[artist_name]], columns = ['artist'])
				problem_names = problem_names.append(weird_artist)
				
		except IndexError as Unverified_So_Name_Different:
		
			print('The account with the literal artist name is unverified...look through these')
			weird_artist = pd.DataFrame([[artist_name]], columns = ['artist'])
			problem_names = problem_names.append(weird_artist)
			
		
		time.sleep(5)
		
	print(insta_df)		
	print(problem_names)

		
Instagram_Pull()
	


