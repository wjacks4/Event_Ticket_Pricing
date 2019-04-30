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
import MySQLdb
import base64

#-----------------------------------------------------------#
#---------------WE GOT THE MF STUBHUB API BOIII-------------#
#-----------------------------------------------------------#

Stubhub_Key = b'VOU4xvGfhGO9qpVxGo3SABeebnpTmAJw'

Stubhub_Secret = b'RR2tFwHG7Pinv4ik'

Cat_Key_Secret = (Stubhub_Key + b":" + Stubhub_Secret)
print(Cat_Key_Secret)

Cat_Key_encode = base64.standard_b64encode(Cat_Key_Secret)

print(Cat_Key_encode)

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
	print(req)	
	res = requests.get(request_url, data=json.dumps(payload), headers=headers)
	json_obj = req.json()
	print(json_obj)
	
	
Get_Access_Token()
