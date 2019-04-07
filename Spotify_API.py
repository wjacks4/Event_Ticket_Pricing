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

Spotify_client_ID = 'ab3b70083f5f469188f8e49b79d5eadb'

Spotify_client_secret = '6ecf81925e2740c9adecaad28685457a'

base_endpoint = 'https://api.spotify.com/v1'


authorization_endpoint = 'https://accounts.spotify.com/authorize?response_type=code&client_id='


def generate_token():
	
	credentials = SpotifyClientCredentials(
		client_id = Spotify_client_ID,
		client_secret = Spotify_client_secret
	)
	
	token = credentials.get_access_token()
	print(token)
	return token

generate_token()



spotify = spotipy.Spotify(auth=generate_token())

def playlist_search():


	raw_Dat = spotify.search(q='rap caviar', type='playlist')			
	
	playlist_ID = raw_Dat['playlists']['items'][0]['id']
	
	print(playlist_ID)
	

playlist_search()	







