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

#---------------------------------#
#----------GLOBAL DATA------------#
#---------------------------------#

Spotify_client_ID = 'ab3b70083f5f469188f8e49b79d5eadb'

Spotify_client_secret = '6ecf81925e2740c9adecaad28685457a'

Spotify_Playlist_list = pd.read_csv('C:/Users/whjac/Desktop/Ticket Flipping/Event_Ticket_Pricing/Data/Spotify Chart Names.csv')

sample = Spotify_Playlist_list.head(2)

print(Spotify_Playlist_list)



#----------------------------------------------------------------#
#---GENERATE MY ACCESS TOKEN TO USE THROUGHOUT REST OF PROGRAM---#
#----------------------------------------------------------------#

def generate_token():
	
	credentials = SpotifyClientCredentials(
		client_id = Spotify_client_ID,
		client_secret = Spotify_client_secret
	)
	
	token = credentials.get_access_token()
	print(token)
	return token

generate_token()



#---------------------------------------------------#
#---CALL THE SPOTIPY LIBRARY WITH GENERATED TOKEN---#
#---------------------------------------------------#

spotify = spotipy.Spotify(auth=generate_token())


def ID_Gen(name):

	raw_Dat = spotify.search(q=name, type='playlist')

	encoded_Dat = str(raw_Dat).encode('utf-8')	
	
	#print(encoded_Dat)
	
	playlist_ID = raw_Dat['playlists']['items'][0]['id']
	playlist_Name = raw_Dat['playlists']['items'][0]['name']
	
	print(playlist_ID)	
	print(playlist_Name)
	
	return playlist_ID
	

playlist_IDs=pd.DataFrame()

for playlist in Spotify_Playlist_list.iterrows():
#for playlist in sample.iterrows():

	title=("'" + (playlist[1]['Playlist Name']) + "'")
	
	playlist_Name = (playlist[1]['Playlist Name'])
	genre=(playlist[1]['Genre'])

	print(title)

	playlist_ID = ID_Gen(title)
	
	each_Playlist = pd.DataFrame([[playlist_Name, genre, playlist_ID]], columns=['Playlist_Name', 'Genre', 'playlist_ID'])
	
	playlist_IDs = playlist_IDs.append(each_Playlist)
	

print(playlist_IDs)




	






