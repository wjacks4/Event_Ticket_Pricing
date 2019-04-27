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

#print(Spotify_Playlist_list)



#----------------------------------------------------------------#
#---GENERATE MY ACCESS TOKEN TO USE THROUGHOUT REST OF PROGRAM---#
#----------------------------------------------------------------#

def generate_token():
	
	credentials = SpotifyClientCredentials(
		client_id = Spotify_client_ID,
		client_secret = Spotify_client_secret
	)
	
	token = credentials.get_access_token()
	#print(token)
	return token

generate_token()



#---------------------------------------------------#
#---CALL THE SPOTIPY LIBRARY WITH GENERATED TOKEN---#
#---------------------------------------------------#

spotify = spotipy.Spotify(auth=generate_token())


#FUNCTION TO PULL IDS FOR ALL CHOSEN SPOTIFY PLAYLISTS#

def ID_Gen(name):

	raw_Dat = spotify.search(q=name, type='playlist')

	encoded_Dat = str(raw_Dat).encode('utf-8')
	
	playlist_ID = raw_Dat['playlists']['items'][0]['id']
	playlist_Name = raw_Dat['playlists']['items'][0]['name']
	playlist_User = raw_Dat['playlists']['items'][0]['owner']['id']
	
	#print(playlist_ID)	
	#print(playlist_Name)
	#print(playlist_User)
	
	return playlist_ID, playlist_User
	
	
	
#FUNCTION TO RETURN ARRAY OF ARTISTS FROM A GIVEN PLAYLIST#

def Playlist_Artists(user_in, ID_in):

		raw_dat = spotify.user_playlist_tracks(user = user_in, playlist_id = ID_in)
		artist_array= []
		song_list = raw_dat['items']
		
		for song in song_list:
			
			artists = song['track']['artists']
			
			for artist in artists:
				
				#artist_encode = artist.encode('utf-8')
				#artist_decode = unidecode(str(artist_encode, encoding = "utf-8"))	
				
				artist_name = unidecode(str( (artist['name'].encode('utf-8')), encoding="utf-8"))
				artist_array.append(artist_name)
				
		return(artist_array)
				
playlist_IDs=pd.DataFrame()


for playlist in Spotify_Playlist_list.iterrows():

	title=("'" + (playlist[1]['Playlist Name']) + "'")
	
	playlist_Name = (playlist[1]['Playlist Name'])
	genre=(playlist[1]['Genre'])

	playlist_ID = ID_Gen(title)[0]
	playlist_User = ID_Gen(title)[1]
	
	each_Playlist = pd.DataFrame([[playlist_Name, genre, playlist_ID, playlist_User]], columns=['playlist_Name', 'genre', 'playlist_ID', 'playlist_User'])
	
	playlist_IDs = playlist_IDs.append(each_Playlist)


test = playlist_IDs.head(3)

for playlist_ID in playlist_IDs.iterrows():
#for playlist_ID in test.iterrows(): 
	
	each_Name = ((playlist_ID[1]['playlist_Name']))
	each_genre = ((playlist_ID[1]['genre']))
	each_ID = ((playlist_ID[1]['playlist_ID']))
	each_User = ((playlist_ID[1]['playlist_User']))

	Artists_list = Playlist_Artists(each_User, each_ID)
	
	for artist in Artists_list:
	
		print(artist)
		
		TestQL = "INSERT INTO ARTISTS(artist_name, artist_genre, playlist) VALUES ('%s', '%s', '%s');" %(artist, each_genre, each_Name)

		print(TestQL)

		connection=MySQLdb.connect('ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', 'tickets_user', 'tickets_pass', 'tickets_db')
		cursor=connection.cursor()

		cursor.execute(TestQL)
		#data=cursor.fetchall()
		connection.commit()			
		
		
	
	print(each_Name)
	print(each_genre)
	print(each_ID)
	print(Artists_list)

	







	






