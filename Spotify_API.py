#import mysqlclient as mysql
#from mysql.connector import Error
import psycopg2 as p
import json
#from dateutil import parser
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
import MySQLdb
from urllib import parse

#---------------------------------#
#----------GLOBAL DATA------------#
#---------------------------------#

Spotify_client_ID = 'ab3b70083f5f469188f8e49b79d5eadb'
Spotify_client_secret = '6ecf81925e2740c9adecaad28685457a'
#Spotify_Playlist_list = pd.read_csv('C:/Users/whjac/Desktop/Ticket Flipping/Event_Ticket_Pricing/Data/Spotify Chart Names2.csv')
Spotify_Playlist_list = pd.read_csv('C:/Users/wjack/Desktop/Event_Ticket_Pricing/Event_Ticket_Pricing/Data/Spotify Chart Names2.csv')
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

	
	
	
#--------------------------------------------------------------------------#
#--------FUNCTION TO RETURN ARRAY OF ARTISTS FROM A GIVEN PLAYLIST---------#
#--------------------------------------------------------------------------#
def Playlist_Artists(user_in, ID_in):

	#------------------------------------------------------------------------------------------#
	#--------USE SPOTIPY'S SPOTIFY FUNCTION TO GET TRACKS FROM A SUPPLIED USER & ID------------#
	#------------------------------------------------------------------------------------------#
	raw_dat = spotify.user_playlist_tracks(user = user_in, playlist_id = ID_in)
	song_list = raw_dat['items']
	
	#--------------------------------------------------#
	#----CREATE EMPTY DATAFRAME FOR APPENDING LATER----#
	#--------------------------------------------------#
	artist_df= pd.DataFrame()
	
	
	#--------------------------------------------------------------#
	#--------------LOOP THROUGH SONGS IN PLAYLIST------------------#
	#--------------------------------------------------------------#
	for song in song_list:
		
		#-------------------------------------------------------------------------------------#
		#----TRY PULLING ARTIST INFO FOR EVERY SONG IN PLAYLIST, EXCEPT WHEN NO DATA EXISTS---#
		#-------------------------------------------------------------------------------------#
		try:
		
			#-------------------------------------------#
			#---GET ARTISTS FROM JSON OBJECT ARTISTS----#
			#-------------------------------------------#
			artists = song['track']['artists']
			
			
			#--------------------------------------------------------------------------------------#
			#----------PULL DETAILED ARTIST INFO FOR EVERY ARTIST ON EVERY TRACK IN PLAYLIST-------#
			#--------------------------------------------------------------------------------------#
			for artist in artists:
				
				artist_name = unidecode(str( (artist['name'].encode('utf-8')), encoding="utf-8"))
				artist_ID = str(artist['uri']).replace('spotify:artist:', '')
	
				#----------------------------------------------------------------------------------#
				#------USE SPOTIPY'S ARTIST SEARCH FUNCTION TO PULL POPULARITY INFO ON ARTIST------#
				#----------------------------------------------------------------------------------#
				artist_dat = spotify.artist(artist_id = artist_ID)
				artist_followers = artist_dat['followers']['total']
				artist_popularity = artist_dat['popularity']

				#-------------------------------------------------------------#
				#-------CREATE A TEMPORARY DATAFRAME FOR EACH ARTIST----------#
				#-------------------------------------------------------------#
				artist_array = pd.DataFrame([[artist_name, artist_ID, artist_followers, artist_popularity]], columns =['artist_name', 'artist_ID', 'artist_followers', 'artist_popularity'])

				
				#---------------------------------------------------#
				#-------APPEND EACH ARTIST TO MASTER DATAFRAME------#
				#---------------------------------------------------#
				artist_df = artist_df.append(artist_array)
		
		
		
		#-----------------------------------------------------------------------#
		#----------JUST ADD A BLANK ROW TO MASTER DF WHEN NO DATA EXISTS--------#
		#-----------------------------------------------------------------------#
		except TypeError as Err:
		
			artist_name = ' ' 
			artist_ID = ' ' 
			artist_followers = ' ' 
			artist_popularity = ' '
			artist_array = pd.DataFrame([[artist_name, artist_ID, artist_followers, artist_popularity]], columns =['artist_name', 'artist_ID', 'artist_followers', 'artist_popularity'])
				
			artist_df = artist_df.append(artist_array)
		
	return(artist_df)


playlist_IDs=pd.DataFrame()


#-----------THIS IS TEST WORK---------------#

def TEST():
	for playlist in sample.iterrows():

		title=("'" + (playlist[1]['Playlist Name']) + "'")
		
		playlist_Name = (playlist[1]['Playlist Name'])
		genre=(playlist[1]['Genre'])

		playlist_ID = ID_Gen(title)[0]
		playlist_User = ID_Gen(title)[1]
		
		each_Playlist = pd.DataFrame([[playlist_Name, genre, playlist_ID, playlist_User]], columns=['playlist_Name', 'genre', 'playlist_ID', 'playlist_User'])
		
		playlist_IDs = playlist_IDs.append(each_Playlist)
		
		
	for playlist_ID in playlist_IDs.iterrows():
			
		each_Name = ((playlist_ID[1]['playlist_Name']))
		each_genre = ((playlist_ID[1]['genre']))
		each_ID = ((playlist_ID[1]['playlist_ID']))
		each_User = ((playlist_ID[1]['playlist_User']))

		Artists_list = Playlist_Artists(each_User, each_ID)
		
		for artist in Artists_list:
		
			#print(artist)
			
			artist = artist.replace('"', ' ')
		

		
#------------------------------------------------------------------------------#
#-----------FUNCTION THAT CALLS ID GEN F'N, THEN PLAYLIST_ARTISTS F'N----------#
#-------------THEN STICKS THE RESULT OF THE PLAYLIST ARTIST F'N IN DB----------#

def Artists_to_DB():


	playlist_IDs=pd.DataFrame()

	#-----------------------------------------------------------------------------------#
	#---------GET PLAYLIST IDS FROM MANUALLY GATHERED SPOTIFY PLAYLIST TABLE------------#
	#-----------------------------------------------------------------------------------#
	for playlist in Spotify_Playlist_list.iterrows():


		title=("'" + (playlist[1]['Playlist Name']) + "'")
		
		playlist_Name = (playlist[1]['Playlist Name'])
		genre=(playlist[1]['Genre'])

		playlist_ID = ID_Gen(title)[0]
		playlist_User = ID_Gen(title)[1]
		
		each_Playlist = pd.DataFrame([[playlist_Name, genre, playlist_ID, playlist_User]], columns=['playlist_Name', 'genre', 'playlist_ID', 'playlist_User'])
		
		playlist_IDs = playlist_IDs.append(each_Playlist)


	test = playlist_IDs.head(3)

	
	
	#-----------------------------------------------------------------------------------------#
	#--------------GET DETAILED ARTIST INFO FOR EVERY PLAYLIST WE NOW HAVE IDS FOR------------#
	#-----------------------------------------------------------------------------------------#
	for playlist_ID in playlist_IDs.iterrows():
		
		each_Name = ((playlist_ID[1]['playlist_Name']))
		each_genre = ((playlist_ID[1]['genre']))
		each_ID = ((playlist_ID[1]['playlist_ID']))
		each_User = ((playlist_ID[1]['playlist_User']))

		Artists_df = Playlist_Artists(each_User, each_ID)
		
		for artist in Artists_df.iterrows():
			
			artist_name = ((artist[1]['artist_name'])).replace('"', ' ')
			id = ((artist[1]['artist_ID']))
			followers = ((artist[1]['artist_followers']))
			popularity = ((artist[1]['artist_popularity']))
			
			
			
			TestQL = 'INSERT INTO Artists_expanded(artist, genre, followers, popularity, playlist, artist_id) VALUES ("%s", "%s", "%s", "%s", "%s", "%s");' %(artist_name, each_genre, followers, popularity, each_Name, id)
			
			try:

				connection=MySQLdb.connect('ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', 'tickets_user', 'tickets_pass', 'tickets_db')
				cursor=connection.cursor()

				cursor.execute(TestQL)
				#data=cursor.fetchall()
				connection.commit()			
			
			except _mysql_exceptions.OperationalError as Err:
			
				print('SSL Connection Error ??')
	
Artists_to_DB()
	
	


	







	






