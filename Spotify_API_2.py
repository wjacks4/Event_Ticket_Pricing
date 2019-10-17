#import mysqlclient as mysql
#from mysql.connector import Error
#import psycopg2 as p
import pandas as pd
import unidecode
from unidecode import unidecode
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from spotipy.client import Spotify
import requests
import pymysql
import base64
import datetime
from datetime import datetime
import json
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy .ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


def generate_token():

	"""
	GETS SPOTIFY ACCESS TOKEN
	"""
	
	credentials = SpotifyClientCredentials(
		client_id = Spotify_client_ID,
		client_secret = Spotify_client_secret
	)
	
	token = credentials.get_access_token()
	return token


def id_gen(name):

	"""
	FUNCTION THAT GETS PLAYLIST IDs FOR EACH OF THE NAMES OF PLAYLISTS THAT I PASS IT
	"""

	raw_Dat = spotify.search(q=name, type='playlist')
	
	playlist_ID = raw_Dat['playlists']['items'][0]['id']
	playlist_User = raw_Dat['playlists']['items'][0]['owner']['id']

	
	return playlist_ID, playlist_User




"""GLOBAL DATA"""
Spotify_client_ID = 'ab3b70083f5f469188f8e49b79d5eadb'
Spotify_client_secret = '6ecf81925e2740c9adecaad28685457a'
Spotify_Playlist_list = pd.read_csv('C:/Users/wjack/Desktop/Event_Ticket_Pricing/Event_Ticket_Pricing/Data/Spotify Chart Names.csv')

Base = declarative_base()

class Artist(Base):
	__tablename__ = "artists_test"

	# id = Column('id', Integer, primary_key=True)
	# username = Column ('username', String(50), unique = True)

	artist_name = Column('artist', String(50), unique=True, primary_key=True)
	genre = Column('genre', String(50))
	followers = Column('genre', Integer)
	popularity = Column('popularity', Integer)
	playlist = Column('playlist', String(50))


class Artist_trimmed(Base):
	__tablename__ = "artists_trimmed_ranked_test"

	artist_name = Column('artist', String(50), unique=True, primary_key=True)
	genre = Column('genre', String(50))
	followers = Column('genre', Integer)
	popularity = Column('popularity', Integer)
	playlist = Column('playlist', String(50))


engine = create_engine('mysql://tickets_user:tickets_pass@ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com:3306/tickets_db', echo=True)
Base.metadata.create_all(bind=engine)
Session = sessionmaker(bind=engine)

session = Session()
artist = Artist()
artist.artist_name = 'TEST artist name'
artist.genre = 'TEST genre name'
artist.followers = 12
artist.popularity = 12
artist.playlist = 'TEST playlist'

session.add(artist)
session.commit()
session.close()



def playlist_artists(user_in, ID_in):

	"""
	USE SPOTIPY TO GET TRACKS FROM SPOTIFY PLAYLISTS
	"""

	raw_dat = spotify.user_playlist_tracks(user = user_in, playlist_id = ID_in)
	song_list = raw_dat['items']

	artist_df= pd.DataFrame()

	for song in song_list:

		try:

			artists = song['track']['artists']

			for artist in artists:
				
				artist_name = unidecode(str( (artist['name'].encode('utf-8')), encoding="utf-8"))
				artist_ID = str(artist['uri']).replace('spotify:artist:', '')

				artist_dat = spotify.artist(artist_id = artist_ID)
				artist_followers = artist_dat['followers']['total']
				artist_popularity = artist_dat['popularity']

				artist_array = pd.DataFrame([[artist_name, artist_ID, artist_followers, artist_popularity]], columns =['artist_name', 'artist_ID', 'artist_followers', 'artist_popularity'])

				artist_df = artist_df.append(artist_array)

		except TypeError as Err:
		
			artist_name = ' ' 
			artist_ID = ' ' 
			artist_followers = ' ' 
			artist_popularity = ' '
			artist_array = pd.DataFrame([[artist_name, artist_ID, artist_followers, artist_popularity]], columns =['artist_name', 'artist_ID', 'artist_followers', 'artist_popularity'])
				
			artist_df = artist_df.append(artist_array)
		
	return artist_df







def artists_to_db(db_endpoint):

	"""
	MAIN SPOTIFY ARTIST FUNCTION

	INGESTS
	- A MANUALLY COLLECTED CSV WITH SPOTIFY PLAYLIST NAMES
	- A FUNCTION 'id_gen' THAT PRODUCES SPOTIFY PLAYLIST IDS FROM A SPOTIFY PLAYLIST NAME
	- A FUNCTION 'playlist_artists' THAT PRODUCES A DATAFRAME OF ARTISTS IN A SPOTIFY PLAYLIST

	CREATES
	- A TABLE 'Artists_expanded' WHICH INCLUDES ALL ARTISTS IN ALL THE SPOTIFY PLAYLISTS IN THE MANUALLY COLLECTED CSV
	- A TABLE 'Artists_trimmed_ranked' WHICH INCLUDES DISTINCT ARTISTS, IN ORDER OF POPULARITY, FROM THAT SAME CSV

	:return:
	"""

	playlist_IDs=pd.DataFrame()

	"""LOOP THROUGH MANUALLY ENTERED LIST OF SPOTIFY PLAYLISTS, CALLING FUNCTIONS ON EACH PLAYLIST"""
	for playlist in Spotify_Playlist_list.iterrows():

		title=("'" + (playlist[1]['Playlist Name']) + "'")
		playlist_Name = (playlist[1]['Playlist Name'])
		genre=(playlist[1]['Genre'])
		playlist_ID = id_gen(title)[0]
		playlist_User = id_gen(title)[1]
		
		each_Playlist = pd.DataFrame([[playlist_Name, genre, playlist_ID, playlist_User]], columns=['playlist_Name', 'genre', 'playlist_ID', 'playlist_User'])
		
		playlist_IDs = playlist_IDs.append(each_Playlist)

	"""STORE DATA IN MYSQL"""
	connection=pymysql.connect(host = 'ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user = 'tickets_user', password = 'tickets_pass', db = 'tickets_db')
	cursor=connection.cursor()

	delete_QL = 'DELETE FROM Artists_expanded;'
	
	cursor.execute(delete_QL)
	connection.commit()

	for playlist_ID in playlist_IDs.iterrows():
		
		each_Name = ((playlist_ID[1]['playlist_Name']))
		each_genre = ((playlist_ID[1]['genre']))
		each_ID = ((playlist_ID[1]['playlist_ID']))
		each_User = ((playlist_ID[1]['playlist_User']))

		"""CALL THE FUNCTION 'playlist_artists' TO GET ARTIST LIST FOR EVERY PLAYLIST OF INTEREST"""
		Artists_df = playlist_artists(each_User, each_ID)
		
		for artist in Artists_df.iterrows():

			print(artist)

			"""MYSQL DATA PREP AND INSERTION"""
			artist_name = ((artist[1]['artist_name'])).replace('"', ' ')
			id = ((artist[1]['artist_ID']))
			followers = ((artist[1]['artist_followers']))
			popularity = ((artist[1]['artist_popularity']))

			artist = Artist()
			artist.artist_name = artist_name
			artist.genre = id
			artist.followers = followers
			artist.popularity = popularity
			artist.playlist = each_Name

			# artist_QL = 'INSERT INTO Artists_test(artist, genre, followers, popularity, playlist, artist_id) VALUES ("%s", "%s", "%s", "%s", "%s", "%s");' %(artist_name, each_genre, followers, popularity, each_Name, id)

			# cursor.execute(artist_QL)
			# connection.commit()

			session.add(artist)

	session.commit()
	session.close()

	"""DE-DUPLICATE AND RANK ARTISTS LIST"""
	drop_QL = 'DROP TABLE Artists_trimmed_ranked;'

	create_QL = 'CREATE TABLE Artists_trimmed_ranked AS SELECT DISTINCT artist, popularity, max(followers) AS current_followers, artist_id FROM Artists_expanded GROUP BY artist_id order by popularity desc;'

	connection = db_endpoint
	cursor = connection.cursor()
	cursor.execute(drop_QL)
	cursor.execute(create_QL)






#
# """DB ENDPOINT DEFINITION"""
# db = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user='tickets_user', password='tickets_pass', db='tickets_db')
#
# """SPOTIFY FUNCTION CALLS"""
# playlist_IDs=pd.DataFrame()
# generate_token()
# spotify = spotipy.Spotify(auth=generate_token())
# artists_to_db(db)
#
# """STUBHUB FUNCTION CALLS"""
# token = keys(b'knI4wisTkeBR4txGgGzUiHvpgAHPfWp8', b'Y37FpPHhIiHJdrWL', 'pluug3123@gmail.com', 'Hester3123')
# artists_with_events(token, db)