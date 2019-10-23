"""
SPOTIFY API DATA PULL
"""

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


def generate_token():

	"""
	GETS SPOTIFY ACCESS TOKEN
	"""
	
	credentials = SpotifyClientCredentials(
		client_id = Spotify_client_ID,
		client_secret = Spotify_client_secret
	)
	
	token = credentials.get_access_token()
	print(token)
	return token


def id_gen(name):

	"""
	FUNCTION THAT GETS PLAYLIST IDs FOR EACH OF THE NAMES OF PLAYLISTS THAT I PASS IT
	"""

	raw_Dat = spotify.search(q=name, type='playlist')

	encoded_Dat = str(raw_Dat).encode('utf-8')
	
	playlist_ID = raw_Dat['playlists']['items'][0]['id']
	playlist_Name = raw_Dat['playlists']['items'][0]['name']
	playlist_User = raw_Dat['playlists']['items'][0]['owner']['id']
	
	# print(playlist_ID)
	# print(playlist_Name)
	# print(playlist_User)
	
	return playlist_ID, playlist_User



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

			artist_QL = 'INSERT INTO Artists_expanded(artist, genre, followers, popularity, playlist, artist_id) VALUES ("%s", "%s", "%s", "%s", "%s", "%s");' %(artist_name, each_genre, followers, popularity, each_Name, id)

			cursor.execute(artist_QL)
			connection.commit()

	"""DE-DUPLICATE AND RANK ARTISTS LIST"""
	drop_QL = 'DROP TABLE Artists_trimmed_ranked;'

	create_QL = 'CREATE TABLE Artists_trimmed_ranked AS SELECT DISTINCT artist, popularity, max(followers) AS current_followers, artist_id FROM Artists_expanded GROUP BY artist_id order by popularity desc;'

	connection = db_endpoint
	cursor = connection.cursor()
	cursor.execute(drop_QL)
	cursor.execute(create_QL)

# def artist_trim ():
#
# 	drop_QL = 'DROP TABLE Artists_trimmed_ranked;'
#
# 	create_QL = 'CREATE TABLE Artists_trimmed_ranked AS SELECT DISTINCT artist, popularity, max(followers) AS current_followers, artist_id FROM Artists_expanded GROUP BY artist_id order by popularity desc;'
#
# 	connection=pymysql.connect(host = 'ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user = 'tickets_user', password = 'tickets_pass', db = 'tickets_db')
# 	cursor=connection.cursor()
#
# 	cursor.execute(drop_QL)
# 	cursor.execute(create_QL)
#
# 	connection.commit()


class keys:

	"""

	STREAMLINE RETREIVAL OF STUBHUB ACCESS TOKENS BY PASSING THIS CLASS EACH ACCOUNT'S
	- KEY
	- SECRET
	- USERNAME
	- PASSWORD

	"""

	def __init__(self, key, secret, username, password):

		self.key_encode = (base64.standard_b64encode(key + b":" + secret)).decode("utf-8")

		base_url = 'https://api.stubhub.com/sellers/oauth/accesstoken'
		query_params = 'grant_type=client_credentials'
		request_url = (base_url + "?" + query_params)
		header_auth = ('Basic ' + (base64.standard_b64encode(key + b":" + secret)).decode("utf-8"))

		payload = {"username": username, "password": password}
		headers = {"Authorization": header_auth, "Content-Type": "application/json"}

		req = requests.post(request_url, data=json.dumps(payload), headers=headers)
		json_obj = req.json()
		self.token = json_obj['access_token']


# def data_fetch_pymysql():
#
# 	connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user='tickets_user',
# 								 password='tickets_pass', db='tickets_db')
#
# 	Artists_DF = pd.read_sql('SELECT * FROM Artists_trimmed_ranked WHERE current_followers >= 20000 and current_followers <= 500000 order by current_followers desc', con=connection)
# 	return Artists_DF


def artists_with_events(access_token, db_endpoint):

	connection = db_endpoint
	cursor = connection.cursor()

	# Artists_df = data_fetch_pymysql().head(500)
	Artists_DF = (pd.read_sql('SELECT * FROM Artists_trimmed_ranked WHERE current_followers >= 20000 and current_followers <= 500000 order by current_followers desc', con=connection)).head(500)

	base_url = 'https://api.stubhub.com/sellers/search/events/v3'

	"""STAGE SQL TABLES"""
	drop_QL = 'DROP TABLE ARTISTS_WITH_EVENTS;'
	cursor.execute(drop_QL)
	connection.commit()

	create_QL = 'CREATE TABLE ARTISTS_WITH_EVENTS(artist varchar (256), ' \
				'artist_id varchar(256), ' \
				'popularity int, ' \
				'current_followers int,' \
				'event_count int);'
	cursor.execute(create_QL)
	connection.commit()

	for artist_dat in Artists_DF.iterrows():

		"""EXTRACT ARTIST STRINGS FROM DF"""
		spotify_artist = artist_dat[1]['artist']
		spotify_artist_id = artist_dat[1]['artist_id']
		spotify_artist_popularity = artist_dat[1]['popularity']
		spotify_artist_followers = artist_dat[1]['current_followers']

		"""ENCODE ARTIST STRINGS FOR STUBHUB API CALL"""
		artist_encode = spotify_artist.replace(" ", "%20")
		query_params = ("q=" + artist_encode + "&" + "rows=100")
		artist_url = (base_url + "?" + query_params)

		try:
			Auth_Header = ("Bearer " + access_token.token)
			headers = {"Authorization": Auth_Header, "Accept": "application/json"}
			req = requests.get(artist_url, headers=headers)
			json_obj = req.json()

			event_list = json_obj['events']
			event_count = len(event_list)

			insert_tuple = (spotify_artist, spotify_artist_id, spotify_artist_popularity, spotify_artist_followers, event_count)

			insert_QL = 'INSERT INTO `ARTISTS_WITH_EVENTS` (`artist`, `artist_id`, `popularity`, `current_followers`, `event_count`) VALUES (%s, %s, %s, %s, %s)'

			cursor.execute(insert_QL, insert_tuple)
			connection.commit()

		except KeyError:
			print(KeyError)
			# print('exceeded quota for stubhub API')


"""GLOBAL DATA"""
Spotify_client_ID = 'ab3b70083f5f469188f8e49b79d5eadb'
Spotify_client_secret = '6ecf81925e2740c9adecaad28685457a'
Spotify_Playlist_list = pd.read_csv('C:/Users/wjack/Desktop/Event_Ticket_Pricing/Event_Ticket_Pricing/Data/Spotify Chart Names.csv')


"""DB ENDPOINT DEFINITION"""
# db = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user='tickets_user', password='tickets_pass', db='tickets_db')

"""SPOTIFY FUNCTION CALLS"""
playlist_IDs=pd.DataFrame()
generate_token()
spotify = spotipy.Spotify(auth=generate_token())
# artists_to_db(db)

"""STUBHUB FUNCTION CALLS"""
# token = keys(b'knI4wisTkeBR4txGgGzUiHvpgAHPfWp8', b'Y37FpPHhIiHJdrWL', 'pluug3123@gmail.com', 'Hester3123')
# artists_with_events(token, db)


	


	







	






