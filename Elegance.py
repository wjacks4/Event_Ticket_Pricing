import itertools
from dataclasses import dataclass
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.client import Spotify
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy .ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import pandas as pd
import math

@dataclass
class Credential:

	Spotify_client_ID: str
	Spotify_client_secret: str
	uses: int

cred_list = [Credential(Spotify_client_ID = "ab3b70083f5f469188f8e49b79d5eadb", Spotify_client_secret = "6ecf81925e2740c9adecaad28685457a", uses = 10)]

def get_tokens(cred_list_in):

	"""
	LOOPS THROUGH CREDENTIALS, SEEING IF THEY WORK
	"""
	for cred in cred_list_in:
		token = generate_token(cred.Spotify_client_ID, cred.Spotify_client_secret)
		yield from itertools.repeat(token, cred.n)


def generate_token(client_ID_in, client_secret_in):

	print(client_ID_in)
	"""
	GETS SPOTIFY ACCESS TOKEN
	"""
	credentials = SpotifyClientCredentials(
		client_id = client_ID_in,
		client_secret = client_secret_in
	)

	token = credentials.get_access_token()
	print(token)
	return token

Spotify_Playlist_list = pd.read_csv('C:/Users/wjack/Desktop/Event_Ticket_Pricing/Event_Ticket_Pricing/Data/Spotify Chart Names.csv')
# Spotify_Playlist_list = pd.read_csv('C:/Users/bswxj01/Desktop/Event_Ticket_Pricing/Data/Spotify Chart Names.csv')

"""
INITIALIZE ACCESS TO SPOTIPY ENGINE
"""

get_tokens(cred_list)

spotify_access = spotipy.Spotify(auth=get_tokens(cred_list))



def id_gen():

	"""
	FUNCTION THAT GETS PLAYLIST IDs FOR EACH OF THE NAMES OF PLAYLISTS THAT I PASS IT
	"""
	for playlist in Spotify_Playlist_list.iterrows():

		title=("'" + (playlist[1]['Playlist Name']) + "'")

		raw_Dat = spotify_access.search(q=title, type='playlist')

		encoded_Dat = str(raw_Dat).encode('utf-8')

		# playlist_ID = raw_Dat['playlists']['items'][0]['id']
		# playlist_Name = raw_Dat['playlists']['items'][0]['name']
		# playlist_User = raw_Dat['playlists']['items'][0]['owner']['id']

		# print(playlist_ID)
		# print(playlist_Name)
		# print(playlist_User)

# id_gen()