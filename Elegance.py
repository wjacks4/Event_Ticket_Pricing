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
    
    # def __init__(self, Spotify_client_ID: str, Spotify_client_secret: str, uses: int =0 ):
    #     self.Spotify_client_ID = Spotify_client_ID
    #     self.Spotify_client_secret = Spotify_client_secret
    #     self.uses = uses

    Spotify_client_ID: str
    Spotify_client_secret: str
    uses: int

cred_list = [Credential(Spotify_client_ID = "ab3b70083f5f469188f8e49b79d5eadb", Spotify_client_secret = "6ecf81925e2740c9adecaad28685457a", uses = 10),
             Credential(Spotify_client_ID = "TEST", Spotify_client_secret = "TEST_SECRET", uses = 10)]

def get_tokens(cred_list):

    """
    LOOPS THROUGH CREDENTIALS, SEEING IF THEY WORK
    """
    for cred in cred_list:
         token = generate_token(cred_list.Spotify_client_ID, cred_list.Spotify_client_secret)
         yield from itertools.repeat(token, cred.n)


def generate_token(client_ID_in, client_secret_in):

    """
    GETS SPOTIFY ACCESS TOKEN
    """
    credentials = SpotifyClientCredentials(
        client_id = client_ID_in,
        client_secret = client_secret_in
    )
	
    token = credentials.get_access_token()
    # print(token)
    return token


# Spotify_Playlist_list = pd.read_csv('C:/Users/wjack/Desktop/Event_Ticket_Pricing/Event_Ticket_Pricing/Data/Spotify Chart Names.csv')
Spotify_Playlist_list = pd.read_csv('C:/Users/bswxj01/Desktop/Event_Ticket_Pricing/Data/Spotify Chart Names.csv')

"""
INITIALIZE ACCESS TO SPOTIPY ENGINE
"""

spotify_access = spotipy.Spotify(auth=get_tokens(cred_list))

def id_gen(name):

	"""
	FUNCTION THAT GETS PLAYLIST IDs FOR EACH OF THE NAMES OF PLAYLISTS THAT I PASS IT
	"""

	raw_Dat = spotify_access.search(q=name, type='playlist')

	encoded_Dat = str(raw_Dat).encode('utf-8')
	
	playlist_ID = raw_Dat['playlists']['items'][0]['id']
	playlist_Name = raw_Dat['playlists']['items'][0]['name']
	playlist_User = raw_Dat['playlists']['items'][0]['owner']['id']
	
	# print(playlist_ID)
	# print(playlist_Name)
	# print(playlist_User)
	
	return playlist_ID, playlist_User



def artists_to_db():

	playlist_IDs=pd.DataFrame()

	"""LOOP THROUGH MANUALLY ENTERED LIST OF SPOTIFY PLAYLISTS, CALLING FUNCTIONS ON EACH PLAYLIST"""
	for playlist in Spotify_Playlist_list.head(20).iterrows():

        
        
		title=("'" + (playlist[1]['Playlist Name']) + "'")
		playlist_Name = (playlist[1]['Playlist Name'])
		genre=(playlist[1]['Genre'])
		playlist_ID = id_gen(title)[0]
		playlist_User = id_gen(title)[1]
		
        print(playlist_Name)
        

artists_to_db()











"""
DEFINE CLASS FOR DATA INSERTION
"""

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
    
    
    
    

