"""
SPOTIFY API DATA PULL
"""

# import mysqlclient as mysql
# from mysql.connector import Error
# import psycopg2 as p
import json
# from dateutil import parser
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
import boto3
from spotipy.client import Spotify
import requests
import urllib
import pymysql
from urllib import parse

"""
GLOBAL DATA
"""

Spotify_client_ID = 'ab3b70083f5f469188f8e49b79d5eadb'
Spotify_client_secret = '6ecf81925e2740c9adecaad28685457a'
Spotify_Playlist_list = pd.read_csv(
    'C:/Users/wjack/Desktop/Event_Ticket_Pricing/Event_Ticket_Pricing/Data/Spotify Chart Names.csv')

"""
GENERATE ACCESS TOKEN
"""


def generate_token():
    credentials = SpotifyClientCredentials(
        client_id=Spotify_client_ID,
        client_secret=Spotify_client_secret
    )

    token = credentials.get_access_token()
    # print(token)
    return token


generate_token()

"""
CALL SPOTIFY API WITH TOKEN
"""

spotify = spotipy.Spotify(auth=generate_token())


# FUNCTION TO PULL IDS FOR ALL CHOSEN SPOTIFY PLAYLISTS#

def ID_Gen(name):
    raw_Dat = spotify.search(q=name, type='playlist')

    encoded_Dat = str(raw_Dat).encode('utf-8')

    playlist_ID = raw_Dat['playlists']['items'][0]['id']
    playlist_Name = raw_Dat['playlists']['items'][0]['name']
    playlist_User = raw_Dat['playlists']['items'][0]['owner']['id']

    # print(playlist_ID)
    # print(playlist_Name)
    # print(playlist_User)

    return playlist_ID, playlist_User


"""
FUNCTION TO RETURN LIST OF ARTISTS FROM A GIVEN PLAYLIST
"""


def Playlist_Artists(user_in, ID_in):
    """
    USE SPOTIPY TO GET TRACKS FROM 'USER' PLAYLIST..WHERE 'USER' IS SPOTIFY
    """

    raw_dat = spotify.user_playlist_tracks(user=user_in, playlist_id=ID_in)
    song_list = raw_dat['items']

    artist_df = pd.DataFrame()

    for song in song_list:

        try:

            artists = song['track']['artists']

            for artist in artists:
                artist_name = unidecode(str((artist['name'].encode('utf-8')), encoding="utf-8"))
                artist_ID = str(artist['uri']).replace('spotify:artist:', '')

                artist_dat = spotify.artist(artist_id=artist_ID)
                artist_followers = artist_dat['followers']['total']
                artist_popularity = artist_dat['popularity']

                artist_array = pd.DataFrame([[artist_name, artist_ID, artist_followers, artist_popularity]],
                                            columns=['artist_name', 'artist_ID', 'artist_followers',
                                                     'artist_popularity'])

                artist_df = artist_df.append(artist_array)

        except TypeError as Err:

            artist_name = ' '
            artist_ID = ' '
            artist_followers = ' '
            artist_popularity = ' '
            artist_array = pd.DataFrame([[artist_name, artist_ID, artist_followers, artist_popularity]],
                                        columns=['artist_name', 'artist_ID', 'artist_followers', 'artist_popularity'])

            artist_df = artist_df.append(artist_array)

    return (artist_df)


playlist_IDs = pd.DataFrame()

"""
FUCTION THAT CALLS BOTH THE ID GENERATOR FUNCTION AND THE ARTIST PLAYLIST FUNCTION
"""


def Artists_to_DB():
    playlist_IDs = pd.DataFrame()

    """
    LOOP THROUGH MANUALLY ENTERED LIST OF SPOTIFY PLAYLISTS, CALLING FUNCTIONS ON EACH PLAYLIST
    """
    for playlist in Spotify_Playlist_list.iterrows():
        title = ("'" + (playlist[1]['Playlist Name']) + "'")

        playlist_Name = (playlist[1]['Playlist Name'])
        genre = (playlist[1]['Genre'])

        playlist_ID = ID_Gen(title)[0]
        playlist_User = ID_Gen(title)[1]

        each_Playlist = pd.DataFrame([[playlist_Name, genre, playlist_ID, playlist_User]],
                                     columns=['playlist_Name', 'genre', 'playlist_ID', 'playlist_User'])

        playlist_IDs = playlist_IDs.append(each_Playlist)

    """
    THIS IS THE MYSQL WAY TO STORE DATA
    """
    connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user='tickets_user',
                                 password='tickets_pass', db='tickets_db')
    cursor = connection.cursor()

    delete_QL = 'DELETE FROM Artists_expanded;'

    cursor.execute(delete_QL)
    connection.commit()

    """
    AND THE DYNAMODB WAY TO STORE DATA
    """
    dynamodb = boto3.resource('dynamodb')
    dynamoTable = dynamodb.Table('Artist_Table')

    """
    LOOP THRU DATA AND STORE IN EACH RESPECTIVE SYSTEM
    """
    for playlist_ID in playlist_IDs.iterrows():

        each_Name = ((playlist_ID[1]['playlist_Name']))
        each_genre = ((playlist_ID[1]['genre']))
        each_ID = ((playlist_ID[1]['playlist_ID']))
        each_User = ((playlist_ID[1]['playlist_User']))

        Artists_df = Playlist_Artists(each_User, each_ID)

        for artist in Artists_df.iterrows():
            print(artist)

            """
            MYSQL DATA PREP AND INSERTION
            """
            artist_name = ((artist[1]['artist_name'])).replace('"', ' ')
            id = ((artist[1]['artist_ID']))
            followers = ((artist[1]['artist_followers']))
            popularity = ((artist[1]['artist_popularity']))

            artist_QL = 'INSERT INTO Artists_expanded(artist, genre, followers, popularity, playlist, artist_id) VALUES ("%s", "%s", "%s", "%s", "%s", "%s");' % (
            artist_name, each_genre, followers, popularity, each_Name, id)

            cursor.execute(artist_QL)
            connection.commit()

            """
            DYNAMO DATA INSERTION
            """
            artist_dat = {'artist': ((artist[1]['artist_name'])).replace('"', ' '), 'genre': each_genre,
                          'playlist': each_Name, 'artist_id': (artist[1]['artist_ID']),
                          'followers': (artist[1]['artist_followers']), 'popularity': (artist[1]['artist_popularity'])}
            print(artist_dat)

            dynamoTable.put_item(

                Item={
                    'artist_id': artist_dat['artist_id'],
                    'artist': artist_dat['artist'],
                    'genre': artist_dat['genre'],
                    'followers': artist_dat['followers'],
                    'popularity': artist_dat['popularity']
                }
            )


Artists_to_DB()


def Artist_trim():
    drop_QL = 'DROP TABLE Artists_trimmed;'

    create_QL = 'CREATE TABLE Artists_trimmed AS SELECT DISTINCT artist, popularity, max(followers) AS current_followers, artist_id FROM Artists_expanded GROUP BY artist_id;'

    # ----------CONNECT TO DB------------#
    connection = pymysql.connect(host='ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com', user='tickets_user',
                                 password='tickets_pass', db='tickets_db')
    cursor = connection.cursor()

    cursor.execute(drop_QL)
    cursor.execute(create_QL)

    connection.commit()