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


TestQL = "INSERT INTO TEST_Table(event_name, event_venue, city, event_date, sale_start, lowest_price) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" %(gameIDrep, awayTeam, homeTeam, openSpreadAway, openSpreadHome, currentSpreadAway, currentSpreadHome, publicBetsAway, publicBetsHome, publicMoneyAway, publicMoneyHome, currentOddsAway, currentOddsHome)

print(TestQL)

connection=MySQLdb.connect('localhost', 'root', 'root', 'book')
cursor=connection.cursor()

cursor.execute(TestQL)
#data=cursor.fetchall()
connection.commit()

