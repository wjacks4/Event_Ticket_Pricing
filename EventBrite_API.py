# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 15:00:58 2019

@author: bswxj01
"""

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