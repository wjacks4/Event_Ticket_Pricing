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



class Employee:
	pass


#BOTH OF THESE ARE 'INSTANCES' OF THE EMPLOYEE CLASS#

emp_1 = Employee()
emp_2 = Employee()


print(emp_1)
print(emp_2)


#WE WANT EMPLOYEE 1 TO HAVE FIRST NAME 'COREY' AND LAST NAME 'SCHAFER'#

emp_1.first='Corey'
emp_1.last='Schafer'
emp_1.pay=50000

#AND EMPLOYEE 2 TO HAVE DIFFERENT STUFF#

emp_2.first='Bob'
emp_2.last='Jones'
emp_2.pay=60000

#TEST#
print(emp_1.first)
print(emp_1.last)

print(emp_2.first)
print(emp_2.last)

