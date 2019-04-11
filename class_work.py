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



class Employee_v1:
	pass


#BOTH OF THESE ARE 'INSTANCES' OF THE EMPLOYEE CLASS#

emp_1 = Employee_v1()
emp_2 = Employee_v1()


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


#--------------OK NOW WE START USING INIT METHODS---------------#


class Employee_v2:

	#THIS IS AN 'INSTANCE'#
	#IT RECEIVES THE INSTANCE OF THE CLASS AS ITS FIRST ARGUMENT AUTOMATICALLY#
	#THE INSTANCE IS USUALLY CALLED 'SELF'#
	
	def __init__(self, first, last, pay):
		self.first = first
		self.last = last
		self.pay = pay

		
	

















