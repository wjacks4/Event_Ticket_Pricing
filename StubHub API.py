# -*- coding: utf-8 -*-
"""
Created on Fri Apr  5 09:35:23 2019

@author: bswxj01
"""

import urllib
import pandas as pd
import numpy as np


base_url = ('https://api.seatgeek.com/2/')

client_id = ('MTM4MTIyMDZ8MTU1NDQ3MTkxMy43Ng')

client_secret = ('c49766eaad2bc8bc33810d112d141ca9a09b0a78b1be52c459eb19c5fd3527a5')

events_endpoint = (base_url + 'events?' + 'client_id=' + client_id + '&' + 'client_secret=' + client_secret)

print(events_endpoint)


urllib.request.urlopen(events_endpoint)