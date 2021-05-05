# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 12:20:42 2020

@author: aboutet
"""

import datetime
import pandas as pd
from functions import getDayMonthYear, index_on_date

def meteo_before(es, infos, insert):
        return {}

def meteo(es, infos, data, documents):    
    to_send = []
    
    for json in data:
        if "Date" not in json.keys():
            continue

        if json["Annee"] < 2009:
            continue
        for key, value in json.items():
            if (isinstance(value, float) or isinstance(value, int)) and (abs(value) == 9999 or abs(value) == 999.9):
                json[key] = None        

        to_send.append({"index":{"_id":json["Date"]}})
        to_send.append(json)
            
    if to_send:
        resp = es.bulk(index=infos["meteo"], body=to_send, request_timeout=3000)
            
def meteo_after(es, infos):
    return
