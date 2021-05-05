# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 17:41:43 2020

@author: aboutet
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 12:54:46 2020

@author: aboutet
"""
from functions import list_directory, find_last_id
import pandas as pd
import sys
import Get_Location as gl
from datetime import datetime, timedelta

def waze_before(es, infos, insert):    
    id_waze = find_last_id(es, "id_waze", infos["accidents"])
    return {"id_waze":id_waze}

def waze(es, infos, data, documents):  
      
    to_send = []
        
    for json in data:
        documents["id_waze"]+=1
        json["id_waze"] = documents["id_waze"]
        json["point"] = json["point"] = float(json["geometry"]["lon"]), float(json["geometry"]["lat"])  
        
        resp = es.search(index=infos["here"], body={"query":{"bool":{"must":{"term":{"LINK_ID":json["LINK_ID"]}}}}})
        if not resp["hits"]["hits"]:
            continue
        j = resp["hits"]["hits"][0]["_source"]
        new_json = {key:value for key, value in list(json.items()) + list(j.items())}
        
        new_json["ID_INCIDENT"] = json["ID"]
        new_json["Type"] = new_json["TYPE"]
        new_json["TYPE"] = "INCIDENT_WAZE"
                
        to_send.append({"index":{}})
        to_send.append({key:value for key, value in new_json.items()})
    
    if to_send:
        resp = es.bulk(index=infos["accidents"], body=to_send, request_timeout=300)

def waze_after(es, infos):
    return