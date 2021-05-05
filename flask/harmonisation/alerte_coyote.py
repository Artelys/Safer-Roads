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

def coyote_before(es, infos, insert):
    id_coyote = find_last_id(es, "id_coyote", infos["accidents"])
    return {"id_coyote":id_coyote}

def coyote(es, infos, data, documents):  
      
    to_send = []
    
    for json in data:
        documents["id_coyote"]+=1
        json["id_coyote"] = documents["id_coyote"]
        json["ID_INCIDENT"] = documents["id_coyote"]
        json["point"] = json["point"] = float(json["geometry"]["lon"]), float(json["geometry"]["lat"])
    
        if "Type" in json.keys():
            type = json["Type"]
            if type == "24-122":
                json["Type"] = "Rétrécissement"
            elif type == "25-0":
                json["Type"] = "Accident"
            elif type == "26-0":
                json["Type"] = "Véhicule arrêté"
            elif type == "28-132":
                json["Type"] = "Chaussée dégradée"
            elif type == "28-133":
                json["Type"] = "Visibilité réduite"
            elif type == "28-139":
                json["Type"] = "Route glissante"
            else:
                json["Type"] = ""
        else:
            json["Type"] = json["TYPE"]        
        
        json["TYPE"] = "INCIDENT_COYOTE"
        
        resp = es.search(index=infos["here"], body={"query":{"bool":{"must":{"term":{"LINK_ID":json["LINK_ID"]}}}}})
        if not resp["hits"]["hits"]:
            continue
        j = resp["hits"]["hits"][0]["_source"]
        new_json = {key:value for key, value in list(json.items()) + list(j.items())}
        
        to_send.append({"index":{}})
        to_send.append({key:value for key, value in new_json.items()})
    
    if to_send:
        resp = es.bulk(index=infos["accidents"], body=to_send, request_timeout=3000)

    return {}

def coyote_after(es, infos):
    return

