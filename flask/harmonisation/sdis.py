# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 12:54:46 2020

@author: aboutet
"""
from functions import list_directory, getContext, find_last_id
import pandas as pd
import sys
import Get_Location as gl
from datetime import datetime, timedelta

def sdis_before(es, infos, insert):    
    id_sdis = find_last_id(es, "id_sdis", infos["accidents"])
    
    return {"id_sdis":id_sdis}

def sdis(es, infos, data, documents):  
      
    to_send = []
        
    for json in data:
        documents["id_sdis"]+=1
        # Find the lat, lon of the accident
        print(json["geometry"])
        json["point"] = float(json["geometry"]["lon"]), float(json["geometry"]["lat"])
        point = json["point"]
        json = {
                "Date":json["Date"]+":00",
                "point":json["point"],
                "TYPE":"MATERIEL",
                "ID_ACCIDENT":json["ID"],
                "id_sdis":documents["id_sdis"],
                "Libelle":json["Libellé"]
            }
        link_ids = gl._get_link_in_envelope(es, point, index_name=infos["here"])
        link_id = gl._get_closest_link(point, link_ids)
    
        if link_id[2] == 0:
            continue
    
        resp = es.search(index=infos["here"], body={"query":{"bool":{"must":{"term":{"_id":link_id[2]}}}}})
        j = resp["hits"]["hits"][0]["_source"]
        new_json = {key:value for key, value in list(json.items()) + list(j.items())}
              
    
        date =  datetime.strptime(new_json["Date"], "%d/%m/%Y %H:%M:%S")
        
        date_inf = date - timedelta(minutes=15)
        date_sup = date + timedelta(minutes=15)

        # Not take something already present
        query = {
                "query":{
                    "bool":{
                        "should":[
                            {
                            "bool":{
                                "must":[
                                    {
                                    "match":{
                                        "ID_ACCIDENT": json["ID_ACCIDENT"]
                                        }
                                    }
                                    ]
                                }        
                            },{
                            "bool":{
                                "must":[
                                    {
                                    "range" : {
                                        "Date" : {
                                            "gte" : str(date_inf),
                                            "lte" : str(date_sup)
                                            }
                                        }
                                    },{
                                    "match":{
                                        "LINK_ID": new_json["LINK_ID"]
                                        }
                                    },
                                    ],
                                    "must_not":{
                                        "exists":{
                                            "field":"id_sdis"
                                            }
                                        }
                                }
                            
                            }
                            ]
                        }
                    }
                }
        
        r = es.search(index=infos["accidents"], body=query)
        if r["hits"]["hits"]:
            continue


        # ADD value of meteo base
        date_meteo = date.replace(minute=0, second=0)
        if  date.minute > 29:
            date_meteo = date_meteo + timedelta(hours=1)
        query = {
                "query":{
                    "bool":{
                        "must":{
                            "match":{
                                "Date":str(date_meteo)
                                }
                            }
                        }
                    }
                } 
        r = es.search(index=infos["meteo"], body=query)
        if r["hits"]["hits"]:
            meteo_json = r["hits"]["hits"][0]["_source"]
            new_json = {key:value for key, value in list(meteo_json.items()) + list(new_json.items())}

         # Add context
        j = getContext(date)
        new_json = {key:value for key, value in list(j.items()) + list(new_json.items())}
                
        to_send.append({"index":{}})
        to_send.append(new_json)
    
    if to_send:
        resp = es.bulk(index=infos["accidents"], body=to_send, request_timeout=3000)

def sdis_after(es, infos):
    return