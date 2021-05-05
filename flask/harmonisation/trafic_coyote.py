# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 11:53:09 2020

@author: aboutet
"""


from functions import list_directory
import pandas as pd
import sys
sys.path.insert(0, "../geolocalisation")
import Get_Location as gl
from datetime import datetime, timedelta


def trafic_before(es, infos, insert):    
    return {}
    
def trafic(es, infos, data, documents):  
      
    to_send = []
        
    for json in data:

        resp = es.search(index=infos["here"], body={"query":{"bool":{"must":{"term":{"LINK_ID":json["LINK_ID"]}}}}}, request_timeout=3000)
        if not resp["hits"]["hits"]:
            print("no link_id")
            continue
        id = resp["hits"]["hits"][0]["_id"]
        
        
        to_send.append({"update":{"_id":id}})
        to_send.append({"doc":{"Trafic":json["Trafic"]}})
    
    if to_send:
        resp = es.bulk(index=infos["here"], body=to_send, request_timeout=3000)

def trafic_after(es, infos):
    # Add coeff of normalisation
    query = {
            "query":{
                "match_all":{
                }
            },
            "script": "ctx._source.coeff_normalisation = 1 / (ctx._source.Trafic * ctx._source.Meters);"
        }


    resp = es.update_by_query(index=infos["here"], body=query, request_timeout=10000) 
    print("final")
    types = ["Type_Voie.keyword", "Categorie_Route.keyword", "Type_Zone.keyword", "Vitesse_Autorisee.keyword"]
    names = ["coeff_normalisation_type_voie", "coeff_normalisation_categorie_route", "coeff_normalisation_type_zone", "coeff_normalisation_vitesse_autorisee"]
    for i, type in enumerate(types):
        query = {
                "aggs":{
                    type:{
                        "terms":{
                            "field":type,
                            "size":30
                            },
                            "aggs":{
                                "sum":{
                                    "sum":{
                                        "field":"Meters"
                                        }
                                    },
                                "avg":{
                                    "avg":{
                                        "field":"Trafic"
                                        }
                                    }
                                }
                        }
                    },
                "size":0
                }
        resp = es.search(index=infos["here"], body=query, request_timeout=30000) 
        es.indices.refresh(index=infos["here"])
        buckets = resp["aggregations"][type]["buckets"]
        print(buckets)
        for bucket in buckets:
            if bucket["avg"]["value"] is None:
                norm = 0
            else:
                norm = 1 / (bucket["avg"]["value"]*bucket["sum"]["value"])
            query = {
                "query":{
                    "bool":{
                        "must":{
                            "match":{
                                type:bucket["key"]
                                }
                            }
                    }
                },
                "script": "ctx._source."+names[i]+" = "+str(norm)+";"
            }
            
            resp = es.update_by_query(index=infos["here"], body=query, request_timeout=300000) 
