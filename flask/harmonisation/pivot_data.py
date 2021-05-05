# -*- coding: utf-8 -*-
"""
Created on Mon Dec  9 10:51:03 2019

@author: aboutet
"""
import os
import sys
import time

import json
from get_connection import get_connection
import json
from datetime import datetime, date

from functions import typeOfJson, create_error
from meteo import meteo_before, meteo, meteo_after
from here import here_before, here, here_after
from street_pattern import street_pattern_before, street_pattern, street_pattern_after
from admin import admin_before, admin, admin_after
from corine import corine_before, corine, corine_after
from corporel import corporel_before, corporel, corporel_after
from materiel import materiel_before, materiel, materiel_after
from sdis import sdis_before, sdis, sdis_after
from alerte_waze import waze_before, waze, waze_after
from alerte_coyote import coyote_before, coyote, coyote_after
from trafic_coyote import trafic_before, trafic, trafic_after

def pivot(infos, insert=True):
    print('Try Harmonisating')
    date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    name = infos["to_send"]
    # Open elasticsearch
    es = get_connection()
    
    # If it's not curently harmonisate
    infos_indexes = "infos-indexes"
    query = {
            "query":{
                "bool":{
                    "must":{
                        "match":{
                            "name.keyword":name
                            }
                        }
                }
            }
        }
    resp = es.search(index=infos_indexes, body=query)
    error = "Successfully harmonisate "+str(name)+" to Elasticsearch"
    if resp["hits"]["hits"][0]["_source"]["current"] == "Harmonisating":
        error = "Already harmonisating "+name
        create_error(es, name, date, 'harmonise', error)
        print(error)
        return

    with open("harmonisation/mapping_harmonise.json", "r") as mapping_file:
        mappings = json.load(mapping_file)
    
    print("start integration")
    n = 1000
    
    

    
    # Get mapping of index
    map = es.search(index=infos["to_send"], body={"size":1})["hits"]["hits"][0]["_source"]             
    json_type = typeOfJson(infos["to_send"], map, category=infos["category"])
    print(json_type)
    index_name = infos["type_mapping"][json_type]
    pivot = infos["pivot_data"][json_type]
    
    infos["mapping"] = mappings[index_name]
    infos["name"] = index_name    
    infos["scroll"] = pivot["scroll"]
    infos["timeout"] = pivot["timeout"]
    
    resp = es.indices.create(index=infos[index_name], ignore=400, body={"mappings":infos["mapping"]})
    query = {
            "query":{
                "bool":{
                    "must":{
                        "match":{
                            "name.keyword":name
                            }
                        }
                }
            },
            "script": "ctx._source.current = 'Harmonisating';"
        }
        
    resp = es.update_by_query(index=infos_indexes, body=query, request_timeout=30000) 
    
    try:
        
        documents = pivot["before"](es, infos, insert)
        # get data from elasticsearch by bulk
        res = es.search(index=infos["to_send"], body={"size":n},scroll=infos["scroll"],
              request_timeout=infos["timeout"])
        
        while res["hits"]["hits"]:
            scroll = res["_scroll_id"]
            data = res["hits"]["hits"]
            
            if "keep" in documents.keys():
                data = [
                    {
                        key:value 
                        for key, value in json["_source"].items() if key in documents["keep"]
                     } 
                    for json in data
                    ]
            else:
                data = [json["_source"] for json in data]
            print("current")
            pivot["current"](es, infos, data, documents)
                
            # Continue with other documents
            res = es.scroll(scroll_id = scroll, scroll = infos["scroll"],
              request_timeout=infos["timeout"])
        pivot["after"](es, infos)
        print("end_after")

        query = {
            "query":{
                "bool":{
                    "must":{
                        "match":{
                            "name.keyword":name
                            }
                        }
                }
            },
            "script": "ctx._source.current = 'harmonise';ctx._source.harmonise = 1;"
        }
        
        resp = es.update_by_query(index=infos_indexes, body=query, request_timeout=30000) 
    except Exception as e:
        print(e)
        time.sleep(1)
        error = str(e)
        query = {
            "query":{
                "bool":{
                    "must":{
                        "match":{
                            "name.keyword":name
                            }
                        }
                }
            },
            "script": "ctx._source.current = 'Send';"
        }
        
        resp = es.update_by_query(index=infos_indexes, body=query, request_timeout=30000)


    finally:
        create_error(es, name, date, 'harmonise', error)


if __name__ == "__main__":
    print(pivot)
    pivot_data = {
        "admin":
            {
                "scroll":"60m",
                "timeout":300,
                "before": admin_before,
                "current":admin,
                "after": admin_after
            },
        "corine":
            {
                "scroll":"60m",
                "timeout":300,
                "before": corine_before,
                "current":corine,
                "after": corine_after
            },
        "here":
            {
                "scroll":"30m",
                "timeout":30,
                "before": here_before,
                "current":here,
                "after": here_after
            },
        "street_pattern":
            {
                "scroll":"30m",
                "timeout":30,
                "before": street_pattern_before,
                "current":street_pattern,
                "after": street_pattern_after
            },
        "trafic":
            {
                "scroll":"30m",
                "timeout":30,
                "before": trafic_before,
                "current":trafic,
                "after": trafic_after
            },
        "coyote":
            {
                "scroll":"30m",
                "timeout":30,
                "before": coyote_before,
                "current":coyote,
                "after": coyote_after
            },
        "waze":
            {
                "scroll":"30m",
                "timeout":30,
                "before": waze_before,
                "current":waze,
                "after": waze_after
            },
        "corporels":
            {
                "scroll":"30m",
                "timeout":30,
                "before": corporel_before,
                "current":corporel,
                "after": corporel_after
            },
        "materiels":
            {
                "scroll":"30m",
                "timeout":30,
                "before": materiel_before,
                "current":materiel,
                "after": materiel_after
            },
        "sdis":
            {
                "scroll":"30m",
                "timeout":30,
                "before": sdis_before,
                "current":sdis,
                "after": sdis_after
            },
        "meteo":
            {
                "scroll":"5m",
                "timeout":30,
                "before": meteo_before,
                "current":meteo,
                "after": meteo_after
            }
        }

    type_mapping = {
        "meteo":"meteo",
        "here":"here",
        "street_pattern":"here",
        "admin":"admin",
        "corine":"here",
        "corporels":"accidents",
        "materiels":"accidents",
        "sdis":"accidents",
        "coyote":"accidents",
        "waze":"accidents",
        "trafic":"here"
        
        }

    infos = {
        "to_send":json.loads(sys.argv[1])["name"],
        "here":"here",
        "meteo":"meteo",
        "accidents":"accidents",
        "admin":"communes",
        "street_pattern":"street_pattern",
        "trafic":"trafic",
        "corine":"corine",
        "category":"",
        "type_mapping":type_mapping,
        "pivot_data":pivot_data
        }
    print("pivot_data")
    pivot(infos)

