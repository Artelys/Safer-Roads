# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 11:53:09 2020

@author: aboutet
"""

def street_pattern_before(es, infos, insert):   
    return {}

def street_pattern(es, infos, data, documents):  
      
    to_send = []
        
    for json in data:
        if "LINK_ID" in json.keys():
            link_id = "LINK_ID"
        else:
            link_id = "LINK_PVID"

        r = es.search(index=infos["here"], body={"query": {"match": {"LINK_ID": json[link_id]}}})["hits"]["hits"]
        json["TRAVEL_DIRECTION"] = True if json["TRAVEL_DIRECTION"] == "T" else False
        if r:
            if "geometry" in json:
                del json["geometry"]
            to_send.append({"update":{"_id":r[0]["_id"]}})
            to_send.append({"doc": {key:value for key, value in json.items()}})
            

    if to_send:
        resp = es.bulk(index=infos["here"], body=to_send, request_timeout=3000)

def street_pattern_after(es, infos):
    return    
