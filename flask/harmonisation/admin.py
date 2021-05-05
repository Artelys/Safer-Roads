# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 12:26:41 2020

@author: aboutet
"""

def admin_before(es, infos, insert):
    
    return {}
def admin(es, infos, data, documents):  
    for json in data:

        properties = ["geometry", "LINK_ID"]
        
        # if there is not geometry for the town
        if "geometry" not in json.keys():
            continue

        # Get every link_id that haven't town and are in the town
        query ={
            "_source": { "includes": properties},
             "size": 10000,
              "query": {"bool": {"must": {"match_all": {}},
                      "filter": {"geo_shape": {"geometry": {"shape": {
                                                                  "type": json["geometry"]["type"],
                                                                   "coordinates":json["geometry"]["coordinates"],
                                                                    "relation": "within"}}}},
                            "must_not":{
                                    "exists": {
                                            "field": "Commune"
                                            }   
                                    }              
                        }                
                }
              }
        json["Commune"] = json["POLYGON_NM"]
        resp = es.index(index=infos["admin"], id=json["Commune"], body=json, request_timeout=3000)

        res = es.search(index=infos["here"], body=query, request_timeout=600)
    
        print("Add " + str(len(res["hits"]["hits"])) + " link_id affiliate to "+str(json["POLYGON_NM"]))
        
        if res["hits"]["hits"]:
            to_send = []
            for j in res["hits"]["hits"]:
                to_send.append({"update":{"_id":j["_id"]}})
                to_send.append({"doc":{"Commune":json["POLYGON_NM"], "COMMUNE_ID":json["AREA_ID"]}})                
    
            resp = es.bulk(index=infos["here"], body=to_send,
      request_timeout=3000)  

def admin_after(es, infos):
    return
