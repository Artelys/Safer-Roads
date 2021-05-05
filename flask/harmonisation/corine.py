# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 12:26:41 2020

@author: aboutet
"""
import pandas as pd

def corine_before(es, infos, insert):
    # Open data in server
    nomenclature1_name = 'Data/corine/nomenclature_1.csv'                
    nomenclature2_name = "Data/corine/nomenclature_2.csv"     
    nomenclature3_name = "Data/corine/nomenclature_3.csv"   
                    
    nc1 = pd.read_csv(nomenclature1_name, delimiter=";", 
                                encoding='latin-1')
    nc2 = pd.read_csv(nomenclature2_name, delimiter=";", 
                                encoding='latin-1')
    nc3 = pd.read_csv(nomenclature3_name, delimiter=";", 
                                encoding='latin-1')
    
    
    return {"nc1":nc1, "nc2":nc2, "nc3":nc3}

def corine(es, infos, data, documents):  
      
      for json in data:

            foreign_key = json['CODE_18']
           
            type_terrain_3 = documents["nc3"].loc[documents["nc3"]["code_clc_niveau_3"] == int(foreign_key)]
            type_terrain_2 = documents["nc2"].loc[documents["nc2"]["code_clc_niveau_2"] == int(foreign_key[:2])]
            type_terrain_1 = documents["nc1"].loc[documents["nc1"]["code_clc_niveau_1"] == int(foreign_key[:1])]
            if type_terrain_1.shape[0]:
                terrain_1 = list(type_terrain_1["libelle_fr"])[0]
                terrain_2 = list(type_terrain_1["libelle_fr"])[0]
                terrain_3 = list(type_terrain_1["libelle_fr"])[0]
            else:
                terrain_1 = ""
                terrain_2 = ""
                terrain_3 = ""

            if type_terrain_2.shape[0]:
                terrain_2 = list(type_terrain_2["libelle_fr"])[0]
                terrain_3 = list(type_terrain_2["libelle_fr"])[0]
            
            if type_terrain_3.shape[0]:
                terrain_3 = list(type_terrain_3["libelle_fr"])[0]
            
            properties = ["geometry", "LINK_ID"]
            # Don't get the link_if which already have a Corine field
            query ={
                "_source": { "includes": properties},
                 "size": 10000,
                 "query":{
                         "bool": {
                                 "must": {
                                         "match_all":{}
                                         },
                                 "filter": {
                                         "geo_shape": {
                                                 "geometry": {
                                                         "shape": {
                                                                 "type": json["geometry"]["type"],
                                                                 "coordinates":json["geometry"]["coordinates"],
                                                                 "relation": "within"}
                                                         }
                                                    }
                                                },
                                "must_not":{
                                        "exists": {
                                                "field": "CORINE_ID"
                                                }   
                                        }               
                                }                
                        }
                  }              
                                  
            res = es.search(index=infos["here"], body=query)
            
            print("Add " + str(len(res["hits"]["hits"])) + " link_id affiliate to "+str(json["POLYGON_NM"])+" de valeur "+terrain_3)
            
            if res["hits"]["hits"]:
                to_send = []
                for j in res["hits"]["hits"]:
                    to_send.append({"update":{"_id":j["_id"]}})
                    to_send.append({"doc":
                        {"COMMUNE_ID":json["AREA_ID"], 
                         "CORINE_ID":json["ID"], 
                         "Type_Zone":terrain_3,
                         "Type_Zone_2":terrain_2,
                         "Type_Zone_3":terrain_1}
                        })                

                resp = es.bulk(index=infos["here"], body=to_send, 
                               request_timeout=3000) 

def corine_after(es, infos):
    return
