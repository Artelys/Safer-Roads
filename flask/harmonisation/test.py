# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 12:54:46 2020

@author: aboutet
"""
import pandas as pd
import sys
import Get_Location as gl
from datetime import datetime, timedelta

# TODO : change this path and mount it!
sys.path.append( "/home/begood/flask/cluster/")
from functions import list_directory, getContext, find_last_id
from get_cluster import get_cluster
from sklearn.preprocessing import Normalizer
import pickle


def corporel_before(es, infos, insert):    
    print("start corporel")
    path = "Data/ref_baac/"
    files_list = list_directory(path, ".csv")
    dfs = {"id": {dic["label"]: pd.read_csv(path+dic["label"], encoding="latin-1") for dic in files_list}}
    
    id_corporel = find_last_id(es, "id_corpo", infos["accidents"])
    path = "/home/begood/flask/cluster/"
    dic = {"id_corpo":id_corporel}  
    dic.update(dfs)
    dic["pickle"] = pickle.load(open(path+"DecisionTree_clustering_corporels.pkl", 'rb'))
    dic["normalize"] = pickle.load(open(path+"Normalizer_Quanti.pkl", 'rb'))
    dic["encoder"] = pickle.load(open(path+"Encoder_Quali.pkl", 'rb'))
    dic["data"] = pd.read_csv(path+"data.csv", sep = ';', encoding = 'latin1', decimal = ',')
    return dic

def corporel(es, infos, data, documents):  
    to_send = []
    for json in data:
        documents["id_corpo"]+=1
        if "geometry" in json:
            point = float(json["geometry"]["lon"]), float(json["geometry"]["lat"])
        else:
            if json["GPS_LATITUDE"] is None:
                continue
            point = [json["GPS_LONGITUDE"], json["GPS_LATITUDE"]]
        #if "Addr_Type" in json.keys() and not (json["Addr_Type"] in ["Postal", "PostalCode"]):
        #    continue
        link_ids = gl._get_link_in_envelope(es, point, index_name=infos["here"])
        link_id = gl._get_closest_link(point, link_ids)
        json["point"] = point
        json["ID_ACCIDENT"] = json["ID_BAAC"]
        
        json["TYPE"] = "CORPOREL"
        if link_id[2] == 0:
            continue

        for name, df in documents["id"].items():
            name = name[4:-4]
            value = json["ID_"+name.upper()]
            if value is None:
                value = 0
            if isinstance(value, str):
                split = value.split(",")
                value = int(split[0])
            val = df[df["ID_"+name.upper()] == value]["LIBELLE"]
            json[name] = list(val)[0]
        #ADD value of here base
        resp = es.search(index=infos["here"], body={"query":{"bool":{"must":{"term":{"_id":link_id[2]}}}}})
        j = resp["hits"]["hits"][0]["_source"]
        if "Date" in json.keys() and len(json["Date"]) == 16:
            json["Date"] = json["Date"] +":00"
        new_json = {key:value for key, value in list(json.items()) + list(j.items())}
        if "Tué" in json["gravite"]:
            new_json["mortel"] = 1
            new_json["corporel"] =0
        else:
            new_json["corporel"] =1
            new_json["mortel"] = 0
        
        #ADD ID
        new_json["id_corporel"] = documents["id_corpo"]
        
        date =  datetime.strptime(new_json["Date"], "%d/%m/%Y %H:%M:%S")
        if new_json["ID_USAGER"] is None:
            new_json["ID_USAGER"] = 0
        if new_json["AGE_USAGER"] is None:
            new_json["AGE_USAGER"] = 0
        if new_json["AGE_VEHICULE"] is None:
            new_json["AGE_VEHICULE"] = 0
        # Not take it is already present
        query = {
                "query":{
                    "bool":{
                        "must":[
                            {
                            "term":{
                                "ID_BAAC":new_json["ID_BAAC"]
                                }
                            },{
                            "term":{
                                "AGE_USAGER":new_json["AGE_USAGER"]
                                }
                            },{
                            "term":{
                                "AGE_VEHICULE":new_json["AGE_VEHICULE"]
                                }
                            },{
                            "term":{
                                "ID_USAGER":new_json["ID_USAGER"]
                                }
                            }
                            ]
                             
                        }
                    }
                }
        r = es.search(index=infos["accidents"], body=query)
        if r["hits"]["hits"]:
            print(7)
            continue
        
        
        date_inf = date - timedelta(minutes=15)
        date_sup = date + timedelta(minutes=15)

        # If materiel are already present, delete them
        query = {
                "query":{
                    "bool":{
                        "must":[{
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
                                            "field":"id_corporel"
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
            for j in r["hits"]["hits"]:
                resp = es.delete(index=infos["accidents"], doc_type="_doc", id=j["_id"], ignore=404)
                
                                    
            
        # Find every document related to the same accident
        hits = es.search(index=infos["accidents"], body={"size":10000, "query":{"match":{"ID_ACCIDENT":json["ID_BAAC"]}}})["hits"]["hits"]
        if hits:
            mortels = [h["_source"]["mortel"] for h in hits]
            if 1 in mortels:
                new_json["mortel"] = 1
                new_json["corporel"] = 0
            else:
                if "mortel" in new_json.keys() and new_json["mortel"] == 1:
                    to_send_update = []
                    for j in hits:
                        to_send_update.append({"update":{"_id":j["_id"]}})
                        to_send_update.append({"doc":{"mortel":1, "corporel":0}})                
    
                    resp = es.bulk(index=infos["accidents"], body=to_send_update, 
                                   request_timeout=30)
                
        
        # ADD meteo
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
            new_json["meteo"] = True
        # Do not take it if there is no meteo associated
        else:
            new_json["meteo"] = False 
        
         
        # Add context
        j = getContext(date)
        new_json = {key:value for key, value in list(j.items()) + list(new_json.items())}                   
        
        # Get cluster
        new_json["Cluster_Accident"] = int(get_cluster(new_json, documents["pickle"], documents["normalize"], documents["encoder"], documents["data"])[0])
        print(new_json["Cluster_Accident"])
        to_send.append({"index":{}})
        to_send.append(new_json)
    
    if to_send:
        resp = es.bulk(index=infos["accidents"], body=to_send, request_timeout=300)
        print(resp)
def corporel_after(es, infos):
    return
