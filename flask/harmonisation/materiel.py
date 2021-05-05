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


def materiel_before(es, infos, insert):    
    id_materiel = find_last_id(es, "id_materiel", infos["accidents"])
    keep = ["ResultID", "Type du véhicule", "Addr_Type", 'Resumé des dégats', 'Nombre de passagers', 'Date de création du dossier', "(Eve) Distance domicile lieu d'é", 'Resumé des dégats', 'Nombre de passagers', 'Date de création du dossier', "(Eve) Distance domicile lieu d'é", 'Zone', 'Postal', 'USER_ADRES', 'USER_CodeP', 'geometry', 'Date']

    return {"id_materiel":id_materiel,"keep":keep}

def materiel(es, infos, data, documents):  
      
    to_send = []
        
    for json in data:
        documents["id_materiel"]+=1
        if json['USER_ADRES'] == "NON EXPLOITABLE":
            continue
        #if "Addr_Type" in json.keys() and "Postal" in json["Addr_Type"]:
        #    continue
        point = float(json["geometry"]["lon"]), float(json["geometry"]["lat"])
        link_ids = gl._get_link_in_envelope(es, point, index_name=infos["here"])
        link_id = gl._get_closest_link(point, link_ids)
        json["point"] = point
        if link_id[2] == 0:
            continue
        
    
        json["Distance domicile"] = json["(Eve) Distance domicile lieu d'é"]
        del json["(Eve) Distance domicile lieu d'é"]
        json["ID_ACCIDENT"] = json["ResultID"]
        json["TYPE"] = "MATERIEL"
        #ADD value of here base
        resp = es.search(index=infos["here"], body={"query":{"bool":{"must":{"term":{"_id":link_id[2]}}}}})
        j = resp["hits"]["hits"][0]["_source"]
        new_json = {key:value for key, value in list(json.items()) + list(j.items())}
        new_json["materiel"] = 1
        
        type_vehicule = json["Type du véhicule"]
        if type_vehicule == "UTILITAIRE < 9m3 < 3.5 T":
            t = "VL"
        elif type_vehicule == "VH MONOSPACE":
            t = "VL"
        elif type_vehicule == "VH TOURISME":
            t = "VL"
        elif type_vehicule == "VOITURETTE":
            t = "VL"
        elif type_vehicule == "VU":
            t = "VL"
        elif type_vehicule == "VH 4X4":
            t = "VL"
        elif type_vehicule == "AUTRE":
            t = "Autre"
        elif type_vehicule == "CAMPING CAR":
            t = "PL"
        elif type_vehicule == "MOTO":
            t = "Moto"
        elif type_vehicule == "NR":
            t = "Autre"
        elif type_vehicule == "SCOOTER":
            t = "Moto"
        elif type_vehicule == "VU2":
            t = "VL"
        elif type_vehicule == "AUTOCAR":
            t = "PL"
        elif type_vehicule == "CARAVANE":
            t = "PL"
        elif type_vehicule == "QUAD":
            t = "Autre"
        elif type_vehicule == "UTILITAIRE > 3.5 T":
            t = "PL"
        elif type_vehicule == "REMORQUE":
            t = "PL"
        elif type_vehicule == "VH MINIVAN":
            t = "VL"
        else:
            t = "Autre"
        
        new_json["Type_Vehicule_Assure"] = t
         
        new_json["id_materiel"] = documents["id_materiel"]   
        if len(new_json["Date"]) == 16:
            new_json["Date"] +=":00"       
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
                                "must":[{
                                    "match":{
                                        "ResultID": json["ResultID"]
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
                                            "field":"id_materiel"
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
        # Do not take it if there is no meteo associated
        
        #Add context
        j = getContext(date)
        new_json = {key:value for key, value in list(j.items()) + list(new_json.items())}                   
        
        to_send.append({"index":{}})
        to_send.append(new_json)
    
    if to_send:
        resp = es.bulk(index=infos["accidents"], body=to_send, request_timeout=300)

def materiel_after(es, infos):
    return
