# -*- coding: utf-8 -*-
"""
Created on Mon Dec  9 10:51:03 2019

@author: aboutet
"""
import os
import datetime

import pandas as pd


def list_directory(path, extention=".json"):
        filelist = []
        try: lst = os.listdir(path)
        except OSError as e:
            print(e)
        else:
            for name in lst:
                fn = os.path.join(path, name)
                if (not os.path.isdir(fn)) and str.endswith(fn, extention):
                    filelist.append(dict(name=name, value=name, label=name))
        return filelist
    
def getDayMonthYear(datetime):
    iso = datetime.isoformat()
    return iso[8:10]+"/"+iso[5:7]+"/"+iso[:4]

def typeOfJson(index_name, map, category=False):
    if not category:
        split_name = index_name.split("-")
        category = split_name[0]

    if category == "contexte":
        return "meteo"
    elif category == "voirie_de_reference":
        return "here"
    elif category == "infos_routes":
        if "Trafic" in map.keys():
            return "trafic"
        else:
            return "street_pattern"
    elif category == "zones":
        if "Y" in map.keys():
            return "land"
        elif "CODE_18" in map.keys():
            return "corine"
        else:   
            return "admin"
    elif category == "accidents_corporels":
        return "corporels"
    elif category == "accidents_materiels":
        if "Code Insee" in map.keys():
            return "sdis"
        return "materiels"
    elif category == "positions":
        if "ID_POI" in map.keys():
            return "coyote"
        else:
            return "waze"
    else:
        return 


def create_error(es, name, date, _type, error):
    j = {
            "name":name,
            "date":date,
            "type":_type,
            "error":error
        }
    resp = es.index(index="error-indexes", doc_type="_doc", body=j)
    print(resp)

def send_info(es, name, date, category, infos):
    j = {
    "name":name,
    "date":date.strftime("%Y-%m-%d %H:%M:%S"),
    "category":category,
    "harmonise":0,
    "current":"Sending"
    }
    resp = es.index(index=infos, doc_type="_doc", body=j)
    print(resp)

def getContext(date, vacance, ferie):
    json = {}
    
    jour = date.weekday()
    jours = ["1-Lundi", "2-Mardi", "3-Mercredi", "4-Jeudi", "5-Vendredi", "6-Samedi", "7-Dimanche"]
    json["jour_semaine"] = jours[jour]
    if date.hour <= 19 and date.hour > 16:
        json["creneau"] = "3-soir (17h-20h)"
    if date.hour < 10 and date.hour >= 7:
        json["creneau"] = "1-matin (7h-10h)"
    if date.hour > 19 or date.hour < 7:
        json["creneau"] = "4-nuit (20h-7h)"
    if date.hour <= 16 and date.hour >= 10:
        json["creneau"] = "2-jour (10h-17h)"

    # "day of year" ranges for the northern hemisphere
    spring = range(80, 172)
    summer = range(172, 264)
    fall = range(264, 355)
    # winter = everything else
    doy = date.timetuple().tm_yday
    if doy in spring:
      json["saison"] = '1-printemps'
    elif doy in summer:
      json["saison"] = '2-ete'
    elif doy in fall:
      json["saison"] = '3-automne'
    else:
      json["saison"] = '4-hiver'
      
    f = France()
    periode = "Ouvre"
    vacance = 0
    ferie = 0
    if date.weekday() <= 4 and not f.is_holiday(date) and not f.is_working_day(date):
        periode = "Ferie"
        ferie = 1
    if date.weekday() > 4:
        periode = "Week-end"
    if f.is_holiday(date) or f.is_holiday(date +datetime.timedelta(days=1)):
        periode = "Vacance scolaire"
        vacance = 1

    json["ferie"] = ferie
    json["vacance"] = vacance
    json["Periode_Specifique"] = periode

    return json
    
    

def find_last_id(es, id_name, index_name):
    # find the last id
    query = {
            "query":{
                "bool":{
                    "must":{
                        "exists":{
                            "field":id_name
                            }
                        }
                    }
                },
            "size":1,
            "sort":[{
                id_name:{"order":"desc"}
                }]
            }
    r = es.indices.exists(index=index_name)
    if r:
        r = es.search(index=index_name, body=query, ignore=400)
        if "error" in r.keys() or not r["hits"]["hits"]:
            return 0 
        else:
            return r["hits"]["hits"][0]["_source"][id_name]
    else:
        return 0
    
    
def index_on_date(es, infos):
    # Get data from elasticsearch by bulk
    res = es.search(index=infos["to_send"], body={"size":1000},scroll="120m",
          request_timeout=3000)
    
    while res["hits"]["hits"]:
        scroll = res["_scroll_id"]
        data = res["hits"]["hits"]
        
        data = [json["_source"] for json in data]
        
        send = []
        
        for json in data:
            if "date" not in json.keys():
                continue
            
            date = datetime.strptime(json["date"], '%m/%d/%y %H:%M:%S')
            send.append({"index":{"id":date.timestamp()}})
            send.append(json)

        if send:
            resp = es.bulk(index=infos[infos["name"]], body=send, request_timeout=3000)
            
            # Continue with other documents
            res = es.scroll(scroll_id = scroll, scroll = "120m",
          request_timeout=3000)

def get_data_frame_elastic(es, index, columns=[], exists=[]):
    print(es, index, columns)
    if columns:
        body = {
                "size":1000,
                "_source": columns
            }
    else:
        body = {
                "size":1000
            }
    if exists:
        query={"bool":{"must":[]}}
        for value in exists:
            query["bool"]["must"].append({
                "exists":{"field":value}
                })
        body["query"] = query
    donnees = []
    res = es.search(index=index, body=body, request_timeout=3000, scroll="10m")
    while res["hits"]["hits"]:
        scroll = res["_scroll_id"]
        data = res["hits"]["hits"]
        for json in data:
            j = json["_source"]
            j["id"] = json["_id"]
            donnees.append(j)
        res = es.scroll(scroll_id=scroll, request_timeout=3000, scroll="10m")
    return pd.DataFrame(donnees)


    
    
    
    
    
    
    
    
    
    
    
    
    
