# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 11:04:34 2020

@author: aboutet
"""
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch

def here_before(es, infos, insert):
    g_axe = pd.read_csv("Data/GrandsAxesLoiret_pretraite.csv", sep=";")
    
    speed_cat = {
            "0":"Non défini", 
            "1":"> 130 KPH", 
            "2":"101-130 KPH", 
            "3":"91-100 KPH", 
            "4":"71-90 KPH", 
            "5":"51-70 KPH", 
            "6":"31-50 KPH", 
            "7":"11-30 KPH", 
            "8":"<11 KPH"
            }

    keep = [
            'LINK_ID', 'ST_NAME', 'FEAT_ID', 'ST_LANGCD', 'NUM_STNMES', 
            'ST_NM_PREF', 'ST_TYP_BEF', 'ST_NM_BASE', 'ST_NM_SUFF', 
            'ST_TYP_AFT', 'ST_TYP_ATT', 'ADDR_TYPE', 'L_REFADDR', 'L_NREFADDR', 
            'L_ADDRSCH', 'L_ADDRFORM', 'R_REFADDR', 'R_NREFADDR', 'R_ADDRSCH', 
            'R_ADDRFORM', 'REF_IN_ID', 'NREF_IN_ID', 'N_SHAPEPNT', 
            'FUNC_CLASS', 'SPEED_CAT', 'FR_SPD_LIM', 'TO_SPD_LIM', 'TO_LANES', 
            'FROM_LANES', 'ENH_GEOM', 'LANE_CAT', 'DIVIDER', 'DIR_TRAVEL', 
            'L_AREA_ID', 'R_AREA_ID', 'L_POSTCODE', 'R_POSTCODE', 'L_NUMZONES', 
            'R_NUMZONES', 'NUM_AD_RNG', 'AR_AUTO', 'AR_BUS', 'AR_TAXIS', 
            'AR_CARPOOL', 'AR_PEDEST', 'AR_TRUCKS', 'AR_TRAFF', 'AR_DELIV', 
            'AR_EMERVEH', 'AR_MOTOR', 'PAVED', 'PRIVATE', 'FRONTAGE', 'BRIDGE', 
            'TUNNEL', 'RAMP', 'TOLLWAY', 'POIACCESS', 'CONTRACC', 'ROUNDABOUT', 
            'INTERINTER', 'UNDEFTRAFF', 'FERRY_TYPE', 'MULTIDIGIT', 'MAXATTR', 
            'SPECTRFIG', 'INDESCRIB', 'MANOEUVRE', 'DIVIDERLEG', 'INPROCDATA', 
            'FULL_GEOM', 'URBAN', 'ROUTE_TYPE', 'DIRONSIGN', 'EXPLICATBL', 
            'NAMEONRDSN', 'POSTALNAME', 'STALENAME', 'VANITYNAME', 
            'JUNCTIONNM', 'EXITNAME', 'SCENIC_RT', 'SCENIC_NM', 'FOURWHLDR', 
            'COVERIND', 'PLOT_ROAD', 'REVERSIBLE', 'EXPR_LANE', 'CARPOOLRD', 
            'PHYS_LANES', 'VER_TRANS', 'PUB_ACCESS', 'LOW_MBLTY', 'PRIORITYRD', 
            'SPD_LM_SRC', 'EXPAND_INC', 'TRANS_AREA', 'NOMBLOC', 'L_INSEE', 
            'R_INSEE', 'FT_TimeZoneID', 'TF_TimeZoneID', 'ST_NAME_Alt', 
            'ST_LANGCD_Alt', 'ST_NM_PREF_Alt', 'ST_TYP_BEF_Alt', 
            'ST_NM_BASE_Alt', 'ST_NM_SUFF_Alt', 'ST_TYP_AFT_Alt', 
            'DIRONSIGN_Alt', 'F_ZLEV', 'T_ZLEV', 'Meters', 'KPH', 'Language', 
            'Language_Alt', 'ClosedForConstruction', 'UFR_AUTO', 'UFR_BUS', 
            'UFR_TAXIS', 'UFR_CARPOOL', 'UFR_PEDSTRN', 'UFR_TRUCKS', 
            'UFR_THRUTR', 'UFR_DELIVER', 'UFR_EMERVEH', 'UFR_MOTOR', 
            'min_Pieton', 'min_Velo', 'Shape_Length', 'geometry'
            ]
    
    return {"g_axe":g_axe, "speed_cat":speed_cat, "keep":keep}
def here(es, infos, data, documents):  
      
    to_send = []
        
    for json in data:
        json["Vitesse_Autorisee"] = documents["speed_cat"][json["SPEED_CAT"]]
        
        nom_route = json["ST_NAME"]
        json['Nom_Route'] = nom_route
        func_class = json["FUNC_CLASS"]
        if func_class == "1":
           json["Categorie_Route"] = "autoroute"
        elif func_class == "2" or func_class == "3":
            json["Categorie_Route"] = "departementale"
        elif json["PRIVATE"]  == "Y":
            json["Categorie_Route"] = "privee"
        else:
            json["Categorie_Route"] = "Autre"
        
        json["Courbure"] = json['N_SHAPEPNT'] / json["Shape_Length"]
        prioritaire = json['PRIORITYRD']
        
        if prioritaire == "Y":
            json['Prioritaire'] = "oui"
        else:
            json["Prioritaire"] = "non"
            
            
        # ADD grand axe ou nom
        json["Grand_Axe"] = False
        if func_class == "1" or func_class == "2":
            json["Grand_Axe"] = True
        else:
            if not nom_route or not nom_route[0] == "D":
                if "ST_NAME_Alt" in json.keys():
                    nom_route = json["ST_NAME_Alt"]
            
            if nom_route:
                dep = nom_route.split(" ")[0]
                if len(dep) > 0 and dep[0] == "D":
                    dep = "0000"+dep[1:]
                    dep = "D"+dep[-4:]
                    axe = documents["g_axe"][documents["g_axe"]["ROUTE"] == dep]
                    if not axe.empty and list(axe["CLASSEMENT"])[0] == "Grand Axe":
                        json["Grand_Axe"] = True
    
        # Get the road type
        road_type = "Goudronnée publique"
        if json["PAVED"] == "N":
            road_type = "Non goudronnée"
        elif json["PRIVATE"] == "Y":
            road_type = "Privé"
        elif json["ROUNDABOUT"] == "Y":
            road_type = "Rond-point"
        elif json["FRONTAGE"] == "Y":
            road_type = "Frontage"
        elif json["TOLLWAY"] == "Y":
            road_type = "Tronçon à péage"
        elif json["BRIDGE"] == "Y":
            road_type = "Pont"
        elif json["TUNNEL"] == "Y":
            road_type = "Tunnel"
        elif json["RAMP"] == "Y":
            road_type = "Rampe"
        elif json["CONTRACC"] == "Y":
            road_type = "Accès régulé"
        elif json["POIACCESS"] == "Y":
            road_type = "Accès point d'intérêt"
    
        json["Type_Voie"] = road_type
        
        to_send.append({"index":{"_id":json["LINK_ID"]}})
        to_send.append(json)
        
    if to_send:
        resp = es.bulk(index=infos["here"], body=to_send, request_timeout=3000)

def here_after(es, infos):    
    NODES_1 = 'REF_IN_ID'
    NODES_2 = 'NREF_IN_ID'
    ADJ_SUP = "Vitesse adjacente supérieure"
    ADJ_INF = "Vitesse adjacente inférieure"
    ID_SEGT = 'LINK_ID'
    CONTIENT_INTERSECTION = "Contient intersection"

    here = get_data_frame_elastic(es, infos["here"], [ID_SEGT, NODES_1, NODES_2, 'SPEED_CAT'])


    ## Présence d'une intersection sur la route
    # On compte tous les noeuds et on repère ceux qui sont présents > 2 fois
    noeuds = pd.DataFrame({"ID_NOEUD":np.concatenate((here[NODES_1],here[NODES_2])), 
                           ID_SEGT:np.concatenate((here[ID_SEGT],here[ID_SEGT]))})
    
    intersections = noeuds["ID_NOEUD"].value_counts()[noeuds["ID_NOEUD"].value_counts()>2].index.unique()
    print("Nombre d'intersections")
    print(len(intersections))
    
    print("Nombre de tronçons concernés")
    link_intersections = noeuds[noeuds["ID_NOEUD"].isin(intersections)][ID_SEGT].unique()
    print(len(link_intersections))
    
    # Puis on associe cette informations aux LINK_ID
    here[CONTIENT_INTERSECTION] = ["Oui" if link in link_intersections else "Non" for link in here[ID_SEGT]]
    
    ## Présence d'un tronçon adjacent avec une vitesse max supérieure ou inférieure
    
    vitesses = here[[ID_SEGT, NODES_1,NODES_2, 'SPEED_CAT']].copy()
    
    # Fonction de détection
    def vitesse_adjacente(x):
        nodeA = x[NODES_1].values[0]
        nodeB = x[NODES_2].values[0]
        vitesse = x['SPEED_CAT'].values[0]
        df = vitesses[(vitesses[NODES_1]==nodeA)|(vitesses[NODES_1]==nodeB)|(vitesses[NODES_2]==nodeA)|(vitesses[NODES_2]==nodeB)]
        inf = 0
        sup = 0
        if len(df[df['SPEED_CAT']>vitesse]) > 0:
            sup = 1
        if len(df[df['SPEED_CAT']<vitesse]) > 0:
            inf = 1
        x[ADJ_SUP] = sup
        x[ADJ_INF] = inf
        return x
    
    here = here.groupby(ID_SEGT, as_index = True).apply(vitesse_adjacente)
    
    to_send = []
    n = 1000
    for index, row in here.iterrows():
        print(row["_id"], row[CONTIENT_INTERSECTION])
        if (index+1)%n == 0:
            es.bulk(index=infos["here"], body=to_send, request_timeout=3000)
            to_send = []
        to_send.append({"update":{"_id":row["_id"]}})
        j = {CONTIENT_INTERSECTION:row[CONTIENT_INTERSECTION], ADJ_SUP:row[ADJ_SUP], ADJ_INF:row[ADJ_INF]}
        to_send.append({"doc":j})
    if to_send:
        resp = es.bulk(index=infos["here"], body=to_send, request_timeout=3000)
    return

def get_data_frame_elastic(es, index, columns):

    body = {
            "size":1000,
            "_source": columns
        }
    donnees = []
    res = es.search(index=index, body=body, request_timeout=3000, scroll="10m")
    while res["hits"]["hits"]:
        scroll = res["_scroll_id"]
        data = res["hits"]["hits"]
        for json in data:
            json["_source"]["_id"] = json["_id"]
            donnees.append(json["_source"])
        res = es.scroll(scroll_id=scroll, request_timeout=3000, scroll="10m")
    return pd.DataFrame(donnees)
