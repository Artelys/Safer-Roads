# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 12:54:46 2020

@author: aboutet
"""
import pandas as pd
import sys
import os
import Get_Location as gl
from datetime import datetime, timedelta

sys.path.append( "/home/begood/flask/cluster/")
from functions import list_directory, getContext, find_last_id, get_data_frame_elastic
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
        if "Tu??" in json["gravite"]:
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
        
        to_send.append({"index":{}})
        to_send.append(new_json)
    
    if to_send:
        resp = es.bulk(index=infos["accidents"], body=to_send, request_timeout=300)
def corporel_after(es, infos):
    DATA_PATH = "cluster/"
    FILE_READY = 'accidents_corporels_pourclustering.csv'
    FILE_ACC = "Corporels.csv"
    FILE_PICKLE = "DecisionTree_clustering_corporels.pkl"
    FILE_NORMALIZE = "Normalizer_Quanti.pkl"
    FILE_LIST = "list_columns.csv"
    FILE_ENCODER = "Encoder_Quali.pkl"
    FILE_DATA = 'data.csv'
    
    ID_ACC = 'ID_ACCIDENT'
    file_ = open(os.path.join(DATA_PATH, FILE_NORMALIZE), 'rb')
    normalizer = pickle.load(file_)
    
    file = open(os.path.join(DATA_PATH, FILE_PICKLE), 'rb')
    clf = pickle.load(file)
    
    file__ = open(os.path.join(DATA_PATH, FILE_ENCODER), 'rb')
    encoder = pickle.load(file__)
    
    df = pd.read_csv(os.path.join(DATA_PATH, FILE_DATA), sep = ';', decimal = ',')
    
    # Import corporel
    corporels = get_data_frame_elastic(es, "accidents", exists=["id_corporel"])
    
    columns = list(df.columns.difference(corporels.columns)) + ["LINK_ID"]
    df = corporels.merge(df[columns], on = 'LINK_ID', how = 'left')
    
    
    mappingRef = {"gravite":"ID_GRAVITE", "obstacle_fixe":"ID_OBSTACLE_FIXE", "obstacle_mobile":"ID_OBSTACLE_MOBILE", 
              "trajet":"ID_TRAJET", "etat_surface":"ID_ETAT_SURFACE", "condition_atmos":"ID_CONDITION_ATMOS", "lumiere":"ID_LUMIERE",
             "intersection":"ID_INTERSECTION", "facteur_usager":"ID_FACTEUR_USAGER", "fact_vl":"ID_FACT_VL", "categorie_usager":"ID_CATEGORIE_USAGER","cat_admin":"ID_CAT_ADMIN"}
    
    for key, value in mappingRef.items():
        df[value] = df[key]
    
    df["Type_Zone"] = df["Type_Zone_2"]
    
    cols = ['ID_FACTEUR_USAGER', "ID_FACT_VL", "ID_INTERSECTION", "ID_GRAVITE", "ID_CAT_ADMIN", "ID_TRAJET", "ID_CATEGORIE_USAGER"]
    other_cols = list(df.columns)
    for col in cols:
        other_cols.remove(col)
    
    def feature_engineering_baac(df):
        x = df.reset_index().loc[0,other_cols].copy()
        
        # Facteur usager
        fact = df["ID_FACTEUR_USAGER"].values
        if "Ivresse apparente" in fact:
            val = "Implique ivresse d'un usager"
        elif "Malaise - fatigue" in fact or "Assoupissement" in fact:
            val = "Implique le facteur fatigue"
        else:
            val = "Autre"
        x['ID_FACTEUR_USAGER'] = val
        
        # Facteur v??hicule
        fact = df["ID_FACT_VL"].values
        if "Pneumatique us??" in fact or "??clatement de pneumatique" in fact or "Chargement" in fact or "??clairage - signalisation" in fact or "D??fectuosit?? m??canique" in fact:
            val = "Implique d??fectuosit?? d'un v??hicule"
        elif "D??placement du v??hicule" in fact or "V??hicule peu familier au conducteur" in fact or "Aide ?? la conduite d??faillante" in fact:
            val = "Implique un facteur de manoeuvre du v??hicule"
        elif "Visibilit?? restreinte depuis l???habitacle" in fact:
            "Implique un facteur de visibilit?? restreinte dans l'habitacle"
        else:
            val = "Autres"
        x['ID_FACT_VL'] = val
        
        # Intersection
        inter = df["ID_INTERSECTION"].values[0]
        if inter == "Giratoire":
            val = "Giratoire"
        elif inter in ["Hors intersection", "Non renseign??", "Intersection inconnue"]:
            val = "Hors intersection"
        else:
            val = "Autres intersections"
        x['ID_INTERSECTION'] = val
        
        # Cat??gorie v??hicule
        fact = df["ID_CAT_ADMIN"].astype(str).values
        sentence = '-'.join(fact)
        if "Bicyclette" in sentence:
            val = "Implique un v??lo"
        elif "Cyclo" in sentence or "Moto" in sentence or "Scooter" in sentence or "Quad" in sentence or "3 RM" in sentence:
            val = "Implique un 2RM"
        elif "P.L." in sentence or "Tracteur" in sentence or "Auto" in sentence:
            val = "Implique un poids lourd"
        elif "V.U." in sentence:
            val = "Implique un v??hicule utilitaire"
        elif "Train" in sentence or "Tramway" in sentence:
            val = "Implique un train ou tramway"
        else:
            val = "Implique un v??hicule l??ger"
        x['ID_CAT_ADMIN'] = val
        
        # Gravit??
        fact = df["ID_GRAVITE"].astype(str).values
        sentence = '-'.join(fact)
        if "Tu??" in sentence:
            val = "Implique un mort"
        elif "hospitalis??" in sentence:
            val = "Implique un bless?? hospitalis??"
        else:
            val = "Tous les usager indemnes ou bless??s l??gers"
        x['ID_GRAVITE'] = val
        
        # Type de trajet
        fact = df["ID_TRAJET"].astype(str).values
        sentence = '-'.join(fact)
        if "loisir" in sentence:
            val = "Implique un trajet de loisir"
        elif "professionnelle" in sentence:
            val = "Implique un trajet profesionnel"
        elif "Domicile" in sentence or "Course" in sentence:
            val = "Implique un trajet du quotidien"
        else:
            val = "Implique d'autres trajets"
        x['ID_TRAJET'] = val
        
        # Usagers impliqu??s
        fact = df["ID_CATEGORIE_USAGER"].astype(str).values
        sentence = '-'.join(fact)
        if "Pi??ton" in sentence:
            val = "Implique un pi??ton"
        elif sum(fact=="Conducteur") > 1:
            val = "Implique plusieurs conducteurs"
        elif "Passager" in fact:
            val = "Implique un seul conducteur et ses passagers"
        else:
            val = "Implique un seul conducteur sans passagers"
        x['ID_CATEGORIE_USAGER'] = val
        return x
    
    
    df_ = df.groupby(ID_ACC, as_index = False).apply(feature_engineering_baac)
    
    fixe_replace = {'Foss??. talus. paroi rocheuse':'Foss??', 'B??timent. mur. pile de pont':'B??timent', '??lot. refuge. borne haute':'??lot', "Obstacle fixe inconnu":"Sans objet"}
    mobile_replace ={"Obstacle mobile inconnu":'Autre', '0':"Non renseign?? ou sans objet"}
    df_['ID_OBSTACLE_FIXE'] = df_['ID_OBSTACLE_FIXE'].fillna('Sans objet').replace(fixe_replace)
    df_['ID_OBSTACLE_MOBILE'] = df_['ID_OBSTACLE_MOBILE'].fillna("Non renseign?? ou sans objet").replace(mobile_replace)
    
    colsQuali = ['creneau',
             'Periode_Specifique',
             'Type_Voie',
             'Vitesse_Autorisee',
             'Type_Zone',
             'Categorie_Route',
             'ID_ETAT_SURFACE',
             'ID_CONDITION_ATMOS',
             'ID_LUMIERE',
             'ID_OBSTACLE_FIXE',
             'ID_OBSTACLE_MOBILE',
             'ID_FACTEUR_USAGER',
             'ID_FACT_VL',
             'ID_INTERSECTION',
             'ID_GRAVITE',
             'ID_CAT_ADMIN',
             'ID_TRAJET',
             'ID_CATEGORIE_USAGER']

    colsQuanti = ['Pluviometrie 6h',
                 'Trafic',
                 'Courbure',
                 'NB_MATERIEL',
                 'Accident',
                 'Chauss??e d??grad??e',
                 'JAM',
                 'ROAD_CLOSED',
                 'Retrecissement',
                 'Visibilit?? r??duite',
                 'Route glissante',
                 'V??hicule arr??t??',
                 'WEATHERHAZARD']
    
    cols = colsQuali + colsQuanti + ["ID_BAAC"]
    dataset = df_[cols].reset_index(drop = True)

    def encoding_from_pickle(dataset, encoder, cols_learning_quanti, cols_learning_quali):
        train = dataset.copy()
        
        ## Colonnes quantitatives
        for col in cols_learning_quanti:
            train[col] = train[col].astype(float)
        
        ## Colonnes qualitatives
        #encoder = OneHotEncoder(drop='first')
        #encoder.fit(train[cols_learning_quali])
        quali = encoder.transform(train[cols_learning_quali]).toarray()
    
    
        # Bien nommer les colonnes par cat??gories
        train.drop(cols_learning_quali, axis = 1, inplace = True)
        qualiCols = []
        for originalCol, catList in zip(cols_learning_quali, encoder.categories_):
            catList = catList[1:]
            l = [originalCol + '__' + str(cat) for cat in catList]
            qualiCols = qualiCols + l
    
        # Ajout des nouvelles colonnes encod??es
        for col in qualiCols:
            train[col] = 0
    
        train[qualiCols] = quali
    
        return train
    dataset_ = dataset[cols]
    train = encoding_from_pickle(dataset_, encoder,colsQuanti, colsQuali)
    train = train.fillna(0)
    train[colsQuanti] = normalizer.transform(train[colsQuanti])
    del train["ID_BAAC"]
    new_clus = clf.predict(train)
    cluster = "Cluster Accident"
    dataset[cluster] = new_clus
    short_data = dataset[["ID_BAAC", cluster]]
    del corporels[cluster]
    data_ready = corporels.merge(short_data, how="left", on="ID_BAAC")
    data_ready = data_ready[["id", cluster]]
    to_send = []
    for index, row in data_ready.iterrows():
        to_send.append({"update":{"_id":row["id"]}})
        to_send.append({"doc":{cluster:row[cluster]}})
    
    es.bulk(index="accidents", body=to_send, request_timeout=3000)
    return
