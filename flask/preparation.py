# -*- coding: utf-8 -*-
"""
Created on Tue Mar 10 14:10:53 2020

@author: aboutet
"""

# Modules Python
import numpy as np
import pandas as pd
import datetime

from variables import *

def get_data_frame_elastic(index, columns):
<<<<<<< HEAD
    #TODO CATASTROPHIQUE !!
=======
>>>>>>> update python repo
    es = Elasticsearch(http_auth=("elastic", "elastic"))
    body = {
            "size":1000,
            "_source": columns
        }
    donnees = []
    res = es.search(index=index, body=body, request_timeout=3000)
    while res["hits"]["hits"]:
        scroll = res["_scroll_id"]
        data = res["hits"]["hits"]
        for json in data:
            donnees.append(json["_source"])
        res = es.scroll(scroll_id=scroll, body=body, request_timeout=3000)
    return pd.DataFrame(donnees)

def data_preparation():
    
    
    FEATURES_HERE_QUALI_NO_ORDER = ["PRIORITYRD", "PAVED","PRIVATE", CORINE, TRAFIC]
    FEATURES_HERE_QUALI_ORDERED = ["SPEED_CAT","FUNC_CLASS"]
    FEATURES_HERE_QUANTI = ["N_SHAPEPNT","Shape_Length"]
    
    here_columns = [
        "LINK_ID", "PRIVATE", "PAVED", "PRIORITYRD", "Type_Zone_3", "Trafic", 
        "SPEED_CAT", "FUNC_CLASS", "N_SHAPEPNT", "Shape_Length", "REF_IN_ID", 
        "NREF_IN_ID", "Courbure"
                    ]
    # Import des données here
    here = get_data_frame_elastic("here_1", here_columns)
    here["N_SHAPEPNT"] = here["Courbure"]
    here[CORINE] = here["Type_Zone_3"]
    here[TRAFIC] = here["Trafic"]
    
    # On garde qu'un certains nombres de colonnes
    X = here[[ID_SEGT]+FEATURES_HERE_QUALI_ORDERED+FEATURES_HERE_QUALI_NO_ORDER+FEATURES_HERE_QUANTI]
    
    accidents_columns = [
            "id_corporel", "id_sdis", "id_materiel", "id_waze", "id_coyote",
            ID_SEGT, TYPE, SUBTYPE, WAZE_TOTAL, WAZE_JAM, WAZE_HAZARD, DATE,
            ID_ACC
            ]
    accidents = get_data_frame_elastic("accidents", accidents_columns)
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
    X[CONTIENT_INTERSECTION] = ["Oui" if link in link_intersections else "Non" for link in X[ID_SEGT]]
    X[CONTIENT_INTERSECTION].value_counts()
    
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
    
    hereT = here.groupby(ID_SEGT, as_index = True).apply(vitesse_adjacente)
    X = X.merge(hereT[[ID_SEGT, ADJ_INF, ADJ_SUP]], on = ID_SEGT, how = 'left')
    
    
    # Import de waze
    waze = accidents[accidents["id_waze"].notna()]
    waze[DATE] = pd.to_datetime(waze[DATE], infer_datetime_format=True)
    FEATURES_HERE_QUANTI =FEATURES_HERE_QUANTI+ [WAZE_TOTAL, WAZE_JAM, WAZE_HAZARD]
    
    # Calcul de features sur la base des alertes
    def compter_alertes(x):
        df = pd.DataFrame({ID_SEGT:x[ID_SEGT].unique()})
        df[WAZE_TOTAL] = len(x)
        df[WAZE_JAM] = len(x[x[TYPE]=="JAM"])
        df[WAZE_HAZARD] = len(x[(x[TYPE]=="WEATHERHAZARD")&~(x[SUBTYPE].isin(["HAZARD_ON_ROAD_ICE","HAZARD_WEATHER_FOG","HAZARD_WEATHER_HAIL", "HAZARD_WEATHER_HEAVY_RAIN", "HAZARD_WEATHER_FLOOD", "HAZARD_WEATHER"]))])    
        return df
    
    wazeRoads = waze.groupby(ID_SEGT, as_index=False).apply(compter_alertes)
    
    wazeRoads[ID_SEGT] = wazeRoads[ID_SEGT].astype(int)
    X = X.merge(wazeRoads, on = ID_SEGT, how = 'left')
    X[[WAZE_TOTAL, WAZE_JAM, WAZE_HAZARD]] = X[[WAZE_TOTAL, WAZE_JAM, WAZE_HAZARD]].fillna(0)
    
    
    # Import de coyote
    coyote = accidents[accidents["id_coyote"].notna()]
    coyote[DATE] = pd.to_datetime(coyote[DATE], infer_datetime_format=True)
    # Calcul de features sur la base des alertes
    def compter_alertes_coyotes(x):
        df = pd.DataFrame({ID_SEGT:x[ID_SEGT].unique()})
        df[COYOTE] = len(x)
        return df
         
    coyote = coyote[[ID_SEGT]]
    coyoteRoad = coyote.groupby(ID_SEGT, as_index=False).apply(compter_alertes_coyotes)
    coyoteRoad[ID_SEGT] = coyoteRoad[ID_SEGT].astype(int)
    X = X.merge(coyoteRoad, on = ID_SEGT, how = 'left')
    X[COYOTE] = X[COYOTE].fillna(0)
    
    
    # Ajout des données Corine
    FEATURES_HERE_QUALI_NO_ORDER = FEATURES_HERE_QUALI_NO_ORDER + [CORINE]
    # EVentuellement: regroupement des catégories CORINE LAND COVER
    # Cas particulier: Corine Land Cover: on ne va pas séparer par tous les types de terrains mais seulement ceux qui nous intéressent
    URBAIN = "Zones articialisées"
    AGRICOLE = "Zones agricoles"
    NATURELS = "Forêts et autres milieux semi-naturels"
    AUTRE = "Autres"
    
    corineReplace = {"Surfaces en eau":AUTRE, "Zones humides":AUTRE}
    
    X[CORINE] = X[CORINE].replace(corineReplace)
    
    # Arrangement des données
    # Suppression de l'effet de la longueur du tronçon
    for col in [WAZE_TOTAL, WAZE_JAM, WAZE_HAZARD, COYOTE]:
        X[col] = [val/lon for val, lon in zip(X[col],X['Shape_Length'])]
    
    # import des donénes d'accidents materiel
    thelem = accidents[accidents["id_materiel"].notna()]
    sdis = accidents[accidents["id_sdis"].notna()]
    
    materiel = thelem.append(sdis)
    materiel[DATE] = pd.to_datetime(materiel[DATE], infer_datetime_format=True)
    materiel = materiel[materiel[DATE] >= np.datetime64('2014-01-01')]
    
    # import des données corporels
    corporel = accidents[accidents["id_corporel"].notna()]
    corporel[DATE] = pd.to_datetime(corporel[DATE], infer_datetime_format=True)
    corporel = corporel[corporel[DATE] >= np.datetime64('2017-01-01')]
    
    X_mat = X.merge(materiel[[ID_ACC,ID_SEGT]], on = ID_SEGT, how = 'outer')
    X_corp = X.merge(corporel[[ID_ACC,ID_SEGT]], on = ID_SEGT, how = 'outer')
    # Création d'une colonne nombre et une colonne classe
    func_dict = dict()
    for col in X_mat.columns:
        if col != ID_SEGT:
            func_dict[col] = 'first'
    func_dict[ID_ACC] = 'count'

    X_mat = X_mat.groupby([ID_SEGT], as_index = False).agg(func_dict)
    X_mat[NB_ACC] = X_mat[[ID_ACC]].sum(axis = 1)
    X_mat[CLASSES_SIMPLES] = [0 if c > 0 else 1 for c in X_mat[NB_ACC]]
    
    X_corp = X_corp.groupby([ID_SEGT], as_index = False).agg(func_dict)
    X_corp[NB_ACC] = X_corp[[ID_ACC]].sum(axis = 1)
    X_corp[CLASSES_SIMPLES] = [0 if c > 0 else 1 for c in X_corp[NB_ACC]]
    
    # Création des agrégats
    aggregation = {
        WAZE_TOTAL:'mean', 
        "N_SHAPEPNT":'mean',
        WAZE_JAM:'mean', 
        WAZE_HAZARD:'mean',
        TRAFIC:"mean",
        COYOTE:"mean"
    }
    QUANTI_KEEP = list(aggregation.keys())
    
    # Agregat materiel
    QUALI_ORDERED_KEEP_MAT = ["FUNC_CLASS","SPEED_CAT"]
    QUALI_NOORDER_KEEP_MAT = [CORINE, "PAVED", "PRIVATE", "PRIORITYRD"]
    # Agregat corporel
    QUALI_ORDERED_KEEP_CORP = ["FUNC_CLASS", "SPEED_CAT", ADJ_SUP, CONTIENT_INTERSECTION]
    QUALI_NOORDER_KEEP_CORP = ["PAVED"]
    
    # Constitution des agrégats
    AGREGATS = 'Agregat'
    
    X_agg_mat = X_mat[[ID_SEGT]+QUANTI_KEEP+QUALI_ORDERED_KEEP_MAT+QUALI_NOORDER_KEEP_MAT]
    X_agg_mat[AGREGATS] = 0
    
    X_agg_corp = X_corp[[ID_SEGT]+QUANTI_KEEP+QUALI_ORDERED_KEEP_CORP+QUALI_NOORDER_KEEP_CORP]
    X_agg_corp[AGREGATS] = 0
    
    np.random.seed(0)
    def attribuer_agregat(df):
        df[AGREGATS] = np.random.uniform(low = 0, high = 1000000)
        return df
    
    agregats_mat = X_agg_mat.groupby(QUALI_ORDERED_KEEP_MAT+QUALI_NOORDER_KEEP_MAT).apply(attribuer_agregat)
    X_MAT = agregats_mat.groupby([AGREGATS]+QUALI_ORDERED_KEEP_MAT+QUALI_NOORDER_KEEP_MAT, as_index=False).agg(aggregation)
    agregats_mat.to_csv("cle_agregat_materiel.csv", sep = ';', index = False, encoding = 'utf-8-sig', decimal = ',')
    
    agregats_corp = X_agg_corp.groupby(QUALI_ORDERED_KEEP_CORP+QUALI_NOORDER_KEEP_CORP).apply(attribuer_agregat)
    X_CORP = agregats_corp.groupby([AGREGATS]+QUALI_ORDERED_KEEP_CORP+QUALI_NOORDER_KEEP_CORP, as_index=False).agg(aggregation)
    agregats_corp.to_csv("cle_agregat_corporel.csv", sep = ';', index = False, encoding = 'utf-8-sig', decimal = ',')
    
    
    ## Lecture des données de contexte
    DATE='Date'
    
    # Lire données météo pour avoir tous les pas de temps
    meteo = pd.read_csv("meteo.csv", sep = ";", encoding = 'utf-8-sig', decimal = ',')
    agregations = {'Temperature':'mean', 'Pression 0m':'mean', 'Direction Vent':'mean', 'Vitesse Vent':'mean', 
                    'Pluviometrie 1h':'mean', 'Pluviometrie 6h':'mean'}
    METEO_QUANTI = list(agregations.keys())
    
    meteo[METEO_QUANTI] = meteo[METEO_QUANTI].fillna(method = 'bfill')
    meteo[DATE] = pd.to_datetime(meteo[DATE], infer_datetime_format=True)
    
    meteo = meteo[METEO_QUANTI+[DATE]].fillna(method = 'bfill')
    meteo[DATE] = pd.to_datetime(meteo[DATE], infer_datetime_format=True)
    meteo = meteo.drop_duplicates(subset = [DATE], keep = 'first')
    print(list(meteo.keys()))
    # Add meteo to empty spot
    debut = materiel[DATE].min()
    fin = materiel[DATE].max()
    
    timeline = pd.DataFrame({DATE:np.arange(debut, fin, dtype='datetime64[h]')})
    meteo = timeline.merge(meteo, on = DATE, how = 'left')
    
    meteo = meteo.fillna(method = 'bfill').fillna(method = 'ffill')
    
    # Evenements à joindre à la météo
    ######### TEMPO
    PATH_FERIE = "Data/Ferie.csv"
    PATH_VACS = "Data/vacances.csv"
    ferie = pd.read_csv(PATH_FERIE,sep = ";", encoding = 'utf-8-sig', decimal = '.')
    ferie["date"] = pd.to_datetime(ferie["date"], infer_datetime_format=True)
    vacs = pd.read_csv(PATH_VACS,sep = ";", encoding = 'utf-8-sig', decimal = '.')
    vacs["date"] = pd.to_datetime(vacs["date"], infer_datetime_format=True)
    
    # Extraire les variables saisonnières (jusqu'au pas de temps)
    
    CRENEAU = "Creneau_Horaire"
    JOUR_MOIS = "Jour du mois"
    JOUR_SEMAINE = "Jour de la semaine"
    MOIS = "Mois"
    ANNEE = "Annee"
    SAISON = "Saison"
    
    
    def extract_time_features(df, colDate):
        df[CRENEAU] = ['Pic_Matin' if date.hour >= 7 and date.hour < 10 else 'Pic_Soir' if date.hour >= 16 and date.hour <19 else 'Journee' if date.hour >= 10 and date.hour<16 else 'Nuit' for date in df[colDate]]
        df[ANNEE] = [date.year for date in df[colDate]]
        df[MOIS] = [date.month for date in df[colDate]]
        df[JOUR_SEMAINE] = [date.weekday() for date in df[colDate]]
        df[JOUR_MOIS] = [date.day for date in df[colDate]]
        return df
    
    meteo = extract_time_features(meteo, DATE)
    ferie = extract_time_features(ferie, "date")
    vacs = extract_time_features(vacs, "date")
    print(list(meteo.keys()))
    
    materiel = extract_time_features(materiel, DATE)
    corporel = extract_time_features(corporel, DATE)
    
    # Jointure des événements sur la météo
    print(len(meteo))
    meteo = meteo.merge(ferie[[JOUR_MOIS,MOIS,ANNEE,'ferie']], how = 'left', on = [JOUR_MOIS,MOIS,ANNEE])
    meteo = meteo.merge(vacs[[JOUR_MOIS,MOIS,ANNEE,'vacances_zone_a','vacances_zone_b','vacances_zone_c']], how = 'left', on = [JOUR_MOIS,MOIS,ANNEE])
    print(len(meteo))
    
    # On rassemble en un vecteur
    INFO_JOUR = 'Information jour'
    meteo[INFO_JOUR] = ['Vacances_Scolaires' if (bool(zoneA) or bool(zoneB) or bool(zoneC)) else 'WeekEnd' if wday in [6,7] else 'Ferie' if ferie == 1 else 'Ouvré'
                       for zoneA,zoneB,zoneC,wday,ferie in zip(meteo['vacances_zone_a'], meteo['vacances_zone_b'], meteo['vacances_zone_c'], meteo[JOUR_SEMAINE], meteo['ferie'])]
    
    meteo.drop(columns = ['vacances_zone_a', 'vacances_zone_b', 'vacances_zone_c','ferie'], inplace = True)
    
    # Regroupement par pas de temps: météo
    TIMESTEP = [CRENEAU, JOUR_MOIS, MOIS, ANNEE]
    
    agregations[JOUR_SEMAINE] = "first"
    agregations[INFO_JOUR] = "first"
    
    meteo = meteo.groupby(TIMESTEP, as_index = False).agg(agregations)
    


    # Regroupement par pas de temps: accidents
    materiel = materiel[[ID_ACC,ID_SEGT]+TIMESTEP].groupby([ID_SEGT]+TIMESTEP, as_index = False).count()
    corporel = corporel[[ID_ACC,ID_SEGT]+TIMESTEP].groupby([ID_SEGT]+TIMESTEP, as_index = False).count()
    
    # Transformation des LINK_ID en agregats
    print(len(materiel))
    materiel = materiel.merge(agregats_mat[[ID_SEGT,AGREGATS]], on = ID_SEGT, how = 'left')
    materiel.drop(columns = ID_SEGT, inplace = True)
    materiel = materiel.groupby([AGREGATS]+TIMESTEP, as_index = False).count()
    print(len(materiel))
    
    # Transformation des LINK_ID en agregats
    print(len(corporel))
    corporel = corporel.merge(agregats_corp[[ID_SEGT,AGREGATS]], on = ID_SEGT, how = 'left')
    corporel.drop(columns = ID_SEGT, inplace = True)
    corporel = corporel.groupby([AGREGATS]+TIMESTEP, as_index = False).count()
    print(len(corporel))
    
    # Ajout des agrégats de routes à la météo
    TEMPO = 'tempo'
    meteo[TEMPO] = 'tempo'
    X_MAT[TEMPO] = 'tempo'
    X_CORP[TEMPO] = 'tempo'
    
    data_mat = meteo.merge(X_MAT, how = 'left', on = TEMPO)
    data_mat.drop(columns = [TEMPO], inplace = True)
    data_mat = data_mat.merge(materiel, on = [AGREGATS]+TIMESTEP, how = 'left')
    # Important: les NAs sont des non accidents
    data_mat[[ID_ACC]] = data_mat[[ID_ACC]].fillna(0)
    data_mat.to_csv("apprentissage_materiel.csv", sep = ';', index = False, encoding = 'utf-8-sig', decimal = ',')
    
    
    data_corp = meteo.merge(X_CORP, how = 'left', on = TEMPO)
    data_corp.drop(columns = [TEMPO], inplace = True)
    data_corp = data_corp.merge(corporel, on = [AGREGATS]+TIMESTEP, how = 'left')
    # Important: les NAs sont des non accidents
    data_corp[[ID_ACC]] = data_corp[[ID_ACC]].fillna(0)
    data_corp = data_corp[data_corp[ANNEE] >= 2017]
    data_corp.to_csv("apprentissage_corporel.csv", sep = ';', index = False, encoding = 'utf-8-sig', decimal = ',')
    
    return data_mat, agregats_mat, data_corp, agregats_corp