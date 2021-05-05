# -*- coding: utf-8 -*-
"""
Created on Tue Mar 10 14:10:53 2020

@author: aboutet
"""



# Modules Python
import numpy as np
import pandas as pd
import datetime
import os
import sys

from variables import *
from workalendar.europe import France
sys.path.append(os.getcwd())

from functions import get_data_frame_elastic

def data_preparation(es):
    
    DATE = "Date"
    
    FEATURES_HERE_QUALI_NO_ORDER = ["PRIORITYRD", "PAVED","PRIVATE", CORINE, TRAFIC, CONTIENT_INTERSECTION, ADJ_SUP]
    FEATURES_HERE_QUALI_ORDERED = ["SPEED_CAT","FUNC_CLASS"]
    FEATURES_HERE_QUANTI = ["N_SHAPEPNT","Shape_Length"]
    
    here_columns = [
        "LINK_ID", "PRIVATE", "PAVED", "PRIORITYRD", "Type_Zone_3", "Trafic", 
        "SPEED_CAT", "FUNC_CLASS", "N_SHAPEPNT", "Shape_Length", "REF_IN_ID", 
        "NREF_IN_ID", "Courbure", CONTIENT_INTERSECTION, ADJ_INF, ADJ_SUP
                    ]
    # Import des données here
    here = get_data_frame_elastic(es, "here", here_columns)
    here = here[here_columns]
    here["N_SHAPEPNT"] = here["Courbure"]
    here[CORINE] = here["Type_Zone_3"]
    here[TRAFIC] = here["Trafic"]
    here["SPEED_CAT"] = here["SPEED_CAT"].astype(int)
    
    # On garde qu'un certains nombres de colonnes
    X = here[[ID_SEGT]+FEATURES_HERE_QUALI_ORDERED+FEATURES_HERE_QUALI_NO_ORDER+FEATURES_HERE_QUANTI]
    accidents_columns = [
            "id_corporel", "id_sdis", "id_materiel", "id_waze", "id_coyote",
            ID_SEGT, TYPE, SUBTYPE, WAZE_TOTAL, WAZE_JAM, WAZE_HAZARD, DATE,
            ID_ACC
            ]
    accidents = get_data_frame_elastic(es, "accidents", accidents_columns)

    ####
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
        df[TOTAL_COYOTE] = len(x)
        return df
         
    coyote = coyote[[ID_SEGT]]
    coyoteRoad = coyote.groupby(ID_SEGT, as_index=False).apply(compter_alertes_coyotes)
    coyoteRoad[ID_SEGT] = coyoteRoad[ID_SEGT].astype(int)
    X = X.merge(coyoteRoad, on = ID_SEGT, how = 'left')
    X[TOTAL_COYOTE] = X[TOTAL_COYOTE].fillna(0)
    
    
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
    for col in [WAZE_TOTAL, WAZE_JAM, WAZE_HAZARD, TOTAL_COYOTE]:
        X[col] = [val/lon for val, lon in zip(X[col],X['Shape_Length'])]
    
    # import des donénes d'accidents materiel
    thelem = accidents[accidents["id_materiel"].notna()]
    sdis = accidents[accidents["id_sdis"].notna()]
    
    materiel = thelem.append(sdis)
    materiel[DATE] = pd.to_datetime(materiel[DATE], infer_datetime_format=True)
    materiel = materiel[materiel[DATE] >= np.datetime64('2014-01-01')]
    
    def compter_materiel(x):
        df = pd.DataFrame({ID_SEGT:x[ID_SEGT].unique()})
        df["Total materiel"] = len(x)
        return df
    materiel_count = materiel[[ID_SEGT]].astype(int).groupby(ID_SEGT, as_index=False).apply(compter_materiel)
    X = X.merge(materiel_count, on=ID_SEGT, how="left")
    X[MATERIEL] = X[MATERIEL].fillna(0)
    
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
    
    
    QUANTI_KEEP = [WAZE_TOTAL, "N_SHAPEPNT", WAZE_JAM, WAZE_HAZARD, TRAFIC, TOTAL_COYOTE]
    # Création des agrégats
    
    # Agregat materiel
    QUANTI_KEEP_MAT = QUANTI_KEEP
    QUALI_ORDERED_KEEP_MAT = ["FUNC_CLASS","SPEED_CAT"]
    QUALI_NOORDER_KEEP_MAT = [CORINE, "PAVED", "PRIVATE", "PRIORITYRD"]
    # Agregat corporel
    QUANTI_KEEP_CORP = QUANTI_KEEP + [MATERIEL]
    QUALI_ORDERED_KEEP_CORP = ["FUNC_CLASS", "SPEED_CAT"]
    QUALI_NOORDER_KEEP_CORP = ["PAVED", ADJ_SUP, CONTIENT_INTERSECTION]
    
    # Constitution des agrégats
    AGREGATS = 'Agregat'
    
    X_agg_mat = X_mat[[ID_SEGT]+QUANTI_KEEP_MAT+QUALI_ORDERED_KEEP_MAT+QUALI_NOORDER_KEEP_MAT]
    X_agg_mat[AGREGATS] = 0
    
    X_agg_corp = X_corp[[ID_SEGT]+QUANTI_KEEP_CORP+QUALI_ORDERED_KEEP_CORP+QUALI_NOORDER_KEEP_CORP]
    X_agg_corp[AGREGATS] = 0
    
    np.random.seed(0)
    def attribuer_agregat(df):
        df[AGREGATS] = np.random.randint(0,1000000)
        return df
    
    agregats_mat = X_agg_mat.groupby(QUALI_ORDERED_KEEP_MAT+QUALI_NOORDER_KEEP_MAT).apply(attribuer_agregat)
    aggregation = {value:"mean" for value in QUANTI_KEEP_MAT}
    X_MAT = agregats_mat.groupby([AGREGATS]+QUALI_ORDERED_KEEP_MAT+QUALI_NOORDER_KEEP_MAT, as_index=False).agg(aggregation)
    agregats_mat.to_csv("training/cle_agregat_materiel.csv", sep = ';', index = False, encoding = 'utf-8-sig', decimal = ',')
    
    agregats_corp = X_agg_corp.groupby(QUALI_ORDERED_KEEP_CORP+QUALI_NOORDER_KEEP_CORP).apply(attribuer_agregat)
    aggregation = {value:"mean" for value in QUANTI_KEEP_CORP}
    X_CORP = agregats_corp.groupby([AGREGATS]+QUALI_ORDERED_KEEP_CORP+QUALI_NOORDER_KEEP_CORP, as_index=False).agg(aggregation)
    agregats_corp.to_csv("training/cle_agregat_corporel.csv", sep = ';', index = False, encoding = 'utf-8-sig', decimal = ',')

    ## Lecture des données de contexte
    DATE='Date'
    
    # Lire données météo pour avoir tous les pas de temps
    meteo = get_data_frame_elastic(es, "meteo")
    agregations = {'Temperature':'mean', 'Pression 0m':'mean', 'Direction Vent':'mean', 'Vitesse Vent':'mean', 
                    'Pluviometrie 1h':'mean', 'Pluviometrie 6h':'mean'}
    METEO_QUANTI = list(agregations.keys())
    
    meteo[METEO_QUANTI] = meteo[METEO_QUANTI].fillna(method = 'bfill')
    meteo[DATE] = pd.to_datetime(meteo[DATE], infer_datetime_format=True)
    
    meteo = meteo[METEO_QUANTI+[DATE]].fillna(method = 'bfill')
    meteo[DATE] = pd.to_datetime(meteo[DATE], infer_datetime_format=True)
    meteo = meteo.drop_duplicates(subset = [DATE], keep = 'first')
    # Add meteo to empty spot
    debut = min(materiel[DATE].min(), corporel[DATE].min())
    fin = max(materiel[DATE].max(), corporel[DATE].max())
    
    timeline = pd.DataFrame({DATE:np.arange(debut, fin, dtype='datetime64[h]')})
    meteo = timeline.merge(meteo, on = DATE, how = 'left')
    meteo = meteo.fillna(method = 'bfill').fillna(method = 'ffill')

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
    
    materiel = extract_time_features(materiel, DATE)
    corporel = extract_time_features(corporel, DATE)

    corporel_min_date = corporel[DATE].min()
    corporel_max_date = corporel[DATE].max()
    materiel_min_date = materiel[DATE].min()
    materiel_max_date = materiel[DATE].max()
   
    def extract_infos(row):
        date = row[DATE]
        f = France()
        periode = "Ouvré"
        if date.weekday() <= 4 and not f.is_holiday(date) and not f.is_working_day(date):
            periode = "Ferie"
        if date.weekday() > 4:
            periode = "WeekEnd"
        if f.is_holiday(date) or f.is_holiday(date +datetime.timedelta(days=1)):
            periode = "Vacances_Scolaires"
        
        row[INFO_JOUR] = periode
        return row
    
    meteo = meteo.apply(extract_infos, axis=1)
    
    # Regroupement par pas de temps: météo
    TIMESTEP = [CRENEAU, JOUR_MOIS, MOIS, ANNEE]
    
    agregations[JOUR_SEMAINE] = "first"
    agregations[INFO_JOUR] = "first"
    
    meteo = meteo.groupby(TIMESTEP, as_index = False).agg(agregations)
    


    # Regroupement par pas de temps: accidents
    materiel = materiel[[ID_ACC,ID_SEGT]+TIMESTEP].groupby([ID_SEGT]+TIMESTEP, as_index = False).count()
    corporel = corporel[[ID_ACC,ID_SEGT]+TIMESTEP].groupby([ID_SEGT]+TIMESTEP, as_index = False).count()
    
    # Transformation des LINK_ID en agregats
    materiel = materiel.merge(agregats_mat[[ID_SEGT,AGREGATS]], on = ID_SEGT, how = 'left')
    materiel.drop(columns = ID_SEGT, inplace = True)
    materiel = materiel.groupby([AGREGATS]+TIMESTEP, as_index = False).count()
    # Transformation des LINK_ID en agregats
    corporel = corporel.merge(agregats_corp[[ID_SEGT,AGREGATS]], on = ID_SEGT, how = 'left')
    corporel.drop(columns = ID_SEGT, inplace = True)
    corporel = corporel.groupby([AGREGATS]+TIMESTEP, as_index = False).count()
    
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
    data_mat[DATE] = pd.to_datetime(data_mat[[ANNEE, MOIS, JOUR_MOIS]].rename(columns={ANNEE:"year", MOIS:"month", JOUR_MOIS:"day"}))
    data_mat = data_mat[data_mat[DATE] >= materiel_min_date]
    data_mat = data_mat[data_mat[DATE] <= materiel_max_date]
    data_mat.drop([DATE], axis=1)
    data_mat.to_csv("training/apprentissage_materiel.csv", sep = ';', index = False, encoding = 'utf-8-sig', decimal = ',')
    
    
    data_corp = meteo.merge(X_CORP, how = 'left', on = TEMPO)
    data_corp.drop(columns = [TEMPO], inplace = True)
    data_corp = data_corp.merge(corporel, on = [AGREGATS]+TIMESTEP, how = 'left')
    # Important: les NAs sont des non accidents
    data_corp[[ID_ACC]] = data_corp[[ID_ACC]].fillna(0)
    data_corp[DATE] = pd.to_datetime(data_corp[[ANNEE, MOIS, JOUR_MOIS]].rename(columns={ANNEE:"year", MOIS:"month", JOUR_MOIS:"day"}))
    data_corp = data_corp[data_corp[DATE] >= corporel_min_date]
    data_corp = data_corp[data_corp[DATE] <= corporel_max_date]
    data_corp.drop([DATE], axis=1)
    data_corp.to_csv("training/apprentissage_corporel.csv", sep = ';', index = False, encoding = 'utf-8-sig', decimal = ',')
    
    return data_mat, agregats_mat, data_corp, agregats_corp
