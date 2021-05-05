
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 18:07:11 2020

@author: aboutet
"""
import pickle
import pandas as pd
import sys
import os
import time
import json

sys.path.append(os.getcwd())
# TODO CATASTROPHIQUE !!!

wd ="/home/begood/flask"
os.chdir(wd)

from get_connection import get_connection
from training_corporel import barometre_accidents_corporels
from training_materiel import barometre_accidents_materiels

from functions import get_data_frame_elastic
from variables import *

import datetime
from workalendar.europe import France


def prediction(contexte):
    index = "here"

    pklName = 'training/Barometre_corporel.pkl'
    _file = open(pklName, 'rb')
    baro_corp = pickle.load(_file)

    pklName = 'training/Barometre_materiel.pkl'
    _file = open(pklName, 'rb')
    baro_mat = pickle.load(_file)

    date = datetime.datetime.strptime(contexte["Date"], "%Y-%m-%dT%H:%M:%S.%fZ")
    contexte["Date"] = date.strftime("%m/%d/%Y %H:%M:%S")
    f = France()
    periode = "Ouvré"
    if date.weekday() <= 4 and not f.is_holiday(date) and not f.is_working_day(date):
        periode = "Ferie"
    if date.weekday() > 4:
        periode = "WeekEnd"
    if f.is_holiday(date) or f.is_holiday(date + datetime.timedelta(days=1)):
        periode = "Vacances_Scolaires"

    contexte["Information jour"] = periode
    contexte["Mois"] = date.month
    contexte["Jour de la semaine"] = date.weekday()
    contexte["Direction Vent"] = float(contexte["Direction Vent"])
    contexte["Vitesse Vent"] = float(contexte["Vitesse Vent"])
    if isinstance(contexte["Pluviometrie 1h"], str):
        contexte["Pluviometrie 1h"] = float(contexte["Pluviometrie 1h"].replace(",", "."))
 
    if isinstance(contexte["Pluviometrie 6h"], str):
        contexte["Pluviometrie 6h"] = float(contexte["Pluviometrie 6h"].replace(",", "."))   
    
    if isinstance(contexte["Temperature"], str):
        contexte["Temperature"] = float(contexte["Temperature"].replace(",", "."))
 
    if isinstance(contexte["Pression 0m"], str):
        contexte["Pression 0m"] = float(contexte["Pression 0m"].replace(",", ".")) 
    
    es = get_connection()

    mapping_context = open("mapping.json")
    mappings = json.load(mapping_context)
    context_index = "context_prediction"
    es.indices.create(index=context_index, body={"mappings":mappings[context_index]}, ignore=400)
    es.index(index=context_index, body=contexte, id="context")

    contexte["Vitesse Vent"] /= 3.6

    del contexte["Date"]

    aggregation = ["N_SHAPEPNT", WAZE_TOTAL, WAZE_JAM, WAZE_HAZARD, "Nombre total d'alertes Coyote", TRAFIC]
    aggregation = {key: "mean" for key in aggregation}
    
    mat = pd.read_csv("training/cle_agregat_materiel.csv", delimiter=";", decimal=",")
    data = mat.groupby(["Agregat", "FUNC_CLASS", "SPEED_CAT", "PAVED", CORINE, "PRIVATE", "PRIORITYRD"],
                            as_index=False).agg(aggregation)
    data_mat = data
    for key, value in contexte.items():
        data_mat[key] = value
    aggregation["Total materiel"] = "mean"
    
    corp = pd.read_csv("training/cle_agregat_corporel.csv", delimiter=";", decimal=",")
    data = corp.groupby(["Agregat", "FUNC_CLASS", "SPEED_CAT", "PAVED", ADJ_SUP, CONTIENT_INTERSECTION],
                             as_index=False).agg(aggregation)
    data_corp = data
    for key, value in contexte.items():
        data_corp[key] = value
    
    value = baro_mat.predict(data_mat, learning_data=False, probas=True)
    data_mat["Risque materiel"] = [x[0] for x in value]
    data_mat = data_mat.merge(mat, on=AGREGATS, how="left")
    data_mat = data_mat[["Risque materiel", "LINK_ID"]]
    value = baro_corp.predict(data_corp, learning_data=False, probas=True)
    data_corp["Risque corporel"] = [x for x, y in value]
    data_corp = data_corp.merge(corp, on=AGREGATS, how="left")
    data_corp = data_corp[["Risque corporel", "LINK_ID"]]

    df_here = get_data_frame_elastic(es, index, ["LINK_ID"])
    data = df_here.merge(data_mat, on="LINK_ID", how="left").replace({pd.np.nan: None})
    data = data.merge(data_corp, on="LINK_ID", how="left").replace({pd.np.nan: None})
    to_send = []
    i = 0
    for row in data.to_dict(orient="records"):
        i += 1
        if (len(to_send) + 2) % 1000 == 0:
            resp = es.bulk(index=index, body=to_send, request_timeout=3000)
            if resp["errors"]:
                for i, item in enumerate(resp["items"]):
                    if "error" in item["index"].keys():
                        return
            to_send = []
        to_send.append({"update": {"_id": row["id"]}})
        del row["id"]
        del row["LINK_ID"]
        to_send.append({"doc": row})
    if to_send:
        es.bulk(index=index, body=to_send, request_timeout=3000)

if __name__ == "__main__":
    j = json.loads(sys.argv[1])
    print(j)
    del j["path_python"]
    prediction(j)
