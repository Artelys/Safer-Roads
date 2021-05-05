import os
import sys

sys.path.append(os.getcwd())
wd = "/home/begood/flask"
os.chdir(wd)

from get_connection import get_connection

from functions import create_error
from preparation import data_preparation
from training_corporel import training_corporel
from training_materiel import training_materiel

from get_infos_training import get_infos_training

from datetime import datetime

if __name__ == "__main__":
    print("TRAINING")
    error = "The model was successfully created"
    es = get_connection()
    # On vérifie que l'on est pas déjà en train d'entrainer
    infos_training = "infos-training"
    infos = get_infos_training()
    date = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    for info in infos[:3]:
        if info['Name'] == "Training":
            error = "Currently training"
            create_error(es, "Training", date, "Training", error)
            sys.exit()
        if info['Name'] == "Train":
            date_last = info["Date"]

    es.index(index=infos_training, id="Training",
             body={"Name": "Training", "Date": date})
    er = False
    try:
        data_mat, agregats_mat, data_corp, agregats_corp = data_preparation(es)
        print("PREPARATION FINI")
        baro_corp = training_corporel(es, data_corp)
        baro_mat = training_materiel(es, data_mat)

        importance_corp = baro_corp.feature_importances_(plot=False)
        importance_mat = baro_mat.feature_importances_(plot=False)

        index_corp = "facteurs_accidentogenes_corporels"
        index_mat = "facteurs_accidentogenes_materiels"

        values, names = importance_corp.values, importance_corp.index
        to_send = []
        for value, name in zip(values, names):
            to_send.append({"index": {"_id": name}})
            to_send.append({"Score_importance": value, "Nom_variable": name})
        resp = es.bulk(index=index_corp, body=to_send, request_timeout=3000)

        values, names = importance_mat.values, importance_mat.index
        to_send = []
        for value, name in zip(values, names):
            to_send.append({"index": {"_id": name}})
            to_send.append({"Score_importance": value[0], "Nom_variable": name})
        resp = es.bulk(index=index_mat, body=to_send, request_timeout=3000)

        v = baro_corp.predict_with_result(None, None, learning_data=True, print_=False, retourne=True)
        resp = es.bulk(index=infos_training,
                       body=[{"index": {"_id": "Corporel"}},
                             {"Name": "Corporel", "Confusion": v[1].tolist(), "Precision": v[2], "Recall": v[3],
                              "Giny": v[4]}],
                       request_timeout=3000)

        v = baro_mat.predict_with_result(None, None, learning_data=True, print_=False, retourne=True)
        resp = es.bulk(index=infos_training,
                       body=[{"index": {"_id": "Materiel"}},
                             {"Name": "Materiel", "Confusion": v[1].tolist(), "Precision": v[2], "Recall": v[3],
                              "Giny": v[4]}],
                       request_timeout=3000)
    except Exception as e:
        error = "" + repr(e)
        er = True
    finally:
        print("end of training")
        create_error(es, "Training", date, "Training", error)
        if er:
            date = date_last
        es.index(index=infos_training, id="Training", body={"Name": "Train", "Date": date})
