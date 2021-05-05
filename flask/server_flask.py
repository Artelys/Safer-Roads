# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 17:28:34 2019

@author: aboutet
"""
"""
from sklearn import tree, ensemble, metrics, model_selection
from sklearn.preprocessing import scale, StandardScaler
from sklearn.tree import export_graphviz
from sklearn.externals.six import StringIO  
from sklearn.metrics import make_scorer
from sklearn.metrics import roc_curve, auc, roc_auc_score
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.model_selection import ShuffleSplit

from sklearn.linear_model import LogisticRegression
from sklearn.cluster import KMeans
import pickle
"""
import subprocess
import sys
import os
import json

from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
from send.get_error_send import get_error_send
from get_index import get_index
from get_infos_training import get_infos_training



app = Flask(__name__)

@app.route('/', methods=['POST','GET'])
def home():
    print("You made a request to the flask server")
    print("Write in python")
    return 'Hello world'


@app.route('/harmonise', methods=['POST','GET'])
def harmonise():
    print("harmonise")
    j = request.get_json()
    j = json.dumps(j)
    process = subprocess.run(
        ["python", "harmonisation/pivot_data.py", j], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        universal_newlines=True
        )
    print(process.stdout)
    print(process.stderr)
    return ""


@app.route('/training', methods=['POST','GET'])
def training():
    print("training")
    process = subprocess.Popen(
        ["python", "training/training.py"], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        universal_newlines=True
        )
    print(process)
    print(process.stdout.read())
    print(process.stderr.read())
    return ""

@app.route('/prediction', methods=['POST','GET'])
def prediction():
    j = request.get_json()
    j = json.dumps(j)
    f = open("te.txt", "w")
    f.write(j)
    f.close()
    process = subprocess.Popen(
        ["python", "training/prediction.py", j], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        universal_newlines=True
        )
    print(process.stdout.read())
    print(process.stderr.read())
    return ""

@app.route('/get_index', methods=['POST','GET'])
def index():
    return json.dumps(get_index())

@app.route('/get_infos_training', methods=['POST','GET'])
def infos_training():
    return json.dumps(get_infos_training())

@app.route('/send', methods=['POST','GET'])
def send():
    j = request.get_json()
    j = json.dumps(j)
    process = subprocess.Popen(
        ["python", "send/send.py", j], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        universal_newlines=True
        )
    print(process.stdout.read())
    print(process.stderr.read())
    return ""

@app.route('/get_error_send', methods=['POST','GET'])
def get_error():
    j = request.get_json()
    r = False
    if j is not None and j != '':
        r = get_error_send(j["name"], j["type"])
    if r:
        return json.dumps(r)
    else:
        return ""

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
