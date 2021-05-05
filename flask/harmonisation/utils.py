# -*- coding:utf8 -*-
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import requests
import time
import json
import os


def create_index(es, index, mapping, crush_previous = False):
    # print("Creation of index "+index)
    if mapping is None:
        try:
            es.indices.create(index=index)
        except:
            if crush_previous:
                print("(remplacement of existing index)")
                es.indices.delete(index=index, ignore=400)
                es.indices.create(index=index, ignore=400)
            else:
                return
    else:
        try:
            es.indices.create(index=index, body=mapping)
        except:
            if crush_previous:
                es.indices.delete(index=index, ignore=400)
                res = es.indices.create(index=index, ignore=400, body=mapping)
                print(res)
            else:
                return

def raw_indexation_json(es, filePath, index, type, limit = None):
    print("Launch bulk json")
    file = open(filePath,"r")
    l = file.read()
    l = l.replace("\n","")
    l = l.replace("\r","")

    content = json.loads(l)
    if limit is None:
        toIterate = content['features']
    else:
        toIterate = content['features'][:limit]
    for i,f in enumerate(toIterate):
        #print(f)
        #exit()
        res = es.index(index=index, doc_type=type, body=f, id = i)
        print(res)


def mapping_geom(es, index, json_file):
    index = index
    type_ = "_doc"
    mapping = {"mappings": {
          "properties": {
              "geometry": {
              "type": "geo_shape",
              "tree":"quadtree",
              "precision": "1m" # Ajouter stratégie?
            }
        }
      }
    }

    create_index(es, index, mapping = mapping, crush_previous = True)
    raw_indexation_json(es, json_file, index, type_, limit = None)



def csv_reader(file_obj, index, delimiter=','):
    reader = csv.DictReader(file_obj)
    i = 1
    results = []
    for row in reader:
        es.index(index=index, doc_type='prod', #id=i,
                 body=json.dumps(row))
        #i = i + 1

        results.append(row)
