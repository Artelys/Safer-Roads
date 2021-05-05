from get_connection import get_connection
import json

def get_infos_training():
	infos = "infos-training"
	# Open elasticsearch
	es = get_connection()
	resp = es.indices.create(index=infos, ignore=400, body={})
	resp1 = es.search(index=infos, body={"size":100}, ignore=404)
	query = {
		"sort":{
			"date":{"order": "desc"}
		},
		"query":{
			"term":{
				"type.keyword":"Training"
			}
		}
	}
	resp2 = es.search(index="error-indexes", body=query, ignore=404)
	if resp1 and resp2:
		if "hits" in resp1 and "hits" in resp2:
			return [v["_source"] for v in resp1["hits"]["hits"]] + [resp2["hits"]["hits"][0]["_source"]["error"]]
	return []

	
