from get_connection import get_connection
import json

def get_index():
	print("GET INDEX")
	infos = "infos-indexes"
	file_mapping = open("mapping.json")
	mappings = json.load(file_mapping)
	# Open elasticsearch
	es = get_connection()
	resp = es.indices.create(index=infos, ignore=400, body=mappings[infos])
	resp = es.search(index=infos, body={"size":100}, ignore=404)
	if resp:
		if "hits" in resp.keys():
			return [v["_source"] for v in resp["hits"]["hits"] if v["_source"]["current"] != "Sending"]
	print(resp)
	return []