import sys
import os
import json
from get_connection import get_connection

def get_error_send(name, type):
	# Open elasticsearch
	es = get_connection()
	resp = es.search(index="error-indexes", body={"query":{"bool":{"must":
		[{"term":{"name.keyword":name}}, 
		{"term":{"type.keyword":type}}]
		}}})
	if resp:
		if "hits" in resp.keys():
			if resp["hits"]["hits"]:
				return resp["hits"]["hits"][0]["_source"]
	return ""