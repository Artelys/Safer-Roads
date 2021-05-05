import sys
import os
import json
from get_connection import get_connection
from datetime import datetime
import logging

sys.path.insert(0, os.getcwd())
from corporels_upload import corporels_upload
from materiels_upload import materiels_upload
from positions_upload import positions_upload
from contexte_upload import contexte_upload
from zones_upload import zones_upload
from here_upload import here_upload
from infos_routes_upload import infos_routes_upload

from functions import create_error, send_info


def integrate(path, category, name, date):
	name = category+"-"+date+"-"+name
	name = name.lower()
	file_mapping = open("mapping.json")
	mappings = json.load(file_mapping)
	# Open elasticsearch
	es = get_connection()
	date = datetime.strptime(date, "%Y.%m.%d.%H.%M.%S")
	es.indices.create(index=name, body=mappings[category])
	# Create and update information of index
	infos = "infos-indexes"
	error_index = "error-indexes"
	resp = es.indices.create(index=infos, ignore=400, body=mappings[infos])
	resp = es.indices.create(index=error_index, ignore=400, body=mappings[error_indexes])
	send_info(es, name, date, category, infos)

	categories_upload_function = {
		"accidents_corporels": corporels_upload,
		"accidents_materiels": materiels_upload,
		"positions": positions_upload,
		"contexte": contexte_upload,
		"zones": zones_upload,
		"voirie_de_reference": here_upload,
		"infos_routes" : infos_routes_upload
	}

	# Launch upload
	try:
		error, category = categories_upload_function[category](es, path, name)
		if not error:
			print('no error')
			query = {
				"query": {
					"bool": {
						"must": {
							"match": {
								"name.keyword": name
							}
						}
					}
				},
	             "script": "ctx._source.current = 'Send';ctx._source.category = '"+category+"';"
			}
			resp = es.update_by_query(index=infos, body=query, request_timeout=30000)
			error = "Successfully integrate "+str(name)+" to Elasticsearch"
		else:

			print(error)
			# Delete index
			es.delete_by_query(index=infos, 
				body={"query":{
		                "bool":{
		                    "must":{
		                        "match":{
		                            "name.keyword":name
		                            }
		                        }
		                }
		            }})
			es.indices.delete(index=name, ignore=[400, 404])
	except Exception as e:
		error = repr(e)
		# Delete index
		es.delete_by_query(index=infos,
						   body={"query": {
							   "bool": {
								   "must": {
									   "match": {
										   "name.keyword": name
									   }
								   }
							   }
						   }})
		es.indices.delete(index=name, ignore=[400, 404])
	finally:
		create_error(es, name, date.strftime("%Y-%m-%d %H:%M:%S"), "send", error)

if __name__ == "__main__":
	j = json.loads(sys.argv[1])
	path = j["path"]
	category = j["type"]
	name = j["name"]
	date = j["date"]
	integrate(path, category, name, date)
