
import pandas as pd
import json

from functions import typeOfJson

def here_upload(es, path, name):
	with open(path, "r", encoding="utf-8") as f:
		data = json.load(f)	
		data = data["features"]

	err= ""
	important_columns = [
		"LINK_ID",
		"SPEED_CAT",
		"ST_NAME_Alt",
		"ST_NAME",
		"FUNC_CLASS",
		"N_SHAPEPNT",
		"Shape_Length",
		"PRIVATE",
		"PAVED",
		"ROUNDABOUT",
		"FRONTAGE",
		"TOLLWAY",
		"BRIDGE",
		"TUNNEL",
		"RAMP",
		"CONTRACC",
		"POIACCESS",
		"Meters"
	]
	for column in important_columns:
		if column not in data[0]["properties"]:
			err += " ERREUR : La colonne "+column+" n'est pas présente\n"

	if err:
		return err, "here"

	n = 1000
	to_send = []
	for index, row in enumerate(data):
		geometry = row["geometry"]
		row = row["properties"]
		row["geometry"] = geometry

		if (index+1)%n == 0:
			if not to_send:
				continue
			resp = es.bulk(index=name, body=to_send, request_timeout=3000)
			# If there are errors
			if resp["errors"]:
				for i, item in enumerate(resp["items"]):
					if "error" in item["index"].keys():
						err += "ERREUR : La ligne "+str(index+2-n+i)+ " contient une erreur\n"
						err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
						return err, 'here'

			to_send = []
		to_send.append({"index":{}})
		to_send.append(row)
		
	if to_send:
		resp = es.bulk(index=name, body=to_send, request_timeout=3000)
		if resp["errors"]:
			for i, item in enumerate(resp["items"]):
				if "error" in item["index"].keys():
					err += "ERREUR : La ligne "+str((data.shape[0]//n)*data.shape[0]+i)+ " contient une erreur\n"
					err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
					return err, 'here'	

	return err, 'here'