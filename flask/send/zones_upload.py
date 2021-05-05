
import pandas as pd
import json

from functions import typeOfJson

def zones_upload(es, path, name):
	with open(path, "r") as f:
		data = json.load(f)	
		data = data["features"]
		category = typeOfJson(name, data[0]["properties"])
		if category == "corine":
			return corine_upload(es, data, name)
		else:
			return admin_upload(es, data, name)

def corine_upload(es, data, name):
	err= ""
	important_columns = [
		"ID",
		"CODE_18",
		"AREA_ID",
		"POLYGON_NM"
	]
	for column in important_columns:
		if column not in data[0]["properties"]:
			err += " ERREUR : La colonne "+column+" n'est pas présente\n"

	if err:
		return err, "corine"

	n = 1000
	to_send = []
	for index, row in enumerate(data):
		geometry = row["geometry"]
		row = row["properties"]
		row["geometry"] = geometry

		if (index+1)%n == 0:
			if not to_send:
				continue
			resp = es.bulk(index=name, body=to_send, request_timeout=30000)
			# If there are errors
			if resp["errors"]:
				for i, item in enumerate(resp["items"]):
					if "error" in item["index"].keys():
						err += "ERREUR : La ligne "+str(index+2-n+i)+ " contient une erreur\n"
						err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
						print(err)
						return err, 'corine'

			to_send = []
		to_send.append({"index":{}})
		to_send.append(row)
		
	if to_send:
		resp = es.bulk(index=name, body=to_send, request_timeout=30000)
		if resp["errors"]:
			for i, item in enumerate(resp["items"]):
				if "error" in item["index"].keys():
					err += "ERREUR : La ligne "+str((data.shape[0]//n)*data.shape[0]+i)+ " contient une erreur\n"
					err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
					return err, 'corine'	

	return err, 'corine'

def admin_upload(es, data, name):
	err= ""
	important_columns = [
		"AREA_ID",
		"POLYGON_NM"
	]
	for column in important_columns:
		if column not in data[0]["properties"]:
			err += " ERREUR : La colonne "+column+" n'est pas présente\n"

	if err:
		return err, "admin"

	n = 1000
	to_send = []
	for index, row in enumerate(data):
		geometry = row["geometry"]
		row = row["properties"]
		row["geometry"] = geometry

		if (index+1)%n == 0:
			if not to_send:
				continue
			resp = es.bulk(index=name, body=to_send, request_timeout=30000)
			# If there are errors
			if resp["errors"]:
				for i, item in enumerate(resp["items"]):
					if "error" in item["index"].keys():
						err += "ERREUR : La ligne "+str(index+2-n+i)+ " contient une erreur\n"
						err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
						return err, 'admin'

			to_send = []
		to_send.append({"index":{}})
		to_send.append(row)
		
	if to_send:
		resp = es.bulk(index=name, body=to_send, request_timeout=30000)
		if resp["errors"]:
			for i, item in enumerate(resp["items"]):
				if "error" in item["index"].keys():
					err += "ERREUR : La ligne "+str((data.shape[0]//n)*data.shape[0]+i)+ " contient une erreur\n"
					err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
					return err, 'admin'	

	return err, 'admin'