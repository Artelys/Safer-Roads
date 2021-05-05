
import pandas as pd

from functions import typeOfJson

def infos_routes_upload(es, path, name):
	data = pd.read_csv(path, delimiter=";", decimal=".")
	print(data.shape)
	category = typeOfJson(name, {key:0 for key in data.columns})
	if category == "trafic":
		return trafic_upload(es, data, name)
	else:
		return street_pattern_upload(es, data, name)

def street_pattern_upload(es, data, name):
	err= ""
	columns_names = list(data)
	important_columns = [
		"LINK_ID",
		"TRAVEL_DIRECTION",
		"AverageSpeed",
		"BaseSpeed",
		"EdgeFCID",
		"EdgeFID",
		"EdgeFromPos",
		"EdgeToPos"
	]
	for column in important_columns:
		if column not in columns_names:
			err += " ERREUR : La colonne "+column+" n'est pas présente\n"
	if err:
		return err, "street_pattern"

	data = data.replace({pd.np.nan: None})
	n = 1000
	to_send = []
	for index, row in enumerate(data.to_dict(orient='records')):

		if (index+1)%n == 0:
			if not to_send:
				continue
			resp = es.bulk(index=name, body=to_send, request_timeout=300)
			# If there are errors
			if resp["errors"]:
				for i, item in enumerate(resp["items"]):
					if "error" in item["index"].keys():
						err += "ERREUR : La ligne "+str(index+2-n+i)+ " contient une erreur\n"
						err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
						return err, 'street_pattern'

			to_send = []
		to_send.append({"index":{}})
		to_send.append(row)
		
	if to_send:
		resp = es.bulk(index=name, body=to_send, request_timeout=300)
		if resp["errors"]:
			for i, item in enumerate(resp["items"]):
				if "error" in item["index"].keys():
					err += "ERREUR : La ligne "+str((data.shape[0]//n)*data.shape[0]+i)+ " contient une erreur\n"
					err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
					return err, 'street_pattern'	

	return err, 'street_pattern'

def trafic_upload(es, data, name):
	err= ""
	columns_names = list(data)
	important_columns = [
		"LINK_ID",
		"Trafic"
	]
	for column in important_columns:
		if column not in columns_names:
			err += " ERREUR : La colonne "+column+" n'est pas présente\n"
	if err:
		return err, "trafic"

	data = data.replace({pd.np.nan: None})
	n = 1000
	to_send = []
	for index, row in enumerate(data.to_dict(orient='records')):

		if (index+1)%n == 0:
			if not to_send:
				continue
			resp = es.bulk(index=name, body=to_send, request_timeout=300)
			# If there are errors
			if resp["errors"]:
				for i, item in enumerate(resp["items"]):
					if "error" in item["index"].keys():
						err += "ERREUR : La ligne "+str(index+2-n+i)+ " contient une erreur\n"
						err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
						return err, 'trafic'

			to_send = []
		to_send.append({"index":{}})
		to_send.append(row)
		
	if to_send:
		resp = es.bulk(index=name, body=to_send, request_timeout=300)
		if resp["errors"]:
			for i, item in enumerate(resp["items"]):
				if "error" in item["index"].keys():
					err += "ERREUR : La ligne "+str((data.shape[0]//n)*data.shape[0]+i)+ " contient une erreur\n"
					err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
					return err, 'trafic'	

	return err, 'trafic'