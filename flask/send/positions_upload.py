
import pandas as pd

from functions import typeOfJson

def positions_upload(es, path, name):
	data = pd.read_csv(path, delimiter=";", encoding ='utf-8', decimal=".")
	print(data.shape)
	category = typeOfJson(name, {key:0 for key in data.columns})
	if category == "waze":
		return wazes_upload(es, data, name)
	else:
		return coyotes_upload(es, data, name)

def wazes_upload(es, data, name):
	err= ""
	# Check the important column present
	columns_names = list(data)
	important_columns = [
		"ID",
		"Date",
		"TYPE",
		"SUBTYPE",
		"LATITUDE",
		"LONGITUDE"
	]
	for column in important_columns:
		if column not in columns_names:
			err += " ERREUR : La colonne "+column+" n'est pas présente\n"

	if err:
		return err, "wazes"
	# Traitement
	data[["LONGITUDE", "LATITUDE"]] = data[["LONGITUDE", "LATITUDE"]].replace(",", ".")
	data[["LONGITUDE", "LATITUDE"]] = data[["LONGITUDE", "LATITUDE"]].apply(check_float, axis=1)

	data = data.replace({pd.np.nan: None})
	n = 1000
	to_send = []
	for index, row in enumerate(data.to_dict(orient='records')):
		if (index+1)%n == 0:
			resp = es.bulk(index=name, body=to_send, request_timeout=300)
			# If there are errors
			if resp["errors"]:
				for i, item in enumerate(resp["items"]):
					if "error" in item["index"].keys():
						print(item)
						err += "ERREUR : La ligne "+str(index+2-n+i)+ " contient une erreur\n"
						err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
						return err, 'wazes'

			to_send = []
		row["geometry"] = {"lat":row["LATITUDE"], "lon":row["LONGITUDE"]}
		to_send.append({"index":{}})
		to_send.append(row)
	if to_send:
		resp = es.bulk(index=name, body=to_send, request_timeout=300)
		if resp["errors"]:
			for i, item in enumerate(resp["items"]):
				if "error" in item["index"].keys():
					err += "ERREUR : La ligne "+str(data.shape[0]%n+i)+ " contient une erreur\n"
					err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
					return err, 'wazes'	

	return err, 'wazes'


def coyotes_upload(es, data, name): 
	err= ""
	# Check the important column present
	columns_names = list(data)
	important_columns = [
		"ID",
		"Date",
		"TYPE",
		"LATITUDE",
		"LONGITUDE"
	]
	for column in important_columns:
		if column not in columns_names:
			err += " ERREUR : La colonne "+column+" n'est pas présente\n"

	if err:
		return err, "coyotes"
	# Traitement
	data[["LONGITUDE", "LATITUDE"]] = data[["LONGITUDE", "LATITUDE"]].replace(",", ".")
	data[["LONGITUDE", "LATITUDE"]] = data[["LONGITUDE", "LATITUDE"]].apply(check_float, axis=1)

	data = data.replace({pd.np.nan: None})
	n = 1000
	to_send = []
	for index, row in enumerate(data.to_dict(orient='records')):
		if (index+1)%n == 0:
			resp = es.bulk(index=name, body=to_send, request_timeout=300)
			# If there are errors
			if resp["errors"]:
				for i, item in enumerate(resp["items"]):
					if "error" in item["index"].keys():
						print(item)
						err += "ERREUR : La ligne "+str(index+2-n+i)+ " contient une erreur\n"
						err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
						return err, 'coyotes'

			to_send = []
		row["geometry"] = {"lat":row["LATITUDE"], "lon":row["LONGITUDE"]}
		to_send.append({"index":{}})
		to_send.append(row)
	if to_send:
		resp = es.bulk(index=name, body=to_send, request_timeout=300)
		if resp["errors"]:
			for i, item in enumerate(resp["items"]):
				if "error" in item["index"].keys():
					err += "ERREUR : La ligne "+str(data.shape[0]%n+i)+ " contient une erreur\n"
					err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
					return err, 'coyotes'	

	return err, 'coyotes'

def check_float(row):
	if not (isinstance(row["LONGITUDE"], float) or isinstance(row["LATITUDE"], float)):
		return pd.np.nan
	return row