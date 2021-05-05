
import pandas as pd

from functions import typeOfJson

def materiels_upload(es, path, name):
	data = pd.read_csv(path, delimiter=";", decimal=".")
	print(data.columns)
	category = typeOfJson(name, {key:0 for key in data.columns})
	if category == "sdis":
		return sdis_upload(es, data, name)
	else:
		return thelem_upload(es, data, name)

def sdis_upload(es, data, name):
	err= ""
	columns_names = list(data)
	important_columns = [
		"ID",
		"Date",
		"Libellé",
		"LATITUDE",
		"LONGITUDE"
	]
	for column in important_columns:
		if column not in columns_names:
			err += " ERREUR : La colonne "+column+" n'est pas présente\n"

	if err:
		return err, "sdis"
	# Traitement
	data[["LONGITUDE", "LATITUDE"]] = data[["LONGITUDE", "LATITUDE"]].replace(",", ".")
	data[["LONGITUDE", "LATITUDE"]] = data[["LONGITUDE", "LATITUDE"]].apply(check_float, axis=1)

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
						return err, 'sdis'

			to_send = []
		row["geometry"] = {"lat":row["LATITUDE"], "lon":row["LONGITUDE"]}
		to_send.append({"index":{}})
		to_send.append(row)
		
	if to_send:
		resp = es.bulk(index=name, body=to_send, request_timeout=300)
		if resp["errors"]:
			for i, item in enumerate(resp["items"]):
				if "error" in item["index"].keys():
					err += "ERREUR : La ligne "+str((data.shape[0]//n)*data.shape[0]+i)+ " contient une erreur\n"
					err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
					return err, 'sdis'	

	return err, 'sdis'

def thelem_upload(es, data, name):
	err= ""
	columns_names = list(data)
	important_columns = [
		"ResultID",
		"Date",
		"Type du véhicule",
		"RANG",
		"Distance",
		"LATITUDE",
		"LONGITUDE"
	]
	for column in important_columns:
		if column not in columns_names:
			err += " ERREUR : La colonne "+column+" n'est pas présente\n"
	
	if err:
		return err, "thelem"
	# Traitement
	data[["LONGITUDE", "LATITUDE"]] = data[["LONGITUDE", "LATITUDE"]].replace(",", ".")
	data[["LONGITUDE", "LATITUDE"]] = data[["LONGITUDE", "LATITUDE"]].apply(check_float, axis=1)

	data = data.replace({pd.np.nan: None})
	n = 1000
	to_send = []
	for index, row in enumerate(data.to_dict(orient='records')):
		if (index+1)%n == 0:
			print("bulk")
			resp = es.bulk(index=name, body=to_send, request_timeout=300)
			# If there are errors
			if resp["errors"]:
				for i, item in enumerate(resp["items"]):
					if "error" in item["index"].keys():
						print(item["index"]["error"])
						err += "ERREUR : La ligne "+str(index+2-n+i)+ " contient une erreur\n"
						err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
						return err, 'thelem'

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
					return err, 'thelem'	

	return err, 'thelem'

def check_float(row):
	if not (isinstance(row["LONGITUDE"], float) or isinstance(row["LATITUDE"], float)):
		return pd.np.nan
	return row