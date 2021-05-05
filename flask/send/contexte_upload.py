
import pandas as pd

def contexte_upload(es, path, name):
	data = pd.read_csv(path, delimiter=";", encoding ='utf-8', decimal=".")

	err= ""
	# Check the important column present
	columns_names = list(data)
	important_columns = [
		"Temperature",
		"Date",
		"Pluviometrie 6h",
		"Pluviometrie 1h",
		"Vitesse Vent",
		"Pression 0m"
	]
	for column in important_columns:
		if column not in columns_names:
			err += " ERREUR : La colonne "+column+" n'est pas présente\n"

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
						return err, 'meteo'

			to_send = []
		to_send.append({"index":{}})
		to_send.append(row)
	if to_send:
		resp = es.bulk(index=name, body=to_send, request_timeout=300)
		if resp["errors"]:
			for i, item in enumerate(resp["items"]):
				if "error" in item["index"].keys():
					err += "ERREUR : La ligne "+str(data.shape[0]%n+i)+ " contient une erreur\n"
					err += item["index"]["error"]["reason"]+" caused by "+item["index"]["error"]["caused_by"]["reason"]+"\n"
					return err, 'meteo'	

	return err, 'meteo'