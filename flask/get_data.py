import pandas as pd
from functions import get_data_frame_elastic
from get_connection import get_connection

index = "accidents"
es = get_connection()
accidents = get_data_frame_elastic(es, index, ["id_coyote", "id_waze", "LINK_ID", "Type", "SUBTYPE", "ID_INCIDENT", "Date"])
df = accidents[accidents["id_coyote"].notna()]
df = df.append(accidents[accidents["id_waze"].notna()])

df.to_csv("waze_coyote.csv", sep=";")
