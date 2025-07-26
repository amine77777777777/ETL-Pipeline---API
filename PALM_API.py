import pandas as pd
import urllib
import os
import requests
import datetime
import pyodbc
import sqlalchemy
import os
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

cnx_str = os.getenv("cnx_str")
cnx = pyodbc.connect(cnx_str)



def get_last_processed_date():
    if os.path.exists("Ultima_Data_Agg.txt"):
        with open("Ultima_Data_Agg.txt", "r") as file:
            return file.read().strip()
    else:
        return "2025-01-01T00:00:00.Z" 

def update_last_processed_date(new_date):
    with open("Ultima_Data_Agg.txt", "w") as file:
        file.write(new_date)

end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.Z")


start_date = get_last_processed_date()

username = os.getenv("username")
password = os.getenv("password")
token = os.getenv("Token")
login_data = {"username": username, "password": password}
headers = {'Content-Type': 'application/json',
           'authorization' : token}



###Aggiornamento dei device ID 
url = f"http://palm.eu.elipsis.it/api/device_metrics_summary/?limit=100000&offset=0"

response_device = requests.get(url, json=login_data, headers=headers)
if response_device.status_code == 200:
      data_device = response_device.json()

      print("Login successful")
else:
        print(f"Device ID  non esiste")
df_device = pd.DataFrame(data_device['results'])

##################################



dati_finale = []

for _, row in df_device.iterrows():
    device_id_value = row['device']
    nome_dispositivo = row['device_name']
 
    print(f"Processing Device ID: {device_id_value}")
    login_url = f"http://palm.eu.elipsis.it/api/device_metrics/{device_id_value}/?limit=100000&offset=0&start_date={start_date}&end_date={end_date}"

    response = requests.get(login_url, json=login_data, headers=headers)
    if response.status_code == 200:
      data = response.json()
      if "results" in data:
            for result in data["results"]:
                result["device"] = device_id_value  
                result["device name"] = nome_dispositivo
                dati_finale.append(result)
      print("Login successful")
    else:
        print(f"Device ID {device_id_value}: non esiste")

  
col_map_query = "SELECT Chiave, Descrizione FROM Descrizione_Colonne"
col_map_df = pd.read_sql(col_map_query, cnx)
col_map = dict(zip(col_map_df['Chiave'], col_map_df['Descrizione']))
df = pd.DataFrame(dati_finale)
df.rename(columns=col_map, inplace=True)

sqlalchemy_conn_str = (
    f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(cnx_str)}"
)

engine = create_engine(sqlalchemy_conn_str)

df.to_sql("device_metrics", engine, if_exists="append", index=False)

print(f"Data Dal {start_date} ======> {end_date} inserita con successo")

update_last_processed_date(end_date)

