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
import json


load_dotenv()

cnx_str = os.getenv("cnx_str")


sqlalchemy_conn_str = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(cnx_str)}"
engine = create_engine(sqlalchemy_conn_str)



username = os.getenv("username")
password = os.getenv("password")
token = os.getenv("Token")
login_data = {"username": username, "password": password}
headers = {'Content-Type': 'application/json',
           'authorization' : token}


def inserimento_diretto(endpoint_url , nome_tabella):
    response = requests.get(endpoint_url, json=login_data, headers=headers)
    if response.status_code == 200:
        data = response.json()
        print(f"Data essiste da {endpoint_url}")
        if 'results' in data and data["results"]:
            df = pd.DataFrame(data["results"])
            dict_columns = ['datapoint_protocol_settings', 'device_protocol_settings']

            for col in dict_columns:
              if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
            df.to_sql(nome_tabella, engine, if_exists='replace', index=False)
            print(f"dati gia salvati su sql per {nome_tabella}")
        else:
            print(f"dati non salvati per {nome_tabella}")
    else:
        print(f"dati non essiste da{endpoint_url}")


endpoints = [

    ("http://palm.eu.elipsis.it/api/controller/", "Controller"),
    ("http://palm.eu.elipsis.it/api/controller_system_status/", "Controller_System_Status"),
    ("http://palm.eu.elipsis.it/api/datastreams/", "DataStreams"),
    ("http://palm.eu.elipsis.it/api/device_category/", "Device_Category"),
    ("http://palm.eu.elipsis.it/api/device_metrics_summary/", "Device_Metrics_Summary"),
    ("http://palm.eu.elipsis.it/api/device_os/", "Device_OS"),
    ("http://palm.eu.elipsis.it/api/device_typology/", "Device_Typology"),
    ("http://palm.eu.elipsis.it/api/devices/", "Devices"),
    ("http://palm.eu.elipsis.it/api/elipsis_events/", "Elipsis_Events"),
    ("http://palm.eu.elipsis.it/api/first_custom_table/", "First_Custom_Table"),
    ("http://palm.eu.elipsis.it/api/nodeflow_logs/", "NodeFlow_Logs"),
    ("http://palm.eu.elipsis.it/api/second_custom_table/", "Second_Custom_Table"),
    ("http://palm.eu.elipsis.it/api/site_plant/", "Site_Plant"),
    ("http://palm.eu.elipsis.it/api/third_custom_table/", "Third_Custom_Table")

]


for url, table_name in endpoints:
    inserimento_diretto(url, table_name)
