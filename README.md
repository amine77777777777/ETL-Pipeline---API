# ETL-Pipeline---API
📌 Project Description
This project is a data integration pipeline designed to extract, transform, and load (ETL) data from the Elipsis IoT Platform into a Microsoft SQL Server database. It is composed of two main Python scripts, each with a distinct purpose, working together to ensure that both historical metrics and static metadata are kept up to date in your local data warehouse.

 Script 1: Pipeline_API.py — Incremental Device Metrics Loader
This script is responsible for retrieving time-series metrics from the Elipsis API for all registered devices. It uses a timestamp-based incremental loading mechanism to fetch only the new data since the last successful run.

Key Features:

Connects securely to the API using credentials and token stored in a .env file.

Fetches device IDs and their names from the /device_metrics_summary/ endpoint.

For each device, retrieves metrics using the /device_metrics/{device_id} endpoint, filtered by a start_date and end_date.

Dynamically maps raw column keys to human-readable names using the Descrizione_Colonne table from SQL Server.

Appends the results to a SQL table named device_metrics.

Saves the last successful timestamp in a file Ultima_Data_Agg.txt for future incremental loads.

 Script 2: Pipeline_API_extra.py — Static Tables Loader
This script handles the one-time or periodic full extraction of static tables (such as devices, controllers, typologies, system statuses, and other configuration data) from a variety of Elipsis API endpoints.

Key Features:

Pulls data from 14+ endpoints such as:

/controller/

/devices/

/datastreams/

/device_category/

/site_plant/

... and more.

Converts nested JSON fields (like device_protocol_settings, datapoint_protocol_settings) into JSON strings to ensure compatibility with SQL storage.

Overwrites existing SQL tables with the latest snapshot of data for each API endpoint.

Ensures each API dataset is mapped to a uniquely named SQL table (e.g., Device_OS, Controller, Elipsis_Events).

🔧 Technologies Used
Python – Main programming language

Pandas – Data manipulation

Requests – API interaction

SQLAlchemy + pyodbc – SQL Server connectivity

dotenv – Secure environment variable management

Microsoft SQL Server – Data storage

✅ Use Cases
Synchronizing Elipsis IoT data into your internal BI/data warehouse environment

Monitoring device-level metrics and statuses over time

Enriching operational dashboards and predictive maintenance models

Automating recurring data ingestion workflows