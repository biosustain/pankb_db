# Author: Liubov Pashkova
# Email: liupa@dtu.dk

# The ETL scripts:
# global functions and variables.


from pymongo import MongoClient, InsertOne
import os, time
from dotenv import load_dotenv
import logging
import requests
import urllib
from bioservices.kegg import KEGG
import re
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json
import threading
from multiprocessing.pool import ThreadPool
from io import StringIO
import pandas as pd
from pathlib import Path
import config

out_path = Path("./web_data/")
out_path.mkdir(exist_ok=True, parents=True)

# Load the .env file with the DB access credentials: ----
load_dotenv(".env")

# Connect to the DB differently based on the server: ----
if config.db_type == "self_deployed":
    mongo_username = os.getenv("MONGO_INITDB_ROOT_USERNAME")
    mongo_password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
    client = MongoClient(username=mongo_username, password=mongo_password)      # host and port are automatically set to 'localhost' and '27017' respectively
if config.db_type == "cloud":
    mongo_conn_string = os.getenv("MONGODB_CONN_STRING")
    client = MongoClient(mongo_conn_string)

# Obtain the db object instance: ----
mongo_dbname = os.getenv("MONGODB_NAME")
db = client[mongo_dbname]

# Obtain the list of species to work with: ----
if config.all_species == False:
    pangenome_analyses_species_dict_list = config.species_list
else:
    pangenome_analyses_species_dict_list = []
    for data in db.pankb_organisms.find():     # the "organisms" collection must be populated at this point
        pangenome_analyses_species_dict_list.append(
            {"pangenome_analysis": data["pangenome_analysis"], "species": data["species"]})

# Obtain the total number of species to process: ----
total_species = len(pangenome_analyses_species_dict_list)

# Access to the Microsoft Azure Blob Storage: ----
blob_storage_conn_string = os.getenv('BLOB_STORAGE_CONN_STRING')
service_client = BlobServiceClient.from_connection_string(blob_storage_conn_string)
container_client = service_client.get_container_client("data")


# A misc function that uploads a log to the Microsoft Azure Blob Storage: ----
def upload_blob_log(filename: str):
    container_client = service_client.get_container_client(container="data")
    with open(file=os.path.join("/projects/pankb_web/logs/etl/mongodb/" + config.db_server, filename), mode="rb") as data:
        blob_client = container_client.upload_blob(name=os.path.join("PanKB/etl/logs/", filename), data=data, overwrite=True)
        return blob_client