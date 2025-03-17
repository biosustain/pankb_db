import os, time
from dotenv import load_dotenv
import logging
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings
from pathlib import Path
import config_nova as config

# Load the .env file with the DB access credentials: ----
load_dotenv(".env")

out_path = Path(config.output_path)
out_path.mkdir(exist_ok=True, parents=True)

class DBConnection:
    def __init__(self):
        from pymongo import MongoClient

        # Connect to the DB differently based on the server: ----
        if config.db_type == "self_deployed":
            mongo_username = os.getenv("MONGO_INITDB_ROOT_USERNAME")
            mongo_password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
            self.client = MongoClient(username=mongo_username, password=mongo_password)      # host and port are automatically set to 'localhost' and '27017' respectively
        if config.db_type == "cloud":
            mongo_conn_string = os.getenv("MONGODB_CONN_STRING")
            self.client = MongoClient(mongo_conn_string)

        # Obtain the db object instance: ----
        mongo_dbname = os.getenv("MONGODB_NAME")
        self.db = self.client[mongo_dbname]
    # def __del__(self):
    #     self.client.close()

# Obtain the list of species to work with: ----
pangenome_analyses = {x: x.replace("_", " ") for x in config.pangenome_analyses}

class BlobConnection:
    base_url = "https://pankb.blob.core.windows.net/data/"
    web_data_path = "PanKB/web_data_v2/"

    def __init__(self):
        # Access to the Microsoft Azure Blob Storage: ----
        blob_storage_conn_string = os.getenv('BLOB_STORAGE_CONN_STRING')
        self.service_client = BlobServiceClient.from_connection_string(blob_storage_conn_string)
        self.container_client = self.service_client.get_container_client("data")

    def set_gzip_encoding(self, blob_path):
        blob_client = self.container_client.get_blob_client(blob_path)

        # Get the existing blob properties
        properties = blob_client.get_blob_properties()
        if properties.content_settings.content_encoding == "GZIP":
            return

        # Set the content_type and content_language headers, and populate the remaining headers from the existing properties
        blob_headers = ContentSettings(content_type=properties.content_settings.content_type,
                                    content_encoding="GZIP",
                                    content_language=properties.content_settings.content_language,
                                    content_disposition=properties.content_settings.content_disposition,
                                    cache_control=properties.content_settings.cache_control,
                                    content_md5=properties.content_settings.content_md5)
        
        blob_client.set_http_headers(blob_headers)

class TimedLogger:
    def __init__(self, name):
        self.name = name
        # Obtain the script execution start date and time: ----
        start_strftime = time.strftime("%Y-%m-%d_%H%M%S")
        self.script_start_time = time.time()

        # Set up the logging: ----
        self.logger = logging.getLogger(name)
        # Use the timestamp as a suffix for the log file name: ----
        logfile_name = f"{config.logs_folder}{config.db_server}/{name}__{start_strftime}.log"
        logging.basicConfig(level=logging.INFO, handlers=[
            logging.FileHandler(logfile_name),
            logging.StreamHandler()
        ])
    
    def log_execution_time(self):
        self.logger.info("Execution time: %.2f seconds" % (time.time() - self.script_start_time))
    
    def info(self, message):
        self.logger.info(message)
    def warning(self, message):
        self.logger.warning(message)
    def error(self, message):
        self.logger.error(message)