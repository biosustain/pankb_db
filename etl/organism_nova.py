from connections import *
from pymongo import InsertOne
import requests
import json

if __name__ == "__main__":
    db_conn = DBConnection()
    logger = TimedLogger("organisms")

    # Obtain the db collection object: ----
    collection = db_conn.db["pankb_organisms"]

    if config.drop_collection:
        # Drop the collection if it exists: ----
        collection.drop()

    requesting = []

    for pangenome_analysis, species in pangenome_analyses.items():
        logger.info(f" - Processing {pangenome_analysis}")
        # Retrieve the respective *.json file content from the Blob storage: ----
        organism_dict = requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/nova/organism.json').json()

        requesting.append(InsertOne(organism_dict))

    # Insert rows into the MongoDB and print some stats: ----
    logger.info("--- DB Insertion ---")
    result = collection.bulk_write(requesting, ordered=True)
    logger.log_execution_time()
    logger.info("Documents inserted: %s" % (len(requesting)))
