from connections import *
from pymongo import InsertOne
import json
import requests

if __name__ == "__main__":
    db_conn = DBConnection()
    logger = TimedLogger("pangene")

    # Obtain the db collection object: ----
    collection = db_conn.db["pankb_gene_annotations_nova"]

    if config.drop_collection:
        # Drop the collection if it exists: ----
        collection.drop()

    requesting = []

    for pangenome_analysis, species in pangenome_analyses.items():
        logger.info(f" - Processing {pangenome_analysis}")
        # Retrieve the respective *.json file content from the Blob storage: ----
        pangene_dict = requests.get(f'https://pankb.blob.core.windows.net/data/PanKB/web_data/species/{pangenome_analysis}/nova/pangene.json').json()

        requesting.extend([InsertOne(d) for d in pangene_dict])

    # Insert rows into the MongoDB and print some stats: ----
    logger.info("--- DB Insertion ---")
    result = collection.bulk_write(requesting, ordered=True)
    logger.log_execution_time()
    logger.info("Documents inserted: %s" % (len(requesting)))
