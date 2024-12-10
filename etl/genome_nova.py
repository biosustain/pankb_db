from connections import *
import gzip
import json
import requests
from pymongo import InsertOne

if __name__ == "__main__":
    db_conn = DBConnection()
    blob_conn = BlobConnection()
    logger = TimedLogger("genome")

    # Obtain the db collection object: ----
    collection = db_conn.db["pankb_genome_info"]

    if config.drop_collection:
        # Drop the collection if it exists: ----
        collection.drop()
    logger.info("Creating the indexes on the collection...")
    collection.create_index(['pangenome_analysis', 'genome_id'])
    logger.info("The indexes have been successfully created.")

    requesting = []

    for pangenome_analysis, species in pangenome_analyses.items():
        logger.info(f" - Processing {pangenome_analysis}")
        # Retrieve the respective *.json file content from the Blob storage: ----
        with requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/nova/genome.jsonl', stream=True) as r:
            r.raise_for_status()
            for line in r.iter_lines():
                genome_dict = json.loads(line)
                requesting.append(InsertOne(genome_dict))
    logger.info("--- Final DB Insertion ---")
    result = collection.bulk_write(requesting, ordered=True)
    logger.log_execution_time()
    logger.info(f"Documents inserted: {len(requesting)}")
