from connections import *
from pymongo import UpdateOne
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

    logger.info("Creating the indexes on the collection...")
    collection.create_index(['pangenome_analysis'], name="lookup_index")
    collection.create_index(['species'], name="species_index")
    collection.create_index(['family'], name="family_index")
    logger.info("The indexes have been successfully created.")

    requesting = []

    for pangenome_analysis, species in pangenome_analyses.items():
        logger.info(f" - Processing {pangenome_analysis}")
        # Retrieve the respective *.json file content from the Blob storage: ----
        organism_dict = requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/nova/organism.json').json()

        # Define the filter and update for the upsert operation
        filter_query = {"pangenome_analysis": pangenome_analysis}
        update_query = {"$set": organism_dict}

        requesting.append(UpdateOne(filter_query, update_query, upsert=True))

    # Insert rows into the MongoDB and print some stats: ----
    logger.info("--- DB Insertion ---")
    result = collection.bulk_write(requesting, ordered=True)
    logger.log_execution_time()
    logger.info("Documents upserted: %s" % (len(requesting)))


