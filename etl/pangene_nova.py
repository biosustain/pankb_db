from connections import *
from pymongo import UpdateOne
import json
import requests

if __name__ == "__main__":
    db_conn = DBConnection()
    logger = TimedLogger("pangene")

    # Obtain the db collection object: ----
    collection = db_conn.db["pankb_gene_annotations"]

    if config.drop_collection:
        # Drop the collection if it exists: ----
        collection.drop()

    logger.info("Creating the indexes on the collection...")
    collection.create_index(["pangenome_analysis"])
    collection.create_index(['pangenome_analysis', 'gene'], name="lookup_index")
    collection.create_index(['kegg_pathway', 'pangenome_analysis', 'gene'], name="pathway_index")
    logger.info("The indexes have been successfully created.")

    requesting = []

    for pangenome_analysis, species in pangenome_analyses.items():
        logger.info(f" - Processing {pangenome_analysis}")
        # Retrieve the respective *.json file content from the Blob storage: ----
        pangene_dicts = requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/nova/pangene.json').json()

        for pangene_dict in pangene_dicts:
            filter_query = {"pangenome_analysis": pangenome_analysis, "gene": pangene_dict["gene"]}
            update_query = {"$set": pangene_dict}
            requesting.append(UpdateOne(filter_query, update_query, upsert=True))


        # Insert rows into the MongoDB and print some stats: ----
        logger.info("--- DB Insertion ---")
        result = collection.bulk_write(requesting, ordered=True)
        logger.log_execution_time()
        logger.info("Documents upserted: %s" % (len(requesting)))
        requesting = []
