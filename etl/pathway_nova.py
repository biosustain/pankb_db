from connections import *
from pymongo import InsertOne, ReplaceOne
import requests
import json

if __name__ == "__main__":
    db_conn = DBConnection()
    logger = TimedLogger("pathways")

    # Obtain the db collection object: ----
    collection = db_conn.db["pankb_pathway_info"]

    if config.drop_collection:
        # Drop the collection if it exists: ----
        collection.drop()

    for pangenome_analysis, species in pangenome_analyses.items():
        logger.info(f" - Processing {pangenome_analysis}")
        requesting = []
        # Retrieve the respective *.json file content from the Blob storage: ----
        pathways_list = requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/nova/pathway.json').json()

        for pathway in pathways_list:
            pathway_filter = {"pathway_id": pathway["pathway_id"]}
            doc = collection.find_one(pathway_filter)
            if doc is None:
                doc = {**pathway}
                doc["genes"] = []
                for gene in pathway["genes"]:
                    gene_enc = f"{gene['pangenome_analysis']}:{gene['gene']}"
                    if not gene_enc in doc["genes"]:
                        doc["genes"].append(gene_enc)
                requesting.append(InsertOne(doc))
            else:
                for gene in pathway["genes"]:
                    gene_enc = f"{gene['pangenome_analysis']}:{gene['gene']}"
                    if not gene_enc in doc["genes"]:
                        doc["genes"].append(gene_enc)
                requesting.append(ReplaceOne(pathway_filter, doc))

        # Insert rows into the MongoDB and print some stats: ----
        logger.info("--- DB Insertion ---")
        result = collection.bulk_write(requesting, ordered=True)
        logger.info("Documents inserted: %s" % (len(requesting)))
    
    logger.log_execution_time()