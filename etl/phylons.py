from connections import *
from pymongo import UpdateOne
import requests
import json
from config_nova import gene_batch_size

if __name__ == "__main__":
    db_conn = DBConnection()
    logger = TimedLogger("phylons")

    collections = {
        "gene": db_conn.db["pankb_gene_phylons"],
        "genome": db_conn.db["pankb_genome_phylons"]
    }

    if config.drop_collection:
        logger.info("Dropping collections")
        for collection in collections.values():
            collection.drop()
        logger.info("Collections have been successfully dropped.")

    logger.info("Creating the indexes on the collections")
    collections["genome"].create_index('genome_id')
    collections["genome"].create_index('pangenome_analysis')
    collections["genome"].create_index(['pangenome_analysis', 'genome_id'])

    collections["gene"].create_index('gene')
    collections["gene"].create_index('pangenome_analysis')
    collections["gene"].create_index(['pangenome_analysis', 'gene'])

    logger.info("The indexes have been successfully created.")

    requesting = []
    for pangenome_analysis in pangenome_analyses.keys():
        logger.info(f" - Processing {pangenome_analysis}")

        phylons_data = requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/phylons.json').json()

        for collection_type in ("gene", "genome"):
            id_name = "gene" if collection_type == "gene" else "genome_id"

            for id, pylons in phylons_data[f"{collection_type}_phylons"].items():
                phylon_weights = phylons_data[f"{collection_type}_phylon_weights"][id]

                filter_query = {"pangenome_analysis": pangenome_analysis, id_name: id}
                update_query = {"$set":
                    {
                        "pangenome_analysis": pangenome_analysis,
                        id_name: id,
                        "phylons": pylons,
                        "phylon_weights": phylon_weights
                    }
                }

                requesting.append(UpdateOne(filter_query, update_query, upsert=True))

                if len(requesting) >= gene_batch_size:
                    logger.info("--- DB Insertion ---")
                    collections[collection_type].bulk_write(requesting, ordered=True)
                    logger.info(f"Upserted {len(requesting)} documents")
                    requesting = []

            if requesting:
                logger.info("--- DB Insertion ---")
                collections[collection_type].bulk_write(requesting, ordered=True)
                logger.info(f"Upserted {len(requesting)} documents")
                requesting = []

    logger.log_execution_time()
