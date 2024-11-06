from connections import *
import gzip
import json
import requests
from pymongo import InsertOne

if __name__ == "__main__":
    db_conn = DBConnection()
    blob_conn = BlobConnection()
    logger = TimedLogger("gene")

    # Obtain the db collection object: ----
    collection = db_conn.db["pankb_gene_info"]

    if config.drop_collection:
        # Drop the collection if it exists: ----
        collection.drop()
        db_conn.db.create_collection(
                "pankb_gene_info",
                storageEngine={"wiredTiger": {"configString": "block_compressor=zlib"}}
            )

    logger.info("Creating the indexes on the collection...")
    collection.create_index(['pangenome_analysis', 'gene'])
    collection.create_index(['pangenome_analysis', 'genome_id'])
    logger.info("The indexes have been successfully created.")

    requesting = []
    inserted_total = 0

    for pangenome_analysis, species in pangenome_analyses.items():
        logger.info(f" - Processing {pangenome_analysis}")
        # Retrieve the respective *.json file content from the Blob storage: ----
        blob_conn.set_gzip_encoding(f'PanKB/web_data/species/{pangenome_analysis}/nova/gene.jsonl.gz') # Make sure ContentEncoding is set to GZIP on the azure file
        with requests.get(f'https://pankb.blob.core.windows.net/data/PanKB/web_data/species/{pangenome_analysis}/nova/gene.jsonl.gz', stream=True) as r:
            r.raise_for_status()
            for line in r.iter_lines():
                gene_dict = json.loads(line)
                requesting.append(InsertOne(gene_dict))
                if len(requesting) >= config.gene_batch_size:
                    # Insert rows into the MongoDB and print some stats: ----
                    logger.info("--- DB Insertion ---")
                    result = collection.bulk_write(requesting, ordered=True)
                    inserted_total += len(requesting)
                    requesting = []
    if len(requesting) > 0:
        logger.info("--- Final DB Insertion ---")
        result = collection.bulk_write(requesting, ordered=True)
        inserted_total += len(requesting)

    logger.log_execution_time()
    logger.info(f"Documents inserted: {inserted_total}")
