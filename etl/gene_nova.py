from connections import *
import gzip
import json
import requests
from pymongo import UpdateOne
import traceback
import config_nova as config 

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
    failed_analyses = []
    for pangenome_analysis, species in pangenome_analyses.items():
        logger.info(f" - Processing {pangenome_analysis}")
        try:
            # Retrieve the respective *.json file content from the Blob storage: ----
            blob_conn.set_gzip_encoding(f'{BlobConnection.web_data_path}species/{pangenome_analysis}/nova/gene.jsonl.gz') # Make sure ContentEncoding is set to GZIP on the azure file
            with requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/nova/gene.jsonl.gz', stream=True) as r:
                r.raise_for_status()
                for line in r.iter_lines():
                    gene_dict = json.loads(line)

                    filter_query = {"pangenome_analysis": pangenome_analysis, "gene": gene_dict["gene"]}
                    update_query = {"$set": gene_dict}

                    requesting.append(UpdateOne(filter_query, update_query, upsert=True))

                    if len(requesting) >= config.gene_batch_size:
                        # Insert rows into the MongoDB and print some stats: ----
                        logger.info("--- DB Insertion ---")
                        result = collection.bulk_write(requesting, ordered=True)
                        inserted_total += len(requesting)
                        requesting = []
        except Exception as err:
            if config.halt_on_error:
                raise err
            else:
                failed_analyses.append(pangenome_analysis)
                logger.error(f"Failed while processing pangenome analysis {pangenome_analysis}: {err}\n{traceback.format_exc()}")
                logger.warning(f"Continuing to insert entries. Likely incomplete set of genes for pangenome analysis {pangenome_analysis}")


    if len(requesting) > 0:
        logger.info("--- Final DB Insertion ---")
        result = collection.bulk_write(requesting, ordered=True)
        inserted_total += len(requesting)

    logger.log_execution_time()
    logger.info(f"Documents upserted: {inserted_total}")

    if len(failed_analyses) > 0:
        logger.error(f"Failed to process the following pangenome analyses: {','.join(failed_analyses)}. Their gene info will likely be incomplete. Re-running this program again for those pangenomes should fix the issue")
