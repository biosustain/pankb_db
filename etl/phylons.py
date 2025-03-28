from connections import *
from pymongo import UpdateOne
import requests
import json

if __name__ == "__main__":
    db_conn = DBConnection()
    logger = TimedLogger("phylons")

    # Obtain the db collection object: ----
    collection = db_conn.db["pankb_phylons"]

    if config.drop_collection:
        # Drop the collection if it exists: ----
        collection.drop() 

    logger.info("Creating the indexes on the collection...")
    collection.create_index(['pangenome_analysis'], name="lookup_index")
    logger.info("The indexes have been successfully created.")

    requesting = []

    # pangenome_analyses = {"Streptomyces_mirabilis": "Streptomyces mirabilis"}

    for pangenome_analysis in pangenome_analyses.keys():
        logger.info(f" - Processing {pangenome_analysis}")

        genome_to_phylons = requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/phylons/genome_to_phylons.json').json()
        genome_to_phylon_weights = requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/phylons/genome_to_phylon_weights.json').json()
        phylon_to_genomes = requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/phylons/phylon_to_genomes.json').json()
        phylon_to_genome_weights = requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/phylons/phylon_to_genome_weights.json').json()
        gene_to_phylons = requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/phylons/gene_to_phylons.json').json()
        phylon_to_gene_weights = requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/phylons/phylon_to_gene_weights.json').json()
        gene_to_phylon_weights = requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/phylons/gene_to_phylon_weights.json').json()
        phylon_to_genes = requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/phylons/phylon_to_genes.json').json()

        phylons_dict = {
            "pangenome_analysis": pangenome_analysis,
            "genome_phylons": genome_to_phylons,
            "genome_phylon_weights": genome_to_phylon_weights,
            "phylon_genomes": phylon_to_genomes,
            "phylon_genome_weights": phylon_to_genome_weights,
            "gene_phylons": gene_to_phylons,
            "gene_phylon_weights": gene_to_phylon_weights,
            "phylon_genes": phylon_to_genes,
            "phylon_gene_weights": phylon_to_gene_weights
        }

        filter_query = {"pangenome_analysis": pangenome_analysis}
        update_query = {"$set": phylons_dict}

        requesting.append(UpdateOne(filter_query, update_query, upsert=True))

    logger.info("--- DB Insertion ---")
    result = collection.bulk_write(requesting, ordered=True)
    logger.log_execution_time()
    logger.info("Documents upserted: %s" % (len(requesting)))
