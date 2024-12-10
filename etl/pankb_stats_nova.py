from connections import *

from datetime import datetime
from pathlib import Path
import json

if __name__ == "__main__":
    db_conn = DBConnection()
    logger = TimedLogger("pankb_stats")

    # Obtain the db collection object: ----
    collection = db_conn.db["pankb_stats"]
    organism_collection = db_conn.db["pankb_organisms"]

    pankb_dimensions = {"Mutations": 0, "Genes": 0, "Alleleomes": 0, "Genomes": 0, "Species pangenomes": 0}
    species_genome_gene = {}
    organism_genome_count = {}
    organism_gene_count = {}

    for organism_entry in organism_collection.find({}):
        pangenome_analysis = organism_entry["pangenome_analysis"]
        species = organism_entry["species"]

        family = organism_entry["family"]
        n_genome = organism_entry["genomes_num"]
        n_gene = organism_entry["genes_num"]
        n_alleleome = organism_entry["alleleomes_num"]
        n_mut = organism_entry["mutations_num"]

        pankb_dimensions["Mutations"] += n_mut
        pankb_dimensions["Genes"] += n_gene
        pankb_dimensions["Alleleomes"] += n_alleleome
        pankb_dimensions["Genomes"] += n_genome
        pankb_dimensions["Species pangenomes"] += 1

        if not family in species_genome_gene:
            species_genome_gene[family] = {}
        species_genome_gene[family][pangenome_analysis] = [n_genome, n_gene]

        organism_genome_count[family] = organism_genome_count.get(family, 0) + n_genome
        organism_gene_count[family] = organism_gene_count.get(family, 0) + n_gene

    # Sort all dicts in the same way
    sorted_families = [k for k, v in reversed(sorted(organism_genome_count.items(), key=lambda item: item[1]))]

    organism_genome_count = {k: organism_genome_count[k] for k in sorted_families}
    organism_gene_count = {k: organism_gene_count[k] for k in sorted_families}
    species_genome_gene = {k: species_genome_gene[k] for k in sorted_families}

    # with open(out_path / "pankb_dimensions_full_panalleleome.json", "w") as f:
    #     json.dump(pankb_dimensions, f)
    # with open(out_path / "species_genome_gene.json", "w") as f:
    #     json.dump(species_genome_gene, f)
    # with open(out_path / "organism_genome_count.json", "w") as f:
    #     json.dump(organism_genome_count, f)
    # with open(out_path / "organism_gene_count.json", "w") as f:
    #     json.dump(organism_gene_count, f)

    with open(out_path / "treemap_data.json", "r") as f:
        treemap_data = json.load(f)

    data = {
        "date": datetime.now(),
        "pankb_dimensions": json.dumps(pankb_dimensions),
        "species_genome_gene": json.dumps(species_genome_gene),
        "organism_genome_count": json.dumps(organism_genome_count),
        "organism_gene_count": json.dumps(organism_gene_count),
        "treemap": json.dumps(treemap_data)
    }

    # Insert rows into the MongoDB and print some stats: ----
    logger.info("--- DB Insertion ---")
    result = collection.insert_one(data)
    logger.log_execution_time()
    logger.info("Documents inserted: %s" % 1)