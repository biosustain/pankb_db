# Author: Pascal Pieters
# Email: paspie@dtu.dk

from global_func import *
from datetime import datetime
from pathlib import Path

# Obtain the script execution start date and time: ----
start_strftime = time.strftime("%Y-%m-%d_%H%M%S")
script_start_time = time.time()

# Set up the logging: ----
logger = logging.getLogger("pankb_stats")
# Use the timestamp as a suffix for the log file name: ----
logfile_name = config.logs_folder + config.db_server + "/pankb_stats__" + start_strftime + ".log"
logging.basicConfig(filename=logfile_name, level=logging.INFO)

collection = db["pankb_stats"]
# if config.drop_collection:
#     # Drop the collection if it exists: ----
#     collection.drop()


pankb_dimensions = {"Mutations": 0, "Genes": 0, "Alleleomes": 0, "Genomes": 0, "Species pangenomes": 0}
species_genome_gene = {}
organism_genome_count = {}
organism_gene_count = {}

for item in pangenome_analyses_species_dict_list:
    pangenome_analysis = item["pangenome_analysis"]
    species = item["species"]

    # Retrieve the respective *.json file content from the Blob storage: ----
    jsonObj = requests.get(f'https://pankb.blob.core.windows.net/data/PanKB/web_data/species/{pangenome_analysis}/info_panel.json').json()
    organism_dict = json.loads(json.dumps(jsonObj))

    family = organism_dict["Family"]
    n_genome = organism_dict["Number_of_genome"]
    n_gene = organism_dict["Number_of_gene"]
    n_alleleome = organism_dict["Number_of_alleleome"]
    n_mut = organism_dict["Number_of_mutations"]

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

with open(out_path / "pankb_dimensions_full_panalleleome.json", "w") as f:
    json.dump(pankb_dimensions, f)
with open(out_path / "species_genome_gene.json", "w") as f:
    json.dump(species_genome_gene, f)
with open(out_path / "organism_genome_count.json", "w") as f:
    json.dump(organism_genome_count, f)
with open(out_path / "organism_gene_count.json", "w") as f:
    json.dump(organism_gene_count, f)

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
logger.info("Execution time: %.2f seconds" % (time.time() - script_start_time))
logger.info("Documents inserted: %s" % 1)

# Upload the log file to the Microsoft Azure Blob Storage if the respective config option is set to True:
if config.upload_logs_for_organisms == True:
    upload_blob_log(logfile_name)

client.close()