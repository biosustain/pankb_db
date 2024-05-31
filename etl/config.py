### The ETL scripts configuration file ####

## MongoDB type. Only two possible values:
# - 'self_deployed' (standalone, deployed on the DEV server in a docker container)
# or
# - 'cloud' (MongoDB Atlas or Azure CosmosDB for MongoDB)
db_type = 'cloud'

## Server, where the db instance in use is located ('dev', 'prod'):
db_server = 'prod'

# Upload or reupload data for ALL species (True, False): ----
all_species = True

# In case all_species = False, rewrite the data only for the species from the list below.
# This is a list dictionaries, each of which has two keys: 
# 1. 'pangenome_analysis' - the species' id or pangenome's id included into the path on the Microsoft Azure Blob Storage (PanKB/web_data/species/<pangenome_analysis>/). Must contain underscores instead of whitespaces.
# 2. 'species' - a name of the given species. Can contain whitespaces.
species_list = [
                {'pangenome_analysis': 'Parageobacillus_thermoglucosidasius', 'species': 'Parageobacillus thermoglucosidasius'},
                {'pangenome_analysis': 'Lactobacillus_gasseri', 'species': 'Lactobacillus gasseri'}
               ]

# A local folder, where all the logs are to be stored: ----
logs_folder = "../../logs/etl/mongodb/"

# Upload the log to the Microsoft Azure Blob Storage after it is created (separately for each of the collections)
# (the name of the blob will start with PanKB/etl/logs/): ----
upload_logs_for_organisms = False
upload_logs_for_gene_annotations = False
upload_logs_for_gene_info = False
upload_logs_for_genome_info = False
upload_logs_for_pathway_info = False