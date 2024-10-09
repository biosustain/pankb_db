### The ETL scripts configuration file ####

## MongoDB type. Only two possible values:
# - 'self_deployed' (standalone, deployed on the DEV server in a docker container)
# or
# - 'cloud' (MongoDB Atlas or Azure CosmosDB for MongoDB)
db_type = 'self_deployed'

## Server, where the db instance in use is located ('dev', 'prod'):
db_server = 'dev'

# Whether to drop the existing collection in the DB (True, False):
drop_collection = False
# Upload or reupload data for ALL species (True, False): ----
all_species = False

gene_batch_size = 10_000

# In case all_species = False, rewrite the data only for the species from the list below.
# This is a list dictionaries, each of which has two keys: 
# 1. 'pangenome_analysis' - the species' id or pangenome's id included into the path on the Microsoft Azure Blob Storage (PanKB/web_data/species/<pangenome_analysis>/). Must contain underscores instead of whitespaces.
# 2. 'species' - a name of the given species. Can contain whitespaces.
#species_list = [
#                {'pangenome_analysis': 'Aliivibrio_fischeri', 'species': 'Aliivibrio fischeri'},
#                {'pangenome_analysis': 'Aliivibrio_fischeri_B', 'species': 'Aliivibrio fischeri B'}
#               ]

species = """Aliivibrio_fischeri
Aliivibrio_fischeri_B
Salinivibrio_kushneri
Salinivibrio_siamensis
Vibrio_aestuarianus
Vibrio_alginolyticus
Vibrio_anguillarum
Vibrio_breoganii
Vibrio_campbellii
Vibrio_cholerae
Vibrio_cincinnatiensis
Vibrio_coralliilyticus
Vibrio_coralliirubri
Vibrio_crassostreae
Vibrio_crassostreae_C
Vibrio_cyclitrophicus
Vibrio_diabolicus
Vibrio_europaeus
Vibrio_fluvialis
Vibrio_furnissii
Vibrio_harveyi
Vibrio_jasicida
Vibrio_kanaloae
Vibrio_lentus
Vibrio_mediterranei
Vibrio_metoecus
Vibrio_metschnikovii
Vibrio_mimicus
Vibrio_natriegens
Vibrio_navarrensis
Vibrio_nigripulchritudo
Vibrio_owensii
Vibrio_parahaemolyticus
Vibrio_rotiferianus
Vibrio_splendidus
Vibrio_tasmaniensis_A
Vibrio_vulnificus
Vibrio_vulnificus_B"""
# species = """Bacillus_licheniformis
# Bacillus_paralicheniformis
# Bacillus_subtilis
# Bacillus_velezensis
# Corynebacterium_accolens
# Corynebacterium_amycolatum
# Corynebacterium_amycolatum_A
# Corynebacterium_diphtheriae
# Corynebacterium_glutamicum
# Corynebacterium_propinquum
# Corynebacterium_pseudodiphtheriticum
# Corynebacterium_pseudotuberculosis
# Corynebacterium_striatum
# Corynebacterium_ulcerans
# Cupriavidus_necator
# Escherichia_coli
# Lacticaseibacillus_paracasei
# Lacticaseibacillus_rhamnosus
# Lactiplantibacillus_pentosus
# Lactiplantibacillus_plantarum
# Lactobacillus_acidophilus
# Lactobacillus_crispatus
# Lactobacillus_delbrueckii
# Lactobacillus_gasseri
# Lactobacillus_helveticus
# Lactobacillus_iners
# Lactobacillus_johnsonii
# Lactobacillus_paragasseri
# Latilactobacillus_sakei
# Lentilactobacillus_parabuchneri
# Leuconostoc_inhae
# Leuconostoc_mesenteroides
# Levilactobacillus_brevis
# Ligilactobacillus_ruminis
# Ligilactobacillus_salivarius
# Limosilactobacillus_fermentum
# Limosilactobacillus_reuteri
# Oenococcus_oeni
# Parageobacillus_thermoglucosidasius
# Pediococcus_acidilactici
# Pediococcus_pentosaceus
# Pseudomonas_E_alloputida
# Pseudomonas_E_fulva
# Pseudomonas_E_monteilii
# Pseudomonas_E_mosselii
# Pseudomonas_E_putida
# Streptomyces_albidoflavus
# Streptomyces_bacillaris
# Streptomyces_olivaceus
# Weissella_cibaria
# Weissella_confusa"""
species_list = [{"pangenome_analysis": s.strip(), "species": s.replace('_', ' ').strip()} for s in species.split('\n')]

# A local folder, where all the logs are to be stored: ----
logs_folder = "/logs/etl/mongodb/"

# Upload the log to the Microsoft Azure Blob Storage after it is created (separately for each of the collections)
# (the name of the blob will start with PanKB/etl/logs/): ----
upload_logs_for_organisms = False
upload_logs_for_gene_annotations = False
upload_logs_for_gene_info = False
upload_logs_for_genome_info = False
upload_logs_for_pathway_info = False
