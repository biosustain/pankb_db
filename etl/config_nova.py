### The ETL scripts configuration file ####

## MongoDB type. Only two possible values:
# - 'self_deployed' (standalone, deployed on the DEV server in a docker container)
# or
# - 'cloud' (MongoDB Atlas or Azure CosmosDB for MongoDB)
db_type = 'self_deployed'

## Server, where the db instance in use is located ('dev', 'prod'):
db_server = 'dev'

# Whether to drop the existing collection in the DB (True, False):
drop_collection = True

gene_batch_size = 10_000

pangenome_analyses = """Aliivibrio_fischeri
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
Vibrio_vulnificus_B
Bacillus_licheniformis
Bacillus_paralicheniformis
Bacillus_subtilis
Bacillus_velezensis
Corynebacterium_accolens
Corynebacterium_amycolatum
Corynebacterium_amycolatum_A
Corynebacterium_diphtheriae
Corynebacterium_glutamicum
Corynebacterium_propinquum
Corynebacterium_pseudodiphtheriticum
Corynebacterium_pseudotuberculosis
Corynebacterium_striatum
Corynebacterium_ulcerans
Cupriavidus_necator
Escherichia_coli
Lacticaseibacillus_paracasei
Lacticaseibacillus_rhamnosus
Lactiplantibacillus_pentosus
Lactiplantibacillus_plantarum
Lactobacillus_acidophilus
Lactobacillus_crispatus
Lactobacillus_delbrueckii
Lactobacillus_gasseri
Lactobacillus_helveticus
Lactobacillus_iners
Lactobacillus_johnsonii
Lactobacillus_paragasseri
Latilactobacillus_sakei
Lentilactobacillus_parabuchneri
Leuconostoc_inhae
Leuconostoc_mesenteroides
Levilactobacillus_brevis
Ligilactobacillus_ruminis
Ligilactobacillus_salivarius
Limosilactobacillus_fermentum
Limosilactobacillus_reuteri
Oenococcus_oeni
Parageobacillus_thermoglucosidasius
Pediococcus_acidilactici
Pediococcus_pentosaceus
Pseudomonas_E_alloputida
Pseudomonas_E_fulva
Pseudomonas_E_monteilii
Pseudomonas_E_mosselii
Pseudomonas_E_putida
Streptomyces_albidoflavus
Streptomyces_bacillaris
Streptomyces_olivaceus
Weissella_cibaria
Weissella_confusa"""
pangenome_analyses = """Aliivibrio_fischeri"""
pangenome_analyses = [s.strip() for s in pangenome_analyses.split('\n')]

# A local folder, where all the logs are to be stored: ----
logs_folder = "/logs/etl/mongodb/"

output_path = "./web_data/"