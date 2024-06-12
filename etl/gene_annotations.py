# Author: Liubov Pashkova
# Email: liupa@dtu.dk, liubov.pashkova@yahoo.com

# The ETL script:
# 1) Extracts information about the pangenomic gene annotations from the Microsoft Azure Blob Storage (serving as Data Lake)
# 2) Transforms it into the Django- and MongoDB-compatible model
# 3) Loads the transformed data into the MongoDB database

from global_func import *

# Obtain the script execution start date and time: ----
start_strftime = time.strftime("%Y-%m-%d_%H%M%S")
script_start_time = time.time()

# Obtain the db collection object: ----
collection = db["pankb_gene_annotations"]

# Drop the collection if it exists: ----
collection.drop()

# Set up the logging: ----
logger = logging.getLogger("gene_annotations")
# Use the timestamp as a suffix for the log file name: ----
logfile_name = config.logs_folder + config.db_server + "/gene_annotations__" + start_strftime + ".log"
logging.basicConfig(filename=logfile_name, level=logging.INFO)

# Obtain the list of all species existing in the DB
pangenome_analyses_list = []
for data in db.pankb_organisms.find():
    pangenome_analyses_list.append(data["pangenome_analysis"])

# Iterate over all the species in the DB and create the list of rows to insert into the MongoDB: ----
requesting = []
logger.info("--- DB Insertion ---")
for pangenome_analysis in pangenome_analyses_list:
    # Retrieve the respective *.json file content from the Blob storage: ----
    jsonObj = requests.get('https://pankb.blob.core.windows.net/data/PanKB/web_data/species/' + pangenome_analysis + '/All.json').json()
    # Transform the content into a list: ----
    gene_annotations_list = json.loads(json.dumps(jsonObj))

    # Retrieve species name from the organisms table
    # (should already exist and be populated up to the point): ----
    species_name_res = db.pankb_organisms.find({"pangenome_analysis": pangenome_analysis}, {"species": 1, "_id": 0})
    species = list(species_name_res)[0]["species"]

    # Iterate over all gene annotations for the given species: ----
    for gene_annotation in gene_annotations_list:
        data = {
                "gene": gene_annotation[0],
                "cog_category": gene_annotation[1],
                "cog_name": gene_annotation[2],
                "description": gene_annotation[3],
                "protein": gene_annotation[4],
                "pfams": gene_annotation[5],
                "frequency": gene_annotation[6],
                "pangenomic_class": gene_annotation[7],
                "pangenome_analysis": pangenome_analysis,
                "species": species
                }
        requesting.append(InsertOne(data))

# Insert rows into the MongoDB and print some stats: ----
result = collection.bulk_write(requesting, ordered=True)
logger.info("Documents inserted: %s" % len(list(collection.find())))

# Manage the indexes AFTER (!!!) the insertion: ----
logger.info("Creating the indexes on the collection...")
collection.create_index('pangenome_analysis')
collection.create_index([('gene', 1), ('protein', 1), ('pfams', 1)])
logger.info("The indexes have been successfully created.")
logger.info("Execution time: %.2f minutes" % ((time.time() - script_start_time)/60))

# Upload the log file to the Microsoft Azure Blob Storage if the respective config option is set to True:
if config.upload_logs_for_gene_annotations == True:
    upload_blob_log(logfile_name)

# Close the pymongo client: ----
client.close()


