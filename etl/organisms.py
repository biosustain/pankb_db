# Author: Liubov Pashkova
# Email: liupa@dtu.dk

# The ETL script:
# 1) Extracts information about the organisms from the Microsoft Azure Blob Storage (serving as Data Lake)
# 2) Transforms it into the Django- and MongoDB-compatible model
# 3) Loads the transformed data into the MongoDB database

from global_func import *

# Obtain the script execution start date and time: ----
start_strftime = time.strftime("%Y-%m-%d_%H%M%S")
script_start_time = time.time()

# Obtain the db collection object: ----
collection = db["pankb_organisms"]

if config.drop_collection:
    # Drop the collection if it exists: ----
    collection.drop()

# Set up the logging: ----
logger = logging.getLogger("organisms")
# Use the timestamp as a suffix for the log file name: ----
logfile_name = config.logs_folder + config.db_server + "/organisms__" + start_strftime + ".log"
logging.basicConfig(filename=logfile_name, level=logging.INFO)

requesting = []

for item in pangenome_analyses_species_dict_list:
    pangenome_analysis = item["pangenome_analysis"]
    species = item["species"]

    # Retrieve the respective *.json file content from the Blob storage: ----
    jsonObj = requests.get(f'https://pankb.blob.core.windows.net/data/PanKB/web_data/species/{pangenome_analysis}/info_panel.json').json()
    organism_dict = json.loads(json.dumps(jsonObj))

    # Create a list of rows to insert to the MongoDB: ----
    data = {"family": organism_dict["Family"],
            "species": organism_dict["Species"],
            "openness": organism_dict["Openness"],
            "gene_class_distribution": organism_dict["Gene_class"],
            "genomes_num": organism_dict["Number_of_genome"],
            "pangenome_analysis": pangenome_analysis}
    requesting.append(InsertOne(data))

# Insert rows into the MongoDB and print some stats: ----
logger.info("--- DB Insertion ---")
result = collection.bulk_write(requesting, ordered=True)
logger.info("Execution time: %.2f seconds" % (time.time() - script_start_time))
logger.info("Documents inserted: %s" % (len(requesting)))

# Upload the log file to the Microsoft Azure Blob Storage if the respective config option is set to True:
if config.upload_logs_for_organisms == True:
    upload_blob_log(logfile_name)

client.close()