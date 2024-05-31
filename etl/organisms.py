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

# Drop the collection if it exists: ----
collection.drop()

# Set up the logging: ----
logger = logging.getLogger("organisms")
# Use the timestamp as a suffix for the log file name: ----
logfile_name = config.logs_folder + config.db_server + "/organisms__" + start_strftime + ".log"
logging.basicConfig(filename=logfile_name, level=logging.INFO)

# Retrieve the respective *.json file content from the Blob storage: ----
jsonObj = requests.get('https://pankb.blob.core.windows.net/data/PanKB/web_data/species_list.json').json()
organisms_dict = json.loads(json.dumps(jsonObj))

# Create a list of rows to insert to the MongoDB: ----
requesting = []
for c1, c2, c3, c4, c5, c6 in zip(organisms_dict["Family"], organisms_dict["Species"], organisms_dict["Openness"], organisms_dict["Gene_class"], organisms_dict["N_of_genome"], organisms_dict["Pangenome_analyses"]):
    data = {"family": c1,
            "species": c2,
            "openness": c3,
            "gene_class_distribution": c4,
            "genomes_num": c5,
            "pangenome_analysis": c6}
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