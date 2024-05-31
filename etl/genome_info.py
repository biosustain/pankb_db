# Author: Liubov Pashkova
# Email: liupa@dtu.dk

# The ETL script:
# 1) Extracts information about genomes from the Genome Info pages. Source data: JSON files on the Microsoft Azure Blob Storage (serving as Data Lake)
# 2) Transforms it into the Django- and MongoDB-compatible model
# 3) Loads the transformed data into the given MongoDB database

from global_func import *

# Obtain the script execution start date and time: ----
start_strftime = time.strftime("%Y-%m-%d_%H%M%S")
script_start_time = time.time()

# Obtain the db collection object: ----
collection = db["pankb_genome_info"]

# Drop the collection if it exists: ----
collection.drop()

# Set up the logging: ----
logger = logging.getLogger("genome_info")
# Use the timestamp as a suffix for the log file name: ----
logfile_name = config.logs_folder + config.db_server + "/genome_info__" + start_strftime + ".log"
logging.basicConfig(filename=logfile_name, level=logging.INFO)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

# A function that reads one blob from the Microsoft Azure Blob Storage
# and inserts the info into the DB collection
# (later in the code it will be applied to multiple blobs) : ----
def worker(args):
    # Unpack the function arguments: ----
    blob, species, pangenome_analysis = args
    genome_info = {}
    file_content = container_client.get_blob_client(blob).download_blob().read()
    try:
        # Load the blob content into a JSON object: ----
        genome_info = json.loads(file_content)
    except json.decoder.JSONDecodeError:
        logger.warning("The file is not readable on the Blob Storage: %s" % blob.name) # theoretically can be empty (practically not observed to be empty)
    if len(genome_info) > 0:   # insert only if we have smth to insert: ----
        genome_id = list(genome_info.keys())[0]
        # Check if an antiSMASH url exists for the given genome: ----
        antismash_url = 'https://pankb.blob.core.windows.net/data/PanKB/web_data/species/' + pangenome_analysis + '/antismash/' + genome_id + '/index.html'
        if requests.head(antismash_url).status_code == 404:
            antismash_url = ''
        genomes_for_insertion_list = {
            "genome_id": genome_id,
            "strain": genome_info[genome_id]["full_name"],
            "isolation_source": genome_info[genome_id]["isolation_source"],
            "country": genome_info[genome_id]["Country"],
            "geo_loc_name": genome_info[genome_id]["geo_loc_name"],
            "gc_content": genome_info[genome_id]["gc_content"],
            "genome_len": genome_info[genome_id]["genome_len"],
            "gene_class_distribution": genome_info[genome_id]["Gene_class_distribution"],
            "antismash_url": antismash_url,
            "pangenome_analysis": pangenome_analysis,
            "species": species
            }
        collection.insert_one(genomes_for_insertion_list)
    return True


def main():
    logger.info("--- DB Insertion ---")
    num_processes = 10  # number of threads to use for multiprocessing
    logger.info("Number of processes: %d\n" % num_processes)
    species_num = 0
    # Iterate over all species: ----
    for item in pangenome_analyses_species_dict_list:
        iter_start_time = time.time()     # time when the iteration for the current species is started
        species_num += 1
        pangenome_analysis = item["pangenome_analysis"]
        species = item["species"]
        logger.info("Species %d/%d: %s" %  (species_num, total_species, pangenome_analysis))
        # Get the number of distinct genomes annotated for the given species: ----
        annotated_genomes = db.pankb_organisms.find({"pangenome_analysis": pangenome_analysis}, {"genomes_num": 1, "_id": 0}).distinct("genomes_num")[0]
        logger.info("Annotated genomes: %s" % annotated_genomes)
        # List names of all blobs with information about genomes from the respective species folder on the Blob Storage: ----
        genome_page_blobs_names = container_client.list_blob_names(name_starts_with="PanKB/web_data/species/" + pangenome_analysis + "/genome_page/")
        # Finally leave only those blobs that contain info about genomes (there can be be other files on the genome_page): ----
        genome_blobs = [blob_name for blob_name in genome_page_blobs_names if "genome_info.json" in blob_name]
        logger.info("Blobs with genomes on the storage: %s" % len(genome_blobs))
        # Compose a list of arguments for the worker function: ----
        args = [(blob_name, species, pangenome_analysis) for blob_name in genome_blobs]
        # Use multithreading to handle the massive I/O: ----
        with ThreadPool(num_processes) as pool:
            pool.map(worker, args)
            # Exit all the worked processes after all the tasks are completed: ----
            pool.close()
        # The number of inserted genomes can be higher or lower the number of annotated genes: ----
        docs_inserted = collection.count_documents({"pangenome_analysis": pangenome_analysis})
        logger.info("Genomes inserted: %d" % docs_inserted)
        logger.info("Iteration execution time: %.2f minutes\n" % ((time.time() - iter_start_time)/60))

    #logger.info("Total documents inserted: %d" % collection.count_documents({}))
    # Manage the indexes AFTER (!!!) the insertion: ----
    logger.info("Creating the indexes on the collection...")
    collection.create_index(['pangenome_analysis', 'genome_id'])
    collection.create_index(['pangenome_analysis', 'strain'])
    logger.info("The indexes have been successfully created.")
    logger.info("Total execution time: %.2f minutes" % ((time.time() - script_start_time)/60))
    logger.info("--------------------------------------")
    logger.info("Finished")


if __name__ == '__main__':   # check if the script is run as the main program
    # Better protect the main worker function when using multithreading: ----
    main()

    # Upload the log file to the Microsoft Azure Blob Storage if the respective config option is set to True:
    if config.upload_logs_for_genome_info == True:
        upload_blob_log(logfile_name)

    # Close the pymongo client: ----
    client.close()