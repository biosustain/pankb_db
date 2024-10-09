# Author: Liubov Pashkova
# Email: liupa@dtu.dk

# The ETL script:
# 1) Extracts information about genomes from the Gene Info pages. Source data: JSON files on the Microsoft Azure Blob Storage (serving as Data Lake)
# 2) Transforms it into the Django- and MongoDB-compatible model
# 3) Loads the transformed data into the given MongoDB database

from global_func import *

# Obtain the script execution start date and time: ----
start_strftime = time.strftime("%Y-%m-%d_%H%M%S")
script_start_time = time.time()

# Obtain the db collection object: ----
collection = db["pankb_gene_info"]

if config.drop_collection:
    # Drop the collection if it exists: ----
    collection.drop()

# Set up the logging: ----
logger = logging.getLogger("gene_info")
# Use the timestamp as a suffix for the log file name: ----
logfile_name = config.logs_folder + config.db_server + "/gene_info__" + start_strftime + ".log"
logging.basicConfig(filename=logfile_name, level=logging.INFO)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

# For the Big Data collections like this one (over 31 million documents),
# the indexes has to be created before the data insertion.
# Yes, it decreases the data insertion speed, but there is no choice, because trying to create the indexes after the data insertion (both via Studio 3T and pymongo),
# I get the following error every time:
# pymongo.errors.ExecutionTimeout: The command being executed was terminated due to a command timeout. This may be due to concurrent transactions. Consider increasing the maxTimeMS on the command., full error: {'ok': 0.0, 'errmsg': 'The command being executed was terminated due to a command timeout. This may be due to concurrent transactions. Consider increasing the maxTimeMS on the command.', 'code': 50, 'codeName': 'ExceededTimeLimit'}
logger.info("Creating the indexes on the collection...")
collection.create_index(['pangenome_analysis', 'gene'])
collection.create_index(['pangenome_analysis', 'genome_id'])
logger.info("The indexes have been successfully created.")

# Global variables to be shared between the threads: ----
insertion = []           # the list of dictionaries that will accumulate the data to insert into the db from the given species' blobs
processed_blobs = []     # the list that will accumulate names of blobs processed (not all of them are inserted into the db, some of them can be empty)

# Need a lock to safely share global lists between threads: ----
lock = threading.Lock()

# A function that reads one blob from the Microsoft Azure Blob Storage
# and inserts the info into the DB collection
# (later in the code it will be applied to multiple blobs in parallel): ----
def worker(args):
    # Unpack the function arguments: ----
    blob, species, pangenome_analysis, total_blobs = args
    # Introduce the global variables shared between the threads: ----
    global processed_blobs
    global insertion
    # A dict where the locus tag info will be read: ----
    gene_info = {}
    # Read the blob from the Azure Blob Storage: ----
    file_content = container_client.get_blob_client(blob).download_blob().read()
    try:
        # Load the blob content into a JSON object: ----
        gene_info = json.loads(file_content)
    except json.decoder.JSONDecodeError:
        logger.warning("The file is not readable on the Blob Storage: %s" % blob.name) # sometimes can be empty
    # IMPORTANT: the lock should be acquired before the global variables are modified and the critical conditions are checked.
    # Otherwise, for example, two separate threads can check the same condition or modify a global variable simultaneously.
    # In this case, the db insertion will be made inappropriately or with duplicates.
    with lock:
        processed_blobs += [blob.name]
        if len(gene_info) > 0:   # insert only if we have smth to insert: ----
            # Extract the gene name from of the path: ----
            gene = Path(blob.name).stem
            # Add new keys to the list of dictionaries: ----
            gene_info = [{**d, "gene": v} for d, v in zip(gene_info, [gene]*len(gene_info))]
            gene_info = [{**d, "pangenome_analysis": v} for d, v in zip(gene_info, [pangenome_analysis]*len(gene_info))]
            gene_info = [{**d, "species": v} for d, v in zip(gene_info, [species]*len(gene_info))]
            # Rename some dictionary keys in the list of dictionaries: ----
            for d in gene_info:
                d['locus_tag'] = d.pop('Locus_Tag')
                d['genome_id'] = d.pop('Genome_ID')
                d['protein'] = d.pop('Prokka_Annotation')
                d['start_position'] = d.pop('Start_Position')
                d['end_position'] = d.pop('End_Position')
                d['nucleotide_seq'] = d.pop('Nucleotide_Seq')
                d['aminoacid_seq'] = d.pop('Amino_Acid_Seq')
            insertion += gene_info
            if len(processed_blobs) == total_blobs or len(processed_blobs) == config.gene_batch_size:   # insert when all the blobs are processed for the current species: ----
                collection.insert_many(insertion, ordered=False)
                # Clean the global variables for the sake of the next species to be processsed: ----
                insertion = []
                processed_blobs = []




def main():
    logger.info("--- DB Insertion ---")
    num_processes = 10     # number of threads to use for multiprocessing
    logger.info("Number of processes: %d\n" % num_processes)
    species_num = 0     # the number of species currently processed in the interation
    # Iterate over all species one by one: ----
    for item in pangenome_analyses_species_dict_list:
        # Set the time when the iteration for the current species is started: ----
        iter_start_time = time.time()
        species_num += 1
        pangenome_analysis = item["pangenome_analysis"]
        species = item["species"]
        logger.info("Species %d/%d: %s" %  (species_num, total_species, pangenome_analysis))
        # Get the number of genes annotated for the given species: ----
        annotated_genes = len(list(db.pankb_gene_annotations.find({"pangenome_analysis": pangenome_analysis})))
        logger.info("Annotated genes: %s" % annotated_genes)
        # List all files with information about genes from the respective species folder on the Blob Storage: ----
        gene_locustag_blobs = list(container_client.list_blobs(name_starts_with = "PanKB/web_data/species/" + pangenome_analysis + "/gene_locustag/"))
        # Remove the .ipynb_checkpoints from the list of blobs: ----
        gene_locustag_blobs = [blob for blob in gene_locustag_blobs if ".ipynb_checkpoints" not in blob.name]
        total_gene_locustag_blobs = len(gene_locustag_blobs)
        # Process the blobs in chucks to fit the RAM on the DEV server and reduce the CPU load on the PROD: ----
        chunk_size = min(1000, total_gene_locustag_blobs)
        for i in range(0, total_gene_locustag_blobs, chunk_size):
            blobs_in_chuck = gene_locustag_blobs[i:i + chunk_size]
            # Compose a list of arguments for the worker function: ----
            total_blobs_in_chunk = len(blobs_in_chuck)
            args = [(blob, species, pangenome_analysis, total_blobs_in_chunk) for blob in blobs_in_chuck]
            # Create the thread pool and process the data for the given species in parallel: ----
            with ThreadPool(num_processes) as pool:
                pool.map(worker, args)
                pool.close()
        ## Comment the counts below in case of getting pymongo.errors.ExecutionTimeout: ----
        # The number of inserted genes can be higher or lower than the number of annotated genes.
        # Obtain and check this number for every species: ----
        #genes_inserted = len(
        #    list(collection.find({"pangenome_analysis": pangenome_analysis}, {"gene": 1, "_id": 0}).distinct("gene")))
        #logger.info("Genes inserted: %d" % genes_inserted)
        # Total documents inserted for the given species into the gene_info collection: ----
        #docs_inserted = collection.count_documents({"pangenome_analysis": pangenome_analysis})
        #logger.info("Documents inserted: %d" % docs_inserted)
        logger.info("Iteration execution time: %.2f minutes\n" % ((time.time() - iter_start_time)/60))

    logger.info("Total execution time: %.2f minutes" % ((time.time() - script_start_time)/60))
    logger.info("--------------------------------------")
    logger.info("Finished")


if __name__ == '__main__':   # check if the script is run as the main program
    # Better protect the main worker function when using multithreading: ----
    main()

    # Upload the log file to the Microsoft Azure Blob Storage if the respective config option is set to True:
    if config.upload_logs_for_gene_info == True:
        upload_blob_log(logfile_name)

    # Close the pymongo client: ----
    client.close()