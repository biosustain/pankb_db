# Author: Liubov Pashkova
# Email: liupa@dtu.dk

# The ETL script:
# 1) Adds pathway info from the KEGG database to the genomes and genes
# 2) Transforms it into the Django- and MongoDB-compatible model
# 3) Loads the transformed data into the given MongoDB database

from global_func import *

# Obtain the script execution start date and time: ----
start_strftime = time.strftime("%Y-%m-%d_%H%M%S")
script_start_time = time.time()

# Obtain the db collection object: ----
collection = db["pankb_pathway_info"]

# Drop the collection if it exists: ----
collection.drop()

# Set up the logging: ----
logger = logging.getLogger("pathway_info")
# Use the timestamp as a suffix for the log file name: ----
logfile_name = config.logs_folder + config.db_server + "/pathway_info__" + start_strftime + ".log"
logging.basicConfig(filename=logfile_name, level=logging.INFO)

# Need a lock to safely share global lists between threads: ----
lock = threading.Lock()

# Global variables to be shared between the threads: ----
insertion = []             # the list of dictionaries that will accumulate the data to insert into the db
processed_strains = []     # the list that will accumulate names of strains processed or checked

# Obtain the KEGG DB instance: ----
k = KEGG()

# A function that reads pathway info associated with the annotated genes using KEGG API endpoints
# and inserts the info into the DB collection
# (later in the code it will be applied to multiple strains) : ----
def worker(args):
    # Unpack the function arguments: ----
    strain, species, pangenome_analysis, total_strains = args
    # Introduce the global variables shared between the threads: ----
    global insertion
    global processed_strains
    # Look for the given strain in the KEGG DB: ----
    strain_req_res_list = k.lookfor_organism(strain)
    # IMPORTANT: the lock should be acquired before the global variables are modified and the critical conditions are checked.
    # Otherwise, for example, two separate threads can check the same condition or modify a global variable simultaneously.
    # In this case, the db insertion will be made inappropriately or with duplicates.
    with lock:
        # Introduce a pause after processing a certasin amount of strains.
        # Needed to process the E.coli pangenome
        # # without getting Error 403 from the KEGG online database: ----
        if len(processed_strains) % 650 == 0:
            time.sleep(120)
        if len(strain_req_res_list) > 0:   # if the strain name is found in the KEGG database
            kegg_organism_id = strain_req_res_list[0].split()[1]  # get the KEGG oprganism (in fact, strain) id
            # Obtain the genome_ids for the given strain from the MongoDB instance: ----
            # (!!! Sometimes different genome_ids can have the same strain name assigned !!!)
            genome_id_list = list(db.pankb_genome_info.find({"strain": strain}, {"genome_id": 1, "_id": 0}).distinct("genome_id"))
            # Iterate over all genome_ids for the given species: ----
            for genome_id in genome_id_list:
                # Obtain the genes list for the given genome from the MongoDB instance: ----
                genes_list = list(db.pankb_gene_info.find({"pangenome_analysis": pangenome_analysis, "genome_id": genome_id}, {"gene": 1, "_id": 0}).distinct("gene"))

                # Obtain the KEGG file with kegg genes ids mapped to the gene symbols and gene products: ----
                response1 = urllib.request.urlopen("https://rest.kegg.jp/list/" + kegg_organism_id)
                genes_data = response1.read().decode("utf-8")
                genes_data_pd = pd.read_csv(StringIO(genes_data), sep="\t", header=None, usecols=[0, 3])
                # Leave only the columns of interest after transforming the output into the df: ----
                genes_data_pd.columns = ["kegg_gene", "gene"]
                # Split the column to obtain the gene symbols and products separately: ----
                genes_data_pd = genes_data_pd[genes_data_pd["gene"].str.contains(";")]
                str_split = genes_data_pd["gene"].str.split('; ')
                # Split the gene symbol and gene product (protein): ----
                genes_data_pd["gene"] = str_split.str[0]
                genes_data_pd["product"] = str_split.str[1]

                # Consider only those strain genes that we have in the MongoDB instance: ----
                genes_lists_intersection = list(set(genes_data_pd["gene"]) & set(genes_list))

                if len(genes_lists_intersection) > 0:  # the following chunk will be executed only if in the MongoDB we have genes associted with pathways
                    # Leave only those genes in the df: ----
                    genes_data_pd = genes_data_pd[genes_data_pd["gene"].isin(genes_lists_intersection)]

                    # Obtain the KEGG file with kegg gene ids mapped to the pathways: ----
                    response2 = urllib.request.urlopen("https://rest.kegg.jp/link/pathway/" + kegg_organism_id)
                    pathways_data = response2.read().decode("utf-8")
                    pathways_data_pd = pd.read_csv(StringIO(pathways_data), sep="\t", header=None)
                    pathways_data_pd.columns = ["kegg_gene", "pathway_id"]

                    # Inner join the dataframes with the genes' symbols and products and associated pathways using the kegg gene ids: ----
                    res_pd = pd.merge(genes_data_pd, pathways_data_pd, on="kegg_gene", how="inner")
                    res_pd["pathway_id"] = res_pd["pathway_id"].str.split(':').str[1]
                    # Leave only the columns of interest: ----
                    res_pd = res_pd[["pathway_id", "gene", "product"]]

                    # Obtain the KEGG file with kegg pathway ids mapped to pathway names: ----
                    response3 = urllib.request.urlopen("https://rest.kegg.jp/list/pathway/" + kegg_organism_id)
                    pathway_names_data = response3.read().decode("utf-8")
                    pathway_names_data_pd = pd.read_csv(StringIO(pathway_names_data), sep="\t", header=None)
                    # Leave only the columns of interest: ----
                    pathway_names_data_pd.columns = ["pathway_id", "pathway_name"]
                    # Cut the strain names off the pathways names: ----
                    pathway_names_data_pd["pathway_name"] = pathway_names_data_pd["pathway_name"].str.split(" - ").str[0]

                    # Again, inner join the resulting dfs: ----
                    res_pd = pd.merge(res_pd, pathway_names_data_pd, on="pathway_id", how="inner")

                    # Add columns to the resulting df: ----
                    res_pd["strain"] = strain
                    res_pd["species"] = species
                    res_pd["genome_id"] = genome_id
                    res_pd["pangenome_analysis"] = pangenome_analysis

                    # Merge with the gene annotation collection to have the pangenomic_class field available in the pathway info collection.
                    # Having it available there speeds it the search results retrieval significantly, because
                    # the engine does not have to read another collection from other part of the disk in order to merge on the fly.
                    genes_annotations = db.pankb_gene_annotations.find({"pangenome_analysis": pangenome_analysis}, {"pangenome_analysis": 1, "species": 1, "gene": 1, "pangenomic_class": 1, "_id": 0})
                    genes_annotations_pd = pd.DataFrame(list(genes_annotations), index=None)
                    if not genes_annotations_pd.empty:   # the merging is possible only if the first df is not empty: ----
                        res_pd = pd.merge(res_pd, genes_annotations_pd, on=['pangenome_analysis', 'species', 'gene'], how='inner')

                    # Insert the rows to the MongoDB instance: ----
                    insertion += res_pd.to_dict("records")

        processed_strains += [strain]
        # Insert the accumulated data into the collection in case the last strain is processed: ----
        if len(processed_strains) == total_strains:
            if len(insertion) > 0:     # insert only if we have smth to insert (otherwise insert_many() produces an error)
                collection.insert_many(insertion, ordered=True)
            # Clean the global variables for the sake of the next species to be processsed: ----
            insertion = []
            processed_strains = []


def main():
    logger.info("--- DB Insertion ---")
    num_processes = 10  # number of threads to use for multiprocessing
    logger.info("Number of processes: %d\n" % num_processes)
    species_num = 0
    # Iterate over all species: ----
    for item in pangenome_analyses_species_dict_list:
        iter_start_time = time.time()     # time when the iteration for the current species is started
        species_num = species_num + 1
        pangenome_analysis = item["pangenome_analysis"]
        species = item["species"]
        logger.info("Species %d/%d: %s" %  (species_num, total_species, pangenome_analysis))
        # Get the number of distinct genomes(strains) annotated for the given species: ----
        genomes_list = list(db.pankb_genome_info.find({"pangenome_analysis": pangenome_analysis}, {"genome_id": 1, "_id": 0}).distinct("genome_id"))
        annotated_genomes = len(genomes_list)
        logger.info("Unique genome ids: %d" % annotated_genomes)
        strains_list = list(db.pankb_genome_info.find({"pangenome_analysis": pangenome_analysis}, {"strain": 1, "_id": 0}).distinct("strain"))
        total_strains = len(strains_list)
        logger.info("Unique strain names: %d" % total_strains)
        # Compose a list of arguments for the worker function: ----
        args = [(strain, species, pangenome_analysis, total_strains) for strain in strains_list]
        # Use multithreading to handle the massive I/O: ----
        with ThreadPool(num_processes) as pool:
            pool.map(worker, args)
            # Exit all the worked processes after all the tasks are completed: ----
            pool.close()
        logger.info("%d gene-pathway association pairs were inserted" % collection.count_documents({"pangenome_analysis": pangenome_analysis}))
        logger.info("Iteration execution time: %.2f minutes\n" % ((time.time() - iter_start_time)/60))

    logger.info("Total documents inserted: %d" % collection.count_documents({}))
    # Manage the indexes AFTER (!!!) the insertion: ----
    logger.info("Creating the indexes on the collection...")
    collection.create_index([('pathway_id', 1), ('pathway_name', 1), ('product', 1)])
    collection.create_index([('pangenome_analysis', 1), ('gene', 1), ('genome_id', 1)])
    collection.create_index([('pathway_id', 1), ('strain', 1)])
    logger.info("The indexes have been successfully created.")
    logger.info("Total execution time: %.2f minutes" % ((time.time() - script_start_time)/60))
    logger.info("--------------------------------------")
    logger.info("Finished")


if __name__ == '__main__':   # check if the script is run as the main program
    # Better protect the main worker function when using multithreading: ----
    main()

    # Upload the log file to the Microsoft Azure Blob Storage if the respective config option is set to True:
    if config.upload_logs_for_pathway_info == True:
        upload_blob_log(logfile_name)

    # Close the pymongo client: ----
    client.close()