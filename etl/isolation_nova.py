from connections import *
from countries import COUNTRIES
import gzip
import json
import requests
from pymongo import UpdateOne
import isolation_category_helpers as iso
import re

def check_perfect_match(country_entry, countries_dict):
    """
    Check for perfect matches in the country dictionary.
    This includes matching against both the keys and any aliases in the value lists.
    """
    for normalized_country, aliases in countries_dict.items():
        if isinstance(aliases, list):
            if country_entry.strip().lower() in map(str.lower, aliases):
                return normalized_country
        else:
            if country_entry.strip().lower() == aliases.lower():
                return normalized_country
    return None

def check_contains_match(country_entry, countries_dict):
    """
    Check for whole-word 'contains' matches in the country dictionary.
    """
    for normalized_country, aliases in countries_dict.items():
        if isinstance(aliases, list):
            for alias in aliases:
                pattern = r'\b' + re.escape(alias) + r'\b'
                if re.search(pattern, country_entry, re.IGNORECASE):
                    return normalized_country
        else:
            pattern = r'\b' + re.escape(aliases) + r'\b'
            if re.search(pattern, country_entry, re.IGNORECASE):
                return normalized_country
    return None

if __name__ == "__main__":
    db_conn = DBConnection()
    logger = TimedLogger("isolation")

    # Load the tree from a json file.
    isolation_source_json_path = Path("./isolation_source_annotations.json")
    with open(isolation_source_json_path, 'r') as f:
        source_annotation_tree = json.load(f)

    # Obtain the db collection object: ----
    collection = db_conn.db["pankb_isolation_info"]

    if config.drop_collection:
        # Drop the collection if it exists: ----
        collection.drop()
    logger.info("Creating the indexes on the collection...")
    collection.create_index(['genome_id'])
    logger.info("The indexes have been successfully created.")

    requesting = []

    # Create a 'isolation source' to categories mapping
    source_annotation = {}
    iso.unwrap_source_tree_recursively(source_annotation, source_annotation_tree)
    # print(source_annotation)

    cat_not_found = set()
    source_tree = {}
    status = 1

    for pangenome_analysis, species in pangenome_analyses.items():
        logger.info(f" - Processing {pangenome_analysis}")
        # Retrieve the respective *.json file content from the Blob storage: ----
        with requests.get(f'{BlobConnection.base_url}{BlobConnection.web_data_path}species/{pangenome_analysis}/nova/isolation.jsonl', stream=True) as r:
            r.raise_for_status()
            for line in r.iter_lines():
                isolation_dict = json.loads(line)

                # Country standardization
                if (perfect_match := check_perfect_match(str(isolation_dict.get("country", "?")), COUNTRIES)):
                    isolation_dict["country"] = perfect_match
                elif (contains_match := check_contains_match(str(isolation_dict.get("country", "?")), COUNTRIES)):
                    isolation_dict["country"] = contains_match
                else:
                    logger.warning(f"COUNTRY '{isolation_dict['country']}' not found in standardization dict.")
                
                # Isolation source categorization
                iso_source = str(isolation_dict.get("isolation_source", "Missing")).lower()
                if iso_source == "undefined" or iso_source == "missing": # Shortcut for common case
                    cat_data = ("Missing", "Missing", "Missing")
                else:
                    cat_data = source_annotation.get(iso_source, None)

                    if cat_data is None:
                        if status > 0:
                            status, cat_data = iso.input_category_info(iso_source, source_annotation_tree, source_annotation)
                        if status < 0:
                            exit(1)
                        if cat_data is None:
                            cat_data = ("Missing", "Missing", "No Categories")
                            cat_not_found.add(iso_source)
                
                iso.add_to_tree(source_tree, cat_data, 1, treetype="int")

                isolation_dict["iso_cat"] = list(cat_data)

                requesting.append(UpdateOne({"genome_id": isolation_dict["genome_id"]}, {"$set": isolation_dict}, upsert=True))
    logger.info("--- Final DB Insertion ---")
    result = collection.bulk_write(requesting, ordered=True)
    logger.log_execution_time()
    logger.info(f"Documents inserted: {len(requesting)}")
