from global_func import *
from io import StringIO

# Obtain the script execution start date and time: ----
start_strftime = time.strftime("%Y-%m-%d_%H%M%S")
script_start_time = time.time()

# Set up the logging: ----
logger = logging.getLogger("source_info")
# Use the timestamp as a suffix for the log file name: ----
logfile_name = config.logs_folder + config.db_server + "/source_info__" + start_strftime + ".log"
logging.basicConfig(filename=logfile_name, level=logging.INFO)

isolation_source_json_path = Path("./isolation_source_annotations.json")

isolation_source_dict_path = Path("./Isolation_source_dict.csv")
if isolation_source_json_path.is_file():
    with open(isolation_source_json_path, 'r') as f:
        source_annotation_tree = json.load(f)
    
    cols = {"isolation_source": [], "category_1": [], "category_2": [], "category_3": []}
    for cat1, v1 in source_annotation_tree.items():
        for cat2, v2 in v1.items():
            for cat3, v3 in v2.items():
                for keyword in v3:
                    cols["isolation_source"].append(str(keyword).lower())
                    cols["category_1"].append(cat1)
                    cols["category_2"].append(cat2)
                    cols["category_3"].append(cat3)
    source_annotation = pd.DataFrame.from_dict(cols)
    source_annotation.set_index("isolation_source", inplace=True)
else:
    if isolation_source_dict_path.is_file():
        csvObj = isolation_source_dict_path
    else:
        csvObj = StringIO(requests.get('https://pankb.blob.core.windows.net/data/PanKB/Preprocess_code/Isolation_source_dict.csv').text)
    source_annotation = pd.read_csv(csvObj, header=0, index_col=None, usecols=["isolation_source", "category_1", "category_2", "category_3"])
    source_annotation["isolation_source"] = source_annotation["isolation_source"].str.lower()
    source_annotation.drop_duplicates(subset="isolation_source", inplace=True)
    source_annotation.set_index("isolation_source", inplace=True)

    source_annotation_tree = {}
    for keyword, data in source_annotation.iterrows():
        if not data["category_1"] in source_annotation_tree:
            source_annotation_tree[data["category_1"]] = {}
        if not data["category_2"] in source_annotation_tree[data["category_1"]]:
            source_annotation_tree[data["category_1"]][data["category_2"]] = {}
        if not data["category_3"] in source_annotation_tree[data["category_1"]][data["category_2"]]:
            source_annotation_tree[data["category_1"]][data["category_2"]][data["category_3"]] = []
        source_annotation_tree[data["category_1"]][data["category_2"]][data["category_3"]].append(keyword)

with open(isolation_source_dict_path, "w") as f:
    source_annotation.to_csv(f)

with open(isolation_source_json_path, "w") as f:
    json.dump(source_annotation_tree, f)

cat_not_found = set()
source_tree = {}

for item in pangenome_analyses_species_dict_list:
    pangenome_analysis = item["pangenome_analysis"]
    species = item["species"]

    # Retrieve the respective *.json file content from the Blob storage: ----
    jsonObj = requests.get(f'https://pankb.blob.core.windows.net/data/PanKB/web_data/species/{pangenome_analysis}/source_info_core.json').json()
    source_dict = json.loads(json.dumps(jsonObj))

    for genome, data in source_dict.items():
        iso_source = str(data[1]).lower()
        if iso_source == "undefined" or iso_source == "missing": # Shortcut for common case
            cat_data = ("Missing", "Missing", "Missing")
        else:
            try:
                cat_data = source_annotation.loc[iso_source, ["category_1", "category_2", "category_3"]].values
            except:
                cat_data = ("Missing", "Missing", "No Categories")
                cat_not_found.add(iso_source)

        if not cat_data[0] in source_tree:
            source_tree[cat_data[0]] = {}
        if not cat_data[1] in source_tree[cat_data[0]]:
            source_tree[cat_data[0]][cat_data[1]] = {}
        if not cat_data[2] in source_tree[cat_data[0]][cat_data[1]]:
            source_tree[cat_data[0]][cat_data[1]][cat_data[2]] = 0
        source_tree[cat_data[0]][cat_data[1]][cat_data[2]] += 1

highcharts_data = []
def create_highcharts_entry(name, entry_id, value, parent_id=None):
    d = {
        "name": name,
        "id": entry_id,
        "value": value,
    }
    if not parent_id is None:
        d["parent"] = parent_id
    return d

for cat1, v1 in source_tree.items():
    id1 = cat1
    count1 = 0
    for cat2, v2 in v1.items():
        id2 = f"{id1}-{cat2}"
        count2 = 0
        for cat3, count3 in v2.items():
            id3 = f"{id2}-{cat3}"
            highcharts_data.append(create_highcharts_entry(cat3, id3, count3, parent_id=id2))
            count2 += count3
        highcharts_data.append(create_highcharts_entry(cat2, id2, count2, parent_id=id1))
        count1 += count2
    highcharts_data.append(create_highcharts_entry(cat1, id1, count1))

# Convert the hierarchical structure to JSON
json_file_path = './treemap_data.json'
with open(json_file_path, 'w') as f:
    json.dump(highcharts_data, f)

with open("./cat_not_found.txt", "w") as f:
    f.write("\n".join(cat_not_found))