from connections import *

from io import StringIO
from thefuzz import process
import shutil
from datetime import datetime

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def unwrap_source_tree_recursively(result, d, keys=None):
    if keys is None:
        keys = ()
    if isinstance(d, dict):
        for k, v in d.items():
            unwrap_source_tree_recursively(result, v, keys=keys + (k,))
    else:
        for entry in d:
            result[entry.lower()] = keys

def navigate_tree(tree, keys):
    if not keys:
        return tree
    if len(keys) > 1:
        return navigate_tree(tree[keys[0]], keys[1:])
    else:
        return tree[keys[0]]
def add_to_tree(tree, keys, value, treetype="int"):
    if len(keys) > 1:
        if not keys[0] in tree:
            tree[keys[0]] = {}
        add_to_tree(tree[keys[0]], keys[1:], value, treetype=treetype)
    else:
        if not keys[0] in tree:
            if treetype == "int":
                tree[keys[0]] = 0
            elif treetype == "list":
                tree[keys[0]] = []
            else:
                raise Exception()
        if treetype == "int":
            tree[keys[0]] += value
        elif treetype == "list":
            tree[keys[0]].append(value)
        else:
            raise Exception()
def print_tree(tree, depth=0):
    if isinstance(tree, dict):
        for k, v in tree.items():
            print(f"{(depth*2)*' '} - {k}")
            print_tree(v, depth=depth+1)

class Getch:
    def __init__(self):
        import tty, sys

    def __call__(self):
        import sys, tty, termios
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return ch

def get_best_positions_fuzzy(src, ann, limit=3, redundancy=7, min_score=75):
    m = process.extract(src, ann.keys(), limit=limit+redundancy)
    d_src = {}
    d_score = {}
    for s, score in m:
        keys = ann[s]
        d_src[keys] = d_src.get(keys, []) + [s]
        d_score[keys] = max(d_score.get(keys, 0), score)
    sorted_keys = list(d_score.keys())
    sorted_keys.sort(key=lambda x: d_score[x], reverse=True)
    best_keys = sorted_keys[:min(limit, len(sorted_keys))]
    return {k: d_src[k] for k in best_keys if d_score[k] > min_score}

def input_category_info(src, tree, ann, start_pos=None):
    getch = Getch()
    print(f"{bcolors.HEADER}# Isolation Source:{bcolors.ENDC} '{bcolors.BOLD}{src}{bcolors.ENDC}'")

    recommended = get_best_positions_fuzzy(src, ann, limit=4)
    recommended_actions = {"abcd"[i]: k for i, k in enumerate(recommended.keys())}
    if len(recommended) > 0:
        print(bcolors.OKGREEN + "# Suggestions:" + bcolors.ENDC)
        for action, k in recommended_actions.items():
            print(f"{bcolors.OKGREEN}  ? [ {action}] {' > '.join(k)} ({', '.join(recommended[k])}){bcolors.ENDC}")
    
    if start_pos is None:
        cur_pos = []
    else:
        cur_pos = start_pos
    cur_d = None
    res = None
    while True:
        cur_d = navigate_tree(tree, cur_pos)
        if not isinstance(cur_d, dict):
            res = tuple(cur_pos)
            break
        cur_keys = list(cur_d.keys())
        print("")
        if len(cur_pos) > 0:
                print(f" - {' > '.join(cur_pos)}")
        for i, k in enumerate(cur_keys):
            print(f"  - [{i + 1:2d}] {k}")
        print(f"{bcolors.OKBLUE}/ [q]uit{' / [u]p' if len(cur_pos) > 0 else ''} / [n]ew branch / new [l]eaf / [s]kip / skip [r]est / [p]rint tree /{bcolors.ENDC}")
        if len(cur_keys) > 9: # expect multiple numbers, so require ENTER
            action = input("> ").strip().lower()
        else:
            print("> ", end='', flush=True)
            action = getch().lower()
            print(action)
        if action == "q":
            print(bcolors.FAIL + "# Aborted" + bcolors.ENDC)
            return -1, None
        elif action =='s':
            print(bcolors.WARNING + "# Skipping" + bcolors.ENDC)
            return 1, None
        elif action == 'r':
            print(bcolors.WARNING + "# Skipping rest of missing annotations" + bcolors.ENDC)
            return 0, None
        elif action == 'p':
            print_tree(cur_d)
        elif (make_leaf := (action == 'l')) or action == 'n':
            name = ''
            while len(name) == 0:
                name = input("Name: ")
                name = name.strip()
                if name in cur_d:
                    print(f"{bcolors.WARNING}# Name already exists{bcolors.ENDC}")
                    name = ''
            cur_pos.append(name)
            if make_leaf:
                res = tuple(cur_pos)
                break
            else:
                cur_d[name] = {}

        elif action in recommended_actions:
            res = recommended_actions[action]
            break
        elif len(cur_pos) > 0 and action == "u":
            del cur_pos[-1]
        else:
            try:
                new_key = cur_keys[int(action) - 1]
                cur_pos.append(new_key)
            except:
                print(bcolors.WARNING + "# invalid action" + bcolors.ENDC)
    
    if not res is None:
        add_to_tree(tree, res, src, treetype="list")
        ann[src] = res
        return 1, res
    return -1, None

def create_highcharts_entry(name, entry_id, value, parent_id=None):
    d = {
        "name": name,
        "id": entry_id,
        "value": value,
    }
    if not parent_id is None:
        d["parent"] = parent_id
    return d

if __name__ == "__main__":
    logger = TimedLogger("i_source_info")

    # Load the tree from a json file.
    isolation_source_json_path = Path("./isolation_source_annotations.json")
    with open(isolation_source_json_path, 'r') as f:
        source_annotation_tree = json.load(f)

    # Create a 'isolation source' to categories mapping
    source_annotation = {}
    unwrap_source_tree_recursively(source_annotation, source_annotation_tree)
    # print(source_annotation)

    cat_not_found = set()
    source_tree = {}
    status = 1

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
                cat_data = source_annotation.get(iso_source, None)


                if cat_data is None:
                    if status > 0:
                        status, cat_data = input_category_info(iso_source, source_annotation_tree, source_annotation)
                    if status < 0:
                        exit(1)
                    if cat_data is None:
                        cat_data = ("Missing", "Missing", "No Categories")
                        cat_not_found.add(iso_source)
            
            add_to_tree(source_tree, cat_data, 1, treetype="int")

    # Create a highcharts-readable json file
    # TODO: Still assumes 3 levels of categories.
    highcharts_data = []
    for cat1, v1 in source_tree.items():
        if cat1 == "Missing":
            continue
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
    json_file_path = out_path / 'treemap_data.json'
    with open(json_file_path, 'w') as f:
        json.dump(highcharts_data, f)

    with open(out_path / "cat_not_found.txt", "w") as f:
        f.write("\n".join(cat_not_found))
    
    curtime = datetime.now()
    isolation_source_json_path_backup = isolation_source_json_path.with_suffix(f".json.{time.strftime('%y%m%d_%H%M')}")
    shutil.copyfile(isolation_source_json_path, isolation_source_json_path_backup)
    with open(isolation_source_json_path, 'w') as f:
         json.dump(source_annotation_tree, f)