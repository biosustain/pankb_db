from thefuzz import process

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

def create_highcharts_structure(source_tree):
    highcharts_data = []
    
    def _recursive_highcharts_structure(d, parent_id=None):
        if isinstance(d, int):
            return d
        count = 0
        for cat, value in d.items():
            if parent_id is None and cat == "Missing":
                continue
            cur_id = cat if parent_id is None else f"{parent_id}-{cat}"
            cur_count = _recursive_highcharts_structure(value, parent_id=cur_id)
            highcharts_data.append(create_highcharts_entry(cat, cur_id, count, parent_id=parent_id))
            count += cur_count
        return count

    _recursive_highcharts_structure(source_tree)
    return highcharts_data
