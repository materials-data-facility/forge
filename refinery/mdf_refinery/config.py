import os
import json


PATHS = {}
CACHE_FILE = os.path.join(os.path.dirname(__file__), ".mdf_paths.json")

# Get paths to important directories
def build_cache():
    global PATHS
    # If paths have been cached already, just read them
    if os.path.isfile(CACHE_FILE):
        with open(CACHE_FILE) as cache:
            PATHS = json.loads(CACHE_FILE)
    # Need to get paths
    else:
        path_datasets = os.path.normpath(os.path.realpath(input("Input path to datasets:\n")))
        if not os.path.isdir(path_datasets):
            raise NotADirectoryError("'" + path_datasets + "' is not a valid dataset location")
        path_feedstock = os.path.normpath(os.path.realpath(input("Input path to feedstock:\n")))
        if not os.path.isdir(path_feedstock):
            raise NotADirectoryError("'" + path_feedstock + "' is not a valid feedstock location")
        path_schemas = os.path.normpath(os.path.realpath(input("Input path to schemas:\n")))
        if not os.path.isdir(path_schemas):
            raise NotADirectoryError("'" + path_schemas + "' is not a valid schema location")

        PATHS = {
            "datasets": path_datasets,
            "feedstock": path_feedstock,
            "schemas": path_schemas
            }
        with open(CACHE_FILE, "w") as cache:
            json.dump(PATHS, cache)


# Remove old path cache
def clear_cache():
    try:
        global PATHS
        PATHS = {}
        os.remove(CACHE_FILE)
    # If cache does not exist, is clear
    except FileNotFoundError:
        pass


# Return the path to the given location
def get_path(loc):
    if not PATHS:
        build_cache()
    return PATHS[loc]


