import os
import json

from mdf_refinery import MDF_PATH
PATHS = {}
CACHE_FILE = os.path.join(MDF_PATH, "mdf_paths.json")

# Get paths to important directories
def build_cache(prompt_user=False):
    global PATHS
    # If paths have been cached already, just read them
    if os.path.isfile(CACHE_FILE):
        with open(CACHE_FILE) as cache:
            PATHS = json.load(cache)
    # Need to get paths
    elif prompt_user:
        path_datasets = os.path.normpath(os.path.realpath(input("Input path to datasets:\n")))
        if not os.path.isdir(path_datasets):
            raise NotADirectoryError("'" + path_datasets + "' is not a valid dataset location")
        path_feedstock = os.path.normpath(os.path.realpath(input("Input path to feedstock:\n")))
        if not os.path.isdir(path_feedstock):
            raise NotADirectoryError("'" + path_feedstock + "' is not a valid feedstock location")
        path_schemas = os.path.normpath(os.path.realpath(input("Input path to schemas:\n")))
        if not os.path.isdir(path_schemas):
            raise NotADirectoryError("'" + path_schemas + "' is not a valid schema location")
        path_creds = os.path.normpath(os.path.realpath(input("Input path to credentials:\n")))
        if not os.path.isdir(path_creds):
            raise NotADirectoryError("'" + path_creds + "' is not a valid credentials location")

        PATHS = {
            "datasets": path_datasets,
            "feedstock": path_feedstock,
            "schemas": path_schemas,
            "credentials": path_creds
            }
        with open(CACHE_FILE, "w") as cache:
            json.dump(PATHS, cache)
    else:
        PATHS = {
            "datasets": os.path.join(MDF_PATH, "datasets/"),
            "feedstock": os.path.join(MDF_PATH, "feedstock/"),
            "schemas": os.path.join(MDF_PATH, "schemas/"),
            "credentials": os.path.join(MDF_PATH, "credentials/")
            }
        with open(CACHE_FILE, "w") as cache:
            json.dump(PATHS, cache)


# Remove old path cache
def clear_cache():
    try:
        global PATHS
        PATHS = {}
        os.remove(CACHE_FILE)
    # If cache does not exist, it is clear
    except FileNotFoundError:
        pass


# Return the path to the given location
def get_path(loc):
    if not PATHS:
        build_cache()
    return PATHS[loc]


