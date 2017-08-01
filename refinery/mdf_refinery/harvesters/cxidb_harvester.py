import requests
from json import dump
import os
from shutil import rmtree
from tqdm import tqdm

#Collects available data from CXIDB and saves to the given directory
#out_dir: The path to the directory (which will be created) for the data files
#existing_dir:
#       -1: Remove out_dir if it exists
#        0: Error if out_dir exists (Default)
#        1: Overwrite files in out_dir if there are path collisions
#verbose: Print status messages? Default False
def harvest(out_dir, existing_dir=0, verbose=False):
    if os.path.exists(out_dir):
        if existing_dir == 0:
            exit("Directory '" + out_dir + "' exists")
        elif not os.path.isdir(out_dir):
            exit("Error: '" + out_dir + "' is not a directory")
        elif existing_dir == -1:
            rmtree(out_dir)
            os.mkdir(out_dir)
    else:
        os.mkdir(out_dir)

    #Fetch list of ids
    id_res = requests.get("http://cxidb.org/index.json")
    if id_res.status_code != 200:
        exit("IDs GET failure: " + str(id_res.status_code) + " error")
    id_list = id_res.json()

    for id_entry in tqdm(id_list, desc="Fetching metadata", disable= not verbose):
        id_data = requests.get("http://cxidb.org/" + id_entry)
        if id_data.status_code != 200:
            exit("ID fetch failure: " + str(id_data.status_code) + " error")
        with open(os.path.join(out_dir, id_entry), 'w') as out_file:
            dump(id_data.json(), out_file)


