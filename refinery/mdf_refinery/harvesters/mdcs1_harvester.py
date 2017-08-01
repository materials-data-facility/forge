import json
import requests
import os
from shutil import rmtree
from tqdm import tqdm


url = "https://mdcs1.nist.gov/rest/explore/select/all"


# Harvests from mdcs1
#existing_dir:
#       -1: Remove out_dir if it exists
#        0: Error if out_dir exists (Default)
#        1: Overwrite files in out_dir if there are path collisions
def harvest(out_dir, existing_dir=0, verbose=False):
    if verbose:
        print("Begin harvesting")

    # Get login information
    with open(os.path.join(os.path.dirname(__file__), "mdcs1_login.json")) as login:
        creds = json.load(login)

    res = requests.get(url, auth=(creds["user"], creds["pass"]), verify=False)
    if res.status_code != 200:
        raise IOError("Unable to connect: " + res.status_code)

    for entry in tqdm(res.json(), desc="Harvesting data", disable= not verbose):
        out_path, base_dir_name = os.path.split(out_dir[:-1] if out_dir.endswith("/") else out_dir)
        out_path = os.path.join(out_path, base_dir_name + "_" + (entry["schema_title"] or entry["schema"]))
        if not os.path.exists(out_path):
            os.mkdir(out_path)
        with open(os.path.join(out_path, entry["_id"] + ".json"), "w") as out_file:
            json.dump(entry, out_file)

    if verbose:
        print("Finished harvesting")



