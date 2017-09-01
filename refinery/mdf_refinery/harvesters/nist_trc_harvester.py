import requests
import json
import os
from shutil import rmtree
from datetime import date
from tqdm import tqdm

first_year = 1750
last_year = date.today().year + 10

#Collects available data from NIST's TRC and saves to the given directory
#out_dir: The path to the directory (which will be created) for the data files
#existing_dir:
#       -1: Remove out_dir if it exists
#        0: Error if out_dir exists (Default)
#        1: Overwrite files in out_dir if there are path collisions
#verbose: Print status messages? Default False
def harvest(out_dir, start_year=first_year, stop_year=last_year, existing_dir=0, verbose=False):
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

    if verbose:
        print("Harvesting...")

    # Fetch data, one decade at a time
    # decade is the start year of the decade
    trc_data = {
        "refs": {},
        "systems": {},
        "specimen": {},
        "comps": {}
        }
    for decade in tqdm(range(start_year, stop_year, 10), desc="Harvesting data", disable= not verbose):
        q = {
            "startYear": decade,
            "endYear": decade + 9  # TRC year range is inclusive
        }
        res = requests.post("http://trc.nist.gov/applications/metals_data/metals_api.php", json=q)
        if res.status_code != 200:
            # Eat error if data not found, print error otherwise
            if res.json().get("error") == "No data found":
                pass
            else:
                print("Error", str(res.status_code), ": '", res.text, "'", sep="")
        else:
            try:
                new_data = res.json()
                # Need to update each key so the new values don't just overwrite
                # Sub-keys are IDs and should be unique
                trc_data['refs'].update(new_data["refs"])
                trc_data['systems'].update(new_data["systems"])
                trc_data['specimen'].update(new_data["specimen"])
                trc_data['comps'].update(new_data["comps"])
            except (TypeError, KeyError):
                print("Error updating trc_data. Data:", res.status_code, res.text)

    with open(os.path.join(out_dir, "nist_trc.json"), "w") as outfile:
        json.dump(trc_data, outfile)

    if verbose:
        print("Harvesting complete")


