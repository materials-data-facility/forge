import json
import requests
import os
from shutil import rmtree
from tqdm import tqdm


# Harvests from the JCAP XPS Spectral DB, from ID min_id to max_id
#existing_dir:
#       -1: Remove out_dir if it exists
#        0: Error if out_dir exists (Default)
#        1: Overwrite files in out_dir if there are path collisions
def harvest(out_dir, min_id=0, max_id=150, existing_dir=0, verbose=False):
    if verbose:
        print("Begin harvesting")
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

    data_found = []
    not_found = []
    for i in tqdm(range(min_id, max_id), desc="Processing data", disable= not verbose):
        res = requests.get("https://internal.solarfuelshub.org/jcapresources/matchs/export_xps/" + str(i))
        if res.status_code == 200 and res.text != "None":
            data_found.append(i)
            metadata = {
                "link": "https://internal.solarfuelshub.org/jcapresources/matchs/export_xps/" + str(i)
                }
            record_data = ""
            for line in res.text.split("\n"):
                if line.startswith("%"):
                    if line.startswith("% Released on"):
                        metadata["year"] = int(line[14:18])
                    elif line.startswith("% XPS Spectrum from"):
                        metadata["machine"] = line[20:].strip()
                    elif line.startswith("% Material:"):
                        metadata["material"] = line[11:].strip()
                    elif line.startswith("% XPS Region:"):
                        metadata["xps_region"] = line[13:].strip()
                    elif line.startswith("% Pass Energy:"):
                        metadata["pass_energy"] = int(line[14:].strip())
                    elif line.startswith("% Power:"):
                        metadata["power"] = line[8:].strip()
                else:
                    record_data += line+"\n"
            metadata["data"] = record_data
            with open(os.path.join(out_dir, str(i)+"_data.json"), "w") as output:
                json.dump(metadata, output)
        else:
            not_found.append(i)
#            if verbose:
#                print("No file found for ID", i)
    if verbose:
        print("Harvesting complete")
        print("Data found for ID:", data_found)
        print("\nData not found for ID:", not_found)

