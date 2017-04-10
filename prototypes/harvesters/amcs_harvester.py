import requests
from sys import exit
from json import dump, loads
import re
import os
import os.path
from shutil import rmtree
from tqdm import tqdm

import paths

base_url = "http://rruff.geo.arizona.edu/AMS/xtal_data/"
dif_url = base_url + "DIFfiles/"
dif_ext = ".txt"
cif_url = base_url + "CIFfiles/"
cif_ext = ".cif"

#Collects available data from  and saves to the given directory
#out_dir: The path to the directory (which will be created) for the data files
#existing_dir:
#       -1: Remove out_dir if it exists
#        0: Error if out_dir exists (Default)
#        1: Overwrite files in out_dir if there are path collisions
#start_id: int id to start harvest from. Default 1 (00001).
#stop_id: int id to stop harvest at (exclusive). Default 2 (00002), which pulls only one record.
#verbose: Print status messages? Default False
def amcs_harvest(out_dir, existing_dir=0, start_id=1, stop_id=2, verbose=False):
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

    #Fetch records
    for i in tqdm(range(start_id, stop_id), desc="Fetching records", disable= not verbose):
        dif_res = requests.get(dif_url + str(i).zfill(5) + dif_ext) #IDs for AMCS are always 5 digits
        if dif_res.status_code != 200:
            print("Error", dif_res.status_code, "with diffraction harvest on ID", i)
            dif_res = None
        cif_res = requests.get(cif_url + str(i).zfill(5) + cif_ext)
        if cif_res.status_code != 200:
            print("Error", dif_res.status_code, "with diffraction harvest on ID", i)
            cif_res = None

        if dif_res != None:
            with open(os.path.join(out_dir, str(i).zfill(5) + dif_ext), 'w') as dif_out:
                dif_out.write(dif_res.text)
        if cif_res != None:
            with open(os.path.join(out_dir, str(i).zfill(5) + cif_ext), 'w') as cif_out:
                cif_out.write(cif_res.text)

    if verbose:
        print("End harvesting")


if __name__ == "__main__":
    amcs_harvest(paths.datasets+"amcs", existing_dir=1, start_id=1, stop_id=101, verbose=True)




