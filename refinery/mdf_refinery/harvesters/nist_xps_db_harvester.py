import requests
from sys import exit
import sys
import json
from bs4 import BeautifulSoup
import os
from multiprocessing.pool import Pool
from shutil import rmtree
from tqdm import tqdm

base_url = "https://srdata.nist.gov/xps/XPSDetailPage.aspx?AllDataNo="
g_verbose = False

#Collects available data from the NIST XPS DB and saves to the given directory
#out_dir: The path to the directory (which will be created) for the data files
#existing_dir:
#       -1: Remove out_dir if it exists
#        0: Error if out_dir exists (Default)
#        1: Overwrite files in out_dir if there are path collisions
#start_id: int id to start harvest from. Default 1.
#stop_id: int id to stop harvest at (exclusive). Default 100000.
#verbose: Print status messages? Default False
def harvest(out_dir, existing_dir=0, start_id=0, stop_id=100000, verbose=False):
    g_verbose = verbose
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
    arg_list = [ (i, out_dir, verbose) for i in range(start_id, stop_id) ]
    mp = Pool(32)
    mp.starmap(fetch_write, arg_list, chunksize=1)
    mp.close()
    mp.join()
    if verbose:
        print("Harvesting complete")


#Record processing, in function for optimization
def fetch_write(index, out_dir, verbose=False):
    res = requests.get(base_url + str(index))
    soup = BeautifulSoup(res.text, "lxml")
    if len(soup.find_all("table")) >= 2:
        table = soup.find_all("table")[1]  # Second table is only relevant one
        record = {}
        for row in table.find_all("tr"):
            key = row.td.text.strip().strip(":")
            value = []
            for elem in row.find_all("td")[1:]:
                value.append(elem.text.strip())
            if len(value) < 1:
                value = None
            elif len(value) == 1:
                value = value[0]
            record[key] = value
        if record["Element"]:
            with open(os.path.join(out_dir, "nist_xps_"+str(index)+".json"), "w") as output:
                json.dump(record, output)
            if verbose:
                print("Completed: #" + str(index), flush=True)
        else:
            print("Not found: #" + str(index), flush=True)
    else:
        print("Bad table: #" + str(index), flush=True)

