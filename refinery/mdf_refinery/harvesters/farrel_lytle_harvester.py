import requests
from sys import exit
from json import dump, loads
import re
import os
import os.path
from shutil import rmtree
from tqdm import tqdm

base_url = "http://ixs.iit.edu/data/Farrel_Lytle_data/RAW/"

#Collects available data from  and saves to the given directory
#out_dir: The path to the directory (which will be created) for the data files
#existing_dir:
#       -1: Remove out_dir if it exists
#        0: Error if out_dir exists (Default)
#        1: Overwrite files in out_dir if there are path collisions
#verbose: Print status messages? Default False
def harvest(out_dir, existing_dir=0, verbose=False):
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

    #Fetch list of records
    root_res = requests.get(base_url)
    if root_res.status_code != 200:
        exit("Root list GET failure: " + str(root_res.status_code) + " error")

    list_dirs = re.findall("href=\"[^?/].{,}?/\"", str(root_res.content)) #Parse HTML for href="somechemical/", excluding the sort anchors, parent dir anchor, and misc info (which is 403 anyway)
    for dir_href in tqdm(list_dirs, desc="Processing directories", disable= not verbose): #Go through each directory found and get data
        directory = dir_href.strip("href=").strip("\"")
        dir_url = base_url + directory
        dir_res = requests.get(dir_url)
        if dir_res.status_code != 200:
            exit("Dir list GET failure: " + str(dir_res.status_code) + " error")

        #Create output directory to match found directory
        out_sub_dir = os.path.join(out_dir, directory.strip("/"))
        if os.path.exists(out_sub_dir):
            if existing_dir == 0:
                exit("Directory '" + out_sub_dir + "' exists")
            elif not os.path.isdir(out_sub_dir):
                exit("Error: '" + out_dir + "' is not a directory")
            elif existing_dir == -1:
                rmtree(out_sub_dir)
                os.mkdir(out_sub_dir)
        else:
            os.mkdir(out_sub_dir)

        list_recs = re.findall("href=\"[^?/].{,}?\"", str(dir_res.content))
        for rec_href in tqdm(list_recs, desc="Processing " + directory, disable= not verbose):
            record = rec_href.strip("href=").strip("\"")
            rec_url = dir_url + record
            rec_res = requests.get(rec_url)
            if rec_res.status_code != 200:
                exit("Record GET failure: " + str(rec_res.status_code) + " error")

            #Write data out to file in directory
            with open(os.path.join(out_sub_dir, record), 'w') as out_file:
                out_file.write(rec_res.text)

    if verbose:
        print("End harvesting")

