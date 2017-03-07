import requests
from sys import exit
from json import dump, loads
import xmltodict
import os
import os.path
from shutil import rmtree
from tqdm import tqdm

import paths

#List of setSpec resource types to harvest. If empty, will harvest all.
resource_types = []

#Collects available data from MATIN and saves to the given directory
#out_dir: The path to the directory (which will be created) for the data files
#existing_dir:
#       -1: Remove out_dir if it exists
#        0: Error if out_dir exists (Default)
#        1: Overwrite files in out_dir if there are path collisions
#verbose: Print status messages? Default False
def matin_harvest(out_dir, existing_dir=0, verbose=False):
	if os.path.exists(out_dir):
		if existing_dir == 0:
			exit("Directory '" + out_dir + "' exists")
		elif not os.path.isdir(out_dir):
			exit("Error: '" + out_dir + "' is not a directory")
		elif existing_dir == -1:
			rmtree(out_dir)
	else:
		os.mkdir(out_dir)

	#Fetch list of records
	record_res = requests.get("https://matin.gatech.edu/oaipmh/?verb=ListRecords&metadataPrefix=oai_dc")
	if record_res.status_code != 200:
		exit("Records GET failure: " + str(record_res.status_code) + " error")
	result = xmltodict.parse(record_res.content)
	records = result["OAI-PMH"]["ListRecords"]["record"]

	for record in records:
		if not resource_types or record["header"]["setSpec"] in resource_types: #Only grab what is desired
			resource_num = record["header"]["identifier"].rsplit("/", 1)[1] #identifier is `URL/id_num`
			with open(os.path.join(out_dir, resource_num + "_metadata.json"), 'w') as out_file:
				dump(record, out_file)


if __name__ == "__main__":
	matin_harvest(paths.datasets + "matin_metadata", 1)





