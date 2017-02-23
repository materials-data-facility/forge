import requests
from sys import exit
from json import dump, loads
import os
import os.path
from shutil import rmtree
from tqdm import tqdm

import paths

#Collects available data from NIST's DSpace instance and saves to the given directory
#out_dir: The path to the directory (which will be created) for the data files
#existing_dir:
#	-1: Remove out_dir if it exists
#	 0: Error if out_dir exists (Default)
#	 1: Overwrite files in out_dir if there are path collisions
#verbose: Print status messages? Default False
def nist_dspace_harvest(out_dir, existing_dir=0, verbose=False):
	if os.path.exists(out_dir):
		if existing_dir == 0:
			exit("Directory '" + out_dir + "' exists")
		elif not os.path.isdir(out_dir):
			exit("Error: '" + out_dir + "' is not a directory")
		elif existing_dir == -1:
			rmtree(out_dir)
	else:
		os.mkdir(out_dir)

	#Fetch list of item IDs and error out if even one fails
	item_ids = requests.get("https://materialsdata.nist.gov/dspace/rest/items")
	if item_ids.status_code == 200:
		item_ids = [str(item_record["id"]) for item_record in tqdm(loads(item_ids.content), desc="Fetching item IDs", disable= not verbose)]
	else:
		exit("Item ID GET failure: " + str(item_ids.status_code) + " error")

	#Fetch data/metadata from every ID
	for item in item_ids:
		#Make new directory for this data if needed
		item_dir = os.path.join(out_dir, item)
		if existing_dir == 1: #If collisions are possible
			if os.path.exists(item_dir): #And if there is a collision
				if not os.path.isdir(item_dir): #Error-check
					exit("Error: Directory collision with non-directory: '" + item_dir + "'")
			else: #No collision
				os.mkdir(item_dir)
		else: #Collisions not possible
			os.mkdir(item_dir)

		metadata = requests.get("https://materialsdata.nist.gov/dspace/rest/items/" + item + "/metadata")
		metadata = loads(metadata.content) if metadata.status_code == 200 else exit("Metadata GET failure")

		with open(os.path.join(item_dir, item + "_metadata.json"), 'w') as out_file:
			dump(metadata, out_file)

		bitstream_data = requests.get("https://materialsdata.nist.gov/dspace/rest/items/" + item + "/bitstreams")
		if bitstream_data.status_code == 200:
			bitstream_data = [{"bit_id" : str(bit_record["id"]), "bit_name" : bit_record["name"]} for bit_record in tqdm(loads(bitstream_data.content), desc="Fetching bitstream IDs", disable= not verbose)]
		else:
			exit("Bitstream ID GET failure: " + str(bitstream_data.status_code) + " error")

		for bitstream in bitstream_data:
			data = requests.get("https://materialsdata.nist.gov/dspace/rest/bitstreams/" + bitstream["bit_id"] + "/retrieve", stream=True)
			#data = data.content if data.status_code == 200 else exit("Data GET failure")
			if data.status_code != 200:
				exit("Data GET failure: " + str(data.status_code) + " error")

			with open(os.path.join(item_dir, bitstream["bit_name"]), 'wb') as out_file:
				for chunk in data.iter_content(chunk_size=1024):
					out_file.write(chunk)



if __name__ == "__main__":
	nist_dspace_harvest(paths.datasets + "nist_dspace", existing_dir=1, verbose=True)

