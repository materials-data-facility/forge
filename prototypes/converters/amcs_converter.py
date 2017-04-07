from validator import Validator
from parsers.ase_parser import parse_ase
from parsers.utils import find_files

import os
from tqdm import tqdm

#This is a converter for the American Mineralogist Crystal Structure Database
#Arguments:
#	input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#	verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):

	#Collect the metadata
	dataset_metadata = {
		"mdf_source_name" : "amcs",
		"dc.title" : "AMCS DB",
		"dc.creator" : "The American Mineralogist Crystal Structure Database",
		"dc.contributor.author" : ["Downs, R.T.", "Hall-Wallace, M."],
		"dc.identifier" : "http://rruff.geo.arizona.edu/AMS/amcsd.php",
#		"dc.subject" : [],
#		"dc.description" : "",
#		"dc.relatedidentifier" : [],
		"dc.year" : 2003,
		"acl" : ["public"],
#		"mdf-base.material_composition" : chemical_formula,
		"mdf-publish.publication.collection" : "AMCS",
		}
	dataset_metadata["globus_subject"] = dataset_metadata["dc.identifier"]


	
	#Make a Validator to help write the feedstock
	#You can pass the metadata to the constructor
	dataset_validator = Validator(dataset_metadata) 
	#Alternately, if you don't pass the metadata to the constructor, you can write the metadata later, before you write any records
	#Don't give the Validator metadata twice
	#dataset_validator.write_metadata(dataset_metadata)


	#Get the data
	#TODO: Write the code to convert your dataset's records into JSON-serializable Python dictionaries
		#Each record should be exactly one dictionary
		#It is recommended that you convert your records one at a time, but it is possible to put them all into one big list (see below)
		#It is also recommended that you use a parser to help with this process if one is available for your datatype

	for cif in tqdm(find_files(root=input_path, file_pattern=".cif", verbose=verbose), desc="Processing files", disable= not verbose):
		full_path = os.path.join(cif["path"], cif["filename"] + cif["extension"])
		#TODO:FIX
			cif_data = parse_ase(file_path=full_path, data_format="cif", verbose=False)
		if cif_data:
			print(cif_data)
			return
			#Each record also needs its own metadata
			record_metadata = {
				"mdf_source_name" : "amcs",
				"dc.title" : "AMCS - " + chemical_formula,
				"dc.creator" : "The American Mineralogist Crystal Structure Database",
				"dc.contributor.author" : ["Downs, R.T.", "Hall-Wallace, M."],
				"dc.identifier" : "http://rruff.geo.arizona.edu/AMS/minerals/" + _chemical_name_mineral,
#				"dc.subject" : [],
#				"dc.description" : "",
#				"dc.relatedidentifier" : [],
				"dc.year" : 2003,
				"mdf-base.material_composition" : cif_data["chemical_formula"],
				"mdf-publish.publication.collection" : "AMCS",
				"data" : cif_data #This is important: the actual record data (any fields not already here) go here
				}
			
			#Pass each individual record to the Validator
			result = dataset_validator.write_record(record_metadata)

			#Check if the Validator accepted the record, and print a message if it didn't
			#If the Validator returns "success" == True, the record was written successfully
			if result["success"] != True:
				print("Error:", result["message"], ":", result["invalid_data"])

	#Alternatively, if the only way you can process your data is in one large list, you can pass the list to the Validator
	#You still must add the required metadata to your records
	#It is recommended to use the other method if possible
	#result = dataset_validator.write_dataset(your_records_with_metadata)
	#if result["success"] != True:
		#print("Error:", result["message"])


	#TODO: Save your converter as [dataset_name]_converter.py
	#You're done!
	if verbose:
		print("Finished converting")



if __name__ == "__main__":
	import paths
	convert(paths.datasets+"amcs", True)
