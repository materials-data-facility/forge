import os
from tqdm import tqdm
from json import dump, loads
from bson import ObjectId

from utils import find_files, dc_validate
import paths #Contains variables for relative paths to data

from ase_converter import convert_ase_to_json

#Args:
#	in_dir: Directory containing CIFs
#	out_file: File to output feedstock to
#	doi_file: CSV file containing DOI information
#	temp_file: Temporary storage for data. Default "./temp_out"
#	err_log: File to log errors, None disables (will exit on error). Default None
#	sack_size: Size of feedsack, 0 to disable. Default 0.
#	sack_file: Location for feedsack, None to disable. Default None
#	verbose: Show status messages?
def core_mof_convert(in_dir, out_file, doi_file, temp_file="temp.out", err_log=None, sack_size=0, sack_file=None, verbose=False):
	if verbose:
		print("Processing CoRE-MOF CIFs to", out_file)
	#Get DOIs
	doi_dict = {}
	with open(doi_file) as dois:
		for line in dois:
			values = line.split(",")
			doi_dict[values[0]] = values[1] if values[1] != "-" else ""
	#Finding files
	file_list = find_files(root=in_dir, file_pattern=".cif", verbose=verbose)
	with open(out_file, 'w') as output:
		if sack_file:
			count = 0
			feedsack = open(sack_file, 'w')
		if err_log:
			err_open = open(err_log, 'w')
		for cif in tqdm(file_list, desc="Processing CIFs", disable= not verbose):
			#Gather metadata
			doi = ""
			for key in doi_dict.keys():
				if cif["filename"].startswith(key):
					doi = doi_dict[key]
					break
			full_path = os.path.join(cif["path"], cif["filename"] + cif["extension"])
			with open(full_path) as raw_in: #Open input file, has line of non-CIF metadata at start
				raw_metadata = raw_in.readline() #Read metadata line
				with open(temp_file, 'w') as temp_out: #Copy rest of file to temp file to process
					for line in raw_in:
						temp_out.write(line)
			#Process actual CIF
			file_data = convert_ase_to_json(file_path=temp_file, data_format="cif", output_file=None, error_log=err_open if err_log else None, verbose=False)

			if file_data:
				file_data["filename"] = cif["filename"] + cif["extension"]
				#Add metadata
#				cif_metadata = loads(raw_metadata) #Metadata currently incorrect here
				feedstock_data = {
					"dc.title" : "CoRE-MOF - " + file_data["chemical_formula"] + " (" + cif["filename"].split("_")[0] + ")",
					"dc.creator" : "CoRE-MOF",
					"dc.contributor.author" : [],
					"dc.identifier" : "https://raw.githubusercontent.com/gregchung/gregchung.github.io/master/CoRE-MOFs/core-mof-v1.0-ddec/" + file_data["filename"],
#					"dc.subject" : [],
#					"dc.description" : "",
					"dc.related_identifier" : [doi],
#					"dc.year" : None
					"mdf_id" : str(ObjectId()),
					"mdf_source_name" : "core_mof",
					"mdf_source_id" : 19,
					"mdf_datatype" : "cif",
					"acl" : ["public"],
#					"globus_subject" : "https://raw.githubusercontent.com/gregchung/gregchung.github.io/master/CoRE-MOFs/core-mof-v1.0-ddec/" + cif["filename"] + cif["extension"],
					"globus_subject" : "https://github.com/gregchung/gregchung.github.io/blob/master/CoRE-MOFs/core-mof-v1.0-ddec/" + file_data["filename"],
					"mdf-publish.publication.collection" : "CoRE-MOF",
					"mdf-base.material_composition" : file_data["chemical_formula"],
					"data" : file_data
					}

				dump(feedstock_data, output)
				output.write("\n")

				if sack_file and count < sack_size:
					dump(feedstock_data, feedsack)
					feedsack.write("\n")
					count += 1

	os.remove(temp_file)
	if verbose:
		print("Processing complete")


if __name__ == "__main__":
	core_mof_convert(in_dir=paths.datasets+"core_mof/core-mof-v1.0-ddec", out_file=paths.raw_feed+"core_mof_all.json", doi_file=paths.datasets+"core_mof/structure-doi-CoRE-MOFsV2.0.csv", temp_file="temp.out", err_log=paths.datasets+"core_mof/core_mof_errors.txt", sack_size=20, sack_file=paths.sack_feed+"core_mof_20.json", verbose=True)

