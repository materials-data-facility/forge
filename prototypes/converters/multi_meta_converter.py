from json import load, dump, dumps
import os
from tqdm import tqdm
import paths
from utils import find_files, dc_validate


def matin_convert(matin_raw):
	matin_data = matin_raw["metadata"]["oai_dc:dc"]
	dc_matin = {
		"dc.title" : matin_data.get("dc:title", None),
		"dc.creator" : "MATIN",
		"dc.contributor.author" : [matin_data["dc:creator"]] if type(matin_data.get("dc:creator", None)) is str else matin_data.get("dc:creator", None), #TODO: Parse properly
		"dc.identifier" : matin_data.get("dc:identifier", None),
		"dc.subject" : [matin_data["dc:subject"]] if type(matin_data.get("dc:subject", None)) is str else matin_data.get("dc:subject", None),
		"dc.description" : matin_data.get("dc:description", None),
		"dc.relatedidentifier" : [matin_data["dc:relation"]] if type(matin_data.get("dc:relation", None)) is str else matin_data.get("dc:relation", None),
		"dc.year" : int(matin_data["dc:date"][:4]) if matin_data.get("dc:date", None) else None
		}
	none_fields = []
	for key, value in dc_matin.items():
		if not value:
			none_fields.append(key)
	for key in none_fields:
		dc_matin.pop(key)
	matin_raw.update(dc_matin)
	return matin_raw


def cxidb_convert(cxidb_data):
	dc_cxidb = {
		"dc.title" : cxidb_data.get("citation_title", None),
		"dc.creator" : "CXIDB",
		"dc.contributor.author" : [cxidb_data["citation_authors"]] if type(cxidb_data.get("citation_authors", None)) is str else cxidb_data.get("citation_authors", None), #TODO: Parse properly
		"dc.identifier" : cxidb_data.get("entry_DOI", None),
#		"dc.subject" : cxidb_data.get("", None),
#		"dc.description" : cxidb_data.get("", None),
		"dc.relatedidentifier" : [cxidb_data["citation_DOI"]] if type(cxidb_data.get("citation_DOI", None)) is str else cxidb_data.get("citation_DOI", None),
		"dc.year" : int(cxidb_data["summary_deposition_date"][:4]) if cxidb_data.get("summary_deposition_date", None) else None
		}
	none_fields = []
	for key, value in dc_cxidb.items():
		if not value:
			none_fields.append(key)
	for key in none_fields:
		dc_cxidb.pop(key)
	cxidb_data.update(dc_cxidb)
	return cxidb_data


def nist_convert(nist_raw): #TODO: Fix duplicates into list
	nist_data = {}
	for meta_dict in nist_raw:
		if not nist_data.get(meta_dict["key"], None): #No previous value, copy data
			nist_data[meta_dict["key"]] = meta_dict["value"]
		else: #Has value already
			if type(nist_data[meta_dict["key"]]) is not list: #If previous value is not a list, make it one for appending
				nist_data[meta_dict["key"]] = [nist_data[meta_dict["key"]]]
			nist_data[meta_dict["key"]] = nist_data[meta_dict["key"]].append(meta_dict["value"])
	dc_nist = {
#		"dc.title" : str(),
		"dc.creator" : "NIST",
		"dc.contributor.author" : [nist_data["dc.contributor.author"]] if type(nist_data.get("dc.contributor.author", None)) is str else nist_data.get("dc.contributor.author", None),
		"dc.identifier" : nist_data.get("dc.identifier.uri", None),
		"dc.subject" : [nist_data["dc.subject"]] if type(nist_data.get("dc.subject", None)) is str else nist_data.get("dc.subject", None),
		"dc.description" : nist_data.get("dc.description.abstract", None),
		"dc.relatedidentifier" : [nist_data["dc.relation.uri"]] if type(nist_data.get("dc.relation.uri", None)) is str else nist_data.get("dc.relation.uri", None),
		"dc.year" : int(nist_data["dc.date.issued"][:4]) if nist_data.get("dc.date.issued", None) else None
		}
	nist_data.update(dc_nist)
	return nist_data


#Generalized interface for metadata conversion
#in_dir is the directory metadata files are stored
#out_file is the file the converted data should be stored
#conv_func is a function that converts the specified metadata into datacite
#pattern is the regex to match metadata files. Default: None, which matches all files in the directory
#verbose: Print status messages? Default False.
def general_meta_converter(in_dir, out_file, conv_func, file_pattern=None, verbose=False):
	if verbose:
		print("Converting metadata from", in_dir)
		print("Writing JSON to", out_file)
	with open(out_file, 'w') as output:
		for file_data in tqdm(find_files(in_dir, file_pattern=file_pattern, verbose=verbose), desc="Processing metadata", disable= not verbose):
			file_path = file_data["path"]
			file_name = file_data["filename"] + file_data["extension"]
			with open(os.path.join(file_path, file_name), 'r') as in_file:
				try:
					file_data = load(in_file)
				except:
					exit("ERROR ON: " + file_path + "?" + file_name)
			file_json = conv_func(file_data)
			dc_validation = dc_validate(file_json)
			if dc_validation["valid"]:
				dump(dc_validation["validated"], output)
				output.write("\n")
			else:
				print("Error in file '" + file_name + "': Invalid metadata: '" + str(dc_validation["invalid_fields"]) + "'")
	if verbose:
		print("Processing complete")


if __name__ == "__main__":
	print("#####################\nMATIN\n#####################")
	general_meta_converter(paths.datasets + "matin_metadata", paths.raw_feed + "matin_metadata_all.json", matin_convert, verbose=True)
	print("#####################\nCXIDB\n#####################")
	general_meta_converter(paths.datasets + "cxidb_metadata", paths.raw_feed + "cxidb_metadata_all.json", cxidb_convert, verbose=True)
	print("#####################\nNIST\n#####################")
	general_meta_converter(paths.datasets + "nist_dspace", paths.raw_feed + "nist_metadata_all.json", nist_convert, file_pattern="_metadata.json$",  verbose=True)




