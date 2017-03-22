from json import load, dump, dumps
import os
from tqdm import tqdm
import paths
from bson import ObjectId
from utils import find_files, dc_validate

to_convert = []
to_convert.append("matin")
#to_convert.append("cxidb")
#to_convert.append("nist_dspace")
#to_convert.append("materials_commons")


def matin_convert(matin_raw, mdf_meta):
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
	feedstock_data = dc_matin
	feedstock_data["mdf_id"] = str(ObjectId())
	feedstock_data["mdf_source_name"] = mdf_meta["mdf_source_name"]
	feedstock_data["mdf_source_id"] = mdf_meta["mdf_source_id"]
#	feedstock_data["globus_source"] = mdf_meta.get("globus_source", "")
	feedstock_data["mdf_datatype"] = mdf_meta["mdf_datatype"]
	feedstock_data["acl"] = mdf_meta["acl"]
	feedstock_data["globus_subject"] = dc_matin.get("dc.identifier", None)
	feedstock_data["mdf-publish.publication.collection"] = mdf_meta["collection"]
	feedstock_data["data"] = matin_raw

	return feedstock_data


def cxidb_convert(cxidb_data, mdf_meta):
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
	feedstock_data = dc_cxidb
	feedstock_data["mdf_id"] = str(ObjectId())
	feedstock_data["mdf_source_name"] = mdf_meta["mdf_source_name"]
	feedstock_data["mdf_source_id"] = mdf_meta["mdf_source_id"]
#	feedstock_data["globus_source"] = mdf_meta.get("globus_source", "")
	feedstock_data["mdf_datatype"] = mdf_meta["mdf_datatype"]
	feedstock_data["acl"] = mdf_meta["acl"]
	feedstock_data["globus_subject"] = dc_cxidb.get("dc.identifier", None)
	feedstock_data["mdf-publish.publication.collection"] = mdf_meta["collection"]
	feedstock_data["data"] = cxidb_data

	return feedstock_data


def nist_convert(nist_raw, mdf_data): #TODO: Fix duplicates into list
	nist_data = {}
	for meta_dict in nist_raw:
		if not nist_data.get(meta_dict["key"], None): #No previous value, copy data
			nist_data[meta_dict["key"]] = meta_dict["value"]
		else: #Has value already
			new_list = []
			if type(nist_data[meta_dict["key"]]) is not list: #If previous value is not a list, add the single item
				new_list.append(nist_data[meta_dict["key"]])
			else: #Previous value is a list
				new_list += nist_data[meta_dict["key"]]
			#Now add new element and save
			new_list.append(meta_dict["value"])
			nist_data[meta_dict["key"]] = new_list

	#print(dumps(nist_data, sort_keys=True, indent=4, separators=(',', ': ')))
	dc_nist = {
		"dc.title" : nist_data["dc.title"][0] if type(nist_data.get("dc.title", None)) is list else nist_data.get("dc.title"),
		"dc.creator" : "NIST",
		"dc.contributor.author" : [nist_data["dc.contributor.author"]] if type(nist_data.get("dc.contributor.author", None)) is str else nist_data.get("dc.contributor.author", None),	
		"dc.identifier" : nist_data["dc.identifier.uri"][0] if type(nist_data.get("dc.identifier.uri", None)) is list else nist_data.get("dc.identifier.uri", None),
		"dc.subject" : [nist_data["dc.subject"]] if type(nist_data.get("dc.subject", None)) is str else nist_data.get("dc.subject", None),
		"dc.description" : str(nist_data.get("dc.description.abstract", None)) if nist_data.get("dc.description.abstract", None) else None,
		"dc.relatedidentifier" : [nist_data["dc.relation.uri"]] if type(nist_data.get("dc.relation.uri", None)) is str else nist_data.get("dc.relation.uri", None),
		"dc.year" : int(nist_data["dc.date.issued"][:4]) if nist_data.get("dc.date.issued", None) else None
		}
	none_fields = []
	for key, value in dc_nist.items(): #Only delete own fields, don't delete all Nones
		if not value:
			none_fields.append(key)
	for key in none_fields:
		dc_nist.pop(key)
	feedstock_data = dc_nist
	feedstock_data["mdf_id"] = str(ObjectId())
	feedstock_data["mdf_source_name"] = mdf_meta["mdf_source_name"]
	feedstock_data["mdf_source_id"] = mdf_meta["mdf_source_id"]
#	feedstock_data["globus_source"] = mdf_meta.get("globus_source", "")
	feedstock_data["mdf_datatype"] = mdf_meta["mdf_datatype"]
	feedstock_data["acl"] = mdf_meta["acl"]
	feedstock_data["globus_subject"] = dc_nist.get("dc.identifier", None)
	feedstock_data["mdf-publish.publication.collection"] = mdf_meta["collection"]
	feedstock_data["data"] = nist_data

#	if nist_data.get("dc.contributor", None):
#		nist_data["dc_contributor"] = nist_data.pop("dc.contributor")
	return feedstock_data


def materials_commons_convert(mc_data, mdf_data):
	dc_mc = {
		"dc.title" : mc_data.get("title", None),
		"dc.creator" : "Materials Commons",
		"dc.contributor.author" : [author["firstname"] + " " + author["lastname"] for author in mc_data.get("authors", [])],
		"dc.identifier" : mc_data.get("doi", None) if mc_data.get("doi", None) else mc_data.get("id", None),
		"dc.subject" : mc_data.get("keywords", None),
		"dc.description" : mc_data.get("description", None),
#		"dc.relatedidentifier" : mc_data.get("", None),
		"dc.year" : int(mc_data.get("published_date", "0000")[:4]) if mc_data.get("published", False) else None,
		}
	none_fields = []
	for key, value in dc_mc.items():
		if not value:
			none_fields.append(key)
	for key in none_fields:
		dc_mc.pop(key)
	feedstock_data = dc_mc
	feedstock_data["mdf_id"] = str(ObjectId())
	feedstock_data["mdf_source_name"] = mdf_meta["mdf_source_name"]
	feedstock_data["mdf_source_id"] = mdf_meta["mdf_source_id"]
#	feedstock_data["globus_source"] = mdf_meta.get("globus_source", "")
	feedstock_data["mdf_datatype"] = mdf_meta["mdf_datatype"]
	feedstock_data["acl"] = mdf_meta["acl"]
	feedstock_data["globus_subject"] = dc_mc.get("identifier", None)
	feedstock_data["mdf-publish.publication.collection"] = mdf_meta["collection"]
	feedstock_data["data"] = mc_data

	return feedstock_data



#Generalized interface for metadata conversion
#in_dir is the directory metadata files are stored
#out_file is the file the converted data should be stored
#conv_func is a function that converts the specified metadata into datacite
#pattern is the regex to match metadata files. Default: None, which matches all files in the directory
#verbose: Print status messages? Default False.
def general_meta_converter(in_dir, out_file, conv_func, mdf_metadata, file_pattern=None, verbose=False):
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
			file_json = conv_func(file_data, mdf_metadata)
			dc_validation = dc_validate(file_json)
			if dc_validation["valid"]:
				dump(dc_validation["validated"], output)
				output.write("\n")
			else:
				print("Error in file '" + file_name + "': Invalid metadata: '" + str(dc_validation["invalid_fields"]) + "'")
	if verbose:
		print("Processing complete\n")


if __name__ == "__main__":
	if "matin" in to_convert:
		matin_mdf = {
			"mdf_source_name" : "matin",
			"mdf_source_id" : 13,
#			"globus_source" : "MATIN",
			"mdf_datatype" : "oai_pmh_metadata",
			"acl" : ["public"],
			"collection" : "MATIN"
			}
		print("#####################\nMATIN\n#####################")
		general_meta_converter(paths.datasets + "matin_metadata", paths.raw_feed + "matin_metadata_all.json", matin_convert, matin_mdf, verbose=True)
	if "cxidb" in to_convert:
		cxidb_mdf = {
			"mdf_source_name" : "cxidb",
			"mdf_source_id" : 14,
#			"globus_source" : "CXIDB",
			"mdf_datatype" : "cxidb_metadata",
			"acl" : ["public"],
			"collection" : "CXIDB"
			}
		print("#####################\nCXIDB\n#####################")
		general_meta_converter(paths.datasets + "cxidb_metadata", paths.raw_feed + "cxidb_metadata_all.json", cxidb_convert, cxidb_mdf, verbose=True)
	if "nist_dspace" in to_convert:
		nist_dspace_mdf = {
			"mdf_source_name" : "nist_dspace",
			"mdf_source_id" : 12,
#			"globus_source" : "NIST DSpace (Metadata)",
			"mdf_datatype" : "nist_dspace_metadata",
			"acl" : ["public"],
			"collection" : "NIST DSpace (Metadata)"
			}
		print("#####################\nNIST\n#####################")
		general_meta_converter(paths.datasets + "nist_dspace", paths.raw_feed + "nist_metadata_all.json", nist_convert, nist_dspace_mdf, file_pattern="_metadata.json$",  verbose=True)
	if "materials_commons" in to_convert:
		mc_mdf = {
			"mdf_source_name" : "materials_commons",
			"mdf_source_id" : 16,
#			"globus_source" : "Materials Commons",
			"mdf_datatype" : "materials_commons_metadata",
			"acl" : ["public"],
			"collection" : "Materials Commons"
			}
		print("#####################\nMATERIALS COMMONS\n#####################")
		general_meta_converter(paths.datasets + "materials_commons_metadata", paths.raw_feed + "materials_commons_metadata_all.json", mc_mdf, materials_commons_convert,  verbose=True)



