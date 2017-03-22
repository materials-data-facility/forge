from json import load, dump, dumps
from tqdm import tqdm
import os
import paths
import re
from bson import ObjectId
from utils import dc_validate
#import bson
#import bsonjs
#import json_converter
#from utils import find_files

def fix_table(table):
	headers = []
	for head in table["headers"]["column"]:
		try:
			headers.append(head["#text"])
		except KeyError: #Some tables don't have headers
			headers.append("UNKNOWN")
		except TypeError: #Some headers aren't dicts
			headers.append(head)
	data = []
	for row in table["rows"]["row"]:
		if type(row) is not list: #Some rows are lists, some are not
			row_list = [row] #Now all rows are lists
		else:
			row_list = row
		subdata = []
		for row_elem in row_list:
			for column in row_elem["column"]:
				try:
					subdata.append(float(column["#text"]))
				except KeyError: #Some tables don't have data?
					subdata.append(float("nan")) #Fix it with NaNs
				except ValueError: #Some people like to put headers in the data portion of tables
					headers.append(column["#text"])
				except TypeError: #Some columns aren't dicts
					subdata.append(column)
				
		data.append(subdata)
	return {"headers":headers, "data":data}


#Recursive parser, calls fix_table for only change in data
def recursive_parse(data):
	if type(data) is list:
		new_data = []
		for elem in data:
			new_data.append(recursive_parse(elem))
	elif type(data) is dict:
		if "headers" in data.keys() and "rows" in data.keys():
			try:
				new_data = fix_table(data)
			except Exception as err: #There's only one record that causes unknown formatting issues. Ignoring.
				#print(type(err), err)
				new_data = None
		else:
			new_data = {}
			for key, value in data.items():
				new_data[key] = recursive_parse(value)
	else:
		new_data = data
	return new_data

#Pulls out materials information
def get_nanomine_materials(data):
	materials_data = data["content"]["PolymerNanocomposite"]["MATERIALS"]
	to_fetch_polymer = ["ChemicalName", "Abbreviation", "ConstitutionalUnit", "PlasticType", "PolymerClass", "PolymerType"]
	to_fetch_particle = ["ChemicalName", "Abbreviation", "TradeName"]
	materials_list = set()
	
	if not materials_data.get("Polymer", None):
		polymer = []
	elif type(materials_data.get("Polymer", None)) is not list:
		polymer = [materials_data.get("Polymer", None)]
	else:
		polymer = materials_data.get("Polymer", [])

	for elem in polymer:
		for field in to_fetch_polymer:
			if field in elem.keys():
				materials_list.add(elem[field])
	
	if not materials_data.get("Particle", None):
		particle = []
	elif type(materials_data.get("Particle", None)) is not list:
		particle = [materials_data.get("Particle", None)]
	else:
		particle = materials_data.get("Particle", [])
	
	for elem in particle:
		for field in to_fetch_particle:
			if field in elem.keys():
				materials_list.add(elem[field])

	return list(materials_list)


#Pulls out method information
def get_nanomine_methods(data):
	methods_list = [method.replace("_", " ").lower() for method in data["content"]["PolymerNanocomposite"].get("CHARACTERIZATION", {}).keys()]
	microscopy_type = data["content"]["PolymerNanocomposite"].get("MICROSTRUCTURE", {}).get("MicroscopyType", None)
	if microscopy_type:
		methods_list.append(microscopy_type.lower())
	return methods_list


#Formats Nanomine data into dc format
def dc_format_nanomine(nm_data, mdf_meta):
	try:
		nm_cite = nm_data["content"]["PolymerNanocomposite"]["DATA_SOURCE"]["Citation"]["CommonFields"] #Shortcut for long path
	except: #If there's no DATA_SOURCE, there's also no other useful data
		return None
	#DC and mdf-base formatting
	dc_nm = {
		"dc.title" : nm_cite.get("Title", None),
		"dc.creator" : "Nanomine",
		"dc.contributor.author" : [nm_cite["Author"] if type(nm_cite.get("Author", None)) is str else nm_cite.get("Author", None)],
		"dc.identifier" : nm_data.get("uri", None),
#		"dc.subject" : nm_cite.get("", None),
#		"dc.description" : nm_cite.get("", None),
		"dc.relatedidentifier" : [nm_cite.get("DOI", "").replace("doi:", "http://dx.doi.org/"), nm_cite.get("URL", "")],
		"dc.year" : int(nm_cite.get("PublicationYear", 0)) if nm_cite.get("PublicationYear", None) == nm_cite.get("PublicationYear", None) else None,
		"mdf-base.materials_composition" : get_nanomine_materials(nm_data),
		"mdf-base.data_acquisition_method" : get_nanomine_methods(nm_data)
		}

	images = nm_data["content"]["PolymerNanocomposite"].get("MICROSTRUCTURE", {}).get("ImageFile", [])
	image_links = []
	if type(images) is not list:
		images = [images]
	for image in images:
		link = image.get("File", None)
		if link and link == link: #Must check for None and NaN
			image_links.append(image.get("File"))
	dc_nm["dc.relatedidentifier"] += image_links

	none_fields = []
	for key, value in dc_nm.items():
		if not value:
			none_fields.append(key)
	for key in none_fields:
		dc_nm.pop(key)

	valid = dc_validate(dc_nm)
	if not valid["valid"]:
		print(nm_data)
		raise KeyError("Invalid DC fields: " + str(valid["invalid_fields"]))

	#Pulling out useful fields, storing rest in text field
	use_data = {
		"nanomine_id" : nm_data.get("nanomine_id", None),
#		"characterization" : [elem.replace("_", "") for elem in nm_data["content"]["PolymerNanocomposite"].get("CHARACTERIZATION", {}).keys()],
		"name" : nm_data["content"]["PolymerNanocomposite"].get("ID", None),
#		"citation" : nm_data["content"]["PolymerNanocomposite"]["DATA_SOURCE"]["Citation"]["CommonFields"],
#		"microscopy_type" : nm_data["content"]["PolymerNanocomposite"].get("MICROSTRUCTURE", {}).get("MicroscopyType", None),
#		"processing" : nm_data["content"]["PolymerNanocomposite"].get("PROCESSING", None),
#		"materials" : nm_data["content"]["PolymerNanocomposite"].get("MATERIALS", None),
		"uri" : nm_data.get("uri", None),
		"raw" : dumps(nm_data)
		}

	feedstock_data = dc_nm
	feedstock_data["mdf_id"] = str(ObjectId())
	feedstock_data["mdf_source_name"] = mdf_meta["mdf_source_name"]
	feedstock_data["mdf_source_id"] = mdf_meta["mdf_source_id"]
#	feedstock_data["globus_source"] = mdf_meta.get("globus_source", "")
	feedstock_data["mdf_datatype"] = mdf_meta["mdf_datatype"]
	feedstock_data["acl"] = mdf_meta["acl"]
	feedstock_data["globus_subject"] = dc_nm.get("dc.identifier", None)
	feedstock_data["mdf-publish.publication.collection"] = mdf_meta["collection"]
	feedstock_data["data"] = use_data

	return feedstock_data


#Take JSON file and rebuild with sensible data structures
def convert_nanomine(in_file, out_file, mdf_data, uri_prefix="", sack_size=0, sack_file=None, verbose=False):
	all_uri = []
	with open(out_file, 'w') as output_f:
		if sack_size > 0:
			sack = open(sack_file, 'w')
		count = 0
#		for in_file_data in tqdm(find_files(dir_path, verbose=verbose), desc="Processing files", disable= not verbose):
#			in_file = os.path.join(in_file_data["path"], in_file_data["filename"] + in_file_data["extension"])
#			print(in_file)
#			with open(in_file, 'r') as input_f:
#				in_data = load(input_f)
#			converted = rec_parse(in_data)
#		with open(out_file, 'w') as output_f:
#			if sack_size > 0:
#				sack = open(sack_file, 'w')
		with open(in_file, 'r') as input_f:
			for str_record in tqdm(input_f, desc="Processing files", disable= not verbose):
				#Replace BSON terms with Python-friendly ones
				clean_record = str_record.replace("null", 'float("nan")').replace("false", "False")
				record = eval(clean_record) #String version of dict converted to actual dict
				converted = recursive_parse(record)
#				print(converted)
				if converted and type(converted) is dict: #Type should be dict, but check anyway
					#'$oid' causes issues, move to a different key
					converted["nanomine_id"] = converted["_id"]["$oid"]
					converted.pop("_id", None)
					converted['uri'] = uri_prefix + converted["nanomine_id"]
					all_uri.append(converted['uri'])

					formatted = dc_format_nanomine(converted, mdf_data)

					'''
					#In dc_format_nanomine
					#Metadata
					feedstock_data = {}
					feedstock_data["mdf_id"] = str(ObjectId())
					feedstock_data["mdf_source_name"] = mdf_meta["mdf_source_name"]
					feedstock_data["mdf_source_id"] = mdf_meta["mdf_source_id"]
					feedstock_data["globus_source"] = mdf_meta.get("globus_source", "")
					feedstock_data["mdf_datatype"] = mdf_meta["mdf_datatype"]
					feedstock_data["acl"] = mdf_meta["acl"]
					feedstock_data["globus_subject"] = converted["uri"]
					feedstock_data["data"] = converted
					'''

					if formatted:
						dump(formatted, output_f)
						output_f.write('\n')
						if count < sack_size:
							dump(formatted, sack)
							sack.write('\n')
						count += 1
				else:
					raise ValueError("Single record must be dict")
#				elif type(converted) is list:
#					for subrecord in converted:
#						subrecord["uri"] = in_file_data["filename"]
#						dump(subrecord, output_f)
#						output_f.write('\n')
#						if count < sack_size:
#							dump(subrecord, sack)
#							sack.write('\n')
#						count += 1
		if sack_size > 0:
			sack.close()
	if verbose:
		print("Processed", count, "records.")
	duplicates = [x for x in all_uri if all_uri.count(x) > 1]
	if duplicates:
		print("Warning: Duplicate URIs found:\n", set(duplicates))

if __name__ == "__main__":
	mdf_metadata = {
		"mdf_source_name" : "nanomine",
		"mdf_source_id" : 10,
#		"globus_source" : "Nanomine",
		"mdf_datatype" : "nanomine",
		"acl" : ["public"],
		"collection" : "Nanomine"
		}
#	convert_nanomine(paths.datasets + "nanomine/nanomine_results", paths.raw_feed + "nanomine_all.json", 10, paths.sack_feed + "nanomine_10.json", verbose=True)
	convert_nanomine(paths.datasets + "nanomine/nanomine.dump", paths.raw_feed + "nanomine_all.json", mdf_data=mdf_metadata, uri_prefix="http://nanomine.northwestern.edu:8000/explore/detail_result_keyword?id=", sack_size=10, sack_file=paths.sack_feed + "nanomine_10.json", verbose=True)

