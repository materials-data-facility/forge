from json import load, loads, dump
from os.path import join
from tqdm import tqdm
#from ujson import dump
from bson import ObjectId
from utils import find_files
import paths


datasets = []
#datasets.append("cip")
#datasets.append("ido") #Broken for now
datasets.append("nist_ip")

#Generator to run through a JSON record and yield the data
#Data is defined as the first layer that isn't a list
#Pseudo-code examples:
#	[ {1}, {2}, {3}] yields {1} then {2} then {3}
#	[ [ {1}, {2} ], [ {[3]}, {4} ] ] yields {1} then {2} then {[3]} then {4}
#	{1} yields {1}
def find_data(record):
	if type(record) is list:
		for elem in record:
			for result in find_data(elem):
				yield result
	else: #Not list, treat as data
		yield record

#Takes a JSON file and converts it into formatter-compatible JSON
#If feed_size > 0, feed_name is required
def convert_json_to_json(in_name, out_name, uri_loc, mdf_meta, feed_size=0, feed_name=None, verbose=False):
	all_uri = []
	if verbose:
		print("Converting JSON, dumping results to", out_name)
	if type(in_name) is str:
		in_name = [in_name]
	list_of_data = []
	for one_file in tqdm(in_name, desc="Processing", disable= not verbose):
		#print(one_file)
		with open(one_file, 'r') as in_file:
			#if verbose:
			#	print("Processing")
			try: #If input JSON is human-formatted (with newlines), this will fail
				for line in in_file:
					line_data = loads(line)
					for result in find_data(line_data):
						list_of_data.append(result)
						
			except Exception as err: #Fall back to reading the whole thing at once
				#if list_of_data: #If some lines were already processed, is error in JSON
					#print("Possible error in JSON on file:", one_file, ":", err)
					#list_of_data.clear() #Reset and try again
				#if verbose:
				#	print("Line reading failed, falling back to whole-file processing")
				in_file.seek(0) #Reset file to start
				data = load(in_file)
				for result in find_data(data):
					list_of_data.append(result)

	with open(out_name, 'w') as out_file:		
		if list_of_data:
			if feed_size > 0:
				feed_file = open(feed_name, 'w')
			count = 0
			for datum in list_of_data:
				datum["uri"] = eval("datum['" + uri_loc + "']")
				all_uri.append(datum["uri"])

				#Metadata
				feedstock_data = {}
				feedstock_data["mdf_id"] = str(ObjectId())
				feedstock_data["mdf_source_name"] = mdf_meta["mdf_source_name"]
				feedstock_data["mdf_source_id"] = mdf_meta["mdf_source_id"]
				feedstock_data["globus_source"] = mdf_meta.get("globus_source", "")
				feedstock_data["mdf_datatype"] = mdf_meta["mdf_datatype"]
				feedstock_data["acl"] = mdf_meta["acl"]
				feedstock_data["globus_subject"] = datum["uri"]
				feedstock_data["data"] = datum

				dump(feedstock_data, out_file)
				out_file.write('\n')
				if count < feed_size:
					dump(feedstock_data, feed_file)
					feed_file.write('\n')
				count += 1
			print("Data written successfully")
		else:
			print("Error: No data recovered from file")
	duplicates = [x for x in all_uri if all_uri.count(x) > 1]
	if duplicates:
		print("Warning: Duplicate URIs found:\n", set(duplicates))

if __name__ == "__main__":
	verbose = True
	if "cip" in datasets:
		cip_mdf = {
			"mdf_source_name" : "cip",
			"mdf_source_id" : 9,
			"globus_source" : "Evaluation and comparison of classical interatomic potentials through a user-friendly interactive web-interface",
			"mdf_datatype" : "json",
			"acl" : ["public"]
			}
		cip_in = paths.datasets + "10.5061_dryad.dd56c/classical_interatomic_potentials.json"
		cip_out = paths.raw_feed + "cip_all.json"
		cip_uri = "case-number"
		cip_sack_size = 10
		cip_feed = paths.sack_feed + "cip_10.json"
		convert_json_to_json(cip_in, cip_out, cip_uri, cip_mdf, cip_sack_size, cip_feed, verbose=verbose)

	if "ido" in datasets:
		exit("IDO not supported")
		ido_in = paths.datasets + "10.5061_dryad.ph81h/inorganic_dielectric_optical.json"
		ido_out = paths.raw_feed + "ido_all.json"
		ido_sack_size = 10
		ido_feed = paths.sack_feed + "ido_10.json"
		convert_json_to_json(ido_in, ido_out, ido_uri, ido_sack_size, ido_feed, verbose=verbose)

	if "nist_ip" in datasets:
		nist_ip_mdf = {
			"mdf_source_name" : "nist_ip",
			"mdf_source_id" : 11,
			"globus_source" : "NIST Interatomic Potentials",
			"mdf_datatype" : "json",
			"acl" : ["public"]
			}
		nist_ip_in = paths.datasets + "nist_ip/interchange"
		nist_ip_out = paths.raw_feed + "nist_ip_all.json"
		nist_ip_uri = "interatomic-potential']['id"
		nist_ip_sack_size = 10
		nist_ip_feed = paths.sack_feed + "nist_ip_10.json"
		nist_ip_file_list = []
		for file_data in find_files(nist_ip_in, ".*\.json$"):
			nist_ip_file_list.append(join(file_data["path"], file_data["filename"] + file_data["extension"]))
		convert_json_to_json(nist_ip_file_list, nist_ip_out, nist_ip_uri, nist_ip_sack_size, nist_ip_feed, verbose=verbose)
		


