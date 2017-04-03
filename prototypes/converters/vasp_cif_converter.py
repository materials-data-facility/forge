import os
from tqdm import tqdm
from json import dump
import warnings
from bson import ObjectId

from parsers.utils import find_files
import paths #Contains variables for relative paths to data

from parsers.ase_converter import convert_ase_to_json

#Pick one or more datasets to process
datasets_to_process = []
#datasets_to_process.append("danemorgan")
datasets_to_process.append("khazana_polymer")
#datasets_to_process.append("khazana_vasp")
#datasets_to_process.append("sluschi")


#datasets_to_process.append("cod") #Unsupported right now


#Export a smaller feedstock file for testing?
#If False, will still write full feedstock file
feedsack = True
#These are the sizes of the small feedstock
dane_feedsack = 100
khaz_p_feedsack = 100
khaz_v_feedsack = 3
cod_feedsack = 100
sluschi_feedsack = 100

#Currently-supported formats are listed below.
supported_formats = ['vasp', 'cif']

#Calls ase_converter functions and ties everything together
#arg_dict accepts:
#	DEPRECATED: metadata: a dict of desired metadata (e.g. globus_source, context, etc.), NOT including globus_subject. Default nothing.
#
#*	mdf_metadata: Dict of MDF-specific metadata field values. Required.
#	uri: the prefix for globus_subject (ex. if uri="http://globus.org/" and uri_adds (see below) is [dir] then a file found in the directory "data/123/" would have globus_subject="http://globus.org/data/123/") Default is empty string.
#	keep_dir_name_depth: how many layers of directory, counting up from the base file, should be saved and added to the URI. -1 saves the entire path. The default (which is lossy but private) is 0.
#
#	root: The directory containing all directories containing processable files. Default is current directory.
#	file_pattern: A regular expression that matches to processable files of interest. Errors will result if this expression matches to files of a different type than specified. Default is None, which matches to all files.
#*	file_format: The format of the files to process. Only one format may be specified. Acceptable formats are listed in the global variable supported_formats. This is *REQUIRED*.
#	verbose: Bool to show system messages. Default is False (messages off).
#	output_file: The file to output the final list to. If this is not set, the output will not be written to file.
#
#	data_exception_log: File to log exceptions caught when processing the data. If a filename is listed here, such exceptions will be logged but otherwise ignored. Default None, which causes exceptions to terminate the program.
#
#	uri_adds: A list of things to add to the end of 'uri' for each record. Options are 'dir' for the directory structure, 'filename' for the name of the file, and 'ext' for the file extension. Other elements in the list will be added as string literals.
#		Choose any combination, including none (empty list). The URI will be appended with the values specified in the order they are specified. Default 'filename'.
#	max_records: Maximum number of records to return (actual return value may be less due to failed files). Default -1, which returns all (valid) records.
#	archived: Bool, if some desirable files are in archives and should be extracted. This slows down file discovery. Can be turned off after first run, because the archived files will already be extracted. Default False.
#	feedsack_size: Number of records to save in the "feedsack," the smaller feedstock file for testing. Any int <= 0 disables feedsacks. Default -1.
#	feedsack_file: Filename for feedsack output. Ignored if feedsack_size <= 0. Default "feedsack_$SIZE.json" in local directory.
#
def process_data(arg_dict):
	all_uri = []
	mdf_meta = arg_dict.get("mdf_metadata", None)
	if not mdf_meta:
		exit("Error: No mdf_meta argument supplied")
	root = arg_dict.get("root", os.getcwd())
	keep_dir_name_depth = arg_dict.get("keep_dir_name_depth", 0)
	verbose  = arg_dict.get("verbose", False)
	file_pattern = arg_dict.get("file_pattern", None)
	uri_adds = arg_dict.get("uri_adds", ['filename'])
	max_records = arg_dict.get("max_records", -1)
	archived = arg_dict.get("archived", False)
	feedsack_size = arg_dict.get("feedsack_size", -1)
	feedsack_file = arg_dict.get("feedsack_file", "feedsack_" + str(feedsack_size) + ".json") if feedsack_size > 0 else None
	if arg_dict.get("file_format", "NONE") not in supported_formats:
		print("Error: file_format '" + arg_dict.get("file_format", "NONE") + "' is not supported.")
		return None

	if arg_dict.get("data_exception_log", None):
		err_log = open(arg_dict["data_exception_log"], 'w')
	else:
		err_log = None
	output_file = arg_dict.get("output_file", None)
#	if verbose:
#		print("Finding files")
	dir_list = find_files(root=root, file_pattern=file_pattern, keep_dir_name_depth=keep_dir_name_depth, max_files=max_records, uncompress_archives=archived, verbose=verbose)
	if verbose:
		print("Converting file data to JSON")
#	all_data_list = []
	good_count = 0
	all_count = 0
	if output_file:
		out_open = open(output_file, 'w')
	if feedsack_size > 0:
		feed_out = open(feedsack_file, 'w')
	for dir_data in tqdm(dir_list, desc="Processing data files", disable= not verbose):
		all_count += 1
#		print(dir_data, "#count:", all_count)
#		formatted_data = {}
		uri = arg_dict.get("uri", "")
		full_path = os.path.join(dir_data["path"], dir_data["filename"] + dir_data["extension"])
		with warnings.catch_warnings():
			warnings.simplefilter("ignore") #Either it fails (and is logged in convert_ase_to_json) or it's fine.
			file_data = convert_ase_to_json(file_path=full_path, data_format=arg_dict["file_format"], output_file=None, error_log=err_log, verbose=False) #Status messages spam with large datasets
		if file_data:
			file_data["dirs"] = dir_data["dirs"]
			file_data["filename"] = dir_data["filename"]
			file_data["ext"] = dir_data["extension"]

			for addition in uri_adds:
				if addition == 'dir':
					for dir_name in file_data["dirs"]:
						uri += os.path.join(uri, dir_name)
				elif addition == 'filename':
					uri += os.path.join(uri, file_data["filename"])
				elif addition == 'ext':
					uri += file_data["ext"]
				else:
					uri += addition

#			formatted_data["uri"] = uri
#			for key, value in arg_dict.get("metadata", {}).iteritems():
#				formatted_data[key] = value
			
			file_data["uri"] = uri
			all_uri.append(file_data["uri"])
	#		all_data_list.append(formatted_data)
			
			#Metadata
			feedstock_data = {}
			feedstock_data["mdf_id"] = str(ObjectId())
			feedstock_data["mdf_source_name"] = mdf_meta["mdf_source_name"]
			feedstock_data["mdf_source_id"] = mdf_meta["mdf_source_id"]
#			feedstock_data["globus_source"] = mdf_meta.get("globus_source", "")
			feedstock_data["mdf_datatype"] = mdf_meta["mdf_datatype"]
			feedstock_data["acl"] = mdf_meta["acl"]
			feedstock_data["globus_subject"] = formatted_data["uri"]
			feedstock_data["mdf-publish.publication.collection"] = mdf_meta["collection"]
			feedstock_data["data"] = file_data

			dump(feedstock_data, out_open)
			out_open.write("\n")
			if good_count < feedsack_size:
				dump(feedstock_data, feed_out)
				feed_out.write("\n")
			good_count += 1
	if feedsack_size > 0:
		feed_out.close()
		print("Feedsack written to", feedsack_file)
	if output_file:
		out_open.close()
		if verbose:
			print("Data written to", output_file)
	if err_log:
		err_log.close()
	if verbose:
		print(str(good_count) + "/" + str(all_count) + " data files successfully processed")

#	if arg_dict.get("output_file", None):
#		if verbose:
#			print "Writing output to " + arg_dict["output_file"]
#		with open(arg_dict["output_file"], 'w') as out_file:
#			dump(all_data_list, out_file)
#		if verbose:
#			print "Data written"
	if verbose:
		print("Processing complete")

	duplicates = [x for x in all_uri if all_uri.count(x) > 1]
	if duplicates:
		print("Warning: Duplicate URIs found:\n", set(duplicates))

	return True #all_data_list


if __name__ == "__main__":
	print("\nBEGIN")
	
	#Dane Morgan
	if "danemorgan" in datasets_to_process:
		dane_mdf = {
			"mdf_source_name" : "ab_initio_solute_database",
			"mdf_source_id" : 3,
#			"globus_source" : "High-throughput Ab-initio Dilute Solute Diffusion Database",
			"mdf_datatype" : "vasp",
			"acl" : ["public"],
			"collection" : "High-throughput Ab-initio Dilute Solute Diffusion Database"
			}
		dane_args = {
#			"metadata" : {
#				"globus_source" : "High-throughput Ab-initio Dilute Solute Diffusion Database",
#				"globus_id" : "ddmorgan@wisc.edu cmgtam@globusid.org",
#				"context" : {
#					"dc" : "http://dublincore.org/documents/dcmi-terms"
#					}
#				},
			"mdf_metadata" : dane_mdf,
			"uri" : "globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/published/publication_164/data/",
			"keep_dir_name_depth" : 3,
			"root" : paths.datasets + "dane_morgan/data",
			"file_pattern" : "^OUTCAR$",
			"file_format" : "vasp",
			"verbose" : True,
			"output_file" : paths.raw_feed + "danemorgan_all.json",
			"data_exception_log" : paths.datasets + "dane_morgan/dane_errors.txt",
			"uri_adds" : ["dir"],
			"max_records" : -1,
			"archived" : False,
			"feedsack_size" : dane_feedsack if feedsack else -1,
			"feedsack_file" : paths.sack_feed + "danemorgan_" + str(dane_feedsack) + ".json"
			}
		if dane_args["verbose"]:
			print("DANE PROCESSING")
		dane = process_data(dane_args)
		if dane_args["verbose"]:
			print("DONE\n")
		'''
		if feedsack:
			
			print "Creating JSON files"
			print "Dane Morgan All"
			with open("dane_morgan/danemorgan_all.json", 'w') as fd1:
				json.dump(dane, fd1)
			
			if dane_args["verbose"]:
				print "Making Dane Morgan feedsack (" + str(dane_feedsack) + ")"
			with open(paths.sack_feed + "danemorgan_" + str(dane_feedsack) + ".json", 'w') as fd2:
				dump(dane[:dane_feedsack], fd2)
			if dane_args["verbose"]:
				print "Done\n"
		'''


	#Khazana Polymer
	if "khazana_polymer" in datasets_to_process:
		khazana_polymer_mdf = {
			"mdf_source_name" : "khazana_polymer",
			"mdf_source_id" : 4,
#			"globus_source" : "Khazana (Polymer)",
			"mdf_datatype" : "cif",
			"acl" : ["public"],
			"collection" : "Khazana (Polymer)"
			}
		khazana_polymer_args = {
#			"metadata" : {
#				"globus_source" : "",
#				"globus_id" : "khazana",
#				"context" : {
#					"dc" : "http://dublincore.org/documents/dcmi-terms"
#					}
#				},
			"mdf_metadata" : khazana_polymer_mdf,
			"uri" : "http://khazana.uconn.edu/module_search/material_detail.php?id=",
			"keep_dir_name_depth" : 0,
			"root" : paths.datasets + "khazana/polymer_scientific_data_confirmed",
			"file_pattern" : None,
			"file_format" : "cif",
			"verbose" : True,
			"output_file" : paths.raw_feed + "khazana_polymer_all.json",
			"data_exception_log" : paths.datasets + "khazana/khazana_polymer_errors.txt",
			"uri_adds" : ["filename"],
			"max_records" : -1,
			"archived" : False,
			"feedsack_size" : khaz_p_feedsack if feedsack else -1,
			"feedsack_file" : paths.sack_feed + "khazana_polymer_" + str(khaz_p_feedsack) + ".json"
			}
		if khazana_polymer_args["verbose"]:
			print("KHAZANA POLYMER PROCESSING")
		khaz_p = process_data(khazana_polymer_args)
		if khazana_polymer_args["verbose"]:
			print("DONE\n")
		'''
		if feedsack:
		
			print "Creating JSON files"
			print "Khazana Polymer All"
			with open("khazana/khazana_polymer_all.json", 'w') as fk1:
				json.dump(khaz_p, fk1)
			
			if khazana_polymer_args["verbose"]:
				print "Making Khazana Polymer feedsack (" + str(khaz_p_feedsack) + ")"
			with open(paths.sack_feed + "khazana_polymer_" + str(khaz_p_feedsack) + ".json", 'w') as fk2:
				dump(khaz_p[:khaz_p_feedsack], fk2)
			if khazana_polymer_args["verbose"]:
				print "Done\n"
		'''

	#Khazana VASP
	if "khazana_vasp" in datasets_to_process:
		khazana_vasp_mdf = {
			"mdf_source_name" : "khazana_dft",
			"mdf_source_id" : 5,
#			"globus_source" : "Khazana (DFT)",
			"mdf_datatype" : "vasp",
			"acl" : ["public"],
			"collection" : "Khazana (DFT)"
			}
		khazana_vasp_args = {
#			"metadata" : {
#				"globus_source" : "",
#				"globus_id" : "khazana",
#				"context" : {
#					"dc" : "http://dublincore.org/documents/dcmi-terms"
#					}
#				},
			"mdf_metadata" : khazana_vasp_mdf,
			"uri" : "http://khazana.uconn.edu",
			"keep_dir_name_depth" : 0,
			"root" : paths.datasets + "khazana/OUTCARS",
			"file_pattern" : "^OUTCAR",
			"file_format" : "vasp",
			"verbose" : True,
			"output_file" : paths.raw_feed + "khazana_vasp_all.json",		
			"data_exception_log" : paths.datasets + "khazana/khazana_vasp_errors.txt",
			"uri_adds" : ["filename", "ext"],
			"max_records" : -1,
			"archived" : False,
			"feedsack_size" : khaz_v_feedsack if feedsack else -1,
			"feedsack_file" : paths.sack_feed + "khazana_vasp_" + str(khaz_v_feedsack) + ".json",
			}
		if khazana_vasp_args["verbose"]:
			print("KHAZANA VASP PROCESSING")
		khaz_v = process_data(khazana_vasp_args)
		if khazana_vasp_args["verbose"]:
			print("DONE\n")
		'''
		if feedsack:
			
			print "Creating JSON files"
			print "Khazana VASP All"
			with open("khazana/khazana_vasp_all.json", 'w') as fk1:
				json.dump(khaz_v, fk1)
			
			if khazana_vasp_args["verbose"]:
				print "Making Khazana VASP feedsack (" + str(khaz_v_feedsack) + ")"
			with open(paths.sack_feed + "khazana_vasp_" + str(khaz_v_feedsack) + ".json", 'w') as fk2:
				dump(khaz_v[:khaz_v_feedsack], fk2)
			if khazana_vasp_args["verbose"]:
				print "Done\n"
		'''

	#Crystallography Open Database
	if "cod" in datasets_to_process:
		exit("COD not supported")
		cod_mdf = {
			"mdf_source_name" : "cod",
			"mdf_source_id" : 6,
#			"globus_source" : "Crystallography Open Database",
			"mdf_datatype" : "cif",
			"acl" : ["public"],
			"collection" : "Crystallography Open Database"
			}
		cod_args = {
			"mdf_metadata" : cod_mdf,
			"uri" : "http://www.crystallography.net/cod",
			"keep_dir_name_depth" : 0,
			#"root" : paths.datasets + "cod/open-cod",
			"file_pattern" : "\.cif$",
			"file_format" : "cif",
			"verbose" : True,
			#"output_file" : paths.raw_feed + "cod_all.json",
			#"data_exception_log" : paths.datasets + "cod/cod_errors.txt",
			"uri_adds" : ["filename", ".html"],
			"max_records" : -1,
			"archived" : False,
			"feedsack_size" : cod_feedsack if feedsack else -1,
			"feedsack_file" : paths.sack_feed + "cod_" + str(cod_feedsack) + ".json"
			}
		if cod_args["verbose"]:
			print("COD PROCESSING")
		for i in range(9):
			j = i + 1
			cod_args["root"] = paths.datasets + "cod/open-cod/cif/" + str(j)
			cod_args["output_file"] = paths.raw_feed + "cod_" + str(j) + "_all.json"
			if i > 0:
				cod_args["feedsack_size"] = -1 #No multiple feedsacks
			cod = process_data(cod_args)
			if cod_args["verbose"]:
				print("Processed directory", j, '\n')
		if cod_args["verbose"]:
			print("DONE\n")
		'''
		if feedsack:
			if cod_args["verbose"]:
				print "Making COD feedsack (" + str(cod_feedsack) + ")"
			with open(paths.sack_feed + "cod_" + str(cod_feedsack) + ".json", 'w') as fc:
				dump(cod[:cod_feedsack], fc)
			if cod_args["verbose"]:
				print "Done\n"
		'''

	#Sluschi
	if "sluschi" in datasets_to_process:
		sluschi_mdf = {
			"mdf_source_name" : "sluschi",
			"mdf_source_id" : 7,
#			"globus_source" : "SLUSCHI",
			"mdf_datatype" : "vasp",
			"acl" : ["public"],
			"collection" : "SLUSCHI"
			}
		sluschi_args = {
			"mdf_metadata" : sluschi_mdf,
			"uri" : "globus:mostly_melted_snow",
			"keep_dir_name_depth" : -1,
			"root" : paths.datasets + "sluschi/sluschi",
			"file_pattern" : "^OUTCAR$",
			"file_format" : "vasp",
			"verbose" : True,
			"output_file" : paths.raw_feed + "sluschi_all.json",
			"data_exception_log" : paths.datasets + "sluschi/sluschi_errors.txt",
			"uri_adds" : ["dir"],
			"max_records" : -1,
			"archived" : False, #Should be True for first run, afterwards extracted data should be fine
			"feedsack_size" : sluschi_feedsack if feedsack else -1,
			"feedsack_file" : paths.sack_feed + "sluschi_" + str(sluschi_feedsack) + ".json"
			}
		if sluschi_args["verbose"]:
			print("SLUSCHI PROCESSING")
		sluschi = process_data(sluschi_args)
		if sluschi_args["verbose"]:
			print("DONE\n")
		'''
		if feedsack:
			if sluschi_args["verbose"]:
				print "Making sluschi feedsack (" + str(sluschi_feedsack) + ")"
			with open(paths.sack_feed + "sluschi_" + str(sluschi_feedsack) + ".json", 'w') as fc:
				dump(sluschi[:sluschi_feedsack], fc)
			if sluschi_args["verbose"]:
				print "Done\n"
		'''

	
	print("END")

