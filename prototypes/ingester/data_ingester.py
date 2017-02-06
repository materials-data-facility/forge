'''
Ingester
'''
#import globus_auth
#from pickle import load
from sys import exit
from json import loads, dumps
from tqdm import tqdm
#from pymongo import MongoClient

import paths #Contains relative path to data info

#-1 for unlimited
#max_ingests_at_once = 50 #Now per file
max_ingests_total = -1

#Note: All destinations listed here must have:
# 1. Function $NAME_client() that requires no arguments and returns whatever should be given to the ingest function as "client".
# 2. Function $NAME_ingest() that accepts one argument - a dict containing other arguments, including whatever $NAME_client() returned.
#	Arguments given to the function include (more can be added per destination):
#		ingestable: The ingestable data.
#		client: Whatever $NAME_client() returned.
#		verbose: Should the function print status output?
#	In the function:
#		The data must be filtered appropriately
#		The ingestable data must be ingested
#		Any arguments besides ingestable can be ignored if necessary
all_destinations = {"globus_search", "data_pub_service", "local_mongodb"}

ingest_to = set()
#Pick one or more destinations
#ingest_to.add("globus_search")
#ingest_to.add("data_pub_service")
ingest_to.add("local_mongodb")

all_data_files = {
	"oqmd" : {
		"file" : paths.ref_feed +  "oqmd_refined.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 5000
		},
	"janaf" : {
		"file" : paths.ref_feed + "janaf_refined.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 500
		},
	"danemorgan" : {
		"file" : paths.ref_feed + "danemorgan_refined.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 500
		},
	"khazana_polymer" : {
		"file" : paths.ref_feed + "khazana_polymer_refined.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 100
		},
	"khazana_vasp" : {
		"file" : paths.ref_feed +"khazana_vasp_refined.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 1
		},
	"cod" : {
		"file" : paths.ref_feed + "cod_refined.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 5000
		},
	"sluschi" : {
		"file" : paths.ref_feed + "sluschi_refined.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 100
		},
	"hopv" : {
		"file" : paths.ref_feed + "hopv_refined.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 100
		},
	"cip" : {
		"file" : paths.ref_feed + "cip_refined.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 100
		},
	"nanomine" : {
		"file" : paths.ref_feed + "nanomine_refined.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 100
		}
	}
#Pick one or more data files to ingest
data_file_to_use = []
#data_file_to_use.append("oqmd")
#data_file_to_use.append("janaf")
#data_file_to_use.append("danemorgan")
#data_file_to_use.append("khazana_polymer")
data_file_to_use.append("khazana_vasp")
#data_file_to_use.append("cod")
#data_file_to_use.append("sluschi")
#data_file_to_use.append("hopv")
#data_file_to_use.append("cip")
#data_file_to_use.append("nanomine")


#This setting uses the data file(s), but deletes the actual data before ingest. This causes the record to be "deleted."
DELETE_DATA = False

#datacite_namespace = "https://schema.labs.datacite.org/meta/kernel-4.0/metadata.xsd"
#dc_namespace = "http://dublincore.org/documents/dcmi-terms"


#Formats a single gmeta entry from the data dictionary
#With full=false, the returned entry can be added to a gmeta list, but cannot be ingested directly
#With full=true, the returned entry is ready for ingestion, but not inclusion in a gmeta list
def format_single_gmeta(data_dict, full=False):

	if not data_dict.get("context", None):
		data_dict["context"] = {}
#	if not data_dict.get("context", {}).get("datacite", None):
#		data_dict["context"]["datacite"] = datacite_namespace
#	if not data_dict.get("context", {}).get("dc", None):
#		data_dict["context"]["dc"] = dc_namespace
	if not data_dict.get("globus_subject", None):
		data_dict["globus_subject"] = ""
	if not data_dict.get("globus_id", None):
		data_dict["globus_id"] = ""
	if not data_dict.get("globus_source", None):
		data_dict["globus_source"] = ""
	if not data_dict.get("acl", None):
		data_dict["acl"] = ["public"]
#	if data_dict.get("data_context", None):
#		for elem in data_dict.get("data", {}).keys():
#			elem = data_dict["data_context"] + ":" + elem
	gmeta = {
		"@datatype":"GMetaEntry",
		"@version":"2016-11-09",
		"subject":data_dict["globus_subject"],
		"visible_to":data_dict["acl"],
		"id":data_dict["globus_id"],
		"source_id":data_dict["globus_source"],
		"content":data_dict.get("data", {})
		}
	gmeta["content"]["@context"] = data_dict["context"]

	if full:
		gmeta_full = {
			"@datatype":"GIngest",
			"@version":"2016-11-09",
			"ingest_type":"GMetaEntry",
			"source_id":data_dict["globus_source"],
			"ingest_data":gmeta
			}

	if not full:
		return gmeta
	else:
		return gmeta_full

#Formats a list of gmeta entries into a GMetaList, suitable for ingesting
def format_multi_gmeta(data_list):
	gmeta = {
		"@datatype":"GIngest",
		"@version":"2016-11-09",
		"ingest_type":"GMetaList",
		"source_id":data_list[0]["source_id"],
		"ingest_data":{
			"@datatype":"GMetaList",
			"@version":"2016-11-09",
			"gmeta":data_list
			}
		}
	return gmeta

#Globus Search client
def globus_search_client():
	import globus_auth
	return globus_auth.login("https://datasearch.api.demo.globus.org/")

#Ingests data into Globus Search
#Filters:
#	No data nested more than N layers (N = 2)
#	No lists with more than M elements (M = 5)
#	No multi-dimensional lists
#	No empty data structures
#	No nested booleans
def globus_search_ingest(args):
	MAX_LIST = 5
	#print "Test database ingest:"
	filtered_list = []
	data_list = args["ingestable"]["ingest_data"]["gmeta"]
	#All the data must be JSON serializable, and therefore must be a list, dict, or actual data (not in a container)
	for entry in tqdm(data_list, desc="\tFiltering batch", disable= True): #not args["verbose"]): #Current datasets filter too fast for a progress bar to be useful

		filtered_content = {}
		for key, value in entry["content"].items(): #Actual data starts here, first layer. **Assigning filtered_content[key] = value
			#if not hasattr(value, "__iter__") and value: #Not iterable, must be data (str (in Python 2), int, bool, etc.), and data is not nothing
			if value and type(value) is not dict and type(value) is not list: #JSON only supports list, dict, and things we consider data, so this checks if value is (not None) data
				filtered_content[key] = value
			
			elif type(value) is dict:
				first_level_dict = {}
				for key2, value2 in value.items(): #Second layer. **Assigning first_level_dict[key2] = value2
					#if not hasattr(value2, "__iter__") and value2: #Data, and not nothing
					if value2 and type(value2) is not dict and type(value2) is not list and type(value2) is not bool:
						first_level_dict[key2] = value2
	
				if first_level_dict: #If data exists here
					filtered_content[key] = first_level_dict
			
			elif type(value) is list and len(value) <= MAX_LIST:
				first_level_list = []
				for elem in value:
					#if not hasattr(elem, "__iter__") and elem: #Data, not nothing
					if elem and type(elem) is not dict and type(elem) is not list and type(value2) is not bool:
						first_level_list.append(elem)
				if first_level_list: #If data found
					filtered_content[key] = first_level_list
		if filtered_content: #If there's nothing left after filtering, should not ingest
			entry["content"] = filtered_content
			filtered_list.append(entry)
	args["ingestable"]["ingest_data"]["gmeta"] = filtered_list #Can't check this for no data or might break things
	#Actual ingestion
#	print str(args["client"].ingest(args["ingestable"]))
	args["client"].ingest(args["ingestable"])


def data_pub_service_client():
	return "TODO: DPS client"

def data_pub_service_ingest(args):
	print("This would be an ingestion to the DPS")


#Client for local mongodb
def local_mongodb_client():
	from pymongo import MongoClient
	from gridfs import GridFS
	db = MongoClient()["mdf"]
	fs = GridFS(db)
	clients = {
		"db" : db,
		"fs" : fs
		}
	return clients

#Ingest to local mongodb instance
def local_mongodb_ingest(args):
	MAX_LIST = 5
	db = args["client"]["db"]
#	col = db["feedstock"]
	fs = args["client"]["fs"]
	data_list = args["ingestable"]["ingest_data"]["gmeta"]
#	exec("db." + data_list[0]["source_name"].lower() + ".insert_many(data_list)")
#	db.col.insert_many(data_list)
	filtered_list = []
#	total_count = 0
	for entry in tqdm(data_list, desc="\tFiltering batch", disable= True): #not args["verbose"]): #Current datasets filter too fast for a progress bar to be useful

#		total_count += 1
#		print(entry)
		grid_fs_id = fs.put(dumps(entry), encoding="utf-8")

		filtered_content = {"grid_fs_id" : grid_fs_id}
		for key, value in entry["content"].items(): #Actual data starts here, first layer. **Assigning filtered_content[key] = value
			#if not hasattr(value, "__iter__") and value: #Not iterable, must be data (str (in Python 2), int, bool, etc.), and data is not nothing
			if value and type(value) is not dict and type(value) is not list: #JSON only supports list, dict, and things we consider data, so this checks if value is (not None) data
				filtered_content[key] = value
			
			elif type(value) is dict:
				first_level_dict = {}
				for key2, value2 in value.items(): #Second layer. **Assigning first_level_dict[key2] = value2
					#if not hasattr(value2, "__iter__") and value2: #Data, and not nothing
					if value2 and type(value2) is not dict and type(value2) is not list:
						first_level_dict[key2] = value2
	
				if first_level_dict: #If data exists here
					filtered_content[key] = first_level_dict
			
			elif type(value) is list and len(value) <= MAX_LIST:
				first_level_list = []
				for elem in value:
					#if not hasattr(elem, "__iter__") and elem: #Data, not nothing
					if elem and type(elem) is not dict and type(elem) is not list:
						first_level_list.append(elem)
				if first_level_list: #If data found
					filtered_content[key] = first_level_list
		entry["content"] = filtered_content
		filtered_list.append(entry)
#	print("Total:", total_count, "\nList:", len(filtered_list))
	db.feedstock.insert_many(filtered_list)


'''
######################################
#Test ingester. Not to be used for actual ingesting.
def db_test_ingest(ingestable, client):
	from copy import deepcopy
	from json import dumps
	test_i = deepcopy(ingestable)
	MAX_LIST = 5
	#print "Test database ingest:"
	filtered_list = []
	data_list = ingestable["ingest_data"]["gmeta"]
	#All the data must be JSON serializable, and therefore must be a list, dict, or actual data (not in a container)
	for entry in data_list:

		filtered_content = {}
		for key, value in entry["content"].iteritems(): #Actual data starts here, first layer. **Assigning filtered_content[key] = value
			if not hasattr(value, "__iter__") and value: #Not iterable, must be data (str (in Python 2), int, bool, etc.), and data is not nothing
				filtered_content[key] = value
			
			if type(value) is dict:
				first_level_dict = {}
				for key2, value2 in value.iteritems(): #Second layer. **Assigning first_level_dict[key2] = value2
					if not hasattr(value2, "__iter__") and value2: #Data, and not nothing
						first_level_dict[key2] = value2
	
				if first_level_dict: #If data exists here
					filtered_content[key] = first_level_dict
			
			elif type(value) is list and len(value) <= MAX_LIST:
				first_level_list = []
				for elem in value:
					if not hasattr(elem, "__iter__") and elem: #Data, not nothing
						first_level_list.append(elem)
				if first_level_list: #If data found
					filtered_content[key] = first_level_list
		if filtered_content: #If there's nothing left after filtering, should not ingest
			entry["content"] = filtered_content
			filtered_list.append(entry)
	ingestable["ingest_data"]["gmeta"] = filtered_list #Can't check this for no data or might break things

	print "BEFORE:"
	print test_i["ingest_data"]["gmeta"][2]["content"]
	print "\nAFTER:"
	print ingestable["ingest_data"]["gmeta"][2]["content"]
'''
			


#Takes a JSON list of dicts and ingests them into specified destinations
#Arguments:
#	json_filename: Name of json file containing json from converter
#	destinations: Set of locations to ingest to. The list of available destinations is in the global variable "all_destinations". Should never have repeated elements, or ingests may be duplicated.
#	max_ingest_size: Maximum size of a single ingestion. Default -1 (unlimited)
#	ingest_limit: Maximum number of records to ingest overall. Default -1 (unlimited)
#	verbose: Show status messages? Default False.
#	delete_not_ingest: Caution! If True, will overwrite existing data instead of ingesting new data. Default False.
def ingest_refined_feedstock(json_filename, destinations, max_ingest_size=-1,  ingest_limit=-1, verbose=False, delete_not_ingest=False):
	if type(destinations) is not set:
		destinations = set(destinations)
	if not destinations.issubset(all_destinations):
		print("Error: Unknown destinations given\nValid destinations are: " + str(all_destinations) + "\nThe provided destinations were: " + str(destinations))
	dest_args = {}
	for dest in destinations:
		#exec(dest + "_args = {}")
		#exec(dest + "_args['verbose'] = verbose")
		dest_args[dest] = {
			"client" : eval(dest + "_client()"),
			"verbose" : verbose
			}
	'''
	if "globus_search" in destinations:
		globus_search_args["client"] =  globus_auth.login("https://datasearch.api.demo.globus.org/")
	if "data_pub_service" in destinations:
		data_pub_service_args["client"] = "Need client here" #TODO: DPS client
	if "local_mongodb" in destinations:
		local_mongodb_args["client"] = MongoClient()
	'''
	if delete_not_ingest:
		confirm = input("Delete entries y/n: ")
		if confirm.lower() not in ['y', 'yes']:
			print("Cancelling...")
			return None
		else:
			if verbose:
				print("Deleting entries")
	#Record preparation
#	if verbose:
#		print "Opening json"
#	try:
#		json_file = open(json_filename)
#	except IOError as e:
#		print "Cannot open file '" + json_filename + "': " + e.strerror
#		exit(-1)
#	list_of_data = load(json_file)
#	json_file.close()
	if verbose:
		print("Processing json")
#	if type(list_of_data) is not list:
#		print "Error: Cannot process " + str(type(list_of_data)) + ". Must be list."
#		exit(-1)
#	if len(list_of_data) == 0:
#		print "Error: No data found"
#		exit(-1)
		'''
	#Honestly not really useful
	elif len(list_of_data) == 1:
		if delete_not_ingest:
			delete_data = list_of_data[0]
			delete_data.pop("data", None)
			single_ingestable = format_single_gmeta(delete_data, full=True)
		else:
			single_ingestable = format_single_gmeta(list_of_data[0], full=True)
		for dest in destinations:
			eval(dest + "_ingest(single_ingestable, " + dest + "_client)")
		if verbose:
			print "Ingested one record"
		'''
	#elif len(list_of_data) >= 1:
	list_ingestable = []
	total_count = 0
	num_batches = 0
	ingest_file = open(json_filename, 'r')
	if verbose:
		print("Processing records from", json_filename)
	for in_record in tqdm(ingest_file, desc="Ingesting records", disable= not verbose):
		record = loads(in_record)
#		for record in tqdm(list_of_data, desc="Preparing records", disable= not verbose):
		if delete_not_ingest:
			delete_data = record
			delete_data.pop("data", None)
			single_ingestable = format_single_gmeta(delete_data, full=False)
		else:
			single_ingestable = format_single_gmeta(record, full=False)
		list_ingestable.append(single_ingestable)
		total_count += 1

		if (len(list_ingestable) >= max_ingest_size) or (ingest_limit > 0 and total_count >= ingest_limit and len(list_ingestable) > 0): #If batch is full, OR if total ingest limit is reached, need to ingest batch
			multi_ingestable = format_multi_gmeta(list_ingestable)
			for dest in destinations:
				dest_args[dest]["ingestable"] = multi_ingestable
				exec(dest + "_ingest(dest_args[dest])")
			num_batches += 1
			list_ingestable.clear()
			if ingest_limit > 0 and total_count >= ingest_limit: #Additionally, if ingest limit is reached, after ingesting the last batch, stop ingesting
				break

	if list_ingestable: #If data remains uningested (incomplete batch when EOF)
		multi_ingestable = format_multi_gmeta(list_ingestable)
		for dest in destinations:
			dest_args[dest]["ingestable"] = multi_ingestable
			exec(dest + "_ingest(dest_args[dest])")
			num_batches += 1


		'''
		if verbose:
			print("Size: " + str(len(list_ingestable)))
		if max_ingest_size > 0:
			# list // max + 1 = number of iterations to ingest all data
			# But if list % max == 0, adding one gives one too many iterations
			num_rounds = len(list_ingestable) // max_ingest_size
			if len(list_ingestable) % max_ingest_size != 0:
				num_rounds += 1
			for i in tqdm(range(num_rounds), desc="Ingesting data in batches of " + str(max_ingest_size), disable= not verbose):
				multi_ingestable = format_multi_gmeta(list_ingestable[
					i * max_ingest_size : (i + 1) * max_ingest_size
					])
				for dest in destinations:
					exec(dest + "_args['ingestable'] = multi_ingestable")
					exec(dest + "_ingest(" + dest + "_args)")
#				if verbose:
#					print "Ingested batch " + str(i+1)
		else:
			multi_ingestable = format_multi_gmeta(list_ingestable)
			for dest in destinations:
				exec(dest + "_args['ingestable'] = multi_ingestable")
				exec(dest + "_ingest(" + dest + "_args)")
		'''
	if verbose:
		print("Ingested " + str(total_count) + " records in " + str(num_batches) + " batches")
#	if verbose:
#		print("Success")

if __name__ == "__main__":
	print("Ingest start")

###############################
#	ingest_refined_feedstock(paths.ref_feed + "cip_refined.json", ["data_pub_service"], verbose=True)

	for key in data_file_to_use:
		filename = all_data_files[key]["file"]
		ingest_limit = all_data_files[key]["record_limit"]
		max_ingest_size = all_data_files[key]["batch_size"]
		print("Using " + str(ingest_limit) + " records from " + filename + " in batches of " + str(max_ingest_size) + ":")
		ingest_refined_feedstock(filename, ingest_to, max_ingest_size=max_ingest_size, ingest_limit=ingest_limit, verbose=True, delete_not_ingest=DELETE_DATA)
		print("Finished ingesting from " + filename + "\n")
	print("Ingest complete")


