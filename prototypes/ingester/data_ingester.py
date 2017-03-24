'''
Ingester
'''
#import globus_auth
#from pickle import load
from sys import exit
from json import loads, dumps, dump
from tqdm import tqdm
from copy import deepcopy
#from pymongo import MongoClient
#from bson import ObjectId

import paths #Contains relative path to data info

#-1 for unlimited
#max_ingests_at_once = 50 #Now per file
max_ingests_total = -1
std_list_lim = 5
std_nest_lim = 4

#Keys reserved for Globus Search
globus_keys = ["@context", "@datatype", "@version", "subject", "visible_to", "include", "content", "id", "source_id", "mimetype", "gmeta", "ingest_data", "ingest_type"]
#Keys that Searchify should not process anything nested in
globus_stop_keys = ["@context"]
#Default URL to prepend to fields that do not already have a URL
default_key = "http://globus.org"
default_sep = "#"
default_uri = "http://globus.org/"
#globus_keys.append(default_key)
replace_chars = { #Dict of characters to replace and what to replace them with
	"@" : "-"
	}
replace_namespaces = {
	"dc." : "http://datacite.org/schema/kernel-3#",
	"mdf-base." : "http://globus.org/publication-schemas/mdf-base/0.1#",
	"mdf-publish." : "http://globus.org/publish-terms/#"
	}


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
all_destinations = {"globus_search", "data_pub_service", "local_mongodb", "local_elasticsearch"}

ingest_to = set()
#Pick one or more destinations
ingest_to.add("globus_search")
#ingest_to.add("data_pub_service")
#ingest_to.add("local_mongodb")
#ingest_to.add("local_elasticsearch")


#Pick one or more data files to ingest
data_file_to_use = []
#data_file_to_use.append("oqmd")
#data_file_to_use.append("janaf")
#data_file_to_use.append("danemorgan")
#data_file_to_use.append("khazana_polymer")
#data_file_to_use.append("khazana_vasp")
#data_file_to_use.append("cod")
#data_file_to_use.append("sluschi")
#data_file_to_use.append("hopv")
data_file_to_use.append("cip")
data_file_to_use.append("nanomine")
data_file_to_use.append("nist_ip")
#data_file_to_use.append("nist_dspace")
data_file_to_use.append("metadata_matin")
data_file_to_use.append("metadata_cxidb")
data_file_to_use.append("metadata_nist")
data_file_to_use.append("pppdb")
data_file_to_use.append("metadata_materials_commons")

data_file_to_use.append("temp_sluschi")

#Information about each dataset for ingesting
all_data_files = {
	"oqmd" : {
		"file" : paths.ref_feed +  "oqmd_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 5000,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}
		},
	"janaf" : {
		"file" : paths.ref_feed + "janaf_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 500,
		"globus_search" : {
			"list_limit" : -1,
			"nest_limit" : std_nest_lim
			}

		},
	"danemorgan" : {
		"file" : paths.ref_feed + "danemorgan_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 10,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}

		},
	"khazana_polymer" : {
		"file" : paths.ref_feed + "khazana_polymer_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 100,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}

		},
	"khazana_vasp" : {
		"file" : paths.ref_feed +"khazana_vasp_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 1,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}

		},
	"cod" : {
		"file" : paths.ref_feed + "cod_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 5000,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}

		},
	"sluschi" : {
		"file" : paths.ref_feed + "sluschi_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 1,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}

		},
	"hopv" : {
		"file" : paths.ref_feed + "hopv_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 100,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}

		},
	"cip" : {
		"file" : paths.ref_feed + "cip_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 100,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}

		},
	"nanomine" : {
		"file" : paths.ref_feed + "nanomine_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 100,
		"globus_search" : {
			"list_limit" : -1,
			"nest_limit" : -1
			}

		},
	"nist_ip" : {
		"file" : paths.ref_feed + "nist_ip_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 100,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}
		},
	"nist_dspace" : {
		"file" : "",
		"record_limit" : max_ingests_total,
		"batch_size" : 10,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}
		},
	"metadata_matin" : {
		"file" : paths.ref_feed + "matin_metadata_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 100,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}
		},
	"metadata_cxidb" : {
		"file" : paths.ref_feed + "cxidb_metadata_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 100,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}
		},
	"metadata_nist" : {
		"file" : paths.ref_feed + "nist_metadata_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 100,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}
		},
	"pppdb" : {
		"file" : paths.ref_feed + "pppdb_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 100,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}
		},
	"metadata_materials_commons" : {
		"file" : paths.ref_feed + "materials_commons_metadata_all.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 1,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}
		},
	"temp_sluschi":{
		"file":paths.ref_feed+"temp_sluschi.json",
		"record_limit" : max_ingests_total,
		"batch_size" : 1,
		"globus_search" : {
			"list_limit" : std_list_lim,
			"nest_limit" : std_nest_lim
			}

		}
	}


#Default domain (index) for Globus Search
globus_domain = "globus_search"
#globus_domain = "mdf"

#For debugging, to print the ES ingest to a file, put the filename here. Should be None/False/etc. is not used.
#print_ES_to_file = "ingest_doc.json"
print_ES_to_file = None

#This setting uses the data file(s), but deletes the actual data before ingest. This causes the record to be "deleted."
#Only applies to globus_search
DELETE_GS = False

#This setting deletes Elasticsearch data before re-ingesting
#Only applies to local_elasticsearch
DELETE_ES = False

#datacite_namespace = "https://schema.labs.datacite.org/meta/kernel-4.0/metadata.xsd"
#dc_namespace = "http://dublincore.org/documents/dcmi-terms"


#Formats a single gmeta entry from the data dictionary
#With full=false, the returned entry can be added to a gmeta list, but cannot be ingested directly
#With full=true, the returned entry is ready for ingestion, but not inclusion in a gmeta list
def format_single_gmeta(data_dict, full=False):
	#Default values
	if type(data_dict.get("acl", None)) is not list:
		data_dict["acl"] = [data_dict.get("acl", "public")]

#	if not data_dict.get("context", None):
#		data_dict["context"] = {}
#	if not data_dict.get("context", {}).get("datacite", None):
#		data_dict["context"]["datacite"] = datacite_namespace
#	if not data_dict.get("context", {}).get("dc", None):
#		data_dict["context"]["dc"] = dc_namespace
#	if not data_dict.get("globus_subject", None): #Moved
#		data_dict["globus_subject"] = ""
#	if not data_dict.get("globus_id", None):
#		data_dict["globus_id"] = ""
#	if not data_dict.get("globus_source", None):
#		data_dict["globus_source"] = ""

#	if not data_dict.get("__source_name", None):
#		data_dict["__source_name"] = "Source not found"
#	if not data_dict.get("globus_data", None):
#		content = {}

#	content = data_dict["globus_data"]
#	content["mdf_source_name"] = data_dict["mdf_source_name"]
#	content["mdf_source_id"] = data_dict["mdf_source_id"]
#	content["mdf-publish.publication.community"] = "Materials Data Facility"
#	content["mdf_id"] = ObjectId(content["mdf_id"])

#	if data_dict.get("data_context", None):
#		for elem in data_dict.get("data", {}).keys():
#			elem = data_dict["data_context"] + ":" + elem


	data_dict["mdf-publish.publication.community"] = "Materials Data Facility" #Community for filtering

	gmeta = {
		"@datatype" : "GMetaEntry",
		"@version" : "2016-11-09",
		"subject" : data_dict.pop("globus_subject", ""),
		"visible_to" : data_dict.pop("acl"),
		"id" : data_dict.pop("globus_id", ""),
#		"source_id" : data_dict["globus_source"],#Deprecated
		"content" : data_dict
		}
	gmeta["content"]["@context"] = data_dict.pop("context", {})

	if full:
		gmeta_full = {
			"@datatype" : "GIngest",
			"@version" : "2016-11-09",
			"ingest_type" : "GMetaEntry",
#			"source_id" : data_dict["globus_source"],
			"ingest_data" : gmeta
			}

	if not full:
		return gmeta
	else:
		return gmeta_full

#Formats a list of gmeta entries into a GMetaList, suitable for ingesting
def format_multi_gmeta(data_list):
	gmeta = {
		"@datatype" : "GIngest",
		"@version" : "2016-11-09",
		"ingest_type" : "GMetaList",
#		"source_id" : data_list[0]["source_id"],
		"ingest_data" : {
			"@datatype" : "GMetaList",
			"@version" : "2016-11-09",
			"gmeta" : data_list
			}
		}
	return gmeta

#Globus Search client
def globus_search_client():
	import globus_auth
	return globus_auth.login("https://datasearch.api.demo.globus.org/", globus_domain)

#Filter for Globus Search
#Arguments:
#	data: The data to be filtered and returned clean.
#	max_list: Maximum size of a valid list. Default -1 for no max.
#	max_depth: Maximum depth of deepest nested data. Data at max depth will be returned, non-data will be discarded. Default -1 for no max.
#	depth: Used in recursive calls to see how deep in the function is. Don't touch this.
def globus_search_filter(data, max_list=-1, max_depth=-1, depth=0):
	if type(data) is not dict and type(data) is not list and (type(data) is not bool or depth == 0): #JSON only supports list, dict, and things we consider data (except nested booleans), so this checks if value is data
		return data

	elif max_depth >= 0 and depth >= max_depth: #If max_depth is set and has been exceeded, stop processing
		return None

	elif type(data) is list and (len(data) <= max_list or max_list < 0):
		new_list = []
		for elem in data:
			new_elem = globus_search_filter(elem, max_list, max_depth, depth+1)
			if new_elem is not None and type(new_elem) is not list: #No multi-dim lists
				new_list.append(new_elem)
		return new_list if new_list else None

	elif type(data) is dict:
		new_dict = {}
		for key, value in data.items():
			if key in globus_keys:
				#new_key = "__" + key
				new_key = None #Remove keys that conflict
			else:
				new_key = key
			if new_key:
				for char, replacement in replace_chars.items():
					new_key = new_key.replace(char, replacement) 
			new_value = globus_search_filter(value, max_list, max_depth, depth+1)
			if new_key and new_value is not None:
				new_dict[new_key] = new_value
		return new_dict if new_dict else None
	
	else: #Something unexpected! Remove it.
		return None

#Adds a URL to the start of every field because that's what Search requires
def searchify(data):
	if type(data) is not list and type(data) is not dict:
		return data
	elif type(data) is list:
		new_list = []
		for elem in data:
			new_list.append(searchify(elem))
		return new_list
	elif type(data) is dict:
		new_dict = {}
		for key, value in data.items():
			if key in globus_keys or default_sep in key: #If field is a special Search field or already namespaced, don't add a namespace
				if key in globus_stop_keys: #If field is a special Search field and the value should not be processed
					new_dict[key] = value
				else:
					new_dict[key] = searchify(value)
			else: #Add a namespace
				if key.startswith(tuple(replace_namespaces.keys())): #Key has special namespace to expand
					for token in replace_namespaces.keys():
						if key.startswith(token):
							#Key must be .replace()'d first, to not replace periods in the URL
							#But this changes the match on the token (potentially), so the token to replace should be .replace()'d
							new_key = key.replace(".", "/").replace(token.replace(".", "/"), replace_namespaces[token])
							break #Found namespace, no longer need to check
				else: #Key does not
					new_key = default_key + default_sep + key

				new_dict[new_key] = searchify(value)
		return new_dict
	else:
		exit("ERROR: Unidentified data")


#Ingests data into Globus Search
#Filters:
#	No data nested more than N layers
#	No lists with more than M elements
#	No multi-dimensional lists
#	No empty data structures
#	No nested booleans
def globus_search_ingest(args):
	max_list = args.get("list_limit", -1)
	max_nest = args.get("nest_limit", -1)

	#print "Test database ingest:"
	filtered_list = []
	data_list = args["ingestable"]["ingest_data"]["gmeta"]
#	print(data_list)
	#All the data must be JSON serializable, and therefore must be a list, dict, or actual data (not in a container)
	for entry in tqdm(data_list, desc="\tFiltering batch", disable= True): #not args["verbose"]): #Current datasets filter too fast for a progress bar to be useful

		filtered_content = globus_search_filter(entry["content"])
		#for key, value in entry["content"].items(): #Actual data starts here, first layer. **Assigning filtered_content[key] = value
		#	filtered_content[key] = globus_search_filter(value, max_list, max_nest)
		'''
			#Moved to globus_search_filter
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
		'''
		if filtered_content: #If there's nothing left after filtering, should not ingest)
#			if not filtered_content.get("@context", None):
#				filtered_content["@context"] = {}
#			filtered_content["@context"][default_key] = default_uri #@context functionality implemented in Searchify
			entry["content"] = filtered_content
			filtered_list.append(entry)
	args["ingestable"]["ingest_data"]["gmeta"] = filtered_list #Can't check this for no data or might break things
	#Actual ingestion

#	temp_data = dumps(args["ingestable"])
#	print(ingest_data)
#	args["client"].ingest(args["ingestable"])

#	with open("nanomine_ingest.gmeta", 'w') as gout:
#		gout.write(str(ingest_data))
#		gout.write('\n')

#	ingest_data = loads(dumps(args["ingestable"]))

	ingest_data = loads(dumps(searchify(args["ingestable"])))
#	print("\nDATA:", dumps(ingest_data, sort_keys=True, indent=4, separators=(',', ': ')))
	
	#For debugging
	if print_ES_to_file and type(print_ES_to_file) is str:
		with open(print_ES_to_file, 'w') as search_file:
			dump(ingest_data, search_file)

	res = args["client"].ingest(ingest_data)
#	exit("Done")
	if args["verbose"]:
		if not res["success"]:
			print('\t', res)


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

#Recursive filter, removes undesirable elements
def recursive_filter(data):
	if type(data) is list:
		new_data = []
		for elem in data:
			if elem != elem:
				elem = None
			new_data.append(recursive_filter(elem))
	elif type(data) is dict:
		new_data = {}
		for key, value in data.items():
			if value != value:
				value = None
			new_data[key] = recursive_filter(value)
	else:
		new_data = data
	return new_data


#No NaNs - convert to null
def local_elasticsearch_filter(data):
	return recursive_filter(data)


def local_elasticsearch_client():
	from elasticsearch import Elasticsearch
	from elasticsearch.exceptions import RequestError
#	from elasticsearch_dsl import Index
	client = Elasticsearch(timeout=600)
#	Index("mdf", using=client).create()
	try:
		client.indices.create(index="mdf")
	except RequestError: #Index exists
		if DELETE_ES:
			client.indices.delete(index="mdf")
			client.indices.create(index="mdf")
	return client



def local_elasticsearch_ingest(args):
	from elasticsearch import helpers
#	from time import sleep
	if args["ingestable"]["ingest_type"] == "GMetaList":
#		for entry in args["ingestable"]["ingest_data"]["gmeta"]:
#			print(args["client"].create(index="mdf", body=entry))
		ingest = [{
		"_index": "mdf",
		"_type" : "record",
		 "_id"   : entry['content']['mdf_id'],
		"_source": entry,
		} for entry in args["ingestable"]["ingest_data"]["gmeta"]]
#		sleep(0.01)
		clean_ingest = local_elasticsearch_filter(ingest)
		try:
			res = helpers.bulk(args['client'], clean_ingest)
		except Exception as e:
			print("ERROR:", repr(e))
		if args["verbose"] and False: #This is spammy
			print(res)
	else:
		res = args["client"].create(index="mdf", body=args["ingestable"]["ingest_data"]["gmeta"])
		if args["verbose"]:
			print(res)




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
def ingest_refined_feedstock(json_filename, destinations, destination_args={}, max_ingest_size=-1,  ingest_limit=-1, verbose=False, delete_not_ingest=False):
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
		addl_args = destination_args.get(dest, {})
		for key, value in addl_args.items():
			dest_args[dest][key] = value

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
			delete_data.pop("globus_data", None)
			single_ingestable = format_single_gmeta(delete_data, full=False)
		else:
			single_ingestable = format_single_gmeta(record, full=False)
		list_ingestable.append(single_ingestable)
		total_count += 1

		if (len(list_ingestable) >= max_ingest_size) or (ingest_limit > 0 and total_count >= ingest_limit and len(list_ingestable) > 0): #If batch is full, OR if total ingest limit is reached, need to ingest batch
			multi_ingestable = format_multi_gmeta(list_ingestable)
			for dest in destinations:
				dest_args[dest]["ingestable"] = deepcopy(multi_ingestable)
				exec(dest + "_ingest(dest_args[dest])")
			num_batches += 1
			list_ingestable.clear()
			if ingest_limit > 0 and total_count >= ingest_limit: #Additionally, if ingest limit is reached, after ingesting the last batch, stop ingesting
				break

	if list_ingestable: #If data remains uningested (incomplete batch when EOF)
		multi_ingestable = format_multi_gmeta(list_ingestable)
		for dest in destinations:
			dest_args[dest]["ingestable"] = deepcopy(multi_ingestable)
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
	print("Ingesting to", ingest_to)

###############################
#	ingest_refined_feedstock("test.json", ["globus_search"], verbose=True)

	for key in data_file_to_use:
		filename = all_data_files[key]["file"]
		ingest_limit = all_data_files[key]["record_limit"]
		max_ingest_size = all_data_files[key]["batch_size"]
		destination_args = {}
		for dest in ingest_to:
			destination_args[dest] = all_data_files[key].get(dest, {})
		if filename:
			print("Using " + str(ingest_limit) + " records from " + filename + " in batches of " + str(max_ingest_size) + ":")
			ingest_refined_feedstock(filename, ingest_to, destination_args=destination_args, max_ingest_size=max_ingest_size, ingest_limit=ingest_limit, verbose=True, delete_not_ingest=DELETE_GS)
			print("Finished ingesting from " + filename + "\n")
		elif key == "nist_dspace":
			from utils import find_files
			for ref_file in find_files(root=paths.ref_feed, file_pattern="^nist_dspace"):
				ref_path = paths.ref_feed + ref_file["filename"] + ref_file["extension"]
				print("Using " + str(ingest_limit) + " records from " + ref_path + " in batches of " + str(max_ingest_size) + ":")
				ingest_refined_feedstock(ref_path, ingest_to, destination_args=destination_args, max_ingest_size=max_ingest_size, ingest_limit=ingest_limit, verbose=True, delete_not_ingest=DELETE_GS)
				print("Finished ingesting from " + ref_path + "\n")
		else:
			print("Invalid special dataset")
	print("Ingest complete")


