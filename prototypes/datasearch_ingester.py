import globus_auth
from pickle import load
from sys import exit

max_ingests_at_once = 5000
max_ingests_total = 5
#Pick exactly one to uncomment
data_file = "oqmd_json.pickle"
#data_file = "janaf_json.pickle"

#datacite_namespace = "https://schema.labs.datacite.org/meta/kernel-4.0/metadata.xsd"
#dc_namespace = "http://dublincore.org/documents/dcmi-terms"


#Formats a single gmeta entry from the data dictionary
#With full=false, the returned entry can be added to a gmeta list, but cannot be ingested directly
#With full=true, the returned entry is ready for ingestion, but not inclusion in a gmeta list
def format_single_gmeta(data_dict, full=False):
	if not data_dict.get("data", None):
		print "Error: No data in entry"
		return None
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
#	if data_dict.get("data_context", None):
#		for elem in data_dict.get("data", {}).keys():
#			elem = data_dict["data_context"] + ":" + elem
	gmeta = {
		"@datatype":"GMetaEntry",
		"@version":"2016-11-09",
		"subject":data_dict["globus_subject"],
		"visible_to":["public"],
		"id":data_dict["globus_id"],
		"source_id":data_dict["globus_source"],
		"content":data_dict["data"]
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

#Takes a pickled list of dicts and ingests into Globus Search
#Arguments:
#	pickle_filename: Name of pickle file containing json blob from converter
#	max_ingest_size: Maximum size of a single ingestion (-1 is unlimited)
#	ingest_limit: Maximum number of records to ingest overall (-1 is unlimited)
#	verbose: Show status messages?
def ingest_pickle(pickle_filename, max_ingest_size=-1,  ingest_limit=-1, verbose=False):
	#Authentication
	if verbose:
		print "Authenticating"
	client = globus_auth.login("https://datasearch.api.demo.globus.org/")
	#Record preparation
	if verbose:
		print "Opening pickle"
	try:
		pickle_file = open(pickle_filename)
	except IOError as e:
		print "Cannot open file '" + pickle_filename + "': " + e.strerror
		exit(-1)
	list_of_data = load(pickle_file)
	pickle_file.close()
	if verbose:
		print "Processing pickle"
	if type(list_of_data) is not list:
		print "Error: Cannot process " + str(type(list_of_data)) + ". Must be list."
		exit(-1)
	if len(list_of_data) == 0:
		print "Error: No data found"
		exit(-1)
	elif len(list_of_data) == 1:
		single_ingestable = format_single_gmeta(list_of_data[0], full=True)
		client.ingest(single_ingestable)
		if verbose:
			print "Ingested one record"
	elif len(list_of_data) > 1:
		list_ingestable = []
		count = 0
		for record in list_of_data:
			single_ingestable = format_single_gmeta(record, full=False)
			list_ingestable.append(single_ingestable)
			count += 1
			if ingest_limit > 0 and count >= ingest_limit:
				break
		if verbose:
			print "Size: " + str(len(list_ingestable))
		if max_ingest_size > 0:
			# list // max + 1 = number of iterations to ingest all data
			# But if list % max == 0, adding one gives one too many iterations
			num_rounds = len(list_ingestable) // max_ingest_size
			if len(list_ingestable) % max_ingest_size != 0:
				num_rounds += 1
			for i in range(num_rounds):
				multi_ingestable = format_multi_gmeta(list_ingestable[
					i * max_ingest_size : (i + 1) * max_ingest_size
					])
				client.ingest(multi_ingestable)
				if verbose:
					print "Ingested round " + str(i+1)
		else:
			multi_ingestable = format_multi_gmeta(list_ingestable)
			client.ingest(multi_ingestable)
		if verbose:
			print "Ingested " + str(count) + " records"
	if verbose:
		print "Success"

if __name__ == "__main__":
	filename = data_file
	ingest_limit = max_ingests_total
	max_ingest_size=max_ingests_at_once
	print "Using " + str(ingest_limit) + " records from " + filename + ":\n"
	ingest_pickle(filename, max_ingest_size=max_ingest_size, ingest_limit=ingest_limit, verbose=True)


