import globus_auth

verbose = True
ingest_limit = 1000
datacite_namespace = "https://schema.labs.datacite.org/meta/kernel-4.0/metadata.xsd"
dc_namespace = "http://dublincore.org/documents/dcmi-terms"


#Formats a single gmeta entry from the data dictionary
#With full=false, the returned entry can be added to a gmeta list, but cannot be ingested directly
#With full=true, the returned entry is ready for ingestion, but not inclusion in a gmeta list
def format_single_gmeta(data_dict, full=False):
	if not data_dict.get("context", {}).get("datacite", None):
		data_dict["context"]["datacite"] = datacite_namespace
	if not data_dict.get("context", {}).get("dc", None):
		data_dict["context"]["dc"] = dc_namespace
	if not data_dict.get("globus_subject", None):
		data_dict["globus_subject"] = ""
	if not data_dict.get("globus_id", None):
		data_dict["globus_id"] = ""
	if data_dict.get("data_context", None):
		for elem in data_dict.get("data", []):
			elem = data_dict["data_context"] + elem
	gmeta = {
		"@datatype":"GMetaEntry",
		"@version":"2016-11-09",
		"subject":data_dict["globus_subject"],
		"visible_to":["public"],
		"id":data_dict["globus_id"],
		"content":data_dict.get("data", None)
		}
	gmeta["content"]["@context"] = data_dict["context"]

	if full:
		if not data_dict.get("globus_source"):
			data_dict["globus_source"] = ""
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
def format_multi_gmeta(data_list, globus_source=""):
	gmeta = {
		"@datatype":"GIngest",
		"@version":"2016-11-09",
		"ingest_type":"GMetaList",
		"source_id":globus_source,
		"ingest_data":{
			"@datatype":"GMetaList",
			"@version":"2016-11-09",
			"gmeta":data_list
			}
		}
	return gmeta

#
def ingest_pickle(pickle_filename):
	#Authentication
	if verbose:
		print "Authenticating"
	client = globus_auth.login("https://datasearch.api.demo.globus.org/")
	#Record preparation
	if verbose:
		print "Processing pickle"
	list_of_data = pickle_to_dict_list(pickle_filename)
	if len(list_of_data) == 0:
		print "Error: No data found"
		exit(-1)
	elif len(list_of_data) == 1:
		single_ingestable = format_single_gmeta(list_of_data[0], full=True)
		client.ingest(single_ingestable)
		if verbose:
			print "Ingested one record"
	elif len(list_of_data > 1:
		list_ingestable = []
		count = 0
		for record in list_of_data:
			single_ingestable = format_single_gmeta(record, full=False)
			list_ingestable.append(single_ingestable)
			count += 1
			if count >= ingest_limit:
				break
		multi_ingestable = format_multi_gmeta(list_ingestable)
		client.ingest(multi_ingestable)
		if verbose:
			print "Ingested " + str(count) + " records"
	if verbose:
		print "Success"




