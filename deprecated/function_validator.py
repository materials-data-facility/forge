from json import dump
import paths
from bson import ObjectId
import os

#Function to validate metadata fields
def validate_metadata(metadata):
	if metadata.get("mdf_source_name", None): #placeholder for valid metadata check
		return True
	else:
		return False

#Function to validate record fields
def validate_record(record):
	if record.get("mdf_source_name", None): #placeholder
		return True
	else:
		return False

#User-facing function to output metadata
def write_metadata(metadata):
	if validate_metadata(metadata) == False:
		return {"success" : False, "message" : "Error: Bad metadata"}

	#Open feedstock file for the first time
	feedstock_path = paths.feedstock + metadata["mdf_source_name"] + "_all.json"
	metadata["mdf_id"] = str(ObjectId())
	try:
		with open(feedstock_path, 'w') as feedstock:
			dump(metadata, feedstock)
			feedstock.write("\n")
		return {"success" : True}
	except:
		return {"success" : False, "message" : "Error: Bad metadata"}

#User-facing function to output record
def write_record(record):
	if validate_record(record) == False:
		return {"success" : False, "message" : "Error: Bad record"}
	
	#Open feedstock file that should already have metadata
	feedstock_path = paths.feedstock + record["mdf_source_name"] + "_all.json"
	record["mdf_id"] = str(ObjectId())
	if not os.path.isfile(feedstock_path):
		return {"success" : False, "message" : "Feedstock file not found. Must write metadata first."}
	try:
		with open(feedstock_path, 'a') as feedstock:
			dump(record, feedstock)
			feedstock.write("\n")
		return {"success" : True}
	except:
		return {"success" : False, "message" : "Error: Bad record"}

#User-facing function to output whole dataset
#all_records must be a list of all the dataset records
#metadata must be the dataset metadata, unless this metadata is the first element in all_records instead
def write_dataset(all_records, metadata=None):
	if not metadata: #Metadata not supplied in args, must be first element in all_records
		metadata = all_records.pop(0)
	if validate_metadata(metadata) == False:
		return {"success" : False, "message" : "Error: Bad metadata"}

	#Open feedstock file
	feedstock_path = paths.feedstock + metadata["mdf_source_name"] + "_all.json"
	metadata["mdf_id"] = str(ObjectId())
	try:
		with open(feedstock_path, 'w') as feedstock:
			dump(metadata, feedstock)
			feedstock.write("\n")
			for record in all_records:
				record["mdf_id"] = str(ObjectId())
				if validate_record(record) == False:
					print("Bad record:", record)
				else:
					dump(record, feedstock)
					feedstock.write("\n")
		return {"success" : True}
	except:
		return {"success" : False, "message" : "Error: Bad dataset"}




