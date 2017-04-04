import parser
from validator_class import Validator

from sys import exit
import random
random.seed(0)

def get_record():
	return {"link_to_data" : "http://globus.org", "some_values" : list(range(10)), "random_number" : random.random()}

#Example converter using the recommended write record-by-record form
def converter_recommended_form():
	#Collect dataset metadata
	dataset_metadata = {
		"dc.title" : "Testing dataset 1",
		"dc.identifier" : "http://example.com",
		"mdf_source_name" : "test_dataset1"
		}
	#Initialize Validator with metadata
	dataset = Validator(dataset_metadata)

	#Fetch dataset data one record at a time
	for i in range(10): #Placeholder for actual data extraction
		raw_record = get_record()
		
		#Using a parser where possible is recommended
		clean_record = parser.parse_test_single(raw_record)

		#Add record metadata
		clean_record["mdf_source_name"] = "test_dataset1"

		#Write out the record
		result = dataset.write_record(clean_record)

		#Check the Validator's result
		if result["success"] == False:
			print("Error validating record")
			exit()

	#The dataset is processed
	print("Successfully processed dataset feedstock")

#Example converter with the alternate write-all-records-at-once form
def converter_alternate_form():
	#Collect dataset metadata
	dataset_metadata = {
		"dc.title" : "Testing dataset 2",
		"dc.identifier" : "http://example.com",
		"mdf_source_name" : "test_dataset2"
		}
	#Initialize Validator with metadata
	dataset = Validator(dataset_metadata)

	#Fetch dataset data in one list
	record_list = [get_record() for i in range(10)]

	#Add record metadata
	for record in record_list:
		record["mdf_source_name"] = "test_dataset2"

	#Write out all records at once
	dataset.write_dataset(record_list)

	print("Successfully processed dataset feedstock")

