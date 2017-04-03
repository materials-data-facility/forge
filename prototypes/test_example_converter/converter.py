import parser
import validator

from sys import exit
import random
random.seed(0)

def get_record():
	return {"link_to_data" : "http://globus.org", "some_values" : list(range(10)), "random_number" : random.random()}

#Example converter using the recommended write record-by-record form
def convert_recommended_form():
	#Input dataset metadata
	dataset_metadata = {
		"dc.title" : "Testing dataset",
		"dc.identifier" : "http://example.com",
		"mdf_source_name" : "test_dataset"
		}
	#Write out dataset metadata
	result = validator.write_metadata(dataset_metadata)
	#The validator returns a dictionary describing the results
	if result["success"] == False:
		print("Error validating metadata")
		exit()

	#Fetch dataset data one record at a time
	for i in range(10): #Placeholder for actual data extraction
		raw_record = get_record()
		
		#Using a parser where possible is recommended
		clean_record = parser.parse_test_single(raw_record)

		#Add record metadata
		clean_record["mdf_source_name"] = "test_dataset"

		#Write out the record
		result2 = validator.write_record(clean_record)

		#Again, check the validator's result
		if result2["success"] == False:
			print("Error validating record")
			exit()

	#The dataset is processed
	print("Successfully processed dataset feedstock")

