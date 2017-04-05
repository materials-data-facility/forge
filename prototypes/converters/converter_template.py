from validator import Validator

#This is a template converter. It is not complete, and incomplete parts are labelled with "TODO"
#Arguments:
#	input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#	verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):

	#Collect the metadata
	#TODO: Fill in these dictionary fields for your dataset. Required fields are marked with *
	dataset_metadata = {
		"mdf_source_name" : *,
		"dc.title" : 
		}

	
	#Make a Validator to help write the feedstock
	#You can pass the metadata to the constructor
	dataset_validator = Validator(dataset_metadata) 
	#Alternately, if you don't pass the metadata to the constructor, you can write the metadata later, before you write any records
	#Don't give the Validator metadata twice
	#dataset_validator.write_metadata(dataset_metadata)


	#Get the data
	#TODO: Write the code to convert your dataset's records into JSON-serializable Python dictionaries
		#Each record should be exactly one dictionary
		#It is recommended that you convert your records one at a time, but it is possible to put them all into one big list (see below)
		#It is also recommended that you use a parser to help with this process if one is available for your datatype


	#Each record also needs its own metadata
	for record in your_records:
		#TODO: Fill in these dictionary fields for your record. Required fields are marked with *
		record_metadata = {
			"mdf_source_name" : *,
			"mdf-base.material_composition" : ,
			"data" : record #This is important: the actual record data (any fields not already here) go here
			}
		
		#Pass each individual record to the Validator
		result = dataset_validator.write_record(record_metadata)

		#Check if the Validator accepted the record, and print a message if it didn't
		#If the Validator returns "success" == True, the record was written successfully
		if result["success"] != True:
			print("Error:", result["message"], ":", result["invalid_data"])

	#Alternatively, if the only way you can process your data is in one large list, you can pass the list to the Validator
	#You still must add the required metadata to your records
	#It is recommended to use the other method if possible
	#result = dataset_validator.write_dataset(your_records_with_metadata)
	#if result["success"] != True:
		#print("Error:", result["message"])


	#TODO: Save your converter as [dataset_name]_converter.py
	#You're done!
	if verbose:
		print("Finished converting")
