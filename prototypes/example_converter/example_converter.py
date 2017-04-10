from example_validator import Validator

#This is an example of a converter. It is not intended to handle real data.
#Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(input_path, verbose=False):

    #Collect the metadata
    #Fields can be  #REQ (Required, must be present)
            #RCM (Recommended, should be present if possible)
            #OPT (Optional, can be present if useful)
    dataset_metadata = {
        "globus_subject": "https://materialsdatafacility.org/",                    # REQ string: Unique value (should be URI if possible)
        "acl" : ["public"],                                                        # REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
        "mdf_source_name" "example_dataset": ,                                     # REQ string: Unique name for dataset
        "mdf-publish.publication.collection" "examples": ,                         # RCM string: Collection the dataset belongs to

        "dc.title" : "Example of a Dataset",                                       # REQ string: Title of dataset
        "dc.creator" : "MDF",                                                      # REQ string: Creator of dataset
        "dc.identifier" : "http://dx.doi.org/10.12345",                            # REQ string: Link to dataset (dataset DOI if available)
        "dc.contributor.author" : ["Jonathon Gaff", "Ben Blaiszik"],               # RCM list of strings: Author(s) of dataset
        "dc.subject" : ["example", "test", "converter"],                           # RCM list of strings: Keywords about dataset
        "dc.description" : "This is an example dataset for an example converter",  # RCM string: Description of dataset contents
        "dc.relatedidentifier" : ["https://www.globus.org"],                       # RCM list of strings: Link(s) to related materials (such as an article)
        "dc.year" : 2017                                                           # RCM integer: Year of dataset creation
        }


    #Make a Validator to help write the feedstock
    #You must pass the metadata to the constructor
    #Each Validator instance can only be used for a single dataset
    dataset_validator = Validator(dataset_metadata)


    #Get the data
        #Each record should be exactly one dictionary
        #It is recommended that you convert your records one at a time, but it is possible to put them all into one big list (see below)
        #It is also recommended that you use a parser to help with this process if one is available for your datatype

    raw_data = read_data()

    #Each record also needs its own metadata
    for raw_record in raw_data:
        record = example_parser.parse_example_single(raw_record)

        #Fields can be  #REQ (Required, must be present)
                #RCM (Recommended, should be present if possible)
                #OPT (Optional, can be present if useful)
        record_metadata = {
            "globus_subject" : ,            #REQ string: Unique value (should be URI if possible)
            "acl" : ,               #REQ list of strings: UUID(s) of users/groups allowed to access data, or ["public"]
            "mdf-publish.publication.collection" : ,#RCM string: Collection the dataset belongs to
            "mdf_data_class" : ,            #RCM string: Type of data in record
            "mdf-base.material_composition" : , #RCM string: Chemical composition of material in record

            "dc.title" : ,              #REQ string: Title of dataset
            "dc.creator" : ,            #OPT string: Host(?) of dataset
            "dc.identifier" : ,         #RCM string: Link to dataset (dataset DOI if available)
            "dc.contributor.author" : ,     #OPT list of strings: Author(s) of dataset
            "dc.subject" : ,            #OPT list of strings: Keywords about dataset
            "dc.description" : ,            #OPT string: Description of dataset contents
            "dc.relatedidentifier" : ,      #OPT list of strings: Link(s) to related materials (such as an article)
            "dc.year" : ,               #OPT integer: Year of dataset creation

            "data" : {              #REQ dictionary: Other record data (described below)
                "raw" : ,           #RCM string: Original data record text, if feasible
                "files" : ,         #REQ dictionary: {file_type : uri_to_file} pairs, may be empty (Example: {"cif" : "https://example.org/cifs/data_file.cif"})

                #other              #RCM any JSON-valid type: Any other data fields you would like to include go in the "data" dictionary. Keys will be prepended with mdf_source_name
                }
            }
        
        #Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        #Check if the Validator accepted the record, and print a message if it didn't
        #If the Validator returns "success" == True, the record was written successfully
        if result["success"] != True:
            print("Error:", result["message"], ":", result["invalid_data"])

    #Alternatively, if the only way you can process your data is in one large list, you can pass the list to the Validator
    #You still must add the required metadata to your records
    #It is recommended to use the previous method if possible
    #result = dataset_validator.write_dataset(your_records_with_metadata)
    #if result["success"] != True:
        #print("Error:", result["message"])


    #TODO: Save your converter as [dataset_name]_converter.py
    #You're done!
    if verbose:
        print("Finished converting")


def read_data():
    """Dummy function as an example"""
    return range(10)


#Optionally, you can have a default call here for testing
#The convert function should work completely on its own, so anything here is extra
if __name__ == "__main__":
    convert()
