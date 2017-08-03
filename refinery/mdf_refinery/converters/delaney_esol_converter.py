import json
import sys
import os
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: ESOL: Estimating Aqueous Solubility Directly from Molecular Structure
# Arguments:
#   input_path (string): The file or directory where the data resides.
#       NOTE: Do not hard-code the path to the data in the converter (the filename can be hard-coded, though). The converter should be portable.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    # NOTE: For fields that represent people (e.g. mdf-data_contact), other IDs can be added (ex. "github": "jgaff").
    #    It is recommended that all people listed in mdf-data_contributor have a github username listed.
    #
    # If there are other useful fields not covered here, another block (dictionary at the same level as "mdf") can be created for those fields.
    # The block must be called the same thing as the source_name for the dataset.
    if not metadata:
        ## Metadata:dataset
        dataset_metadata = {
            "mdf": {

                "title": "ESOL: Estimating Aqueous Solubility Directly from Molecular Structure",
                "acl": ["public"],
                "source_name": "delaney_esol",

                "data_contact": {

                    "given_name": "John S.",
                    "family_name": "Delaney",
                    "email": "john.delaney@syngenta.com",
                    "institution": "Syngenta, Jealott's Hill International Research Centre",

                },

                "data_contributor": [{

                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Delaney, John S. (2004/05/01). ESOL:  Estimating Aqueous Solubility Directly from Molecular Structure. Journal of Chemical Information and Computer Sciences, 44, 1000-1005. doi: 10.1021/ci034243x"],

                "author": [{

                    "given_name": "John S.",
                    "family_name": "Delaney",
                    "email": "john.delaney@syngenta.com",
                    "institution": "Syngenta, Jealott's Hill International Research Centre",

                }],

                #"license": "",
                "collection": "ESOL",
                #"tags": [""],
                "description": "This paper describes a simple method for estimating the aqueous solubility (ESOL − Estimated SOLubility) of a compound directly from its structure. The model was derived from a set of 2874 measured solubilities using linear regression against nine molecular properties. The most significant parameter was calculated logPoctanol, followed by molecular weight, proportion of heavy atoms in aromatic systems, and number of rotatable bonds. The model performed consistently well across three validation sets, predicting solubilities within a factor of 5−8 of their measured values, and was competitive with the well-established “General Solubility Equation” for medicinal/agrochemical sized molecules.",
                "year": 2004,

                "links": {

                    "landing_page": "http://pubs.acs.org/doi/abs/10.1021/ci034243x#ci034243xAF1",
                    "publication": ["http://pubs.acs.org/doi/full/10.1021/ci034243x#ci034243xAF1"],
                    #"data_doi": "",
                    #"related_id": "",

                    #"data_link": {

                        #"globus_endpoint": ,
                        #"http_host": "",

                        #"path": "",

                    #},

                },

            },

            #"mrr": {

            #},

            #"dc": {

            #},


        }
        ## End metadata
    elif type(metadata) is str:
        try:
            dataset_metadata = json.loads(metadata)
        except Exception:
            try:
                with open(metadata, 'r') as metadata_file:
                    dataset_metadata = json.load(metadata_file)
            except Exception as e:
                sys.exit("Error: Unable to read metadata: " + repr(e))
    elif type(metadata) is dict:
        dataset_metadata = metadata
    else:
        sys.exit("Error: Invalid metadata parameter")


    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    # If the metadata is incorrect, the constructor will throw an exception and the program will exit
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    #    Each record should be exactly one dictionary
    #    You must write your records using the Validator one at a time
    #    It is recommended that you use a parser to help with this process if one is available for your datatype
    #    Each record also needs its own metadata

    with open(os.path.join(input_path, "delaney_esol.txt"), 'r') as raw_in:
        headers = raw_in.readline().strip("\n").split(",")
        data = raw_in.readlines()
        
    for line in data:
        line_data = line.strip("\n").split(",")
        record = {}
        indx = -3
        record[headers[0]] = ",".join(line_data[:indx])
        
        for head in headers[1:]:
            record[head] = line_data[indx]
            indx+=1
        ## Metadata:record
        record_metadata = {
            "mdf": {
    
                "title": "ESOL - " + record["SMILES"],
                "acl": ["public"],
                "composition": record["SMILES"],
    
                #"tags": ,
                #"description": ,
                "raw": json.dumps(record),
    
                "links": {
    
                    #"landing_page": ,
                    #"publication": ,
                    #"data_doi": ,
                    #"related_id": ,
    
                    "txt": {
    
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",
    
                        "path": "/collections/delaney_esol/delaney_esol.txt",
    
                    },
    
                },
    
                #"citation": ,
    
                #"data_contact": {
    
                    #"given_name": ,
                    #"family_name": ,
                    #"email": ,
                    #"institution": ,
    
                #},
    
                #"author": [{
    
                    #"given_name": ,
                    #"family_name": ,
                    #"email": ,
                    #"institution": ,
    
                #}],
    
                #"year": ,
    
            },
    
            #"dc": {
    
            #},
    
    
        }
        ## End metadata
    
        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)
    
        # Check if the Validator accepted the record, and stop processing if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if not result["success"]:
            if not dataset_validator.cancel_validation()["success"]:
                print("Error cancelling validation. The partial feedstock may not be removed.")
            raise ValueError(result["message"] + "\n" + result.get("details", ""))


    # You're done!
    if verbose:
        print("Finished converting")
