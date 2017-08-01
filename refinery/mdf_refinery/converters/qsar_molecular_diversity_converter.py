import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Neighborhood Behavior: A Useful Concept for Validation of “Molecular Diversity” Descriptors

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
    if not metadata:
        dataset_metadata = {
            "mdf": {
                "title": "Neighborhood Behavior:  A Useful Concept for Validation of “Molecular Diversity” Descriptors",
                "acl": ['public'],
                "source_name": "qsar_molecular_diversity",
                "citation": ["David E Patterson, Richard D Cramer, Allan M Ferguson, Robert D Clark, Laurence W Weinberger. Neighbourhood Behaviour: A Useful Concept for Validation of \"Molecular Diversity\" Descriptors. J. Med. Chem. 1996 (39) 3049 - 3059."],
                "data_contact": {
    
                    "given_name": "Richard D.",
                    "family_name": "Cramer",
                    
                    "email": "cramer@tripos.com",
    
                    },
    
                "author": [{
                    
                    "given_name": "David E.",
                    "family_name": "Patterson",
                    
                    },
                    {
                    
                    "given_name": "Richard D.",
                    "family_name": "Cramer",
                    
                    "email": "cramer@tripos.com",
                    
                    },
                    {
                    
                    "given_name": "Allan M.",
                    "family_name": "Ferguson",
                    
                    },
                    {
                    
                    "given_name": "Robert D.",
                    "family_name": "Clark",
                    
                    },
                    {
                    
                    "given_name": "Laurence E.",
                    "family_name": "Weinberger",
                    
                    }],
    
              #  "license": "",
    
                "collection": "QSAR Molecular Diversity",
                #"tags": ,
    
                "description": "If a molecular descriptor is to be a valid and useful measure of “similarity” in drug discovery, a plot of differences in its values vs differences in biological activities for a set of related molecules will exhibit a characteristic trapezoidal distribution enhancement, revealing a “neighborhood behavior” for the descriptor. Applying this finding to 20 datasets allows 11 molecular diversity descriptors to be ranked by their validity for compound library design",
                "year": 1996,
    
                "links": {
    
                    "landing_page": "ftp://ftp.ics.uci.edu/pub/baldig/learning/Patterson/",
    
                    "publication": ["http://pubs.acs.org/doi/abs/10.1021/jm960290n"],
                  #  "data_doi": ,
    
                   # "related_id": ,
    
                    # data links: {
                    
                        #"globus_endpoint": ,
                        #"http_host": ,
    
                        #"path": ,
                        #}
                    },
    
    #            "mrr": ,
    
                "data_contributor": [{
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78"
                    }]
                }
            }
        
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
    for data_file in tqdm(find_files(input_path, "sdf"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "sdf")
        record_metadata = {
            "mdf": {
                "title": "QSAR Molecular Diversity - " + record["chemical_formula"],
                "acl": ['public'],
    
    #            "tags": ,
    #            "description": ,
                
                "composition": record["chemical_formula"],
               # "raw": json.dumps(record),
    
                "links": {
    #                "landing_page": ,
    
    #                "publication": ,
    #                "data_doi": ,
    
    #                "related_id": ,
    
                    "sdf": {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",
    
                        "path": "/collections/qsar_molecular_diversity/" + data_file["no_root_path"] + "/" + data_file["filename"],
                        },
                    },
    
    #            "citation": ,
    #            "data_contact": {
    
    #                "given_name": ,
    #                "family_name": ,
    
    #                "email": ,
    #                "institution":,
    
    #                },
    
    #            "author": ,
    
    #            "license": ,
    #            "collection": ,
    #            "year": ,
    
    #            "mrr":
    
    #            "processing": ,
    #            "structure":,
                }
            }

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])

    # You're done!
    if verbose:
        print("Finished converting")
