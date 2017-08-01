import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0
"""WARNING: DON'T INGEST UNTIL AFTER ASE PATCH"""
# This is the Trinkle Mg-X-Diffusion Dataset
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
                "title": "Mg-X-Diffusion",
                "acl": ['public'],
                "source_name": "trinkle_mg_x_diffusion",
                "citation": ["Citation for dataset Mg-X-Diffusion with author(s): Dallas Trinkle, Ravi Agarwal"],
                "data_contact": {
    
                    "given_name": "Dallas",
                    "family_name": "Trinkle",
    
                    "email": "dtrinkle@illinois.edu",
                    "institution": "University of Illinois at Urbana-Champaign",
                    },
    
                "author": [{
                    
                    "given_name": "Dallas",
                    "family_name": "Trinkle",
                    
                    "email": "dtrinkle@illinois.edu",
                    "instituition": "University of Illinois at Urbana-Champaign"
                    
                    },
                    {
                        
                    "given_name": "Ravi",
                    "family_name": "Agarwal",
                    
                    "institution": "University of Illinois at Urbana-Champaign"
                    
                    }],
    
                #"license": "",
    
                "collection": "Mg-X Diffusion Dataset",
    #            "tags": ,
    
                #"description": ,
                "year": 2017,
    
                "links": {
    
                    "landing_page": "https://data.materialsdatafacility.org/published/#trinkle_mg_x_diffusion",
    
                   # "publication": [""],
                    #"data_doi": "",
    
    #                "related_id": ,
    
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
    total_errors = 0
    for data_file in tqdm(find_files(input_path, "OUTCAR"), desc="Processing files", disable=not verbose):
        try:
            record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "vasp-out")
        except Exception as e:
            #print("Error on: " + data_file["path"] + "/" + data_file["filename"] + "\n" + repr(e))
            total_errors +=1
        record_metadata = {
            "mdf": {
                "title": "Mg-X Diffusion - ",
                "acl": ['public'],
    
    #            "tags": ,
    #            "description": ,
                
                #"composition": ,
    #            "raw": ,
    
                "links": {
                    #"landing_page": ,
    
    #                "publication": ,
    #                "data_doi": ,
    
    #                "related_id": ,
    
                    "outcar": {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",
    
                        "path": "/collections/mg-x/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
        try:
            record_metadata["mdf"]["composition"] = record["mdf"]["chemical_formula"]
            record_metadata["mdf"]["title"] +=  record["mdf"]["chemical_formula"]
        except:
            #parse_ase unable to read composition of record 1386: https://data.materialsdatafacility.org/collections/mg-x/Elements/Eu/Mg-X_Eu/OUTCAR
            #Placing in the correct material composition
            record_metadata["mdf"]["composition"] = "EuMg149"
            record_metadata["mdf"]["title"] += "EuMg149"

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])

    # You're done!
    if verbose:
        print("Total errors: " + str(total_errors))
        print("Finished converting")
