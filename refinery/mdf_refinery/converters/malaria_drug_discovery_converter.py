import json
import sys
import os
from mdf_refinery.parsers.tab_parser import parse_tab
from tqdm import tqdm
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter The Open Access Malaria Box: A Drug Discovery Catalyst for Neglected Diseases
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
                "title": "The Open Access Malaria Box: A Drug Discovery Catalyst for Neglected Diseases",
                "acl": ['public'],
                "source_name": "malaria_drug_discovery",
                "citation": ["Spangenberg T, Burrows JN, Kowalczyk P, McDonald S, Wells TNC, Willis P (2013) The Open Access Malaria Box: A Drug Discovery Catalyst for Neglected Diseases. PLoS ONE 8(6): e62906. https://doi.org/10.1371/journal.pone.0062906"],
                "data_contact": {
    
                    "given_name": "Thomas",
                    "family_name": "Spangenberg",
                    
                    "email": "spangenbergt@mmv.org",
                    "instituition": "Medicines for Malaria Venture"
    
                    },
    
                "author": [{
                    
                    "given_name": "Thomas",
                    "family_name": "Spangenberg",
                    
                    "email": "spangenbergt@mmv.org",
                    "instituition": "Medicines for Malaria Venture"
                    
                    },
                    {
                    
                    "given_name": "Jeremy N.",
                    "family_name": "Burrows",
                    
                    "instituition": "Medicines for Malaria Venture"
                    
                    },
                    {
                    
                    "given_name": "Paul",
                    "family_name": "Kowalczyk",
                    
                    "instituition": "SCYNEXIS Inc."
                    
                    },
                    {
                    
                    "given_name": "Simon",
                    "family_name": "McDonald",
                    
                    "instituition": "Medicines for Malaria Venture"
                    
                    },
                    {
                    
                    "given_name": "Timothy N. C.",
                    "family_name": "Wells",
                    
                    "instituition": "Medicines for Malaria Venture"
                    
                    },
                    {
                    
                    "given_name": "Paul",
                    "family_name": "Willis",
                    
                    "email": "willisp@mmv.org",
                    "instituition": "Medicines for Malaria Venture"
                    
                    }],
    
                "license": "https://creativecommons.org/licenses/by/4.0/",
    
                "collection": "Open Access Malaria Box",
                "tags": ["Malaria", "Malarial parasites", "Antimalarials", "Plasmodium", "Parasitic diseases", "Drug discovery", "Plasmodium falciparum"],
    
                "description": "In most cases it is a prerequisite to be able to obtain physical samples of the chemical compounds for further study, and the groups responsible for screening did not originally plan to provide these molecules. In addition, many of the biological systems in which these compounds would be tested are not suitable for testing such large numbers of compounds. There needs to therefore be some simplification of the collection. To overcome these barriers, a diverse collection of anti-malarial compounds has been designed and assembled.",
                "year": 2013,
    
                "links": {
    
                    "landing_page": "https://doi.org/10.1371/journal.pone.0062906",
    
                  #  "publication": ,
                    "data_doi": "https://ndownloader.figshare.com/files/1090667",
    
                 #   "related_id": ,
    
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
    with open(os.path.join(input_path, "Table_S1.csv"), 'r') as raw_in:
        data_records = raw_in.read()
    for record in tqdm(parse_tab(data_records), desc="Processing Data", disable=not verbose):
        record_metadata = {
            "mdf": {
                "title": "Malaria Drug Discovery - " + record["Smiles"],
                "acl": ['public'],
    
    #            "tags": ,
    #            "description": ,
                
                "composition": record["Smiles"],
    #            "raw": ,
    
                "links": {
    #                "landing_page": ,
    
    #                "publication": ,
    #                "data_doi": ,
    
    #                "related_id": ,
    
                    "csv": {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",
    
                        "path": "/collections/malaria_drug_discovery/Table_S1.csv",
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
