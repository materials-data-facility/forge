import json
import sys
import os
from ..parsers.tab_parser import parse_tab
from tqdm import tqdm
from ..validator.schema_validator import Validator

# VERSION 0.2.0

# This is the converter for: Assigned formula of complex mixture FT-ICR MS datasets
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
            "mdf-title": "Assigned formula of complex mixture FT-ICR MS datasets",
            "mdf-acl": ['public'],
            "mdf-source_name": "ft-icr-ms",
            "mdf-citation": ["Blackburn, John; Uhrin, Dusan. (2017). Assigned formula of complex mixture FT-ICR MS datasets, [dataset]. University of Edinburgh. School of Chemistry. http://dx.doi.org/10.7488/ds/1984"],
            "mdf-data_contact": {

                "given_name": "Dusan",
                "family_name": "Uhrin",
                
                "email": "dusan.uhrin@ed.ac.uk",
                "instituition": "University of Edinburgh"

                },

            "mdf-author": [{

                "given_name": "John",
                "family_name": "Blackburn",
                
                "instituition": "University of Edinburgh"
                
                },
                {
                
                "given_name": "Dusan",
                "family_name": "Uhrin",
                
                "email": "dusan.uhrin@ed.ac.uk",
                "instituition": "University of Edinburgh"
                
                }],

            "mdf-license": "http://creativecommons.org/licenses/by/4.0/legalcode",

            "mdf-collection": "FT-ICR MS Datasets Assigned Formula",
            "mdf-data_format": ["txt"],
        #    "mdf-data_type": ,
            "mdf-tags": ["ESI", "MALDI", "LDI"],

            "mdf-description": "The dataset included is of formula assigned from FT-ICR MS data for samples of Suwannee River fulvic acid (SRFA) and Suwannee River natural organic matter (SRNOM) (both are standards from the International Humic Substances Society) using a variety of ionisation sources. This includes electrospray ionisation (ESI), matrix assisted laser desorption/ionisation (MALDI) and matrix free laser desorption/ionisation (LDI).",
            "mdf-year": 2017,

            "mdf-links": {

                "mdf-landing_page": "http://datashare.is.ed.ac.uk/handle/10283/2640",

                "mdf-publication": ["http://dx.doi.org/10.1021/acs.analchem.6b04817"],
               # "mdf-dataset_doi": "",

          #      "mdf-related_id": ,

                "zip": {
                
                    #"globus_endpoint": ,
                    "http_host": "http://datashare.is.ed.ac.uk",

                    "path": "/download/10283/2640/Assigned_formula_of_complex_mixture_FT-ICR_MS_datasets.zip",
                    }
                },

#            "mdf-mrr": ,

            "mdf-data_contributor": [{
                "given_name": "Evan",
                "family_name": "Pike",
                "email": "dep78@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "dep78"
                }]
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
    with open(os.path.join(input_path, "ft-icr-ms_data.txt")) as raw_in:
        all_data = raw_in.read()
    for record in tqdm(parse_tab(all_data, sep=";"), desc="Processing files", disable=not verbose):
        record_metadata = {
            "mdf-title": "FT-ICR-MS " + record["Molecular Formula"],
            "mdf-acl": ['public'],

#            "mdf-tags": ,
#            "mdf-description": ,
            
            "mdf-composition": record["Molecular Formula"],
            "mdf-raw": json.dumps(record),

            "mdf-links": {
#                "mdf-landing_page": ,

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                "txt": {
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    "http_host": "https://data.materialsdatafacility.org",

                    "path": "/collections/ft-icr-ms/ft-icr-ms_data.txt",
                    },
                },

#            "mdf-citation": ,
#            "mdf-data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

#                },

#            "mdf-author": ,

#            "mdf-license": ,
#            "mdf-collection": ,
#            "mdf-data_format": ,
#            "mdf-data_type": ,
#            "mdf-year": ,

#            "mdf-mrr":

#            "mdf-processing": ,
#            "mdf-structure":,
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
