import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for the W-14 dataset: Accuracy and transferability of Gaussian approximation potential models for tungsten
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
                "title": "Accuracy and transferability of Gaussian approximation potential models for tungsten",
                "acl": ['public'],
                "source_name": "w_14",
                "citation": ["Wojciech J. Szlachta, Albert P. Bartók, Gábor Csányi: Accuracy and transferability of Gaussian approximation potential models for tungsten, Physical Review B 90(10): 104108, 2014. http://dx.doi.org/10.1103/PhysRevB.90.104108"],
                "data_contact": {
    
                    "given_name": "Gábor",
                    "family_name": "Csányi",
                    
                    "email": "gc121@eng.cam.ac.uk",
                    "instituition": "University of Cambridge",
    
                    },
    
                "author": [{
                    
                    "given_name": "Gábor",
                    "family_name": "Csányi",
                    
                    "email": "gc121@eng.cam.ac.uk",
                    "instituition": "University of Cambridge"
                    
                    },
                    {
                        
                    "given_name": "Wojciech J.",
                    "family_name": "Szlachta",
                    
                    "institution": "University of Cambridge"
                    
                    },
                    {
                    
                    "given_name": "Albert P.",
                    "family_name": "Bartók",
                    
                    "instituition": "University of Cambridge",
                    
                    }],
    
    #            "license": "",
    
                "collection": "w_14",
    #            "tags": ,
    
                "description": "158k diverse atomic environments of elemental tungsten. DFT/PBE energies, forces and stresses for tungsten, periodic unit cells in the range of 1-135 atoms, including bcc primitive cell, 128-atom bcc cell, vacancies, low index surfaces, gamma-surfaces, and dislocation cores.",
                "year": 2014,
    
                "links": {
    
                    "landing_page": "http://qmml.org/datasets.html#w-14",
    
                    "publication": ["https://doi.org/10.1103/PhysRevB.88.155105"],
                  #  "data_doi": "",
    
    #                "related_id": ,
    
                    "zip": {
                    
                        #"globus_endpoint": ,
                        "http_host": "http://qmml.org",
    
                        "path": "/Datasets/w-14.zip",
                        }
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
    for data_file in tqdm(find_files(input_path, "xyz"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "xyz")
        record_metadata = {
            "mdf": {
                "title": "w_14 " + data_file["filename"],
                "acl": ['public'],
    
    #            "tags": ,
    #            "description": ,
                
                "composition": record["chemical_formula"],
    #            "raw": ,
    
                "links": {
                   # "landing_page": ,
    
    #                "publication": ,
    #                "data_doi": ,
    
    #                "related_id": ,
    
                    "xyz": {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",
    
                        "path": "/collections/w_14/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
