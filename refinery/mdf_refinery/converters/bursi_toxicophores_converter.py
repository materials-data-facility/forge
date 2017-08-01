import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Derivation and Validation of Toxicophores for Mutagenicity Prediction
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
                "title": "Derivation and Validation of Toxicophores for Mutagenicity Prediction",
                "acl": ['public'],
                "source_name": "bursi_toxicophores",
                "citation": ["Bursi, Roberta (2005/01/01). Derivation and Validation of Toxicophores for Mutagenicity Prediction. Journal of Medicinal Chemistry, 48, 312-320. doi: 10.1021/jm040835a"],
                "data_contact": {
    
                    "given_name": "Roberta",
                    "family_name": "Bursi",
                    
                    "email": "roberta.bursi@organon.com",
                    "instituition": "Molecular Design & Informatics Department, N.V. Organon"
    
                    },
    
                "author": [{
                    
                    "given_name": "Roberta",
                    "family_name": "Bursi",
                    
                    "email": "roberta.bursi@organon.com",
                    "instituition": "Molecular Design & Informatics Department, N.V. Organon"
                    
                    },
                    {
                    
                    "given_name": "Ross",
                    "family_name": "McGuire",
                    
                    "instituition": "Molecular Design & Informatics Department, N.V. Organon"
                    
                    },
                    {
                    
                    "given_name": "Jeroen",
                    "family_name": "Kazius",
                    
                    "instituition": "Universiteit Leiden"
                    
                    }],
    
                #"license": "",
    
                "collection": "Toxicophores for Mutagenicity Prediction",
                #"tags": ,
    
                "description": "Mutagenicity is one of the numerous adverse properties of a compound that hampers its potential to become a marketable drug. Toxic properties can often be related to chemical structure, more specifically, to particular substructures, which are generally identified as toxicophores. A number of toxicophores have already been identified in the literature. This study aims at increasing the current degree of reliability and accuracy of mutagenicity predictions by identifying novel toxicophores from the application of new criteria for toxicophore rule derivation and validation to a considerably sized mutagenicity dataset.",
                "year": 2005,
    
                "links": {
    
                    "landing_page": "http://pubs.acs.org/doi/full/10.1021/jm040835a",
    
                    #"publication": [""],
                    #"data_doi": ,
    
                    #"related_id": ,
    
                    "sdf": {
                    
                        #"globus_endpoint": ,
                        "http_host": "ftp://ftp.ics.uci.edu",
    
                        "path": "/pub/baldig/learning/Bursi/source/cas_4337.sdf",
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
    for data_file in tqdm(find_files(input_path, "sdf"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "sdf")
        record_metadata = {
            "mdf": {
                "title": "Bursi Toxicophores - " + record["chemical_formula"],
                "acl": ['public'],
    
    #            "tags": ,
    #            "description": ,
                
                "composition": record["chemical_formula"],
    #            "raw": ,
    
                "links": {
    #                "landing_page": ,
    
    #                "publication": ,
    #                "data_doi": ,
    
    #                "related_id": ,
    
                    "sdf": {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",
    
                        "path": "/collections/bursi_toxicophores/" + data_file["filename"],
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
