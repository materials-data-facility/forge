import json
import sys
import os
from mdf_refinery.parsers.tab_parser import parse_tab
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Using Machine Learning To Identify Factors That Govern Amorphization of Irradiated Pyrochlores
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
                "title": "Using Machine Learning To Identify Factors That Govern Amorphization of Irradiated Pyrochlores",
                "acl": ['public'],
                "source_name": "irradiated_pyrochlores_ml",
                "citation": ["Using Machine Learning To Identify Factors That Govern Amorphization of Irradiated Pyrochlores Ghanshyam Pilania, Karl R. Whittle, Chao Jiang, Robin W. Grimes, Christopher R. Stanek, Kurt E. Sickafus, and Blas Pedro Uberuaga Chemistry of Materials 2017 29 (6), 2574-2583 DOI: 10.1021/acs.chemmater.6b04666"],
                "data_contact": {
    
                    "given_name": "Blas Pedro",
                    "family_name": "Uberuaga",
                    
                    "email": "blas@lanl.gov",
                    "instituition": "Los Alamos National Laboratory"
    
                    },
    
                "author": [{
                    
                    "given_name": "Blas Pedro",
                    "family_name": "Uberuaga",
                    
                    "email": "blas@lanl.gov",
                    "instituition": "Los Alamos National Laboratory"
                    
                    },
                    {
                    
                    "given_name": "Ghanshyam",
                    "family_name": "Pilania",
                    
                    "instituition": "Los Alamos National Laboratory"
                    
                    },
                    {
                    
                    "given_name": "Karl R.",
                    "family_name": "Whittle",
                    
                    "instituition": "University of Liverpool"
                    
                    },
                    {
                    
                    "given_name": "Chao",
                    "family_name": "Jiang",
                    
                    "instituition": "Idaho National Laboratory"
                    
                    },
                    {
                    
                    "given_name": "Robin W.",
                    "family_name": "Grimes",
                    
                    "instituition": "Imperial College London"
                    
                    },
                    {
                    
                    "given_name": "Christopher R.",
                    "family_name": "Stanek",
                    
                    "instituition": "Los Alamos National Laboratory"
                    
                    },
                    {
                    
                    "given_name": "Kurt E.",
                    "family_name": "Sickafus",
                    
                    "instituition": "University of Tennessee"
                    
                    }],
    
                "license": "https://creativecommons.org/licenses/by-nc/4.0/",
    
                "collection": "ML for Amorphization of Irradiated Pyrochlores",
                "tags": ["ML"],
    
                "description": "Here, we use a machine learning model to examine the factors that govern amorphization resistance in the complex oxide pyrochlore (A2B2O7) in a regime in which amorphization occurs as a consequence of defect accumulation. We examine the fidelity of predictions based on cation radii and electronegativities, the oxygen positional parameter, and the energetics of disordering and amorphizing the material.",
                "year": 2017,
    
                "links": {
    
                    "landing_page": "http://pubs.acs.org/doi/full/10.1021/acs.chemmater.6b04666#showFigures",
    
                 #   "publication": ,
                    #"data_doi": "",
    
                #    "related_id": ,
    
                    "pdf": {
                    
                        #"globus_endpoint": ,
                        "http_host": "https://ndownloader.figshare.com",
    
                        "path": "/files/7712131",
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
    with open(os.path.join(input_path, "irradiated_pyrochlores_ml_data.txt"), 'r') as raw_in:
        headers = raw_in.readline().split("; ")
        for record in parse_tab(raw_in.read(), headers=headers, sep=" "):
            record_metadata = {
                "mdf": {
                    "title": "ML for Amorphization of Irradiated Pyrochlores - " + record["Compound"],
                    "acl": ['public'],
        
        #            "tags": ,
        #            "description": ,
                    
                    "composition": record["Compound"],
        #            "raw": ,
        
                    "links": {
        #                "landing_page": ,
        
        #                "publication": ,
        #                "data_doi": ,
        
        #                "related_id": ,
        
                        "txt": {
                            "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                            "http_host": "https://data.materialsdatafacility.org",
        
                            "path": "/collections/irradiated_pyrochlores_ml/irradiated_pyrochlores_ml_data.txt",
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
