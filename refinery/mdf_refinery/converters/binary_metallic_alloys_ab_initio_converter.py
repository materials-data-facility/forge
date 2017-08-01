import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.tab_parser import parse_tab
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: A compilation of ab-initio calculations of embrittling potencies in binary metallic alloys dataset
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
                "title": "A compilation of ab-initio calculations of embrittling potencies in binary metallic alloys",
                "acl": ['public'],
                "source_name": "binary_metallic_alloys_ab_initio",
                "citation": ["Gibson, Michael A., and Christopher A. Schuh. “A Compilation of Ab-Initio Calculations of Embrittling Potencies in Binary Metallic Alloys.” Data in Brief 6 (2016): 143–148. PMC. Web. 29 June 2017."],
                "data_contact": {
    
                    "given_name": "Michael A.",
                    "family_name": "Gibson",
                    
                    "email": "m_gibson@mit.edu",
                    "instituition": "Massachusetts Institute of Technology"
    
                    },
    
                "author": [{
                    
                    "given_name": "Michael A.",
                    "family_name": "Gibson",
                    
                    "email": "m_gibson@mit.edu",
                    "instituition": "Massachusetts Institute of Technology"
                    
                    },
                    {
                    
                    "given_name": "Christopher A.",
                    "family_name": "Schuh",
                    
                    "email": "schuh@mit.edu",
                    "instituition": "Massachusetts Institute of Technology"
                    
                    }],
    
                "license": "http://creativecommons.org/licenses/by/4.0/",
    
                "collection": "Binary Metallic Alloys Ab-initio Calculations",
                "tags": ["Grain Boundary Segregation", "Embrittlement", "Ab-Initio Calculation", "Surface", "Segregation", "Fracture"],
    
                "description": "Segregation-induced changes in interfacial cohesion often control the mechanical properties of metals. The change in the work of separation of an interface upon segregation of a solute to the interface, termed the embrittling potency, is an atomic-level quantity used to predict and understand embrittlement phenomena. We present a compilation of calculations of embrittling potencies, along with references for these calculations.",
                "year": 2015,
    
                "links": {
    
                    "landing_page": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4706572/",
    
                    #"publication": ,
                   # "data_doi": ,
    
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
    for data_file in find_files(input_path, "csv"):
        with open(os.path.join(data_file["path"], data_file["filename"]) , 'r') as raw_in:
            total_data_lst = raw_in.readlines()
            #remove first line descriptions
            total_data = "".join(total_data_lst[1:])
        for record in tqdm(parse_tab(total_data), desc="Processing file: " + data_file["filename"], disable=not verbose):
            comp = record["Solvent"] + record["Solute"]
            record_metadata = {
                "mdf": {
                    "title": "Binary Metallic Alloys Ab-initio - " + comp,
                    "acl": ['public'],
        
        #            "tags": ,
        #            "description": ,
                    
                    "composition": comp,
        #            "raw": ,
        
                    "links": {
        #                "landing_page": ,
        
        #                "publication": ,
        #                "data_doi": ,
        
        #                "related_id": ,
        
                        "csv": {
                            "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                            "http_host": "https://data.materialsdatafacility.org",
        
                            "path": "/collections/binary_metallic_alloys_ab_initio/" + data_file["filename"],
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
