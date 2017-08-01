import json
import sys
import os
from mdf_forge.toolbox import find_files
from tqdm import tqdm
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for The MPI-Mainz UV/VIS Spectral Atlas of Gaseous Molecules dataset
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
                "title": "The MPI-Mainz UV/VIS Spectral Atlas of Gaseous Molecules",
                "acl": ['public'],
                "source_name": "mpi_mainz",
                "citation": ["Keller-Rudek, H. M.-P. I. for C. M. G., Moortgat, G. K. M.-P. I. for C. M. G., Sander, R. M.-P. I. for C. M. G., & Sörensen, R. M.-P. I. for C. M. G. (2013). The MPI-Mainz UV/VIS Spectral Atlas of Gaseous Molecules [Data set]. Zenodo. http://doi.org/10.5281/zenodo.6951,"],
                "data_contact": {
        
                    "given_name": "Keller-Rudek",
                    "family_name": "Hannelore",
                    
                    "email": "hannelore.keller-rudek@mpic.de",
                    "instituition": "Max-Planck Institute for Chemistry"
        
                    },
        
                "author": [{
                    
                    "given_name": "Keller-Rudek",
                    "family_name": "Hannelore",
                    
                    "email": "hannelore.keller-rudek@mpic.de",
                    "instituition": "Max-Planck Institute for Chemistry"
                    
                    },
                    {
                        
                    "given_name": "Geert K.",
                    "family_name": "Moortgat",
                    
                    "institution": "Max-Planck Institute for Chemistry"
                    
                    },
                    {
                    
                    "given_name": "Sander",
                    "family_name": "Rolf",
                    
                    "email": "rolf.sander@mpic.de",
                    "instituition": "Max-Planck Institute for Chemistry",
                    
                    },
                    {
                    
                    "given_name": "Sörensen",
                    "family_name": "Rüdiger",
                    
                    "instituition": "Max-Planck Institute for Chemistry",
                    
                    }],
        
                "license": "https://creativecommons.org/licenses/by/4.0/",
        
                "collection": "UV/VIS Spectral Atlas",
                "tags": ["cross sections", "quantum yields"],
        
                "description": "This archive contains a frozen snapshot of all cross section and quantum yield data files from the MPI-Mainz UV/VIS Spectral Atlas of Gaseous Molecules.",
                "year": 2013,
        
                "links": {
        
                    "landing_page": "https://doi.org/10.5281/zenodo.6951",
        
                    "publication": ["http://www.earth-syst-sci-data.net/5/365/2013/essd-5-365-2013.pdf"],
                    "data_doi": "https://doi.org/10.5281/zenodo.6951",
        
        #            "related_id": ,
        
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
    for data_file in tqdm(find_files(input_path, ".txt"), desc="Processing files", disable=not verbose):
        with open(os.path.join(data_file["path"], data_file["filename"]), 'r', errors='ignore') as raw_in:
            record = raw_in.read()
        #Get the composition
        comp = data_file["filename"].split("_")[0]
        #Get the temperature
        temp = data_file["filename"].split("_")[2][:-1]
        record_metadata = {
            "mdf": {
                "title": "mpi_mainz - " + comp,
                "acl": ['public'],
    
    #            "tags": ,
    #            "description": ,
                
                "composition": comp,
    #            "raw": ,
    
                "links": {
                    #"landing_page": ,
    
    #                "publication": ,
    #                "data_doi": ,
    
    #                "related_id": ,
    
                    "txt": {
                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",
    
                        "path": "/collections/mpi_mainz/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
                },
                "mpi_mainz": {
                    "temperature": {
                        "value": temp,
                        "unit" : "K"
                        }
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
