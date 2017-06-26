import json
import sys
import os
from ..utils.file_utils import find_files
from tqdm import tqdm
from ..validator.schema_validator import Validator

# VERSION 0.2.0

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
            "mdf-title": "The MPI-Mainz UV/VIS Spectral Atlas of Gaseous Molecules",
            "mdf-acl": ['public'],
            "mdf-source_name": "mpi_mainz",
            "mdf-citation": ["Keller-Rudek, H. M.-P. I. for C. M. G., Moortgat, G. K. M.-P. I. for C. M. G., Sander, R. M.-P. I. for C. M. G., & Sörensen, R. M.-P. I. for C. M. G. (2013). The MPI-Mainz UV/VIS Spectral Atlas of Gaseous Molecules [Data set]. Zenodo. http://doi.org/10.5281/zenodo.6951,"],
            "mdf-data_contact": {

                "given_name": "Keller-Rudek",
                "family_name": "Hannelore",
                
                "email": "hannelore.keller-rudek@mpic.de",
                "instituition": "Max-Planck Institute for Chemistry"

                },

            "mdf-author": [{
                
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

            "mdf-license": "https://creativecommons.org/licenses/by/4.0/",

            "mdf-collection": "UV/VIS Spectral Atlas",
            "mdf-data_format": ["txt"],
#            "mdf-data_type": ,
            "mdf-tags": ["cross sections", "quantum yields"],

            "mdf-description": "This archive contains a frozen snapshot of all cross section and quantum yield data files from the MPI-Mainz UV/VIS Spectral Atlas of Gaseous Molecules.",
            "mdf-year": 2013,

            "mdf-links": {

                "mdf-landing_page": "https://doi.org/10.5281/zenodo.6951",

                "mdf-publication": ["http://www.earth-syst-sci-data.net/5/365/2013/essd-5-365-2013.pdf"],
                "mdf-dataset_doi": "https://doi.org/10.5281/zenodo.6951",

#                "mdf-related_id": ,

                # data links: {
                
                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
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
    for data_file in tqdm(find_files(input_path, ".txt"), desc="Processing files", disable=not verbose):
        with open(os.path.join(data_file["path"], data_file["filename"]), 'r', errors='ignore') as raw_in:
            record = raw_in.read()
        #Get the composition
        in1 = data_file["filename"].find("_")
        comp = data_file["filename"][:in1]
        #Get the temperature
        later = data_file["filename"][in1+1:]
        second = later.find("_")
        last = later[second+1:]
        third = last.find("_")
        temp = last[:third-1]
        uri = "https://data.materialsdatafacility.org/collections/" + "mpi_mainz/" + data_file["no_root_path"] + "/" + data_file["filename"]
        record_metadata = {
            "mdf-title": "mpi_mainz - " + data_file["filename"],
            "mdf-acl": ['public'],

#            "mdf-tags": ,
#            "mdf-description": ,
            
            "mdf-composition": comp,
#            "mdf-raw": ,

            "mdf-links": {
                "mdf-landing_page": uri,

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                "data_links": {
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    #"http_host": ,

                    "path": "/collections/mpi_mainz/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
            "temperature": {
                "value": temp,
                "unit" : "K"
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
