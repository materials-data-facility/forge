import json
import sys
import os

from tqdm import tqdm

from ..validator.schema_validator import Validator
from ..utils.file_utils import find_files

# VERSION 0.2.0

# This is the converter for MATIN
# Arguments:
#   input_path (string): The file or directory where the data resides.
#       NOTE: Do not hard-code the path to the data in the converter. The converter should be portable.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        dataset_metadata = {
            "mdf-title": "MATerials Innovation Network",
            "mdf-acl": ["public"],
            "mdf-source_name": "matin",
            "mdf-citation": ["https://matin.gatech.edu/"],
            "mdf-data_contact": {

                "given_name": "Aleksandr",
                "family_name": "Blekh",

                "email": "aleksandr.blekh@gatech.edu",
                "institution": "Georgia Institute of Technology",

                },

            "mdf-author": {

                "given_name": "Aleksandr",
                "family_name": "Blekh",

                "email": "aleksandr.blekh@gatech.edu",
                "institution": "Georgia Institute of Technology",

                },

#            "mdf-license": ,

            "mdf-collection": "MATIN",
            "mdf-data_format": "json",
            "mdf-data_type": "metadata",
            "mdf-tags": "materials",

            "mdf-description": "An e-collaboration platform for accelerating materials innovation",
#            "mdf-year": ,

            "mdf-links": {

                "mdf-landing_page": "https://matin.gatech.edu/",

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mdf-mrr": ,

            "mdf-data_contributor": {
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
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


    dataset_validator = Validator(dataset_metadata)


    # Get the data
    for dir_data in tqdm(find_files(input_path, file_pattern="json", verbose=verbose), desc="Processing metadata", disable= not verbose):
        with open(os.path.join(dir_data["path"], dir_data["filename"])) as file_data:
            full_record = json.load(file_data)
        matin_data = full_record["metadata"]["oai_dc:dc"]
        record_metadata = {
            "mdf-title": matin_data.get("dc.title", "MATIN Entry " + dir_data["filename"].split("_")[0]),
            "mdf-acl": ["public"],

            "mdf-tags": matin_data.get("dc:subject", []),
            "mdf-description": matin_data.get("dc:description", ""),
            
#            "mdf-composition": ,
            "mdf-raw": json.dumps(full_record),

            "mdf-links": {
                "mdf-landing_page": matin_data.get("dc.identifier", full_record["header"]["identifier"]),

                "mdf-publication": matin_data.get("dc:relation", []),
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                # data links: {
 
                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #},
                },

#            "mdf-citation": ,
#            "mdf-data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

                # IDs
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


    if verbose:
        print("Finished converting")
