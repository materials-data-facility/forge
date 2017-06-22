import json
import sys
import os

from tqdm import tqdm

from ..validator.schema_validator import Validator
from ..utils.file_utils import find_files

# VERSION 0.2.0

# This is the converter for CXIDB.
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
            "mdf-title": "The Coherent X-ray Imaging Data Bank",
            "mdf-acl": ["public"],
            "mdf-source_name": "cxidb",
            "mdf-citation": ["Maia, F. R. N. C. The Coherent X-ray Imaging Data Bank. Nat. Methods 9, 854–855 (2012)."],
            "mdf-data_contact": {

                "given_name": "Filipe",
                "family_name": "Maia",

                "email": "cxidb@cxidb.org",
                "institution": "Lawrence Berkeley National Laboratory",

                # IDs
                },

            "mdf-author": {

                "given_name": "Filipe",
                "family_name": "Maia",

                "institution": "Lawrence Berkeley National Laboratory",

                # IDs
                },

#            "mdf-license": ,

            "mdf-collection": "CXIDB",
            "mdf-data_format": "json",
            "mdf-data_type": "X-ray imaging",
            "mdf-tags": ["x-ray", "coherent"],

            "mdf-description": "A new database which offers scientists from all over the world a unique opportunity to access data from Coherent X-ray Imaging (CXI) experiments.",
            "mdf-year": 2012,

            "mdf-links": {

                "mdf-landing_page": "http://www.cxidb.org/",

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
            cxidb_data = json.load(file_data)
        record_metadata = {
            "mdf-title": cxidb_data["citation_title"],
            "mdf-acl": ["public"],

#            "mdf-tags": ,
#            "mdf-description": ,
            
#            "mdf-composition": ,
            "mdf-raw": json.dumps(cxidb_data),

            "mdf-links": {
                "mdf-landing_page": cxidb_data["url"],

                "mdf-publication": [cxidb_data.get("citation_DOI", None), cxidb_data.get("entry_DOI", None)],
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
