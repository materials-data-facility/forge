import json
import sys
import os

from tqdm import tqdm

from ..validator.schema_validator import Validator
from ..utils.file_utils import find_files

# VERSION 0.2.0

# This is the converter for the NIST-JANAF Thermochemical tables.
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
            "mdf-title": "NIST-JANAF Thermochemical Tables",
            "mdf-acl": ["public"],
            "mdf-source_name": "nist_janaf",
            "mdf-citation": ["M. W. Chase, Jr., JANAF Thermochemical Tables Third Edition, J. Phys. Chem. Ref. Data, Vol. 14, Suppl. 1, 1985."],
            "mdf-data_contact": {

                "given_name": "Malcolm",
                "family_name": "Chase, Jr.",

#                "email": ,
                "institution": "National Institute of Standards and Technology",

                },

            "mdf-author": {

                "given_name": "Malcolm",
                "family_name": "Chase, Jr.",

#                "email": ,
                "institution": "National Institute of Standards and Technology",

                },

            "mdf-license": "Copyright 1986 by the U.S. Department of Commerce on behalf of the United States. All rights reserved.",

            "mdf-collection": "NIST-JANAF",
            "mdf-data_format": "json",
            "mdf-data_type": "thermochemical",
            "mdf-tags": "Thermochemical",

            "mdf-description": "DISCLAIMER: NIST uses its best efforts to deliver a high quality copy of the Database and to verify that the data contained therein have been selected on the basis of sound scientific judgement. However, NIST makes no warranties to that effect, and NIST shall not be liable for any damage that may result from errors or omissions in the Database.",
            "mdf-year": 1985,

            "mdf-links": {

                "mdf-landing_page": "http://kinetics.nist.gov/janaf/",

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
    for entry in tqdm(find_files(input_path, ".*[0-9]\.json$"), desc="Processing data", disable= not verbose):
        with open(os.path.join(entry["path"], entry["filename"])) as in_file:
            data = json.load(in_file)
        record_metadata = {
            "mdf-title": "NIST-JANAF - " + data['identifiers']['chemical formula'] + " " + data['identifiers']['state'],
            "mdf-acl": ["public"],

#            "mdf-tags": ,
#            "mdf-description": ,
            
            "mdf-composition": data['identifiers']['molecular formula'],
#            "mdf-raw": ,

            "mdf-links": {
                "mdf-landing_page": "http://kinetics.nist.gov/janaf/html/" + entry["filename"].replace("srd13_", "").replace(".json", ".html"),

#                "mdf-publication": ,
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

 #           "mdf-license": ,
#            "mdf-collection": ,
#            "mdf-data_format": ,
#            "mdf-data_type": ,
#            "mdf-year": ,

#            "mdf-mrr":

#            "mdf-processing": ,
#            "mdf-structure":,
            "state": "".join([data["state definitions"][st] + ", " for st in data['identifiers']['state'].split(",")])
            }

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])


    if verbose:
        print("Finished converting")
