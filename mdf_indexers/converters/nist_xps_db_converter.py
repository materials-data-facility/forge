import json
import sys
import os

from tqdm import tqdm

from ..utils.file_utils import find_files
from ..validator.schema_validator import Validator

# VERSION 0.2.0

# This is the converter for the NIST XPS database
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
            "mdf-title": "NIST X-ray Photoelectron Spectroscopy Database",
            "mdf-acl": ["public"],
            "mdf-source_name": "nist_xps_db",
            "mdf-citation": ["NIST X-ray Photoelectron Spectroscopy Database, Version 4.1 (National Institute of Standards and Technology, Gaithersburg, 2012); http://srdata.nist.gov/xps/."],
            "mdf-data_contact": {

                "given_name": "Cedric",
                "family_name": "Powell",

                "email": "cedric.powell@nist.gov",
                "institution": "National Institute of Standards and Technology",

                },

            "mdf-author": [{

                "given_name": "Cedric",
                "family_name": "Powell",

                "email": "cedric.powell@nist.gov",
                "institution": "National Institute of Standards and Technology",

                },
                {

                "given_name": "Alexander",
                "family_name": "Naumkin",

                "institution": "National Institute of Standards and Technology",

                },
                {

                "given_name": "Anna",
                "family_name": "Kraut-Vass",

                "institution": "National Institute of Standards and Technology",

                },
                {

                "given_name": "Stephen",
                "family_name": "Gaarenstroom",

                "institution": "National Institute of Standards and Technology",

                },
                {
                "given_name": "Charles",
                "family_name": "Wagner"
                }],

            "mdf-license": "©2012 copyright by the U.S. Secretary of Commerce on behalf of the United States of America. All rights reserved.",

            "mdf-collection": "NIST XPS DB",
            "mdf-data_format": "text",
            "mdf-data_type": "xps",
            "mdf-tags": ["xps", "nist", "srd"],

            "mdf-description": "NIST Standard Reference Database 20",
            "mdf-year": 2000,

            "mdf-links": {

                "mdf-landing_page": "https://srdata.nist.gov/xps/Default.aspx",

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                #: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mdf-mrr": ,

            "mdf-data_contributor": [{
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
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


    dataset_validator = Validator(dataset_metadata)


    # Get the data
    for file_data in tqdm(find_files(input_path, ".json"), desc="Processing files", disable= not verbose):
        with open(os.path.join(file_data["path"], file_data["filename"]), "r") as in_file:
            record = json.load(in_file)
        id_num = file_data["filename"].rsplit("_", 1)[1].split(".", 1)[0]
        link = "https://srdata.nist.gov/xps/XPSDetailPage.aspx?AllDataNo=" + id_num
        record_metadata = {
            "mdf-title": "NIST XPS DB - " + record["Name"],
            "mdf-acl": ["public"],

#            "mdf-tags": ,
            "mdf-description": record.get("Notes"),
            
            "mdf-composition": record["Formula"],
            "mdf-raw": json.dumps(record),

            "mdf-links": {
                "mdf-landing_page": link,

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                # data links: {
 
                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #},
                },

            "mdf-citation": record.get("Citation", None),
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
