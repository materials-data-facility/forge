import json
import sys
import os

from tqdm import tqdm

from mdf_forge.toolbox import find_files
from mdf_refinery.validator import Validator

# VERSION 0.3.0

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
        "mdf": {
            "title": "NIST X-ray Photoelectron Spectroscopy Database",
            "acl": ["public"],
            "source_name": "nist_xps_db",
            "citation": ["NIST X-ray Photoelectron Spectroscopy Database, Version 4.1 (National Institute of Standards and Technology, Gaithersburg, 2012); http://srdata.nist.gov/xps/."],
            "data_contact": {

                "given_name": "Cedric",
                "family_name": "Powell",

                "email": "cedric.powell@nist.gov",
                "institution": "National Institute of Standards and Technology",

                },

            "author": [{

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

            "license": "©2012 copyright by the U.S. Secretary of Commerce on behalf of the United States of America. All rights reserved.",

            "collection": "NIST XPS DB",
            "tags": ["xps", "nist", "srd"],

            "description": "NIST Standard Reference Database 20",
            "year": 2000,

            "links": {

                "landing_page": "https://srdata.nist.gov/xps/Default.aspx",

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                #: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mrr": ,

            "data_contributor": [{
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
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


    dataset_validator = Validator(dataset_metadata)


    # Get the data
    for file_data in tqdm(find_files(input_path, ".json"), desc="Processing files", disable= not verbose):
        with open(os.path.join(file_data["path"], file_data["filename"]), "r") as in_file:
            record = json.load(in_file)
        id_num = file_data["filename"].rsplit("_", 1)[1].split(".", 1)[0]
        link = "https://srdata.nist.gov/xps/XPSDetailPage.aspx?AllDataNo=" + id_num
        record_metadata = {
        "mdf": {
            "title": "NIST XPS DB - " + record["Name"],
            "acl": ["public"],

#            "tags": ,
            "description": record.get("Notes"),
            
            "composition": record["Formula"],
            "raw": json.dumps(record),

            "links": {
                "landing_page": link,

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {
 
                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #},
                },

#            "citation": record.get("Citation", None),
#            "data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

                # IDs
#                },

#            "author": ,

#            "license": ,
#            "collection": ,
#            "data_format": ,
#            "data_type": ,
#            "year": ,

#            "mrr":

#            "processing": ,
#            "structure":,
            }
        }
        if record.get("Citation", None):
            record_metadata["mdf"]["citation"] = record["Citation"]

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])


    if verbose:
        print("Finished converting")
