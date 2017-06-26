import json
import sys

from ..validator.schema_validator import Validator

# VERSION 0.2.0

# This is the converter for datasets that cannot be meaningfully deeply indexed.
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
            "mdf-title": ,
            "mdf-acl": ,
            "mdf-source_name": ,
            "mdf-citation": ,
            "mdf-data_contact": {

                "given_name": ,
                "family_name": ,

                "email": ,
                "institution": ,

                # IDs
                },

            "mdf-author": ,

            "mdf-license": ,

            "mdf-collection": ,
            "mdf-data_format": ,
            "mdf-data_type": ,
            "mdf-tags": ,

            "mdf-description": ,
            "mdf-year": ,

            "mdf-links": {

                "mdf-landing_page": ,

                "mdf-publication": ,
                "mdf-dataset_doi": ,

                "mdf-related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

            "mdf-mrr": ,

            "mdf-data_contributor":
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

    Validator(dataset_metadata)

    if verbose:
        print("Finished converting")
