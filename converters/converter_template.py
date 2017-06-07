import json
import sys
from validation.validator import Validator

# VERSION 0.2.0

# This is the template for new converters. It is not a complete converter. Incomplete parts are labelled with "TODO"
# Arguments:
#   input_path (string): The file or directory where the data resides.
#       NOTE: Do not hard-code the path to the data in the converter, so that the converter is portable.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    # TODO: Make sure the metadata is present in some form.
    # Fields can be:
    #    REQ (Required, must be present)
    #    RCM (Recommended, should be present if possible)
    #    OPT (Optional, can be present if useful)
    if not metadata:
        dataset_metadata = {
            "mdf-title": ,          # REQ string: The title of the dataset
            "mdf-acl": ,            # REQ list of strings: The UUIDs allowed to view this dataset, or ['public']
            "mdf-source_name": ,    # REQ string: A short version of the dataset name, for quick reference, with underscores instead of spaces
            "mdf-citation": ,       # REQ list of strings: The full bibliographic citation(s) for the dataset
            "mdf-data_contact": ,   # REQ string: The contact person/steward/custodian for the dataset

            "mdf-author": ,         # RCM list of strings: The author(s) of the dataset
            "mdf-license": ,        # RCM string: A link to the license for distribution of this dataset

            "mdf-collection": ,     # RCM string: The collection for the dataset, commonly a portion of the title
            "mdf-data_format": ,    # RCM list of strings: The file format(s) of the data (ex. 'OUTCAR')
            "mdf-data_type": ,      # RCM list of strings: The broad categorization(s) of the data (ex. DFT)
            "mdf-tags": ,           # RCM list of strings: Tags, keywords, or other general descriptors for the dataset

            "mdf-description": ,    # RCM string: A description of the dataset
            "mdf-year": ,           # RCM integer: The year of dataset creation

            "mdf-links": {          # REQ dictionary: Links relating to the dataset

                "mdf-landing_page": ,   # REQ string: The human-friendly landing page for the dataset

                "mdf-publication": ,    # RCM list of strings: The DOI(s) (in link form, ex. 'https://dx.doi.org/10.12345') for publications connected to the dataset
                "mdf-dataset_doi": ,    # RCM string: The DOI of the dataset itself, in link form

                "mdf-related_id": ,     # OPT list of strings: The mdf-id(s) of related entries, not including records from this dataset

                # data links: {         # RCM dictionary: A link to a raw data file from the dataset (the key should be the file type, ex. 'tiff')
                                            # Required fields are only required if a data link is present
                    #"globus_endpoint": ,   # REQ string: The ID of the Globus Endpoint hosting the file
                    #"http_host": ,         # RCM string: The fully-qualified HTTP hostname, including protocol, but without the path (ex. 'https://data.materialsdatafacility.org')

                    #"path": ,              # REQ string: The full path to the data file on the host (ex. '/data/file.txt')
                    #}
                },

            "mdf-mrr":              # OPT dictionary: Fields relating to the NIST Materials Resource Registry system
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
    # TODO: Write the code to convert your dataset's records into JSON-serializable Python dictionaries
    #    Each record should be exactly one dictionary
    #    You must write your records using the Validator one at a time
    #    It is recommended that you use a parser to help with this process if one is available for your datatype

    # Each record also needs its own metadata
    for record in your_records:
        # TODO: Fill in these dictionary fields for each record
        # Fields can be:
        #    REQ (Required, must be present)
        #    RCM (Recommended, should be present if possible)
        #    OPT (Optional, can be present if useful)
        record_metadata = {
            "mdf-title": ,          # REQ string: The title of the record
            "mdf-acl": ,            # RCM list of strings: The UUIDs allowed to view this record, or ['public'] (default is dataset setting)

            "mdf-tags": ,           # RCM list of strings: Tags, keywords, or other general descriptors for the record, separate from the dataset tags
            "mdf-description": ,    # RCM string: A description of the record
            
            "mdf-composition": ,    # RCM string: Subject material composition, expressed in a chemical formula (ex. Bi2S3)
            "mdf-raw": ,            # RCM string: The record, converted to JSON, in a string (see json.dumps())

            "mdf-links": {          # REQ dictionary: Links relating to the record
                "mdf-landing_page": ,   # RCM string: The human-friendly landing page for the record (default is dataset page)

                "mdf-publication": ,    # OPT list of strings: The DOI link(s) of record-connected publications, if different from the dataset
                "mdf-dataset_doi": ,    # OPT string: The DOI of the dataset itself, in link form

                "mdf-related_id": ,     # OPT list of strings: The mdf-id(s) of related entries, not including the parent dataset

                # data links: {         # RCM dictionary: A link to a raw data file from the dataset (the key should be the file type, ex. 'tiff')
                                            # Required fields are only required if a data link is present
                    #"globus_endpoint": ,   # REQ string: The ID of the Globus Endpoint hosting the file
                    #"http_host": ,         # RCM string: The fully-qualified HTTP hostname, including protocol, but without the path (ex. 'https://data.materialsdatafacility.org')

                    #"path": ,              # REQ string: The full path to the data file on the host (ex. '/data/file.txt')
                    #},

            "mdf-citation": ,       # OPT list of strings: Record citation(s), if different from the dataset
            "mdf-data_contact": ,   # OPT string: Record contact person/steward/custodian, if different from the dataset
            "mdf-author": ,         # OPT list of strings: Record author(s), if different from the dataset
            "mdf-license": ,        # OPT string: Record license, if different from the dataset
            "mdf-collection": ,     # OPT string: Record collection, if different from the dataset
            "mdf-data_format": ,    # OPT list of strings: Record file format, if different from the dataset
            "mdf-data_type": ,      # OPT list of strings: Record data type, if different from the dataset
            "mdf-year": ,           # OPT integer: Record creation year, if different from the dataset

            "mdf-mrr":              # OPT dictionary: Fields relating to the NIST Materials Resource Registry system

#            "mdf-processing": ,     # OPT undefined: Processing information
#            "mdf-structure":,       # OPT undefined: Structure information
            }

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])


    # TODO: Save your converter as [mdf-source_name]_converter.py
    # You're done!
    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# It is not guaranteed that this is the way the converter will be called in actual use
# This is why the 'input_path' should not be hard-coded, and the script should have no output if 'verbose' is False
if __name__ == "__main__":
    convert()
