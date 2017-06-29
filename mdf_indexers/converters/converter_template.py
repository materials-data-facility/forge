import json
import sys

from ..validator.schema_validator import Validator

# VERSION 0.2.1

# This is the template for new converters. It is not a complete converter. Incomplete parts are labelled with "TODO"
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
    # TODO: Make sure the metadata is present in some form.
    # Fields can be:
    #    REQ (Required, must be present)
    #    RCM (Recommended, should be present if possible)
    #    OPT (Optional, can be present if useful)
    # NOTE: For fields that represent people (e.g. mdf-data_contact), other IDs can be added (ex. "github": "jgaff").
    #    It is recommended that all people listed in mdf-data_contributor have a github username listed.
    if not metadata:
        ## Metadata:dataset
        dataset_metadata = {
            # REQ string: The title of the dataset
            "mdf-title": ,

            # REQ list of strings: The UUIDs allowed to view this metadata, or 'public'
            "mdf-acl": ,

            # REQ string: A short version of the dataset name, for quick reference, with underscores instead of spaces
            "mdf-source_name": ,

            # REQ list of strings: The full bibliographic citation(s) for the dataset
            "mdf-citation": ,

            # REQ dictionary: The contact person/steward/custodian for the dataset
            "mdf-data_contact": {

                # REQ string: The person's given (or first) name
                "given_name": ,

                # REQ string: The person's family (or last) name
                "family_name": ,

                # RCM string: The person's email address
                "email": ,

                # RCM string: The primary affiliation for the person
                "institution": ,

            },

            # RCM list of dictionaries: A list of the authors of this dataset
            "mdf-author": [{

                # REQ string: The person's given (or first) name
                "given_name": ,

                # REQ string: The person's family (or last) name
                "family_name": ,

                # RCM string: The person's email address
                "email": ,

                # RCM string: The primary affiliation for the person
                "institution": ,


            }],

            # RCM string: A link to the license for distribution of the dataset
            "mdf-license": ,

            # RCM string: The collection for the dataset, commonly a portion of the title
            "mdf-collection": ,

            # RCM list of strings: The file format(s) of the data (for example, 'OUTCAR')
            "mdf-data_format": ,

            # RCM list of strings: The broad categorization(s) of the data (for example, 'DFT')
            "mdf-data_type": ,

            # RCM list of strings: Tags, keywords, or other general descriptors for the dataset
            "mdf-tags": ,

            # RCM string: A description of the dataset
            "mdf-description": ,

            # RCM integer: The year of dataset creation
            "mdf-year": ,

            # REQ dictionary: Links relating to the dataset
            "mdf-links": {

                # REQ string: The human-friendly landing page for the dataset
                "mdf-landing_page": ,

                # RCM list of strings: The DOI(s) (in link form, ex. 'https://dx.doi.org/10.12345') for publications connected to the dataset
                "mdf-publication": ,

                # RCM string: The DOI of the dataset itself (in link form)
                "mdf-dataset_doi": ,

                # OPT list of strings: The mdf-id(s) of related entries, not including records from this dataset
                "mdf-related_id": ,

                # RCM dictionary: Links to raw data files from the dataset (multiple allowed, field name should be data type)
                "data_link": {

                    # RCM string: The ID of the Globus Endpoint hosting the file
                    "globus_endpoint": ,

                    # RCM string: The fully-qualified HTTP hostname, including protocol, but without the path (for example, 'https://data.materialsdatafacility.org')
                    "http_host": ,

                    # REQ string: The full path to the data file on the host
                    "path": ,

                },

            },

            # OPT dictionary: Fields relating the the NIST Materials Resource Registry system
            "mdf-mrr": {

            },

            # OPT list of dictionaries: The person/people contributing the tools (harvester, this converter) to ingest the dataset (i.e. you)
            "mdf-data_contributor": [{

                # REQ string: The person's given (or first) name
                "given_name": ,

                # REQ string: The person's family (or last) name
                "family_name": ,

                # RCM string: The person's email address
                "email": ,

                # RCM string: The primary affiliation for the person
                "institution": ,


            }],


        }
        ## End metadata
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
    #    Each record also needs its own metadata
    for record in your_records:
        # Fields can be:
        #    REQ (Required, must be present)
        #    RCM (Recommended, should be present if possible)
        #    OPT (Optional, can be present if useful)
        ## Metadata:record
        record_metadata = {
            # REQ string: The title of the record
            "mdf-title": ,

            # RCM list of strings: The UUIDs allowed to view this metadata, or 'public'. Defaults to the dataset ACL.
            "mdf-acl": ,

            # RCM list of strings: Tags, keywords, or other specific descriptors for the record not in the dataset tags
            "mdf-tags": ,

            # RCM string: A description of the record
            "mdf-description": ,

            # RCM string: Subject material composition, expressed in a chemical formula (ex. Bi2S3)
            "mdf-composition": ,

            # RCM string: The record as a JSON string (see json.dumps())
            "mdf-raw": ,

            # REQ dictionary: Links relating to the record
            "mdf-links": {

                # RCM string: The human-friendly landing page for the record
                "mdf-landing_page": ,

                # OPT list of strings: The DOI(s) (in link form, ex. 'https://dx.doi.org/10.12345') for publications connected to the record, if different from the dataset
                "mdf-publication": ,

                # OPT string: The DOI of the record itself (in link form), if separate from the dataset
                "mdf-dataset_doi": ,

                # OPT string: The mdf-id of this record's dataset
                "mdf-parent_id": ,

                # OPT list of strings: The mdf-id(s) of related entries
                "mdf-related_id": ,

                # RCM dictionary: Links to raw data files from the dataset (multiple allowed, field name should be data type)
                "data_link": {

                    # RCM string: The ID of the Globus Endpoint hosting the file
                    "globus_endpoint": ,

                    # RCM string: The fully-qualified HTTP hostname, including protocol, but without the path (for example, 'https://data.materialsdatafacility.org')
                    "http_host": ,

                    # REQ string: The full path to the data file on the host
                    "path": ,

                },

            },

            # OPT list of strings: The full bibliographic citation(s) for the record, if different from the dataset
            "mdf-citation": ,

            # OPT dictionary: The contact person/steward/custodian for the record, if different from the dataset
            "mdf-data_contact": {

                # REQ string: The person's given (or first) name
                "given_name": ,

                # REQ string: The person's family (or last) name
                "family_name": ,

                # RCM string: The person's email address
                "email": ,

                # RCM string: The primary affiliation for the person
                "institution": ,

            },

            # OPT list of dictionaries: A list of the authors of this record, if different from the dataset
            "mdf-author": [{

                # REQ string: The person's given (or first) name
                "given_name": ,

                # REQ string: The person's family (or last) name
                "family_name": ,

                # RCM string: The person's email address
                "email": ,

                # RCM string: The primary affiliation for the person
                "institution": ,


            }],

            # OPT string: A link to the license for distribution of the record, if different from the dataset
            "mdf-license": ,

            # OPT string: The collection for the record, if different from the dataset
            "mdf-collection": ,

            # OPT list of strings: The file format(s) of the data (for example, 'vasp'), if different from the dataset
            "mdf-data_format": ,

            # OPT list of strings: The broad categorization(s) of the data (for example, DFT), if different from the dataset
            "mdf-data_type": ,

            # OPT integer: The year of record creation, if different from the dataset
            "mdf-year": ,

            # OPT dictionary: Fields relating the the NIST Materials Resource Registry system
            "mdf-mrr": {

            },


        }
        ## End metadata

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
