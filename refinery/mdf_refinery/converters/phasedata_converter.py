import json
import sys
import os

import xmltodict
from tqdm import tqdm

from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for the phasedata MDCS instance.
# Arguments:
#   input_path (string): The file or directory where the data resides.
#       NOTE: Do not hard-code the path to the data in the converter (the filename can be hard-coded, though). The converter should be portable.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Find correct dirs (source_name + _ + schema_id)
    source_name = "phasedata"
    dirs = []
    root_dir = os.path.dirname(input_path.rstrip("/"))
    for d in os.listdir(root_dir):
        if source_name in d:
            dirs.append(d)

    # Collect the metadata
    # Fields can be:
    #    REQ (Required, must be present)
    #    RCM (Recommended, should be present if possible)
    #    OPT (Optional, can be present if useful)
    # NOTE: For fields that represent people (e.g. mdf-data_contact), other IDs can be added (ex. "github": "jgaff").
    #    It is recommended that all people listed in mdf-data_contributor have a github username listed.
    #
    # If there are other useful fields not covered here, another block (dictionary at the same level as "mdf") can be created for those fields.
    # The block must be called the same thing as the source_name for the dataset.
    ## Metadata:dataset
    for schema_dir in tqdm(dirs, desc="Processing directories", disable= not verbose):
        dataset_metadata = {
            # REQ dictionary: MDF-format dataset metadata
            "mdf": {

                # REQ string: The title of the dataset
                "title": schema_dir,

                # REQ list of strings: The UUIDs allowed to view this metadata, or 'public'
                "acl": ["public"],

                # REQ string: A short version of the dataset name, for quick reference. Spaces and dashes will be replaced with underscores, and other non-alphanumeric characters will be removed.
                "source_name": schema_dir,

                # REQ dictionary: The contact person/steward/custodian for the dataset
                "data_contact": {

                    # REQ string: The person's given (or first) name
                    "given_name": "Zachary",

                    # REQ string: The person's family (or last) name
                    "family_name": "Trautt",

                    # REQ string: The person's email address
                    "email": "zachary.trautt@nist.gov",

                    # RCM string: The primary affiliation for the person
                    "institution": "National Institute of Standards and Technology",

                },

                # REQ list of dictionaries: The person/people contributing the tools (harvester, this converter) to ingest the dataset (i.e. you)
                "data_contributor": [{

                    # REQ string: The person's given (or first) name
                    "given_name": "Jonathon",

                    # REQ string: The person's family (or last) name
                    "family_name": "Gaff",

                    # REQ string: The person's email address
                    "email": "jgaff@uchicago.edu",

                    # RCM string: The primary affiliation for the person
                    "institution": "The University of Chicago",

                    # RCM string: The person's GitHub username
                    "github": "jgaff",


                }],

                # RCM list of strings: The full bibliographic citation(s) for the dataset
    #                "citation": ,

                # RCM list of dictionaries: A list of the authors of this dataset
    #                "author": [{

                    # REQ string: The person's given (or first) name
    #                    "given_name": ,

                    # REQ string: The person's family (or last) name
    #                    "family_name": ,

                    # RCM string: The person's email address
    #                    "email": ,

                    # RCM string: The primary affiliation for the person
    #                    "institution": ,


    #                }],

                # RCM string: A link to the license for distribution of the dataset
    #                "license": ,

                # RCM string: The collection for the dataset, commonly a portion of the title
                "collection": "NIST Phase-Based Data Repository",

                # RCM list of strings: Tags, keywords, or other general descriptors for the dataset
                "tags": ["mdcs", "phase data"],

                # RCM string: A description of the dataset
#                "description": ,

                # RCM integer: The year of dataset creation
#                "year": ,

                # REQ dictionary: Links relating to the dataset
                "links": {

                    # REQ string: The human-friendly landing page for the dataset
                    "landing_page": "https://phasedata.nist.gov/rest/explore/select?id=" + schema_dir.split("_")[1],

                    # RCM list of strings: The DOI(s) (in link form, ex. 'https://dx.doi.org/10.12345') for publications connected to the dataset
#                    "publication": ,

                    # RCM string: The DOI of the dataset itself (in link form)
#                    "data_doi": ,

                    # OPT list of strings: The mdf-id(s) of related entries, not including records from this dataset
#                    "related_id": ,

                    # RCM dictionary: Links to raw data files from the dataset (multiple allowed, field name should be data type)
#                    "data_link": {

                        # RCM string: The ID of the Globus Endpoint hosting the file
#                        "globus_endpoint": ,

                        # RCM string: The fully-qualified HTTP hostname, including protocol, but without the path (for example, 'https://data.materialsdatafacility.org')
#                        "http_host": ,

                        # REQ string: The full path to the data file on the host
#                        "path": ,

#                    },

                },

            },

            # OPT dictionary: MRR-format metadata
#            "mrr": {

#            },

            # OPT dictionary: DataCite-format metadata
#            "dc": {

#            },


        }
        ## End metadata 


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
        for record_file in tqdm(os.listdir(os.path.join(root_dir, schema_dir)), desc="Processing files", disable= not verbose):
            with open(os.path.join(root_dir, schema_dir, record_file)) as record_in:
                record = json.load(record_in)
            # Fields can be:
            #    REQ (Required, must be present)
            #    RCM (Recommended, should be present if possible)
            #    OPT (Optional, can be present if useful)
            ## Metadata:record
            record_metadata = {
                # REQ dictionary: MDF-format record metadata
                "mdf": {

                    # REQ string: The title of the record
                    "title": record["title"],

                    # RCM list of strings: The UUIDs allowed to view this metadata, or 'public' (defaults to the dataset ACL)
                    "acl": ["public"],

                    # RCM string: Subject material composition, expressed in a chemical formula (ex. Bi2S3)
#                    "composition": ,

                    # RCM list of strings: Tags, keywords, or other general descriptors for the record
#                    "tags": ,

                    # RCM string: A description of the record
#                    "description": ,

                    # RCM string: The record as a JSON string (see json.dumps())
                    "raw": json.dumps(xmltodict.parse(record["content"])),

                    # REQ dictionary: Links relating to the record
                    "links": {

                        # RCM string: The human-friendly landing page for the record (defaults to the dataset landing page)
                        "landing_page": "https://phasedata.nist.gov/explore/detail_result_keyword?id=" + record["_id"],

                        # RCM list of strings: The DOI(s) (in link form, ex. 'https://dx.doi.org/10.12345') for publications specific to this record
#                        "publication": ,

                        # RCM string: The DOI of the record itself (in link form)
#                        "data_doi": ,

                        # OPT list of strings: The mdf-id(s) of related entries, not including the dataset entry
#                        "related_id": ,

                        # RCM dictionary: Links to raw data files from the dataset (multiple allowed, field name should be data type)
                        "data_link": {

                            # RCM string: The ID of the Globus Endpoint hosting the file
#                            "globus_endpoint": ,

                            # RCM string: The fully-qualified HTTP hostname, including protocol, but without the path (for example, 'https://data.materialsdatafacility.org')
                            "http_host": "https://phasedata.nist.gov",

                            # REQ string: The full path to the data file on the host
                            "path": "/rest/explore/select?id=" + record["_id"],

                        },

                    },

                    # OPT list of strings: The full bibliographic citation(s) for the record, if different from the dataset
#                    "citation": ,

                    # OPT dictionary: The contact person/steward/custodian for the record, if different from the dataset
#                    "data_contact": {

                        # REQ string: The person's given (or first) name
#                        "given_name": ,

                        # REQ string: The person's family (or last) name
#                        "family_name": ,

                        # REQ string: The person's email address
#                        "email": ,

                        # RCM string: The primary affiliation for the person
#                        "institution": ,

#                    },

                    # OPT list of dictionaries: A list of the authors of this record, if different from the dataset
#                    "author": [{

                        # REQ string: The person's given (or first) name
#                        "given_name": ,

                        # REQ string: The person's family (or last) name
#                        "family_name": ,

                        # RCM string: The person's email address
#                        "email": ,

                        # RCM string: The primary affiliation for the person
#                        "institution": ,


#                    }],

                    # OPT integer: The year of dataset creation, if different from the dataset
#                    "year": ,

                }

                # OPT dictionary: DataCite-format metadata
#                "dc": {

#                },


            }
            ## End metadata

            # Pass each individual record to the Validator
            result = dataset_validator.write_record(record_metadata)

            # Check if the Validator accepted the record, and stop processing if it didn't
            # If the Validator returns "success" == True, the record was written successfully
            if not result["success"]:
                if not dataset_validator.cancel_validation()["success"]:
                    print("Error cancelling validation. The partial feedstock may not be removed.")
                raise ValueError(result["message"] + "\n" + result.get("details", ""))


    # You're done!
    if verbose:
        print("Finished converting")
