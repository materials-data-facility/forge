import json
import sys
import os

from schema_validator_copy import Validator
import parser_example


# VERSION 0.2.0

# This is an example of a converter.
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
    # Make sure the metadata is present in some form.
    # Fields can be:
    #    REQ (Required, must be present)
    #    RCM (Recommended, should be present if possible)
    #    OPT (Optional, can be present if useful)
    if not metadata:
        dataset_metadata = {
            "mdf-title": "Dataset Example",                                 # REQ string: The title of the dataset
            "mdf-acl": ["public"],                                          # REQ list of strings: The UUIDs allowed to view this dataset, or ['public']
            "mdf-source_name": "example",                                   # REQ string: A short version of the dataset name, for quick reference, with underscores instead of spaces
            "mdf-citation": ["A.B. Chesterson, On Examples, 2017"],         # REQ list of strings: The full bibliographic citation(s) for the dataset
            "mdf-data_contact": {                                           # REQ dictionary: The contact person/steward/custodian for the dataset

                "given_name": "Ben",                                            # REQ string: The person's given (or first) name
                "family_name": "Blaiszik",                                      # REQ string: The person's family (or last) name

                "email": "blaszik@uchicago.edu",                                # RCM string: The person's email address
                "institution": "The University of Chicago",                     # RCM string: The primary affiliation for the person

                },

            "mdf-author": [                                                 # RCM list of dictionaries: The author(s) of the dataset
                                                                                # Same fields as mdf-data_contact
                {
                "given_name": "Ben",
                "family_name": "Blaiszik",

                "email": "blaiszik@uchicago.edu",
                "institution": "The University of Chicago"
                },
                {
                "given_name": "Jonathon",
                "family_name": "Gaff",

                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago"
                },
                {
                "given_name": "Alfred",
                "family_name": "Chesterson",

                "email": "abc@example.com",
                "institution": "Exemplar University"
                }],

            "mdf-license": "https://creativecommons.org/licenses/by/4.0/",  # RCM string: A link to the license for distribution of this dataset

            "mdf-collection": "Examples",                                   # RCM string: The collection for the dataset, commonly a portion of the title
            "mdf-data_format": "dat",                                       # RCM list of strings: The file format(s) of the data (ex. 'OUTCAR')
            "mdf-data_type": "Sample",                                      # RCM list of strings: The broad categorization(s) of the data (ex. DFT)
            "mdf-tags": ["example", "sample"],                              # RCM list of strings: Tags, keywords, or other general descriptors for the dataset

            "mdf-description": "An example dataset",                        # RCM string: A description of the dataset
            "mdf-year": 2017,                                               # RCM integer: The year of dataset creation

            "mdf-links": {                                                  # REQ dictionary: Links relating to the dataset

                "mdf-landing_page": "https://www.materialsdatafacility.org",    # REQ string: The human-friendly landing page for the dataset

                "mdf-publication": ["https://doi.org/10.5555/12345678"],        # RCM list of strings: The DOI(s) (in link form, ex. 'https://dx.doi.org/10.12345') for publications connected to the dataset
                "mdf-dataset_doi": "https://dx.doi.org/10.555-555-5555",        # RCM string: The DOI of the dataset itself, in link form

                "mdf-related_id": ["505050505050b"],                            # OPT list of strings: The mdf-id(s) of related entries, not including records from this dataset

                "zip": {                                                        # RCM dictionary: A link to a raw data file from the dataset (the key should be the file type, ex. 'tiff')
                                                                                    # Required fields are only required if a data link is present

                    "globus_endpoint": "abcdefg123",                                # REQ string: The ID of the Globus Endpoint hosting the file
                    "http_host": "https://example.com",                             # RCM string: The fully-qualified HTTP hostname, including protocol, but without the path (ex. 'https://data.materialsdatafacility.org')

                    "path": "/examples/samples/5/data.zip",                         # REQ string: The full path to the data file on the host (ex. '/data/file.txt')
                    }
                },

#            "mdf-mrr": ,                                                   # OPT dictionary: Fields relating to the NIST Materials Resource Registry system

            "mdf-data_contributor": [                                       # OPT list of dictionaries: The person/people contributing the tools (harvester, this converter) to ingest the dataset (i.e. you)
                                                                                # Same fields as mdf-data_contact
                                                                                # It is strongly recommended that you include your GitHub username as an ID
                {
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



    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    # If the metadata is incorrect, the constructor will throw an exception and the program will exit
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    # Write the code to convert your dataset's records into JSON-serializable Python dictionaries
    #    Each record should be exactly one dictionary
    #    You must write your records using the Validator one at a time
    #    It is recommended that you use a parser to help with this process if one is available for your datatype
    #    Each record also needs its own metadata
    with open(os.path.join(input_path, "data.dat")) as data_file:
        for record in parser_example.parse(data_file):
            # Fields can be:
            #    REQ (Required, must be present)
            #    RCM (Recommended, should be present if possible)
            #    OPT (Optional, can be present if useful)
            record_metadata = {
                "mdf-title": "Example - " + record["composition"],          # REQ string: The title of the record
                "mdf-acl": ["public"],            # RCM list of strings: The UUIDs allowed to view this record, or ['public'] (default is dataset setting)

                "mdf-tags": [record["tag"]],           # RCM list of strings: Tags, keywords, or other general descriptors for the record, separate from the dataset tags
                "mdf-description": record["desc"],    # RCM string: A description of the record
                
                "mdf-composition": record["composition"],    # RCM string: Subject material composition, expressed in a chemical formula (ex. Bi2S3)
                "mdf-raw": json.dumps(record),            # RCM string: The record, converted to JSON, in a string (see json.dumps())

                "mdf-links": {          # REQ dictionary: Links relating to the record
                    "mdf-landing_page": record["url"],   # RCM string: The human-friendly landing page for the record (default is dataset page)

#                    "mdf-publication": ,    # OPT list of strings: The DOI link(s) of record-connected publications, if different from the dataset
#                    "mdf-dataset_doi": ,    # OPT string: The DOI of the dataset itself, in link form

#                    "mdf-related_id": ,     # OPT list of strings: The mdf-id(s) of related entries, not including the parent dataset

                    "txt": {         # RCM dictionary: A link to a raw data file from the dataset (the key should be the file type, ex. 'tiff')
                                                # Required fields are only required if a data link is present
                        "globus_endpoint": "abcdefg123",   # REQ string: The ID of the Globus Endpoint hosting the file
                        "http_host": "https://example.com",         # RCM string: The fully-qualified HTTP hostname, including protocol, but without the path (ex. 'https://data.materialsdatafacility.org')

                        "path": "/examples/sample/" + record["id"],              # REQ string: The full path to the data file on the host (ex. '/data/file.txt')
                        },
                    },

#                "mdf-citation": ,       # OPT list of strings: Record citation(s), if different from the dataset
#                "mdf-data_contact": {   # OPT dictionary: Record contact person/steward/custodian, if different from the dataset
                                            # As usual, required fields are only required if the parent field is present

#                    "given_name": ,         # REQ string: The person's given (or first) name
#                    "family_name": ,        # REQ string: The person's family (or last) name

#                    "email": ,              # RCM string: The person's email address
#                    "institution":,         # RCM string: The primary affiliation for the person

                    # IDs                   # RCM strings: IDs for the person, with the ID type as the field name (ex. "ORCID": "12345")
#                    },

#                "mdf-author": ,         # OPT list of dictionaries: Record author(s), if different from the dataset
                                            # Same fields as mdf-data_contact
#                "mdf-license": ,        # OPT string: Record license, if different from the dataset
#                "mdf-collection": ,     # OPT string: Record collection, if different from the dataset
#                "mdf-data_format": ,    # OPT list of strings: Record file format, if different from the dataset
#                "mdf-data_type": ,      # OPT list of strings: Record data type, if different from the dataset
#                "mdf-year": ,           # OPT integer: Record creation year, if different from the dataset

#                "mdf-mrr":              # OPT dictionary: Fields relating to the NIST Materials Resource Registry system

    #            "mdf-processing": ,     # OPT undefined: Processing information
    #            "mdf-structure":,       # OPT undefined: Structure information

                "other_useful_data": record["useful"]
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

# Here is a convenience call. Real converters cannot be called directly like this.
if __name__ == "__main__":
    print("Running example converter...")
    print("Please note that real converters must be called through a separate script.")
    convert(".", verbose=True)
    print("For this example, the feedstock was written out to this directory. Real feedstock can be found in the feedstock directory.")

