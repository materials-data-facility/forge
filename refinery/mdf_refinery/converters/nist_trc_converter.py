import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator

# VERSION 0.4.0

# This is the converter for the NIST TRC repository.
# Arguments:
#   input_path (string): The file or directory where the data resides.
#       NOTE: Do not hard-code the path to the data in the converter (the filename can be hard-coded, though). The converter should be portable.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")


    # Repository processing
    # Copied from the dataset metadata, so the comments are incorrect
    repo_metadata = {
            # REQ dictionary: MDF-format dataset metadata
            "mdf": {

                # REQ string: The title of the dataset
                "title": "Thermodynamics Research Center Alloy Data",

                # REQ list of strings: The UUIDs allowed to view this metadata, or 'public'
                "acl": ["public"],

                # REQ string: A short version of the dataset name, for quick reference. Spaces and dashes will be replaced with underscores, and other non-alphanumeric characters will be removed.
                "source_name": "nist_trc",

                # REQ dictionary: The contact person/steward/custodian for the dataset
                "data_contact": {

                    # REQ string: The person's given (or first) name
                    "given_name": "Scott",

                    # REQ string: The person's family (or last) name
                    "family_name": "Townsend",

                    # REQ string: The person's email address
                    "email": "TRCalloy@nist.gov",

                    # RCM string: The primary affiliation for the person
                    "institution": "National Institute of Standards and Technology",

                },

                # REQ list of dictionaries: The person/people contributing the tools (harvester, this converter) to ingest the dataset
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
                "citation": ["E. A. Pfeif and K. Kroenlein, Perspective: Data infrastructure for high throughput materials discovery, APL Materials 4, 053203, 2016. doi: 10.1063/1.4942634", "B. Wilthan, E.A. Pfeif, V.V. Diky, R.D. Chirico, U.R. Kattner, K. Kroenlein, Data resources for thermophysical properties of metals and alloys, Part 1: Structured data capture from the archival literature, Calphad 56, pp 126-138, 2017. doi: 10.1016/j.calphad.2016.12.004"],

                # RCM list of dictionaries: A list of the authors of this dataset
                "author": [{

                    # REQ string: The person's given (or first) name
                    "given_name": "Boris",

                    # REQ string: The person's family (or last) name
                    "family_name": "Wilthan",

                    # RCM string: The person's email address
                    "email": "boris.wilthan@nist.gov",

                    # RCM string: The primary affiliation for the person
                    "institution": "National Institute of Standards and Technology",


                },{

                    # REQ string: The person's given (or first) name
                    "given_name": "Erik",

                    # REQ string: The person's family (or last) name
                    "family_name": "Pfeif",

                    # RCM string: The person's email address
#                    "email": "",

                    # RCM string: The primary affiliation for the person
                    "institution": "National Institute of Standards and Technology",


                },{

                    # REQ string: The person's given (or first) name
                    "given_name": "Vladimir",

                    # REQ string: The person's family (or last) name
                    "family_name": "Diky",

                    # RCM string: The person's email address
#                    "email": "",

                    # RCM string: The primary affiliation for the person
                    "institution": "National Institute of Standards and Technology",


                },{

                    # REQ string: The person's given (or first) name
                    "given_name": "Robert",

                    # REQ string: The person's family (or last) name
                    "family_name": "Chirico",

                    # RCM string: The person's email address
#                    "email": "",

                    # RCM string: The primary affiliation for the person
                    "institution": "National Institute of Standards and Technology",


                },{

                    # REQ string: The person's given (or first) name
                    "given_name": "Ursula",

                    # REQ string: The person's family (or last) name
                    "family_name": "Kattner",

                    # RCM string: The person's email address
#                    "email": "",

                    # RCM string: The primary affiliation for the person
                    "institution": "National Institute of Standards and Technology",


                },{

                    # REQ string: The person's given (or first) name
                    "given_name": "Kenneth",

                    # REQ string: The person's family (or last) name
                    "family_name": "Kroenlein",

                    # RCM string: The person's email address
#                    "email": "",

                    # RCM string: The primary affiliation for the person
                    "institution": "National Institute of Standards and Technology",


                }],

                # RCM string: A link to the license for distribution of the dataset
                "license": "©2017 copyright by the US Secretary of Commerce on behalf of the United States of America. All rights reserved.",

                # RCM string: The repository (that should already be in MDF) holding the dataset
#                "repository": ,

                # RCM string: The collection for the dataset, commonly a portion of the title
                "collection": "NIST",

                # RCM list of strings: Tags, keywords, or other general descriptors for the dataset
                "tags": "alloy",

                # RCM string: A description of the dataset
                "description": "The NIST Alloy Data web application provides access to thermophysical property data with a focus on unary, binary, and ternary metal systems. All data is from original experimental publications including full provenance and uncertainty.",

                # RCM integer: The year of dataset creation
                "year": 2016,

                # REQ dictionary: Links relating to the dataset
                "links": {

                    # REQ string: The human-friendly landing page for the dataset
                    "landing_page": "http://trc.nist.gov/applications/metals_data/metals_data.php",

                    # RCM list of strings: The DOI(s) (in link form, ex. 'https://dx.doi.org/10.12345') for publications connected to the dataset
                    "publication": ["http://dx.doi.org/10.1063/1.4942634", "http://dx.doi.org/10.1016/j.calphad.2016.12.004"],

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

            # OPT dictionary: DataCite-format metadata
            "dc": {

            },


        }

    Validator(repo_metadata, resource_type="repository")

    with open(os.path.join(input_path, "nist_trc.json")) as infile:
        all_data = json.load(infile)

    refs = all_data["refs"]
    systems = all_data["systems"]
    specimens = all_data["specimen"]
    comps = all_data["comps"]

    for ref_id, ref_data in tqdm(refs.items(), desc="Processing references", disable= not verbose):
        ## Metadata:dataset
        dataset_metadata = {
            # REQ dictionary: MDF-format dataset metadata
            "mdf": {

                # REQ string: The title of the dataset
                "title": ref_data["title"],

                # REQ list of strings: The UUIDs allowed to view this metadata, or 'public'
                "acl": ["public"],

                # REQ string: A short version of the dataset name, for quick reference. Spaces and dashes will be replaced with underscores, and other non-alphanumeric characters will be removed.
                "source_name": "nist_trc_" + str(ref_id),

                # REQ dictionary: The contact person/steward/custodian for the dataset
                "data_contact": repo_metadata["mdf"]["data_contact"],

                # REQ list of dictionaries: The person/people contributing the tools (harvester, this converter) to ingest the dataset
                "data_contributor": repo_metadata["mdf"]["data_contributor"],

                # RCM list of strings: The full bibliographic citation(s) for the dataset
                "citation": ref_data["refstring"],

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

                # RCM string: The repository (that should already be in MDF) holding the dataset
                "repository": "nist_trc",

                # RCM string: The collection for the dataset, commonly a portion of the title
                "collection": "NIST",

                # RCM list of strings: Tags, keywords, or other general descriptors for the dataset
                "tags": ["alloy"] + ref_data.get("keywords", "").split(" "),

                # RCM string: A description of the dataset
#                "description": ,

                # RCM integer: The year of dataset creation
                "year": int(ref_data["year"]),

                # REQ dictionary: Links relating to the dataset
                "links": {

                    # REQ string: The human-friendly landing page for the dataset
                    "landing_page": "http://trc.nist.gov/applications/metals_data/metals_data.php#" + str(ref_id),

                    # RCM list of strings: The DOI(s) (in link form, ex. 'https://dx.doi.org/10.12345') for publications connected to the dataset
                    "publication": ["https://dx.doi.org/" + doi for doi in (ref_data.get("doi", []) if type(ref_data.get("doi", [])) is list else [ref_data.get("doi", [])] )],

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

            # OPT dictionary: DataCite-format metadata
            "dc": {

            },


        }

        # Make validator
        dataset_validator = Validator(dataset_metadata)

        # Parse out specimens for this reference
        # tqdm disabled due to speed of processing specimens
        for spec_id, spec_data in tqdm([(s_id, s_data) for s_id, s_data in specimens.items() if s_data["refid"] == ref_id], desc="Processing specimens", disable=True):
            ## Metadata:record
            try:
                record_metadata = {
                    # REQ dictionary: MDF-format record metadata
                    "mdf": {

                        # REQ string: The title of the record
                        "title": "TRC Specimen " + str(spec_id),

                        # RCM list of strings: The UUIDs allowed to view this metadata, or 'public' (defaults to the dataset ACL)
                        "acl": ["public"],

                        # RCM string: Subject material composition, expressed in a chemical formula (ex. Bi2S3)
                        "composition": comps[spec_data["cmpid"]]["formula"],

                        # RCM list of strings: Tags, keywords, or other general descriptors for the record
    #                    "tags": ,

                        # RCM string: A description of the record
    #                    "description": spec_data["descr"],

                        # RCM string: The record as a JSON string (see json.dumps())
                        "raw": json.dumps(spec_data),

                        # REQ dictionary: Links relating to the record
                        "links": {

                            # RCM string: The human-friendly landing page for the record (defaults to the dataset landing page)
    #                        "landing_page": ,

                            # RCM list of strings: The DOI(s) (in link form, ex. 'https://dx.doi.org/10.12345') for publications specific to this record
    #                        "publication": ,

                            # RCM string: The DOI of the record itself (in link form)
    #                        "data_doi": ,

                            # OPT list of strings: The mdf-id(s) of related entries, not including the dataset entry
    #                        "related_id": ,

                            # RCM dictionary: Links to raw data files from the dataset (multiple allowed, field name should be data type)
    #                        "data_link": {

                                # RCM string: The ID of the Globus Endpoint hosting the file
    #                            "globus_endpoint": ,

                                # RCM string: The fully-qualified HTTP hostname, including protocol, but without the path (for example, 'https://data.materialsdatafacility.org')
    #                            "http_host": ,

                                # REQ string: The full path to the data file on the host
    #                            "path": ,

    #                        },

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
    #                       "given_name": ,

                            # REQ string: The person's family (or last) name
    #                        "family_name": ,

                            # RCM string: The person's email address
    #                        "email": ,

                            # RCM string: The primary affiliation for the person
    #                        "institution": ,


    #                    }],

                        # OPT integer: The year of dataset creation, if different from the dataset
    #                    "year": ,

                    },

                    # OPT dictionary: DataCite-format metadata
                    "dc": {

                    },


                }
                ## End metadata
            # Ignore records without composition
            except KeyError:
                continue

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
