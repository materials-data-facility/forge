import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Distributed Structure-Searchable Toxicity (DSSTox) Database
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
    # NOTE: For fields that represent people (e.g. mdf-data_contact), other IDs can be added (ex. "github": "jgaff").
    #    It is recommended that all people listed in mdf-data_contributor have a github username listed.
    #
    # If there are other useful fields not covered here, another block (dictionary at the same level as "mdf") can be created for those fields.
    # The block must be called the same thing as the source_name for the dataset.
    if not metadata:
        ## Metadata:dataset
        dataset_metadata = {
            "mdf": {

                "title": "Distributed Structure-Searchable Toxicity (DSSTox) Database",
                "acl": ["public"],
                "source_name": "dss_tox",

                "data_contact": {
                    
                    "given_name": "Dayna",
                    "family_name": "Gibbons",
                    "email": "gibbons.dayna@epa.gov",
                    "institution": "US EPA Research",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Richard, A M. AND C. R. Williams. DISTRIBUTED STRUCTURE-SEARCHABLE TOXICITY (DSSTOX) PUBLIC DATABASE NETWORK: A PROPOSAL. MUTATION RESEARCH NEW FRONTIERS ISSUE 499(1):27-52, (2001)."],

                "author": [{

                    "given_name": "Dayna",
                    "family_name": "Gibbons",
                    "email": "gibbons.dayna@epa.gov",
                    "institution": "US EPA Research",

                }],

                #"license": "",
                "collection": "DSS Tox",
                #"tags": [""],
                "description": "DSSTox provides a high quality public chemistry resource for supporting improved predictive toxicology. A distinguishing feature of this effort is the accurate mapping of bioassay and physicochemical property data associated with chemical substances to their corresponding chemical structures.",
                "year": 2016,

                "links": {

                    "landing_page": "https://www.epa.gov/chemical-research/distributed-structure-searchable-toxicity-dsstox-database",
                    "publication": ["http://cfpub.epa.gov/si/si_lab_search_results.cfm?SIType=PR&TIMSType=Journal&showCriteria=0&view=citation&sortBy=pubDateYear&keyword=DssTox", "https://www.epa.gov/chemical-research/toxicity-forecasting", "https://www.epa.gov/chemical-research/toxicology-testing-21st-century-tox21", "http://actor.epa.gov/dashboard/", "https://www.epa.gov/chemical-research/chemistry-dashboard"],
                    #"data_doi": "",
                    #"related_id": ,

                    #"data_link": {

                        #"globus_endpoint": ,
                        #"http_host": ,

                        #"path": ,
                        #},
                    },
                },

            #"mrr": {

                #},

            #"dc": {

                #},


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
    #    Each record should be exactly one dictionary
    #    You must write your records using the Validator one at a time
    #    It is recommended that you use a parser to help with this process if one is available for your datatype
    #    Each record also needs its own metadata
    total_errors = 0
    for data_file in tqdm(find_files(input_path, "sdf"), desc="Processing files", disable=not verbose):
        try:
            record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "sdf")
        except Exception as e:
            #print("ERROR: \n" + repr(e))
            #print(os.path.join(data_file["path"], data_file["filename"]))
            total_errors +=1
            continue

        with open(os.path.join(data_file["path"], data_file["filename"]), 'r') as raw_in:
            record_data = raw_in.read()

        tox_line = record_data.find(">  <DSSTox_QC-Level>")
        toxicity = record_data[tox_line:].split("\n")[1]

        substance_line = record_data.find(">  <Substance_Name>")
        substance = record_data[substance_line:].split("\n")[1]

        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "DSS Tox - ",
                "acl": ["public"],
                #"composition": ,

#                "tags": ,
#                "description": ,
                #"raw": json.dumps(record),

                "links": {

#                    "landing_page": ,
#                    "publication": ,
#                    "data_doi": ,
#                    "related_id": ,

                    "sdf": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/dss_tox/" + data_file["no_root_path"] + "/" + data_file["filename"],
                        },
                    },

#                "citation": ,

#                "data_contact": {

#                    "given_name": ,
#                    "family_name": ,
#                    "email": ,
#                    "institution": ,

#                    },

#                "author": [{

#                    "given_name": ,
#                    "family_name": ,
#                    "email": ,
#                    "institution": ,

#                    }],

#                "year": ,

                },
            "dss_tox": {
                "toxicity": toxicity,
                "substance_name": substance
            }

           # "dc": {

           # },


        }
        ## End metadata
        if record["chemical_formula"] == "":
            record_metadata["mdf"]["title"] += substance
        else:
            record_metadata["mdf"]["title"] += record["chemical_formula"]
            record_metadata["mdf"]["composition"] = record["chemical_formula"]

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
        print("Total Errors: \n" + str(total_errors))
        print("Finished converting")
