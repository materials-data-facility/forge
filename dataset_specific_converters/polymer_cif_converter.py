import json
import sys
import os
from tqdm import tqdm
from ..utils.file_utils import find_files
from ..parsers.ase_parser import parse_ase
from ..validator.schema_validator import Validator

# VERSION 0.3.0

# This is the converter for: A polymer dataset for accelerated property prediction and design
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

                "title": "A polymer dataset for accelerated property prediction and design",
                "acl": ["public"],
                "source_name": "polymer_cif",

                "data_contact": {
                    
                    "given_name": "Rampi",
                    "family_name": "Ramprasad",
                    "email": "rampi.ramprasad@uconn.edu",
                    "institution": "University of Connecticut",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Huan, Tran Doan et al. “A Polymer Dataset for Accelerated Property Prediction and Design.” Scientific Data 3 (2016): 160012. PMC. Web. 19 July 2017."],

                "author": [{

                    "given_name": "Arun",
                    "family_name": "Mannodi-Kanakkithodi",
                    "institution": "University of Connecticut",

                },
                {

                    "given_name": "Tran Doan",
                    "family_name": "Huan",
                    "institution": "University of Connecticut",

                },
                {

                    "given_name": "Chiho",
                    "family_name": "Kim",
                    "institution": "University of Connecticut",

                },
                {

                    "given_name": "Vinit",
                    "family_name": "Sharma",
                    "institution": "University of Connecticut",

                },
                {

                    "given_name": "Ghanshyam",
                    "family_name": "Pilania",
                    "institution": "Los Alamos National Laboratory",

                },
                {

                    "given_name": "Rampi",
                    "family_name": "Ramprasad",
                    "email": "rampi.ramprasad@uconn.edu",
                    "institution": "University of Connecticut",

                }],

                "license": "http://creativecommons.org/licenses/by/4.0",
                "collection": "Polymer CIF",
                "tags": ["Electronic properties and materials", "Computational chemistry", "Density functional theory", "Atomistic models"],
                "description": "This tarball includes 1073 CIF files, each of them provides the optimized structure and the accompanied properties calculated with first-principles computations.",
                "year": 2016,

                "links": {

                    "landing_page": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4772654/",
                    "publication": ["https://www.nature.com/articles/sdata201612"],
                    "data_doi": "http://dx.doi.org/10.5061/dryad.5ht3n",
                    #"related_id": ,

                    "tgz": {

                        #"globus_endpoint": ,
                        "http_host": "http://datadryad.org",

                        "path": "/bitstream/handle/10255/dryad.107972/Polymer-CIF.tgz?sequence=1",
                        },
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
    for data_file in tqdm(find_files(input_path, "cif"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "cif")
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Polymer Cif - " + record["chemical_formula"],
                "acl": ["public"],
                "composition": record["chemical_formula"],

#                "tags": ,
#                "description": ,
                #"raw": json.dumps(record),

                "links": {

#                    "landing_page": ,
#                    "publication": ,
#                    "data_doi": ,
#                    "related_id": ,

                    "cif": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/polymer_cif/" + data_file["filename"],
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

           # "dc": {

           # },


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
