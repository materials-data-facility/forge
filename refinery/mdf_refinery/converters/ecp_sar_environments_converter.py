import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Prediction of Compounds in Different Local SAR Environments using ECP
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

                "title": "Prediction of Compounds in Different Local SAR Environments using ECP",
                "acl": ["public"],
                "source_name": "ecp_sar_environments",

                "data_contact": {
                    
                    "given_name": "Jürgen",
                    "family_name": "Bajorath",
                    "email": "bajorath@bit.uni-bonn.de",
                    "institution": "Rheinische Friedrich-Wilhelms-Universität Bonn",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Namasivayam, V., Gupta-Ostermann, D., Balfer, J., Heikamp, K., & Bajorath, J. (2014). Prediction of Compounds in Different Local SAR Environments using ECP [Data set]. Zenodo. http://doi.org/10.5281/zenodo.8626"],

                "author": [{

                    "given_name": "Vigneshwaran",
                    "family_name": "Namasivayam",
                    "institution": "Rheinische Friedrich-Wilhelms-Universität Bonn",

                },
                {

                    "given_name": "Disha",
                    "family_name": "Gupta-Ostermann",
                    "institution": "Rheinische Friedrich-Wilhelms-Universität Bonn",

                },
                {

                    "given_name": "Jenny",
                    "family_name": "Balfer",
                    "institution": "Rheinische Friedrich-Wilhelms-Universität Bonn",

                },
                {

                    "given_name": "Kathrin",
                    "family_name": "Heikamp",
                    "institution": "Rheinische Friedrich-Wilhelms-Universität Bonn",

                },
                {

                    "given_name": "Jürgen",
                    "family_name": "Bajorath",
                    "email": "bajorath@bit.uni-bonn.de",
                    "institution": "Rheinische Friedrich-Wilhelms-Universität Bonn",

                }],

                "license": "https://creativecommons.org/publicdomain/zero/1.0/",
                "collection": "SAR Environments using ECP",
                #"tags": [""],
                "description": "Active compounds can participate in different local structure–activity relationship (SAR) environments and introduce different degrees of local SAR discontinuity, depending on their structural and potency relationships in data sets. Such SAR features have thus far mostly been analyzed using descriptive approaches, in particular, on the basis of activity landscape modeling. However, compounds in different local SAR environments have not yet been predicted. Herein, we adapt the emerging chemical patterns (ECP) method, a machine learning approach for compound classification, to systematically predict compounds with different local SAR characteristics. ECP analysis is shown to accurately assign many compounds to different local SAR environments across a variety of activity classes covering the entire range of observed local SARs.",
                "year": 2014,

                "links": {

                    "landing_page": "https://doi.org/10.5281/zenodo.8626",
                    "publication": ["http://pubs.acs.org/doi/abs/10.1021/ci500147b"],
                    #"data_doi": "",
                    #"related_id": ,

                    "zip": {

                        #"globus_endpoint": ,
                        "http_host": "https://zenodo.org",

                        "path": "/record/8626/files/Data_sets.zip",
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
    errors=0
    for data_file in tqdm(find_files(input_path, "sdf"), desc="Processing files", disable=not verbose):
        try:
            record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "sdf")
        except Exception as e:
            errors+=1
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "SAR Environments using ECP - " + record["chemical_formula"],
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

                    "sdf": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/ecp_sar_environments/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
        print("ERRORS: " + str(errors))
        print("Finished converting")
