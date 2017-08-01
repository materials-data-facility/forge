import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Supplementary information for a study of DFT+U in the context of BiFeO3
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

                "title": "Supplementary information for a study of DFT+U in the context of BiFeO3",
                "acl": ["public"],
                "source_name": "bi_fe_o3_dft",

                "data_contact": {
                    
                    "given_name": "J. Kane",
                    "family_name": "Shenton",
                    "email": "john.shenton.10@ucl.ac.uk",
                    "institution": "University College London",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Shenton, J. Kane, Cheah, Wei Li, & Bowler, David R. (2017). Supplementary information for a study of DFT+U in the context of BiFeO3 [Data set]. Zenodo. http://doi.org/10.5281/zenodo.581302"],

                "author": [{

                    "given_name": "Wei Li",
                    "family_name": "Cheah",
                    "institution": "Institute of High Performance Computing, A-STAR, Singapore",

                },
                {

                    "given_name": "David R.",
                    "family_name": "Bowler",
                    "institution": "London Centre for Nanotechnology, International Centre for Materials Nanoarchitectonics (MANA)",

                },
                {

                    "given_name": "J. Kane",
                    "family_name": "Shenton",
                    "email": "john.shenton.10@ucl.ac.uk",
                    "institution": "University College London",

                }],

                #"license": "",
                "collection": "BiFeO3 DFT+U",
                #"tags": [""],
                "description": "Jupyter notebooks containing much of the analysis featured in an upcoming publication. The .xml files contain all of the structure information and input parameters necessary to completely reproduce the work. Examples of how to work with the .xml files using pymatgen, in order to extract and analyse the relevant results are given.",
                "year": 2017,

                "links": {

                    "landing_page": "https://zenodo.org/record/581302#.WWWmB8aZPFQ",
                    "publication": ["https://github.com/jks-science/BFO_dftu_SI/blob/v1.0/dft_u_BFO_SI.ipynb"],
                    #"data_doi": "",
                    #"related_id": ,

                    "zip": {

                        #"globus_endpoint": ,
                        "http_host": "https://zenodo.org",

                        "path": "/record/581302/files/jks-science/BFO_dftu_SI-v1.0.zip",
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
    for data_file in tqdm(find_files(input_path, "(outcar|vasp$)"), desc="Processing files", disable=not verbose):
        dtype = data_file["filename"].split(".")[-1]
        if "outcar" in data_file["filename"]:
            dtype = "outcar"
            ftype = "vasp-out"
        else:
            ftype = "vasp"
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), ftype)
        ## Metadata:record
        title_path = "/".join(os.path.join(data_file["no_root_path"], data_file["filename"]).split("/")[1:])
        record_metadata = {
            "mdf": {

                "title": "BiFeO3 DFT+U - " + title_path,
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

                    dtype: {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/bi_fe_o3_dft/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
