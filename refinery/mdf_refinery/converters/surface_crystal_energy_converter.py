import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Data from: Surface energies of elemental crystals
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

                "title": "Data from: Surface energies of elemental crystals",
                "acl": ["public"],
                "source_name": "surface_crystal_energy",

                "data_contact": {
                    
                    "given_name": "Shyue Ping",
                    "family_name": "Ong",
                    "email": "s2ong@ucsd.edu",
                    "institution": "University of California San Diego",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Tran R, Xu Z, Radhakrishnan B, Winston D, Sun W, Persson KA, Ong SP (2016) Surface energies of elemental crystals. Scientific Data 3: 160080. http://dx.doi.org/10.1038/sdata.2016.80", "Tran R, Xu Z, Radhakrishnan B, Winston D, Sun W, Persson KA, Ong SP (2016) Data from: Surface energies of elemental crystals. Dryad Digital Repository. http://dx.doi.org/10.5061/dryad.f2n6f"],

                "author": [{

                    "given_name": "Richard",
                    "family_name": "Tran",
                    "institution": "University of California San Diego",

                },
                {

                    "given_name": "Zihan",
                    "family_name": "Xu",
                    "institution": "University of California San Diego",

                },
                {

                    "given_name": "Balachandran",
                    "family_name": "Radhakrishnan",
                    "institution": "University of California San Diego",

                },
                {

                    "given_name": "Donald",
                    "family_name": "Winston",
                    "institution": "Lawrence Berkeley National Laboratory",

                },
                {

                    "given_name": "Wenhao",
                    "family_name": "Sun",
                    "institution": "Massachusetts Institute of Technology",

                },
                {

                    "given_name": "Kristin A.",
                    "family_name": "Persson",
                    "institution": "University of California, Berkeley",

                },
                {

                    "given_name": "Shyue Ping",
                    "family_name": "Ong",
                    "email": "s2ong@ucsd.edu",
                    "institution": "University of California San Diego",

                }],

                "license": "http://creativecommons.org/publicdomain/zero/1.0/",
                "collection": "Surface Crystal Energy",
                #"tags": [""],
                "description": "The surface energy is a fundamental property of the different facets of a crystal that is crucial to the understanding of various phenomena like surface segregation, roughening, catalytic activity, and the crystal’s equilibrium shape. Such surface phenomena are especially important at the nanoscale, where the large surface area to volume ratios lead to properties that are significantly different from the bulk. In this work, we present the largest database of calculated surface energies for elemental crystals to date.",
                "year": 2016,

                "links": {

                    "landing_page": "http://dx.doi.org/10.5061/dryad.f2n6f",
                    "publication": ["http://dx.doi.org/10.1038/sdata.2016.80"],
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
    for data_file in tqdm(find_files(input_path, "(txt|cif)"), desc="Processing files", disable=not verbose):
        dtype = data_file["filename"].split(".")[-1]
        if dtype == "cif":
            record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), dtype)
            comp = record["chemical_formula"]
        else:
            with open(os.path.join(data_file["path"], data_file["filename"]), 'r') as raw_in:
                data = json.load(raw_in)
            if "details" in data_file["filename"]:
                comp = data["surfaces"][0]["calculations"]["pretty_formula"]
            else:
                comp = data["pretty_formula"]
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Surface Crystal Energy - " + comp,
                "acl": ["public"],
                "composition": comp,

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

                        "path": "/collections/surface_crystal_energy/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
