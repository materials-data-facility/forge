import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Uncertainty quantification for quantum chemical models of complex reaction networks	
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

                "title": "Uncertainty quantification for quantum chemical models of complex reaction networks	",
                "acl": ["public"],
                "source_name": "reiher_quantum_chemical_models",

                "data_contact": {
                    
                    "given_name": "Markus",
                    "family_name": "Reiher",
                    "email": "markus.reiher@phys.chem.ethz.ch",
                    "institution": "Laboratory of Physical Chemistry, ETH Zürich",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["(2016). Uncertainty quantification for quantum chemical models of complex reaction networks. , 195, 497-520. 10.1039/C6FD00144K"],

                "author": [{

                    "given_name": "Jonny",
                    "family_name": "Proppe",
                    "institution": "Laboratory of Physical Chemistry, ETH Zürich",

                },
                {

                    "given_name": "Tamara",
                    "family_name": "Husch",
                    "institution": "Laboratory of Physical Chemistry, ETH Zürich",

                },
                {

                    "given_name": "Gregor N.",
                    "family_name": "Simma",
                    "institution": "Laboratory of Physical Chemistry, ETH Zürich",

                },
                {

                    "given_name": "Markus",
                    "family_name": "Reiher",
                    "email": "markus.reiher@phys.chem.ethz.ch",
                    "institution": "Laboratory of Physical Chemistry, ETH Zürich",

                }],

                "license": "http://creativecommons.org/licenses/by/3.0/",
                "collection": "Reiher Quantum Chemical Models",
                #"tags": [""],
                "description": "For the quantitative understanding of complex chemical reaction mechanisms, it is, in general, necessary to accurately determine the corresponding free energy surface and to solve the resulting continuous-time reaction rate equations for a continuous state space. For a general (complex) reaction network, it is computationally hard to fulfill these two requirements. However, it is possible to approximately address these challenges in a physically consistent way. On the one hand, it may be sufficient to consider approximate free energies if a reliable uncertainty measure can be provided. On the other hand, a highly resolved time evolution may not be necessary to still determine quantitative fluxes in a reaction network if one is interested in specific time scales. In this paper, we present discrete-time kinetic simulations in discrete state space taking free energy uncertainties into account.",
                "year": 2016,

                "links": {

                    "landing_page": "http://pubs.rsc.org/en/content/articlelanding/fd/2016/c6fd00144k#!divAbstract",
                    "publication": ["http://pubs.rsc.org/doi/c6fd90075e"],
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
    for data_file in tqdm(find_files(input_path, "xyz$"), desc="Processing files", disable=not verbose):
        if "PaxHeaders" in data_file["path"]:
            continue
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "xyz")
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Reiher Quantum Chemical Models - " + record["chemical_formula"],
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

                    "xyz": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/collections/reiher_quantum_chemical_models/" + data_file["no_root_path"] + "/" + data_file["filename"],
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
