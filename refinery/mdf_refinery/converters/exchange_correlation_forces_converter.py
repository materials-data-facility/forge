import json
import sys
import os
from tqdm import tqdm
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for: Non-self-consistent Density-Functional Theory Exchange-Correlation Forces for GGA Functionals
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

                "title": "Non-self-consistent Density-Functional Theory Exchange-Correlation Forces for GGA Functionals",
                "acl": ["public"],
                "source_name": "exchange_correlation_forces",

                "data_contact": {
                    
                    "given_name": "Antonio S.",
                    "family_name": "Torralba",
                    "email": "TORRALBA.Antonio@nims.go.jp",
                    "institution": "London Centre for Nanotechnology",

                },

                "data_contributor": [{
                    
                    "given_name": "Evan",
                    "family_name": "Pike",
                    "email": "dep78@uchicago.edu",
                    "institution": "The University of Chicago",
                    "github": "dep78",

                }],

                "citation": ["Torralba, Antonio S.; Bowler, David R.; Miyazaki, Tsuyoshi; Gillan, Michael J. (2009): Non-self-consistent Density-Functional Theory Exchange-Correlation Forces for GGA Functionals. ACS Publications. https://doi.org/10.1021/ct8005425.s001"],

                "author": [{

                    "given_name": "Michael J.",
                    "family_name": "Gillan",
                    "institution": "London Centre for Nanotechnology",

                },
                {

                    "given_name": "Tsuyoshi",
                    "family_name": "Miyazaki",
                    "institution": "National Institute for Materials Science",

                },
                {

                    "given_name": "David R.",
                    "family_name": "Bowler",
                    "institution": "London Centre for Nanotechnology",

                },
                {

                    "given_name": "Antonio S.",
                    "family_name": "Torralba",
                    "email": "TORRALBA.Antonio@nims.go.jp",
                    "institution": "London Centre for Nanotechnology",

                }],

                "license": "https://creativecommons.org/licenses/by-nc/4.0/",
                "collection": "DFT Exchange-Correlation Forces",
                "tags": ["DFT", "relaxation", "DFT code Conquest", "Example calculations", "alanine peptides", "computer effort", "method", "gradient approximation", "GGA FunctionalsWhen", "expression", "GGA NSC force"],
                "description": "When using density functional theory (DFT), generalized gradient approximation (GGA) functionals are often necessary for accurate modeling of important properties of biomolecules, including hydrogen-bond strengths and relative energies of conformers. We consider the calculations of forces using non-self-consistent (NSC) methods based on the Harris−Foulkes expression for energy. We derive an expression for the GGA NSC force on atoms, valid for a hierarchy of methods based on local orbitals, and discuss its implementation in the linear scaling DFT code Conquest, using a standard (White−Bird) approach. We investigate the use of NSC structural relaxations before full self-consistent relaxations as a method for improving convergence.",
                "year": 2009,

                "links": {

                    "landing_page": "https://figshare.com/articles/Non_self_consistent_Density_Functional_Theory_Exchange_Correlation_Forces_for_GGA_Functionals/2851375",
                    "publication": ["http://pubs.acs.org/doi/abs/10.1021/ct8005425"],
                    #"data_doi": "",
                    #"related_id": ,

                    "zip": {

                        #"globus_endpoint": ,
                        "http_host": "https://ndownloader.figshare.com",

                        "path": "/files/4549114",
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
    for data_file in tqdm(find_files(input_path, "xyz"), desc="Processing files", disable=not verbose):
        record = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "xyz")
        ## Metadata:record
        record_metadata = {
            "mdf": {

                "title": "Exchange Correlation Forces - " + record["chemical_formula"],
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

                        "path": "/collections/exchange_correlation_forces/" + data_file["filename"],
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
