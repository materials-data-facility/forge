import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_forge.toolbox import find_files

# VERSION 0.3.0

# This is the converter for the High-throughput Ab-initio Dilute Solute Diffusion Database dataset from Dane Morgan's research group
# Arguments:
#   input_path (string): The file or directory where the data resides.
#       NOTE: Do not hard-code the path to the data in the converter. The converter should be portable.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict or json.dumps string containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        dataset_metadata = {
        "mdf": {
            "title": "High-throughput Ab-initio Dilute Solute Diffusion Database",
            "acl": ["public"],
            "source_name": "ab_initio_solute_database",
            "citation": ['Wu, Henry; Mayeshiba, Tam; Morgan, Dane, "Dataset for High-throughput Ab-initio Dilute Solute Diffusion Database," 2016, http://dx.doi.org/doi:10.18126/M2X59R'],
            "data_contact": {

                "given_name": "Dane",
                "family_name": "Morgan",

                "email": "ddmorgan@wisc.edu",
                "institution": "University of Wisconsin-Madison"

                # IDs
                },

            "author": [{

                "given_name": "Dane",
                "family_name": "Morgan",

                "email": "ddmorgan@wisc.edu",
                "institution": "University of Wisconsin-Madison"

                # IDs
                },
                {

                "given_name": "Tam",
                "family_name": "Mayeshiba",

                "institution": "University of Wisconsin-Madison"

                # IDs
                },
                {

                "given_name": "Wu",
                "family_name": "Henry",

                "institution": "University of Wisconsin-Madison"

                # IDs
                }
                ],

#            "license": ,

            "collection": "High-Throughput ab-initio Dilute Solute Diffusion Database",
            "tags": ["dilute", "solute", "DFT", "diffusion"],

            "description": "We demonstrate automated generation of diffusion databases from high-throughput density functional theory (DFT) calculations. A total of more than 230 dilute solute diffusion systems in Mg, Al, Cu, Ni, Pd, and Pt host lattices have been determined using multi-frequency diffusion models. We apply a correction method for solute diffusion in alloys using experimental and simulated values of host self-diffusivity.",
            "year": 2016,

            "links": {

                "landing_page": "https://publish.globus.org/jspui/handle/ITEM/164",

                "publication": "http://dx.doi.org/10.1038/sdata.2016.54",
                "data_doi": "http://dx.doi.org/doi:10.18126/M2X59R",

#                "related_id": ,

                "dataset": {

                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    #"http_host": ,

                    "path": "/published/publication_164/",
                    }
                },

#            "mrr": ,

            "data_contributor": {
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
                }
            }
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


    dataset_validator = Validator(dataset_metadata)


    # Get the data
    for dir_data in tqdm(find_files(root=input_path, file_pattern="^OUTCAR$", verbose=verbose), desc="Processing data files", disable= not verbose):
        try:
            file_data = parse_ase(file_path=os.path.join(dir_data["path"], dir_data["filename"]), data_format="vasp-out", verbose=False)
        except Exception as e:
            print("Error:", repr(e))
            file_data = None
        if file_data:
            record_metadata = {
            "mdf": {
                "title": "High-throughput Ab-initio Dilute Solute Diffusion Database - " + file_data["chemical_formula"],
                "acl": ["public"],

#                "tags": ,
#                "description": ,

                "composition": file_data["chemical_formula"],
#                "raw": ,

                "links": {
#                    "landing_page": ,

#                    "publication": ,
#                    "dataset_doi": ,

#                    "related_id": ,

                    "outcar": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/published/publication_164/" + dir_data["no_root_path"] + "/" + dir_data["filename"],
                        },
                    },

#                "citation": ,
#                "data_contact": {

#                    "given_name": ,
#                    "family_name": ,

#                    "email": ,
#                    "institution":,

                    # IDs
#                    },

#                "author": ,

#                "license": ,
#                "collection": ,
#                "data_format": ,
#                "data_type": ,
#                "year": ,

#                "mrr":

    #            "processing": ,
    #            "structure":,
                }
            }

            # Pass each individual record to the Validator
            result = dataset_validator.write_record(record_metadata)

            # Check if the Validator accepted the record, and print a message if it didn't
            # If the Validator returns "success" == True, the record was written successfully
            if result["success"] is not True:
                print("Error:", result["message"])


    if verbose:
        print("Finished converting")
