import json
import sys
import os

from tqdm import tqdm

from ..validator.schema_validator import Validator
from ..parsers.ase_parser import parse_ase
from ..utils.file_utils import find_files

# VERSION 0.2.0

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
            "mdf-title": "High-throughput Ab-initio Dilute Solute Diffusion Database",
            "mdf-acl": ["public"],
            "mdf-source_name": "ab_initio_solute_database",
            "mdf-citation": ['Wu, Henry; Mayeshiba, Tam; Morgan, Dane, "Dataset for High-throughput Ab-initio Dilute Solute Diffusion Database," 2016, http://dx.doi.org/doi:10.18126/M2X59R'],
            "mdf-data_contact": {

                "given_name": "Dane",
                "family_name": "Morgan",

                "email": "ddmorgan@wisc.edu",
                "institution": "University of Wisconsin-Madison"

                # IDs
                },

            "mdf-author": [{

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

#            "mdf-license": ,

            "mdf-collection": "High-Throughput ab-initio Dilute Solute Diffusion Database",
            "mdf-data_format": "vasp",
            "mdf-data_type": "dft",
            "mdf-tags": ["dilute", "solute", "DFT", "diffusion"],

            "mdf-description": "We demonstrate automated generation of diffusion databases from high-throughput density functional theory (DFT) calculations. A total of more than 230 dilute solute diffusion systems in Mg, Al, Cu, Ni, Pd, and Pt host lattices have been determined using multi-frequency diffusion models. We apply a correction method for solute diffusion in alloys using experimental and simulated values of host self-diffusivity.",
            "mdf-year": 2016,

            "mdf-links": {

                "mdf-landing_page": "https://publish.globus.org/jspui/handle/ITEM/164",

                "mdf-publication": "http://dx.doi.org/10.1038/sdata.2016.54",
                "mdf-dataset_doi": "http://dx.doi.org/doi:10.18126/M2X59R",

#                "mdf-related_id": ,

                "dataset": {

                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    #"http_host": ,

                    "path": "/published/publication_164/",
                    }
                },

#            "mdf-mrr": ,

            "mdf-data_contributor": {
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
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
        file_data = parse_ase(file_path=os.path.join(dir_data["path"], dir_data["filename"]), data_format="vasp-out", verbose=False)
        if file_data:
            record_metadata = {
                "mdf-title": "High-throughput Ab-initio Dilute Solute Diffusion Database - " + file_data["chemical_formula"],
                "mdf-acl": ["public"],

#                "mdf-tags": ,
#                "mdf-description": ,

                "mdf-composition": file_data["chemical_formula"],
#                "mdf-raw": ,

                "mdf-links": {
#                    "mdf-landing_page": ,

#                    "mdf-publication": ,
#                    "mdf-dataset_doi": ,

#                    "mdf-related_id": ,

                    "outcar": {

                        "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                        "http_host": "https://data.materialsdatafacility.org",

                        "path": "/published/publication_164/" + dir_data["no_root_path"] + "/" + dir_data["filename"],
                        },
                    },

#                "mdf-citation": ,
#                "mdf-data_contact": {

#                    "given_name": ,
#                    "family_name": ,

#                    "email": ,
#                    "institution":,

                    # IDs
#                    },

#                "mdf-author": ,

#                "mdf-license": ,
#                "mdf-collection": ,
#                "mdf-data_format": ,
#                "mdf-data_type": ,
#                "mdf-year": ,

#                "mdf-mrr":

    #            "mdf-processing": ,
    #            "mdf-structure":,
                }

            # Pass each individual record to the Validator
            result = dataset_validator.write_record(record_metadata)

            # Check if the Validator accepted the record, and print a message if it didn't
            # If the Validator returns "success" == True, the record was written successfully
            if result["success"] is not True:
                print("Error:", result["message"])


    if verbose:
        print("Finished converting")
