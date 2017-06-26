import json
import sys
import os

from tqdm import tqdm

from ..validator.schema_validator import Validator
from ..parsers.ase_parser import parse_ase
from ..utils.file_utils import find_files

# VERSION 0.2.0

# This is the converter for the Strain Effects on Oxygen Migration dataset
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
            "mdf-title": "Strain effects on oxygen migration in perovskites: La[Sc, Ti, V, Cr, Mn, Fe, Co, Ni, Ga]O3",
            "mdf-acl": ["public"],
            "mdf-source_name": "strain_effects_oxygen",
            "mdf-citation": ["Mayeshiba, T. & Morgan, D. Strain effects on oxygen migration in perovskites. Physical chemistry chemical physics : PCCP 17, 2715-2721, doi:10.1039/c4cp05554c (2015).", "Mayeshiba, T. & Morgan, D. Correction: Strain effects on oxygen migration in perovskites. Physical chemistry chemical physics : PCCP, doi:10.1039/c6cp90050j (2016)."],
            "mdf-data_contact": {

                "given_name": "Dane",
                "family_name": "Morgan",

                "email": "ddmorgan@wisc.edu",
                "institution": "University of Wisconsin-Madison",

                },

            "mdf-author": [{

                "given_name": "Dane",
                "family_name": "Morgan",

                "email": "ddmorgan@wisc.edu",
                "institution": "University of Wisconsin-Madison",

                },
                {

                "given_name": "Tam",
                "family_name": "Mayeshiba",

                "institution": "University of Wisconsin-Madison",

                }],

#            "mdf-license": ,

            "mdf-collection": "Strain Effects on Oxygen Migration",
            "mdf-data_format": "vasp",
            "mdf-data_type": "dft",
            "mdf-tags": ["dft", "perovskites"],

            "mdf-description": "This work reports on an ab initio prediction of strain effects on migration energetics for nine perovskite systems of the form LaBO3, where B = [Sc, Ti, V, Cr, Mn, Fe, Co, Ni, Ga].",
            "mdf-year": 2016,

            "mdf-links": {

                "mdf-landing_page": "https://materialsdata.nist.gov/dspace/xmlui/handle/11256/701",

                "mdf-publication": ["https://dx.doi.org/10.1039/c4cp05554c", "https://dx.doi.org/10.1039/c6cp90050j"],
                "mdf-dataset_doi": "http://hdl.handle.net/11256/701",

#                "mdf-related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
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
    for data_file in tqdm(find_files(input_path, "^OUTCAR$", verbose=True), desc="Processing files", disable= not verbose):
        try:
            data = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "vasp-out")
        except Exception as e:
            print("Error on", os.path.join(data_file["path"], data_file["filename"]), ":\n", repr(e))
        record_metadata = {
            "mdf-title": "Oxygen Migration - " + data["chemical_formula"],
            "mdf-acl": ["public"],

#            "mdf-tags": ,
#            "mdf-description": ,
            
            "mdf-composition": data["chemical_formula"],
#            "mdf-raw": ,

            "mdf-links": {
#                "mdf-landing_page": ,

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                "outcar": {
 
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    "http_host": "https://data.materialsdatafacility.org",
                    "path": "/collections/" + data_file["no_root_path"] + "/" + data_file["filename"],
                    },
                },

#            "mdf-citation": ,
#            "mdf-data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

                # IDs
#},

#            "mdf-author": ,

#            "mdf-license": ,
#            "mdf-collection": ,
#            "mdf-data_format": ,
#            "mdf-data_type": ,
#            "mdf-year": ,

#            "mdf-mrr":

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
