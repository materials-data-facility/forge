import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_refinery.parsers.tab_parser import parse_tab

# VERSION 0.3.0

# This is the converter for the NIST X-Ray Transition Energies Database
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
            "title": "NIST X-Ray Transition Energies Database",
            "acl": ["public"],
            "source_name": "nist_xray_tran_en_db",
            "citation": ["http://physics.nist.gov/PhysRefData/XrayTrans/Html/refs.html"],
            "data_contact": {

                "given_name": "Lawrence",
                "family_name": "Hudson",

                "email": "lawrence.hudson@nist.gov",
                "institution": "National Institute of Standards and Technology"

                },

#            "author": ,

#            "license": ,

            "collection": "NIST X-Ray Transition Energies",
            "tags": ["Radiation", "Spectroscopy", "Reference data"],

            "description": "This x-ray transition table provides the energies for K transitions connecting the K shell (n = 1) to the shells with principal quantum numbers n = 2 to 4 and L transitions connecting the L1, L2, and L3 shells (n = 2) to the shells with principal quantum numbers n = 3 and 4. The elements covered include Z = 10, neon to Z = 100, fermium. There are two unique features of this database: (1) all experimental values are on a scale consistent with the International System of measurement (the SI) and the numerical values are determined using constants from the Recommended Values of the Fundamental Physical Constants: 1998 [115] and (2) accurate theoretical estimates are included for all transitions. The user will find that for many of the transitions, the experimental and theoretical values are very consistent. It is our hope that the theoretical values will provide a useful estimate for missing or poorly measured experimental values.",
            "year": 2003,

            "links": {

                "landing_page": "https://www.nist.gov/pml/x-ray-transition-energies-database",

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
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
    headers = ['element', 'A', 'transition', 'theory_(eV)', 'theory_uncertainty_(eV)', 'direct_(eV)', 'direct_uncertainty_(eV)', 'combined_(eV)', 'combined_uncertainty_(eV)', 'vapor_(eV)', 'vapor_uncertainty_(eV)', 'blend', 'reference']
    with open(os.path.join(input_path, "xray_tran_en_db.txt")) as in_file:
        raw_data = in_file.read()
    for record in tqdm(parse_tab(raw_data, sep="\t", headers=headers), desc="Processing data", disable= not verbose):
        record_metadata = {
        "mdf": {
            "title": "X-Ray Transition - " + record["element"],
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
            "composition": record["element"],
            "raw": json.dumps(record),

            "links": {
                "landing_page": "http://physics.nist.gov/PhysRefData/XrayTrans/Html/search.html",

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {
 
                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #},
                },

#            "citation": ,
#            "data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

                # IDs
#                },

#            "author": ,

#            "license": ,
#            "collection": ,
#            "data_format": ,
#            "data_type": ,
#            "year": ,

#            "mrr":

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
