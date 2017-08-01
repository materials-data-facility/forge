import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.parsers.tab_parser import parse_tab
from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for
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
            "title": "National Renewable Energy Laboratory Organic Photovoltaic Database",
            "acl": ["public"],
            "source_name": "nrel_pv",
            "citation": ["Gaussian 09, (Revisions B.01, C.01 and D.01), M. J. Frisch, et al., Gaussian, Inc., Wallingford CT, 2009. See gaussian.com", "Ross E. Larsen, J. Phys. Chem. C, 120, 9650-9660 (2016). DOI: 10.1021/acs .jpcc.6b02138"],
            "data_contact": {

                "given_name": "Ross",
                "family_name": "Larsen",

                "email": "organicelectronics@nrel.gov",
                "institution": "National Renewable Energy Laboratory"

                # IDs
                },

            "author": {

                "given_name": "Ross",
                "family_name": "Larsen",

                "email": "organicelectronics@nrel.gov",
                "institution": "National Renewable Energy Laboratory"

                # IDs
                },

#            "license": ,

            "collection": "NREL Organic Photovoltaic Database",
            "tags": ["dft", "simulation", "organic photovoltaics"],

            "description": "Welcome to the National Renewable Energy Laboratory materials discovery database for organic electronic materials. The focus is on materials for organic photovoltaic (OPV) absorber materials but materials suitable for other applications may be found here as well.",
#            "year": ,

            "links": {

                "landing_page": "https://organicelectronics.nrel.gov",

                "publication": ["https://dx.doi.org/10.1021/acs.jpcc.6b02138"],
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mrr": ,

            "data_contributor": [{
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
                }]
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
    with open(os.path.join(input_path, "polymer_export_0501179397151.csv"), 'r') as raw_in:
        for record in tqdm(parse_tab(raw_in.read()), desc="Processing files", disable= not verbose):
            record_metadata = {
            "mdf": {
                "title": "NREL OPV - " + record["common_tag"],
                "acl": ["public"],

#                "tags": ,
#                "description": ,

                "composition": record["common_tag"],
                "raw": json.dumps(record),

                "links": {
                    "landing_page": record["URL"],

#                    "publication": ,
#                    "dataset_doi": ,

#                    "related_id": ,

                    # data links: {
     
                        #"globus_endpoint": ,
                        #"http_host": ,

                        #"path": ,
                        #},
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
