import json
import sys
import os

from tqdm import tqdm

from ..parsers.tab_parser import parse_tab
from ..validator.schema_validator import Validator

# VERSION 0.2.0

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
            "mdf-title": "National Renewable Energy Laboratory Organic Photovoltaic Database",
            "mdf-acl": ["public"],
            "mdf-source_name": "nrel_pv",
            "mdf-citation": ["Gaussian 09, (Revisions B.01, C.01 and D.01), M. J. Frisch, et al., Gaussian, Inc., Wallingford CT, 2009. See gaussian.com", "Ross E. Larsen, J. Phys. Chem. C, 120, 9650-9660 (2016). DOI: 10.1021/acs .jpcc.6b02138"],
            "mdf-data_contact": {

                "given_name": "Ross",
                "family_name": "Larsen",

                "email": "organicelectronics@nrel.gov",
                "institution": "National Renewable Energy Laboratory"

                # IDs
                },

            "mdf-author": {

                "given_name": "Ross",
                "family_name": "Larsen",

                "email": "organicelectronics@nrel.gov",
                "institution": "National Renewable Energy Laboratory"

                # IDs
                },

#            "mdf-license": ,

            "mdf-collection": "NREL Organic Photovoltaic Database",
            "mdf-data_format": "csv",
            "mdf-data_type": "Simulation",
            "mdf-tags": ["dft", "simulation", "organic photovoltaics"],

            "mdf-description": "Welcome to the National Renewable Energy Laboratory materials discovery database for organic electronic materials. The focus is on materials for organic photovoltaic (OPV) absorber materials but materials suitable for other applications may be found here as well.",
#            "mdf-year": ,

            "mdf-links": {

                "mdf-landing_page": "https://organicelectronics.nrel.gov",

                "mdf-publication": ["https://dx.doi.org/10.1021/acs.jpcc.6b02138"],
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mdf-mrr": ,

            "mdf-data_contributor": [{
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
                }]
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
                "mdf-title": "NREL OPV - " + record["common_tag"],
                "mdf-acl": ["public"],

#                "mdf-tags": ,
#                "mdf-description": ,

                "mdf-composition": record["common_tag"],
                "mdf-raw": json.dumps(record),

                "mdf-links": {
                    "mdf-landing_page": record["URL"],

#                    "mdf-publication": ,
#                    "mdf-dataset_doi": ,

#                    "mdf-related_id": ,

                    # data links: {
     
                        #"globus_endpoint": ,
                        #"http_host": ,

                        #"path": ,
                        #},
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
