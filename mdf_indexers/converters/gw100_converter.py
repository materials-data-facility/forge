import json
import sys
import os

from tqdm import tqdm

from ..validator.schema_validator import Validator
from ..parsers.tab_parser import parse_tab

# VERSION 0.2.0

# This is the converter for the GW100 dataset.
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
            "mdf-title": "Benchmark of G0W0 on 100 Molecules",
            "mdf-acl": ["public"],
            "mdf-source_name": "gw100",
            "mdf-citation": ["M.J. van Setten, F. Caruso, S. Sharifzadeh, X. Ren, M. Scheffler, F. Liu, J. Lischner, L. Lin, J.R. Deslippe, S.G. Louie, C. Yang, F. Weigend, J.B. Neaton, F. Evers, and P. Rinke, GW100: Benchmarking G0W0 for Molecular Systems, J. Chem. Theory Comput. 11, 5665 (2015).", "M. Govoni et al., (2016). In preparation.", "P.J. Linstrom and W.G. Mallard, Eds., NIST Chemistry WebBook, NIST Standard Reference Database Number 69, National Institute of Standards and Technology, Gaithersburg MD, 20899, http://webbook.nist.gov."],
            "mdf-data_contact": {

                "given_name": "Michiel",
                "family_name": "van Setten",

                "email": "michiel.vansetten@uclouvain.be",
                "institution": "Université catholique de Louvain",

                },

#            "mdf-author": 

#            "mdf-license": ,

            "mdf-collection": "GW100",
            "mdf-data_format": "text",
            "mdf-data_type": "simulation",
#            "mdf-tags": ,

            "mdf-description": "This is a benchmark of G0W0 on 100 molecules.",
            "mdf-year": 2015,

            "mdf-links": {

                "mdf-landing_page": "http://www.west-code.org/database/gw100/index.php",

                "mdf-publication": "https://dx.doi.org/10.1021/acs.jctc.5b00453",
#                "mdf-dataset_doi": ,

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
    with open(os.path.join(input_path, "gw100.csv")) as in_file:
        data = in_file.read()
    for record in tqdm(parse_tab(data), desc="Processing records", disable= not verbose):
        record_metadata = {
            "mdf-title": "GW100 - " + record["name"],
            "mdf-acl": ["public"],

#            "mdf-tags": ,
#            "mdf-description": ,
            
            "mdf-composition": record["formula"],
#            "mdf-raw": ,

            "mdf-links": {
                "mdf-landing_page": "http://www.west-code.org/database/gw100/pag/" + record["cas"] + ".php",

#                "mdf-publication": ,
#                "mdf-dataset_doi": ,

#                "mdf-related_id": ,

                # data links: {
 
                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #},
                },

#            "mdf-citation": ,
#            "mdf-data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

                # IDs
#                },

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
