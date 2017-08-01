import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_refinery.parsers.tab_parser import parse_tab

# VERSION 0.3.0

# This is the converter for the GW100 dataset
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
            "title": "Benchmark of G0W0 on 81 Molecules with Spin-Orbit Coupling",
            "acl": ["public"],
            "source_name": "gw_soc81",
            "citation": ["P. Scherpelz, M. Govoni, I. Hamada, and G. Galli, Implementation and Validation of Fully-Relativistic GW Calculations: Spin-Orbit Coupling in Molecules, Nanocrystals and Solids, J. Chem. Theory Comput. 12, 3523 (2016).", "P.J. Linstrom and W.G. Mallard, Eds., NIST Chemistry WebBook, NIST Standard Reference Database Number 69, National Institute of Standards and Technology, Gaithersburg MD, 20899, http://webbook.nist.gov."],
            "data_contact": {

                "given_name": "Peter",
                "family_name": "Scherpelz",

                "email": "pscherpelz@uchicago.edu",
                "institution": "The University of Chicago",
                },

            "author": [{

                "given_name": "Peter",
                "family_name": "Scherpelz",

                "email": "pscherpelz@uchicago.edu",
                "institution": "The University of Chicago",
                },
                {

                "given_name": "Marco",
                "family_name": "Govoni",

                "institution": "The University of Chicago",
                },
                {

                "given_name": "Ikutaro",
                "family_name": "Hamada",

                "institution": "National Institute for Materials Science",
                },
                {

                "given_name": "Giulia",
                "family_name": "Galli",

                "institution": "The University of Chicago",
                }],

#            "license": ,

            "collection": "GW-SOC81",
#            "tags": ,

            "description": "This is a benchmark of G0W0 on 81 molecules.",
            "year": 2016,

            "links": {

                "landing_page": "http://www.west-code.org/database/gwsoc81/index.php",

                "publication": "10.1021/acs.jctc.6b00114",
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
    with open(os.path.join(input_path, "gw_soc81.csv")) as in_file:
        data = in_file.read()
    for record in tqdm(parse_tab(data), desc="Processing records", disable= not verbose):
        link = "http://www.west-code.org/database/gwsoc81/pag/" + record["cas"] + ".php"
        record_metadata = {
        "mdf": {
            "title": "GW-SOC81 - " + record["name"],
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
            "composition": record["formula"],
#            "raw": ,

            "links": {
                "landing_page": "http://www.west-code.org/database/gwsoc81/pag/" + record["cas"] + ".php",

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
 #               },

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
