import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.parsers.pymatgen_parser import parse_pymatgen
from mdf_refinery.validator import Validator
from mdf_forge.toolbox import find_files

# VERSION 0.3.0

# This is the converter for AMCS
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
            "title": "The American Mineralogist Crystal Structure Database",
            "acl": ["public"],
            "source_name": "amcs",
            "citation": ["Downs, R.T. and Hall-Wallace, M. (2003) The American Mineralogist Crystal Structure Database. American Mineralogist 88, 247-250."],
            "data_contact": {

                "given_name": "Robert",
                "family_name": "Downs",

                "email": "rdowns@u.arizona.edu",
                "institution": "University of Arizona"

                # IDs
                },

            "author": [{

                "given_name": "Robert",
                "family_name": "Downs",

                "email": "rdowns@u.arizona.edu",
                "institution": "University of Arizona"
                },
                {
                "given_name": "Michelle",
                "family_name": "Hall-Wallace",
                "institution": "University of Arizona"
                }],

#            "license": ,

            "collection": "AMCS",
            "tags": ["crystal structure", "minerals"],

            "description": "A crystal structure database that includes every structure published in the American Mineralogist, The Canadian Mineralogist, European Journal of Mineralogy and Physics and Chemistry of Minerals, as well as selected datasets from other journals.",
            "year": 2003,

            "links": {

                "landing_page": "http://rruff.geo.arizona.edu/AMS/amcsd.php",

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

 #           "mrr": ,

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
    for cif in tqdm(find_files(root=input_path, file_pattern=".cif", verbose=verbose), desc="Processing files", disable= not verbose):
        cif_data = parse_pymatgen(os.path.join(cif["path"], cif["filename"]))["structure"]
        if cif_data:
            with open(os.path.join(cif["path"], cif["filename"])) as cif_file:
                cif_file.readline()
                mineral_name = cif_file.readline().split("'")[1]
            link = "http://rruff.geo.arizona.edu/AMS/minerals/" + mineral_name
            clink = "/AMS/xtal_data/CIFfiles/" + cif["filename"]
            dlink = "/AMS/xtal_data/DIFfiles/" + cif["filename"].replace(".cif", ".txt")
            record_metadata = {
            "mdf": {
                "title": "AMCS - " + mineral_name,
                "acl": ["public"],

                "tags": [mineral_name],
#                "description": ,
                
                "composition": cif_data["material_composition"],
#                "raw": ,

                "links": {
                    "landing_page": link,

#                    "publication": ,
#                    "dataset_doi": ,

#                    "related_id": ,

                    "cif": {
                        "http_host": "http://rruff.geo.arizona.edu",
                        "path": clink,
                        },

                    "dif": {
                        "http_host": "http://rruff.geo.arizona.edu",
                        "path": dlink
                        }
                    },


#                "citation": ,
#                "data_contact": {

#                    "given_name": ,
#                    "family_name": ,

#                    "email": ,
#                    "institution":,

                    # IDs
#                },

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
