import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for NIST's Classical Interatomic Potentials dataset
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
            "title": "Evaluation and comparison of classical interatomic potentials through a user-friendly interactive web-interface",
            "acl": ["public"],
            "source_name": "cip",
            "citation": ["Choudhary K, Congo FYP, Liang T, Becker C, Hennig RG, Tavazza F (2017) Evaluation and comparison of classical interatomic potentials through a user-friendly interactive web-interface. Scientific Data 4: 160125. http://dx.doi.org/10.1038/sdata.2016.125", "Choudhary K, Congo FYP, Liang T, Becker C, Hennig RG, Tavazza F (2017) Data from: Evaluation and comparison of classical interatomic potentials through a user-friendly interactive web-interface. Dryad Digital Repository. http://dx.doi.org/10.5061/dryad.dd56c"],
            "data_contact": {

                "given_name": "Kamal",
                "family_name": "Choudhary",

                "email": "kamal.choudhary@nist.gov",
                "institution": "National Institute of Standards and Technology"

                # IDs
                },

            "author": [{

                "given_name": "Kamal",
                "family_name": "Choudhary",

                "email": "kamal.choudhary@nist.gov",
                "institution": "National Institute of Standards and Technology"

                # IDs
                },
                {

                "given_name": "Faical",
                "family_name": "Congo",

                "institution": "National Institute of Standards and Technology"

                # IDs
                },
                {

                "given_name": "Tao",
                "family_name": "Liang",

                "institution": "The Pennsylvania State University"

                # IDs
                },
                {

                "given_name": "Chandler",
                "family_name": "Becker",

                "institution": "National Institute of Standards and Technology"

                # IDs
                },
                {

                "given_name": "Richard",
                "family_name": "Hennig",

                "institution": "University of Florida"

                # IDs
                },
                {

                "given_name": "Francesca",
                "family_name": "Tavazza",

                "institution": "National Institute of Standards and Technology"

                # IDs
                }],

            "license": "https://creativecommons.org/publicdomain/zero/1.0/",

            "collection": "NIST Classical Interatomic Potentials",
            "tags": ["interatomic potentials", "force-fields", "total energy", "energy", "elastic matrix", "structure", "elastic modulus", "JARVIS"],

            "description": "We computed energetics and elastic properties of variety of materials such as metals and ceramics using a wide range of empirical potentials and compared them to density functional theory (DFT) as well as to experimental data, where available.",
            "year": 2017,

            "links": {

                "landing_page": "https://www.ctcms.nist.gov/~knc6/periodic.html",

                "publication": "http://dx.doi.org/10.1038/sdata.2016.125",
                "data_doi": "http://dx.doi.org/10.5061/dryad.dd56c",

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
    with open(os.path.join(input_path, "classical_interatomic_potentials.json")) as in_file:
        for record in tqdm(json.load(in_file), desc="Converting data", disable= not verbose):
            record_metadata = {
            "mdf": {
                "title": "NIST Classical Interatomic Potential - " + record["forcefield"] + ", " + record["composition"],
                "acl": ["public"],

#                "tags": ,
#                "description": ,
                
                "composition": record["composition"],
                "raw": json.dumps(record),

                "links": {
#                    "landing_page": ,

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
