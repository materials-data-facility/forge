import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_forge.toolbox import find_files

# VERSION 0.3.0

# This is the converter for Materials Commons.
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
            "title": "Materials Commons Data",
            "acl": ["public"],
            "source_name": "materials_commons",
            "citation": ["Puchala, B., Tarcea, G., Marquis, E.A. et al. JOM (2016) 68: 2035. doi:10.1007/s11837-016-1998-7"],
            "data_contact": {

                "given_name": "Brian",
                "family_name": "Puchala",

                "email": "bpuchala@umich.edu",
                "institution": "University of Michigan",
                "orcid": "https://orcid.org/0000-0002-2461-6614"

                },

            "author": [{

                "given_name": "Brian",
                "family_name": "Puchala",

                "email": "bpuchala@umich.edu",
                "institution": "University of Michigan",
                "orcid": "https://orcid.org/0000-0002-2461-6614"

                },
                {

                "given_name": "Glenn",
                "family_name": "Tarcea",

                "institution": "University of Michigan",

                },
                {

                "given_name": "Emmanuelle",
                "family_name": "Marquis",

                "institution": "University of Michigan",

                },
                {

                "given_name": "Margaret",
                "family_name": "Hedstrom",

                "institution": "University of Michigan",

                },
                {

                "given_name": "Hosagrahar",
                "family_name": "Jagadish",

                "institution": "University of Michigan",

                },
                {

                "given_name": "John",
                "family_name": "Allison",

                "institution": "University of Michigan",

                }],

#            "license": ,

            "collection": "Materials Commons",
            "tags": ["materials"],

            "description": "A platform for sharing research data.",
            "year": 2016,

            "links": {

                "landing_page": "https://materialscommons.org/mcpub/",

                "publication": "https://dx.doi.org/10.1007/s11837-016-1998-7",
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
    for dir_data in tqdm(find_files(input_path, file_pattern="json", verbose=verbose), desc="Processing metadata", disable= not verbose):
        with open(os.path.join(dir_data["path"], dir_data["filename"])) as file_data:
            mc_data = json.load(file_data)
        record_metadata = {
        "mdf": {
            "title": mc_data["title"],
            "acl": ["public"],

            "tags": mc_data["keywords"],
            "description": mc_data["description"],
            
#            "composition": ,
#            "raw": ,

            "links": {
                "landing_page": "https://materialscommons.org/mcpub/#/details/" + mc_data["id"],

                "publication": mc_data["doi"],
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

#            "license": mc_data["license"]["link"],
#            "collection": ,
#            "data_format": ,
#            "data_type": ,
            "year": int(mc_data.get("published_date", "0000")[:4]),

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
