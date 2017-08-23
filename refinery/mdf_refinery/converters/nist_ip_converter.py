import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_forge.toolbox import find_files

# VERSION 0.3.0

# This is the converter for the NIST Interatomic Potentials dataset.
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
            "title": "NIST Interatomic Potentials Repository Project",
            "acl": ["public"],
            "source_name": "nist_ip",
            "citation": ['C.A. Becker, et al., "Considerations for choosing and using force fields and interatomic potentials in materials science and engineering," Current Opinion in Solid State and Materials Science, 17, 277-283 (2013). https://www.ctcms.nist.gov/potentials'],
            "data_contact": {

                "given_name": "Lucas",
                "family_name": "Hale",

                "email": "potentials@nist.gov",
                "institution": "National Institute of Standards and Technology"

                },

            "author": [{

                "given_name": "Lucas",
                "family_name": "Hale",

                "institution": "National Institute of Standards and Technology"

                },
                {

                "given_name": "Zachary",
                "family_name": "Trautt",

                "institution": "National Institute of Standards and Technology"

                },
                {

                "given_name": "Chandler",
                "family_name": "Becker",

                "institution": "National Institute of Standards and Technology"

                }],

#            "license": ,

            "collection": "NIST Interatomic Potentials",
            "tags": ["interatomic potential", "forcefield"],

            "description": "This repository provides a source for interatomic potentials (force fields), related files, and evaluation tools to help researchers obtain interatomic models and judge their quality and applicability.",
            "year": 2013,

            "links": {

                "landing_page": "https://www.ctcms.nist.gov/potentials/",

#                "publication": ,
#                "data_doi": ,

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
    for file_data in tqdm(find_files(input_path, "\.json$"), desc="Processing files", disable= not verbose):
        try:
            with open(os.path.join(file_data["path"], file_data["filename"]), 'r') as ip_file:
                ip_data = json.load(ip_file)["interatomic-potential"]
            if not ip_data:
                raise ValueError("No data in file")
        except Exception as e:
            if verbose:
                print("Error reading '" + os.path.join(file_data["path"], file_data["filename"]) + "'")
            continue
        url_list = []
        link_texts = []
        for artifact in ip_data["implementation"]:
            for web_link in artifact["artifact"]:
                url = web_link.get("web-link", {}).get("URL", None)
                if url:
                    if not url.startswith("http"):
                        url = "http://" + url
                    url_list.append(url)
                link_text = web_link.get("web-link", {}).get("link-text", None)
                if link_text:
                    link_texts.append(link_text)

        record_metadata = {
        "mdf": {
            "title": "NIST Interatomic Potential - " + ", ".join(link_texts),
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
            "composition": "".join(ip_data["element"]),
            "raw": json.dumps(ip_data),

            "links": {
#                "landing_page": ,

                "publication": url_list,
#                "data_doi": ,

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
