import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_forge.toolbox import find_files

# VERSION 0.3.0

# This is the converter for the NIST MML
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
            "title": "NIST Material Measurement Laboratory Data Repository",
            "acl": ["public"],
            "source_name": "nist_mml",
            "citation": ["http://materialsdata.nist.gov/"],
            "data_contact": {

                "given_name": "Michael",
                "family_name": "Fasolka",

                "email": "mmlinfo@nist.gov",
                "institution": "National Institute of Standards and Technology",

                },

#            "author": ,

#            "license": ,

            "collection": "NIST Material Measurement Laboratory",
#            "data_format": ,
#            "data_type": ,
            "tags": ["materials"],

            "description": "The laboratory supports the NIST mission by serving as the national reference laboratory for measurements in the chemical, biological and material sciences.",
            "year": 2013,

            "links": {

                "landing_page": "http://materialsdata.nist.gov/",

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
    for dir_data in tqdm(find_files(input_path, file_pattern="json", verbose=verbose), desc="Processing metadata", disable= not verbose):
        with open(os.path.join(dir_data["path"], dir_data["filename"])) as file_data:
            full_record = json.load(file_data)
        nist_data = {}
        # Collapse XML-style metadata into JSON and collect duplicates in lists
        for meta_dict in full_record:
            if not nist_data.get(meta_dict["key"], None): #No previous value, copy data
                nist_data[meta_dict["key"]] = meta_dict["value"]
            else: #Has value already
                new_list = []
                if type(nist_data[meta_dict["key"]]) is not list: #If previous value is not a list, add the single item
                    new_list.append(nist_data[meta_dict["key"]])
                else: #Previous value is a list
                    new_list += nist_data[meta_dict["key"]]
                #Now add new element and save
                new_list.append(meta_dict["value"])
                nist_data[meta_dict["key"]] = new_list
        uri = nist_data["dc.identifier.uri"][0] if type(nist_data.get("dc.identifier.uri", None)) is list else nist_data.get("dc.identifier.uri", None)
        record_metadata = {
        "mdf": {
            "title": nist_data["dc.title"][0] if type(nist_data.get("dc.title", None)) is list else nist_data.get("dc.title"),
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
#            "composition": ,
#            "raw": ,

            "links": {
                "landing_page": uri,

#                "publication": ,
                "data_doi": uri,

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
        if nist_data.get("dc.subject", None):
            if type(nist_data["dc.subject"]) is not list:
                record_metadata["mdf"]["tags"] = [nist_data["dc.subject"]]
            else:
                record_metadata["mdf"]["tags"] = nist_data["dc.subject"]
        if nist_data.get("dc.description.abstract", None):
            record_metadata["mdf"]["description"] = str(nist_data["dc.description.abstract"])
        if nist_data.get("dc.relation.uri", None):
            record_metadata["mdf"]["links"]["publication"] = nist_data["dc.relation.uri"]
        if nist_data.get("dc.date.issued", None):
            record_metadata["mdf"]["year"] = int(nist_data["dc.date.issued"][:4])


        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])


    if verbose:
        print("Finished converting")
