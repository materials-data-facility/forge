import json
import sys
import os
from tqdm import tqdm
from parsers.utils import find_files
from validator import Validator


# This is the converter for MATIN. 
# Arguments:
#   input_path (string): The file or directory where the data resides. This should not be hard-coded in the function, for portability.
#   metadata (string or dict): The path to the JSON dataset metadata file, a dict containing the dataset metadata, or None to specify the metadata here. Default None.
#   verbose (bool): Should the script print status messages to standard output? Default False.
#       NOTE: The converter should have NO output if verbose is False, unless there is an error.
def convert(input_path, metadata=None, verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    if not metadata:
        dataset_metadata = {
            "globus_subject": "https://matin.gatech.edu/",
            "acl": ["public"],
            "mdf_source_name": "matin",
            "mdf-publish.publication.collection": "MATIN",
            "mdf_data_class": "oai_pmh",

            "cite_as": ["https://matin.gatech.edu/"],
#            "license": ,

            "dc.title": "MATerials Innovation Network",
            "dc.creator": "Georgia Institute of Technology",
            "dc.identifier": "https://matin.gatech.edu/",
#            "dc.contributor.author": ,
#            "dc.subject": ,
            "dc.description": "An e-collaboration platform for accelerating materials innovation"
#            "dc.relatedidentifier": ,
#            "dc.year": 
            }
    elif type(metadata) is str:
        try:
            with open(metadata, 'r') as metadata_file:
                dataset_metadata = json.load(metadata_file)
        except Exception as e:
            sys.exit("Error: Unable to read metadata: " + repr(e))
    elif type(metadata) is dict:
        dataset_metadata = metadata
    else:
        sys.exit("Error: Invalid metadata parameter")



    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
#    dataset_validator = Validator(dataset_metadata)
    # You can also force the Validator to treat warnings as errors with strict=True
    dataset_validator = Validator(dataset_metadata, strict=True)


    # Get the data
    # Each record also needs its own metadata
    for dir_data in tqdm(find_files(input_path, file_pattern="json", verbose=verbose), desc="Processing metadata", disable= not verbose):
        with open(os.path.join(dir_data["path"], dir_data["filename"])) as file_data:
            full_record = json.load(file_data)
        matin_data = full_record["metadata"]["oai_dc:dc"]
        uri = matin_data.get("dc.identifier", full_record["header"]["identifier"])
        record_metadata = {
            "globus_subject": uri,
            "acl": ["public"],
            "mdf-publish.publication.collection": "MATIN",
#            "mdf_data_class": ,
#            "mdf-base.material_composition": ,

#            "cite_as": ,
#            "license": ,

            "dc.title": matin_data.get("dc.title", "MATIN Entry " + dir_data["filename"].split("_")[0]),
#            "dc.creator": ,
            "dc.identifier": uri,
#            "dc.contributor.author": [matin_data["dc:creator"]] if type(matin_data.get("dc:creator", None)) is str else matin_data.get("dc:creator", None),
#            "dc.subject": [matin_data["dc:subject"]] if type(matin_data.get("dc:subject", None)) is str else matin_data.get("dc:subject", None),
#            "dc.description": matin_data.get("dc:description", None),
#            "dc.relatedidentifier": [matin_data["dc:relation"]] if type(matin_data.get("dc:relation", None)) is str else matin_data.get("dc:relation", None),
            "dc.year": int(matin_data["dc:date"][:4]) if matin_data.get("dc:date", None) else None,

            "data": {
                "raw": json.dumps(full_record),
#                "files": ,
                }
            }
        if matin_data.get("dc:creator", None):
            if type(matin_data["dc:creator"]) is not list:
                record_metadata["dc.contributor.author"] = [matin_data["dc:creator"]]
            else:
                record_metadata["dc.contributor.author"] = matin_data["dc:creator"]
        if matin_data.get("dc:subject", None):
            if type(matin_data["dc:subject"]) is not list:
                record_metadata["dc.subject"] = [matin_data["dc:subject"]]
            else:
                record_metadata["dc.subject"] = matin_data["dc:subject"]
        if matin_data.get("dc:description", None):
            record_metadata["dc.description"] = matin_data["dc:description"]
        if matin_data.get("dc:relation", None):
            if type(matin_data["dc:relation"]) is not list:
                record_metadata["dc.relatedidentifier"] = [matin_data["dc:relation"]]
            else:
                record_metadata["dc.relatedidentifier"] = matin_data["dc:relation"]


        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"], ":", result.get("invalid_metadata", ""))
        # The Validator may return warnings if strict=False, which should be noted
        if result.get("warnings", None):
            print("Warnings:", result["warnings"])

    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(paths.datasets+"matin_metadata", verbose=True)
