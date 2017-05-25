import json
import sys
import os
from tqdm import tqdm
from parsers.utils import find_files
from validator import Validator


# This is the converter for the NIST XPS Database 
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
            "globus_subject": "https://srdata.nist.gov/xps/Default.aspx",
            "acl": ["public"],
            "mdf_source_name": "nist_xps_db",
            "mdf-publish.publication.collection": "NIST XPS DB",
#            "mdf_data_class": ,

            "cite_as": ["©2012 copyright by the U.S. Secretary of Commerce on behalf of the United States of America. All rights reserved."],
#            "license": ,

            "dc.title": "NIST X-ray Photoelectron Spectroscopy Database",
            "dc.creator": "NIST",
            "dc.identifier": "https://srdata.nist.gov/xps/Default.aspx",
            "dc.contributor.author": ["Alexander V. Naumkin", "Anna Kraut-Vass", "Stephen W. Gaarenstroom", "Cedric J. Powell"],
#            "dc.subject": ,
            "dc.description": "NIST Standard Reference Database 20",
#            "dc.relatedidentifier": ,
            "dc.year": 2000
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
#    dataset_validator = Validator(dataset_metadata, strict=False)
    # You can also force the Validator to treat warnings as errors with strict=True
    dataset_validator = Validator(dataset_metadata, strict=True)


    # Get the data
    # Each record also needs its own metadata
    for file_data in tqdm(find_files(input_path, ".json"), desc="Processing files", disable= not verbose):
        with open(os.path.join(file_data["path"], file_data["filename"]), "r") as in_file:
            record = json.load(in_file)
        id_num = file_data["filename"].rsplit("_", 1)[1].split(".", 1)[0]
        link = "https://srdata.nist.gov/xps/XPSDetailPage.aspx?AllDataNo=" + id_num
        record_metadata = {
            "globus_subject": link,
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
            "mdf-base.material_composition": record["Formula"],

#            "cite_as": ,
#            "license": ,

            "dc.title": "NIST XPS DB - " + record["Name"],
#            "dc.creator": ,
            "dc.identifier": link,
#            "dc.contributor.author": record["Author Name(s)"].split(","),
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
#            "dc.year": ,

            "data": {
                "raw": json.dumps(record)
#                "cas_number": record["CAS Registry No"]
#                "files": 
                }
            }
        if record["Citation"]:
            record_metadata["cite_as"] = record["Citation"]
        if record["Author Name(s)"]:
            record_metadata["dc.contributor.author"] = record["Author Name(s)"].split(",")
        if record["Notes"]:
            record_metadata["dc.description"] = record["Notes"]

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
    convert(paths.datasets+"nist_xps_db", verbose=True)
