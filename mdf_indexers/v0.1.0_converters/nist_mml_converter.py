import json
import sys
import os
from tqdm import tqdm
from parsers.utils import find_files
from validator import Validator


# This is the converter for the NIST MML
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
            "globus_subject": "http://materialsdata.nist.gov/",
            "acl": ["public"],
            "mdf_source_name": "nist_mml",
            "mdf-publish.publication.collection": "NIST Material Measurement Laboratory",
#            "mdf_data_class": ,

            "cite_as": ["http://materialsdata.nist.gov/"],
#            "license": ,

            "dc.title": "NIST Material Measurement Laboratory Data Repository",
            "dc.creator": "National Institute of Standards and Technology",
            "dc.identifier": "http://materialsdata.nist.gov/",
#            "dc.contributor.author": ,
#            "dc.subject": ,
            "dc.description": "The laboratory supports the NIST mission by serving as the national reference laboratory for measurements in the chemical, biological and material sciences.",
#            "dc.relatedidentifier": ,
            "dc.year": 2013
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
            "globus_subject": uri,
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
#            "mdf-base.material_composition": ,

#            "cite_as": ,
#            "license": ,

            "dc.title": nist_data["dc.title"][0] if type(nist_data.get("dc.title", None)) is list else nist_data.get("dc.title"),
#            "dc.creator": ,
            "dc.identifier": uri,
#            "dc.contributor.author": [nist_data["dc.contributor.author"]] if type(nist_data.get("dc.contributor.author", None)) is str else nist_data.get("dc.contributor.author", None),
#            "dc.subject": [nist_data["dc.subject"]] if type(nist_data.get("dc.subject", None)) is str else nist_data.get("dc.subject", None),
#            "dc.description": str(nist_data.get("dc.description.abstract", None)) if nist_data.get("dc.description.abstract", None) else None,
#            "dc.relatedidentifier": [nist_data["dc.relation.uri"]] if type(nist_data.get("dc.relation.uri", None)) is str else nist_data.get("dc.relation.uri", None)
#            "dc.year": int(nist_data["dc.date.issued"][:4])

#            "data": {
#                "raw": ,
#                "files": ,
#                }
            }
        if nist_data.get("dc.contributor.author", None):
            if type(nist_data["dc.contributor.author"]) is not list:
                record_metadata["dc.contributor.author"] = [nist_data["dc.contributor.author"]]
            else:
                record_metadata["dc.contributor.author"] = nist_data["dc.contributor.author"]
        if nist_data.get("dc.subject", None):
            if type(nist_data["dc.subject"]) is not list:
                record_metadata["dc.subject"] = [nist_data["dc.subject"]]
            else:
                record_metadata["dc.subject"] = nist_data["dc.subject"]
        if nist_data.get("dc.description.abstract", None):
            record_metadata["dc.description"] = str(nist_data["dc.description.abstract"])
        if nist_data.get("dc.relation.uri", None):
            if type(nist_data["dc.relation.uri"]) is not list:
                record_metadata["dc.relatedidentifier"] = [nist_data["dc.relation.uri"]]
            else:
                record_metadata["dc.relatedidentifier"] = nist_data["dc.relation.uri"]
        if nist_data.get("dc.date.issued", None):
            record_metadata["dc.year"] = int(nist_data["dc.date.issued"][:4])

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
    convert(paths.datasets+"nist_mml", verbose=True)
