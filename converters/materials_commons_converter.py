import json
import sys
import os
from tqdm import tqdm
from parsers.utils import find_files
from validator import Validator


# This is the converter for Materials Commons. 
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
            "globus_subject": "https://materialscommons.org/mcpub/",
            "acl": ["public"],
            "mdf_source_name": "materials_commons",
            "mdf-publish.publication.collection": "Materials Commons",
#            "mdf_data_class": ,

            "cite_as": ["Puchala, B., Tarcea, G., Marquis, E.A. et al. JOM (2016) 68: 2035. doi:10.1007/s11837-016-1998-7"],
#            "license": ,

            "dc.title": "Materials Commons Data",
            "dc.creator": "University of Michigan",
            "dc.identifier": "https://materialscommons.org/mcpub/",
            "dc.contributor.author": ["B Puchala", "G Tarcea", "EA Marquis", "M Hedstrom", "HV Hagadish", "JE Allison"],
#            "dc.subject": ,
            "dc.description": "A platform for sharing research data.",
            "dc.relatedidentifier": ["https://dx.doi.org/10.1007/s11837-016-1998-7"],
            "dc.year": 2016
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
            mc_data = json.load(file_data)
        record_metadata = {
            "globus_subject": "https://materialscommons.org/mcpub/#/details/" + mc_data["id"],
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
#            "mdf-base.material_composition": ,

#            "cite_as": ,
#            "license": ,

            "dc.title": mc_data["title"],
#            "dc.creator": ,
            "dc.identifier": "https://materialscommons.org/mcpub/#/details/" + mc_data["id"],
            "dc.contributor.author": [author["firstname"] + " " + author["lastname"] for author in mc_data["authors"]],
#            "dc.subject": mc_data["keywords"],
            "dc.description": mc_data["description"],
#            "dc.relatedidentifier": mc_data["doi"],
            "dc.year": int(mc_data.get("published_date", "0000")[:4])

#            "data": {
#                "raw": ,
#                "files": ,
#                }
            }
        if mc_data["license"]["link"]:
            record_metadata["license"] = mc_data["license"]["link"]
        if mc_data["keywords"]:
            record_metadata["dc.subject"] = mc_data["keywords"]
        if mc_data["doi"]:
            record_metadata["dc.relatedidentifier"] = [mc_data["doi"]]

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
    convert(paths.datasets+"materials_commons_metadata", verbose=True)
