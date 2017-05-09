import json
import sys
import os
from tqdm import tqdm
from parsers.utils import find_files
from validator import Validator


# This is the converter for CXIDB.
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
            "globus_subject": "http://www.cxidb.org/",
            "acl": ["public"],
            "mdf_source_name": "cxidb",
            "mdf-publish.publication.collection": "CXIDB",
#            "mdf_data_class": ,

            "cite_as": ["Maia, F. R. N. C. The Coherent X-ray Imaging Data Bank. Nat. Methods 9, 854–855 (2012)."],
#            "license": ,

            "dc.title": "The Coherent X-ray Imaging Data Bank",
            "dc.creator": "CXIDB",
            "dc.identifier": "http://www.cxidb.org/",
            "dc.contributor.author": ["Maia, F. R. N. C."],
#            "dc.subject": ,
            "dc.description": "A new database which offers scientists from all over the world a unique opportunity to access data from Coherent X-ray Imaging (CXI) experiments.",
#            "dc.relatedidentifier": ,
            "dc.year": 2012
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
            cxidb_data = json.load(file_data)
        record_metadata = {
            "globus_subject": cxidb_data["url"],
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
#            "mdf-base.material_composition": ,

#            "cite_as": ,
#            "license": ,

            "dc.title": cxidb_data["citation_title"],
#            "dc.creator": ,
            "dc.identifier": cxidb_data["url"],
            "dc.contributor.author": [cxidb_data["citation_authors"]] if type(cxidb_data["citation_authors"]) is str else cxidb_data["citation_authors"],
#            "dc.subject": ,
#            "dc.description": ,
            "dc.relatedidentifier": [cxidb_data.get("citation_DOI", None), cxidb_data.get("entry_DOI", None)],
            "dc.year": int(cxidb_data["summary_deposition_date"][:4]),

            "data": {
                "raw": json.dumps(cxidb_data)
#                "files": ,
                }
            }

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
    convert(paths.datasets+"cxidb_metadata", verbose=True)
