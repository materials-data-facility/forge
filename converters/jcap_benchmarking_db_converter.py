import json
import sys
import os
from tqdm import tqdm
from parsers.utils import find_files
from validator import Validator


# This is the converter for the JCAP Benchmarking Database 
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
            "globus_subject": "http://solarfuelshub.org/benchmarking-database",
            "acl": ["public"],
            "mdf_source_name": "jcap_benchmarking_db",
            "mdf-publish.publication.collection": "JCAP Benchmarking DB",
#            "mdf_data_class": ,

            "cite_as": ["McCrory, C. C. L., Jung, S. H., Peters, J. C. & Jaramillo, T. F. Benchmarking Heterogeneous Electrocatalysts for the Oxygen Evolution Reaction. Journal of the American Chemical Society 135, 16977-16987, DOI: 10.1021/ja407115p (2013)", "McCrory, C. C. L. et al. Benchmarking HER and OER Electrocatalysts for Solar Water Splitting Devices. Journal of the American Chemical Society, 137, 4347–4357, DOI: 10.1021/ja510442p (2015)"],
#            "license": ,

            "dc.title": "JCAP Benchmarking Database",
            "dc.creator": "JCAP",
            "dc.identifier": "http://solarfuelshub.org/benchmarking-database",
#            "dc.contributor.author": ,
#            "dc.subject": ,
            "dc.description": "The JCAP Benchmarking scientists developed and implemented uniform methods and protocols for characterizing the activities of electrocatalysts under standard operating conditions for water-splitting devices. They have determined standard measurement protocols that reproducibly quantify catalytic activity and stability. Data for several catalysts studied are made available in this database.",
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
#    dataset_validator = Validator(dataset_metadata, strict=False)
    # You can also force the Validator to treat warnings as errors with strict=True
    dataset_validator = Validator(dataset_metadata, strict=True)


    # Get the data
    # Each record also needs its own metadata
    for data_file in tqdm(find_files(input_path, ".txt"), desc="Processing files", disable= not verbose):
        with open(os.path.join(data_file["path"], data_file["filename"])) as in_file:
            record = {}
            key = ""
            for line in in_file:
                clean_line = line.strip()
                if clean_line.endswith(":"):
                    key = clean_line.strip(": ").lower().replace(" ", "_")
                else:
                    record[key] = clean_line
        link = "https://internal.solarfuelshub.org/jcapresources/benchmarking/catalysts_for_iframe/view/jcapbench_catalyst/" + data_file["filename"][:-4]
        record_metadata = {
            "globus_subject": link,
            "acl": ["public"],
#            "mdf-publish.publication.collection": ,
#            "mdf_data_class": ,
            "mdf-base.material_composition": record["catalyst"],

            "cite_as": [record["publication"]],
#            "license": ,

            "dc.title": "JCAP Benchmark - " + record["catalyst"],
#            "dc.creator": ,
            "dc.identifier": link,
#            "dc.contributor.author": ,
#            "dc.subject": ,
#            "dc.description": ,
#            "dc.relatedidentifier": ,
             "dc.year": int(record["release_date"][:4]),

            "data": {
#                "raw": ,
#                "files": 
                }
            }
        record_metadata["data"].update(record)

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
    convert(paths.datasets+"jcap_benchmarking_db", verbose=True)
