import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_forge.toolbox import find_files

# VERSION 0.3.0

# This is the converter for the JCAP Benchmarking database
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
            "title": "JCAP Benchmarking Database",
            "acl": ["public"],
            "source_name": "jcap_benchmarking_db",
            "citation": ["McCrory, C. C. L., Jung, S. H., Peters, J. C. & Jaramillo, T. F. Benchmarking Heterogeneous Electrocatalysts for the Oxygen Evolution Reaction. Journal of the American Chemical Society 135, 16977-16987, DOI: 10.1021/ja407115p (2013)", "McCrory, C. C. L. et al. Benchmarking HER and OER Electrocatalysts for Solar Water Splitting Devices. Journal of the American Chemical Society, 137, 4347–4357, DOI: 10.1021/ja510442p (2015)"],
            "data_contact": {

                "given_name": "Charles",
                "family_name": "McCrory",

                "email": "info@solarfuelshub.org",
                "institution": "Joint Center for Artificial Photosynthesis",

                },

            "author": [{

                "given_name": "Charles",
                "family_name": "McCrory",

                "institution": "Joint Center for Artificial Photosynthesis",

                },
                {

                "given_name": "Suho",
                "family_name": "Jung",

                "institution": "Joint Center for Artificial Photosynthesis",

                },
                {

                "given_name": "Jonas",
                "family_name": "Peters",

                "institution": "Joint Center for Artificial Photosynthesis",

                },
                {

                "given_name": "Thomas",
                "family_name": "Jaramillo",

                "institution": "Joint Center for Artificial Photosynthesis",

                }],

#            "license": ,

            "collection": "JCAP Benchmarking DB",
            "tags": ["benchmarking", "catalyst"],

            "description": "The JCAP Benchmarking scientists developed and implemented uniform methods and protocols for characterizing the activities of electrocatalysts under standard operating conditions for water-splitting devices. They have determined standard measurement protocols that reproducibly quantify catalytic activity and stability. Data for several catalysts studied are made available in this database.",
            "year": 2013,

            "links": {

                "landing_page": "http://solarfuelshub.org/benchmarking-database",

                "publication": ["https://dx.doi.org/10.1021/ja407115p", "https://dx.doi.org/10.1021/ja510442p"],
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
        record_metadata = {
        "mdf": {
            "title": "JCAP Benchmark - " + record["catalyst"],
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
            "composition": record["catalyst"],
            "raw": json.dumps(record),

            "links": {
                "landing_page": "https://internal.solarfuelshub.org/jcapresources/benchmarking/catalysts_for_iframe/view/jcapbench_catalyst/" + data_file["filename"][:-4],

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {
 
                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #},
                },

            "citation": record["publication"],
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
            "year": int(record["release_date"][:4]),

#            "mrr":

#            "processing": ,
#            "structure":,
            }
        }
#        record_metadata["jcap_benchmarking_db"] = record

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])


    if verbose:
        print("Finished converting")
