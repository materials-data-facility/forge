import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for NIST SRD 81
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
            "title": "NIST Heat Transmission Properties of Insulating and Building Materials",
            "acl": ["public"],
            "source_name": "nist_heat_transmission",
            "citation": ["Robert R. Zarr, Josue A. Chavez, Angela Y. Lee, Geraldine Dalton, and Shari L. Young, NIST Heat Transmission Properties of Insulating and Building Materials, NIST Standard Reference Database Number 81, National Institute of Standards and Technology, Gaithersburg MD, 20899, http://srdata.nist.gov/Insulation/."],
            "data_contact": {

                "given_name": "Robert",
                "family_name": "Zarr",

                "email": "robert.zarr@nist.gov",
                "institution": "National Institute of Standards and Technology",

                },

            "author": {

                "given_name": "Robert",
                "family_name": "Zarr",

                "email": "robert.zarr@nist.gov",
                "institution": "National Institute of Standards and Technology",

                },

#            "license": ,

            "collection": "NIST Heat Transmission Materials",
            "tags": ["heat conductivity", "insulation"],

            "description": "The NIST Database on Heat Conductivity of Building Materials provides a valuable reference for building designers, material manufacturers, and researchers in the thermal design of building components and equipment. NIST has accumulated a valuable and comprehensive collection of thermal conductivity data from measurements performed with a 200-mm square guarded-hot-plate apparatus (from 1933 to 1983). The guarded-hot-plate test method is arguably the most accurate and popular method for determination of thermal transmission properties of flat, homogeneous specimens under steady state conditions.",
            "year": 2015,

            "links": {

                "landing_page": "https://srdata.nist.gov/insulation/home/index",

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
    with open(os.path.join(input_path, "nist_heat_transmission.json")) as in_file:
        dataset = json.load(in_file)
    for record in tqdm(dataset, desc="Processing data", disable= not verbose):
        record_metadata = {
        "mdf": {
            "title": "Heat Transmission Properties - " + (record.get("Material") or record.get("tradename") or record.get("manufacturer", "") + str(record["ID"])),
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
            "composition": record.get("Material"),
            "raw": json.dumps(record),

            "links": {
                "landing_page": "https://srdata.nist.gov/insulation/Search/detail/" + str(record["ID"]),

#                "publication": ,
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
        desc = ""
        if record.get("tradename"):
            desc += record.get("tradename")
        if record.get("manufacturer"):
            if desc:
                desc += " by "
            desc += record.get("manufacturer")
        if desc:
            record_metadata["mdf"]["description"] = desc

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])


    if verbose:
        print("Finished converting")
