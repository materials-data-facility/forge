import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator

# VERSION 0.3.0

# This is the converter for NIST Standard Reference Database 144
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
            "title": "NIST Atomic Weights and Isotopic Compositions with Relative Atomic Masses",
            "acl": ["public"],
            "source_name": "nist_atom_weight_iso_comp",
            "citation": ["NIST Standard Reference Database 144"],
            "data_contact": {

                "given_name": "Karen",
                "family_name": "Olsen",

                "email": "karen.olsen@nist.gov",
                "institution": "National Institute of Standards and Technology",

                },

#            "author": ,

#            "license": ,

            "collection": "NIST Atomic Weights and Isotopic Compositions",
            "tags": ["atomic weight", "isotopic composition"],

            "description": "The atomic weights are available for elements 1 through 118 and isotopic compositions or abundances are given when appropriate.",
            "year": 1999,

            "links": {

                "landing_page": "https://www.nist.gov/pml/atomic-weights-and-isotopic-compositions-relative-atomic-masses",

                "publication": ["http://www.ciaaw.org/atomic-weights.htm", "http://www.iupac.org/publications/pac/83/2/0397/", "http://amdc.impcas.ac.cn/evaluation/data2012/ame.html"],
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

 #           "mrr": ,

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
    with open(os.path.join(input_path, "notes.json")) as notes:
        note_lookup = json.load(notes)
    with open(os.path.join(input_path, "atom_weight_iso_comp.txt")) as raw_file:
        raw = raw_file.read()
    raw = raw.replace("&nbsp;", "")
    record_list = raw.split("\n\n")
    for raw_record in tqdm(record_list, desc="Processing records", disable= not verbose):
        record = {}
        for line in raw_record.split("\n"):
            data_list = line.split("=")
            if len(data_list) > 1 and data_list[1].strip():
                record[data_list[0].strip().lower().replace(" ", "_")] = data_list[1].strip()

        record_metadata = {
        "mdf": {
            "title": "NIST Atomic Weights - " + record["atomic_symbol"] + record["mass_number"],
            "acl": ["public"],

#            "tags": ,
            "description": ",".join([note_lookup[n] for n in record.get("notes", "").split(",") if record.get("notes", "")]),

            "composition": record["atomic_symbol"],
            "raw": json.dumps(record),

            "links": {
#                "landing_page": ,

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

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])


    if verbose:
        print("Finished converting")
