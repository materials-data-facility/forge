import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_forge.toolbox import find_files

# VERSION 0.3.0

# This is the converter for CoRE-MOF
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
            "title": "Computation-Ready Experimental Metal-Organic Frameworks Database",
            "acl": ["public"],
            "source_name": "core_mof",
            "citation": ['D. Nazarian, J. Camp, D.S. Sholl, "A Comprehensive Set of High-Quality Point Charges for Simulations of Metal-Organic Frameworks," Chemistry of Materials, 2016, 28 (3), pp 785–793'],
            "data_contact": {

                "given_name": "Yongchul",
                "family_name": "Chung",

                "email": "greg.chung@pusan.ac.kr",
                "institution": "Pusan National University"
                },

            "author": [{
                "given_name": "Dalar",
                "family_name": "Nazarian",
                "institution": "Georgia Institute of Technology"
                },
                {
                "given_name": "Jeffrey",
                "family_name": "Camp",
                "institution": "Georgia Institute of Technology"
                },
                {
                "given_name": "David",
                "family_name": "Sholl",
                "email": "david.sholl@chbe.gatech.edu",
                "institution": "Georgia Institute of Technology"
                }],

#            "license": ,

            "collection": "CoRE-MOF",
            "tags": ["simulation", "metallic-organic", "framework"],

            "description": "High-throughput computational screening of metal-organic frameworks rely on the availability of disorder-free atomic coordinate files which can be used as input to simulation software packages. We have created CoRE MOF database and its variants which contains almost all MOFs that have been reported in the literature.",
            "year": 2014,

            "links": {

                "landing_page": "http://gregchung.github.io/CoRE-MOFs/",

                "publication": ["https://dx.doi.org/10.1021/acs.chemmater.5b03836", "https://dx.doi.org/10.1021/cm502594j"],
#                "dataset_doi": ,

#                "related_id": ,

                # data links: {

                    #"globus_endpoint": ,
                    #"http_host": ,

                    #"path": ,
                    #}
                },

#            "mrr": ,

            "data_contributor": [{
                "given_name": "Jonathon",
                "family_name": "Gaff",
                "email": "jgaff@uchicago.edu",
                "institution": "The University of Chicago",
                "github": "jgaff"
                }]
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
    doi_path = os.path.join(input_path, "structure-doi-CoRE-MOFsV2.0.csv")
    cif_path = os.path.join(input_path, "core-mof-v1.0-ddec")
    # Get DOIs
    doi_dict = {}
    with open(doi_path) as dois:
        for line in dois:
            values = line.split(",")
            if values[1] != "-":
                doi_dict[values[0]] = values[1]
    # Get CIFs
    for cif in tqdm(find_files(cif_path, file_pattern=".cif", verbose=verbose), desc="Processing CIFs", disable= not verbose):
        with open(os.path.join(cif["path"], cif["filename"])) as cif_in:
            # Discard non-CIF, duplicate metadata in first line
            cif_in.readline()
            file_data = parse_ase(file_path=cif_in, data_format="cif", verbose=False)

        record_metadata = {
        "mdf": {
            "title": "CoRE-MOF - " + file_data["chemical_formula"] + " (" + cif["filename"].split("_")[0] + ")",
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
            "composition": file_data["chemical_formula"],
#            "raw": ,

            "links": {
                "landing_page": "https://github.com/gregchung/gregchung.github.io/blob/master/CoRE-MOFs/core-mof-v1.0-ddec/" + cif["filename"],

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                "cif": {
 
#                    "globus_endpoint": ,
                    "http_host": "https://raw.githubusercontent.com",

                    "path": "/gregchung/gregchung.github.io/master/CoRE-MOFs/core-mof-v1.0-ddec/" + cif["filename"],
                    },

#            "citation": ,
#            "data_contact": {

#                "given_name": ,
#                "family_name": ,

#                "email": ,
#                "institution":,

                # IDs
            },

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
        pubs = [doi_dict[key] for key in doi_dict.keys() if cif["filename"].startswith(key)]
        if pubs:
            record_metadata["mdf"]["links"]["publication"] = pubs

        # Pass each individual record to the Validator
        result = dataset_validator.write_record(record_metadata)

        # Check if the Validator accepted the record, and print a message if it didn't
        # If the Validator returns "success" == True, the record was written successfully
        if result["success"] is not True:
            print("Error:", result["message"])


    if verbose:
        print("Finished converting")
