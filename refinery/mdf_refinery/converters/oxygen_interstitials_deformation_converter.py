import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_forge.toolbox import find_files

# VERSION 0.3.0

# This is the converter for the Oxygen Interstitials and Deformation Twins in alpha-Titanium dataset
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
            "title": "Interaction Between Oxygen Interstitials and Deformation Twins in alpha-Titanium",
            "acl": ["public"],
            "source_name": "oxygen_interstitials_deformation",
            "citation": ["Interaction Between Oxygen Interstitials and Deformation Twins in alpha-Titanium, Acta Materialia v. 105 (2016), pp. 44 - 51 http://dx.doi.org/10.1016/j.actamat.2015.12.019"],
            "data_contact": {

                "given_name": "William",
                "family_name": "Joost",

                "email": "william.joost@gmail.com",
                "institution": "University of Maryland",

                },

            "author": [{

                "given_name": "William",
                "family_name": "Joost",

                "email": "william.joost@gmail.com",
                "institution": "University of Maryland",

                },
                {

                "given_name": "Sreeramamurthy",
                "family_name": "Ankem",

                "institution": "University of Maryland",

                },
                {

                "given_name": "Maija",
                "family_name": "Kuklja",

                "institution": "University of Maryland",

                }],

            "license": "http://creativecommons.org/licenses/by/3.0/us/",

            "collection": "Oxygen Interstitials and Deformation Twins in alpha-Titanium",
            "tags": ["dft", "interstitials"],

            "description": "In this study we interrogate the behavior of oxygen (O) interstitials near a (10-12) twin boundary using a combination of density functional theory (DFT) and modified embedded atom method (MEAM) calculations.",
            "year": 2016,

            "links": {

                "landing_page": "https://materialsdata.nist.gov/dspace/xmlui/handle/11256/272",

                "publication": "http://dx.doi.org/10.1016/j.actamat.2015.12.019",
                "data_doi": "http://hdl.handle.net/11256/272",

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
    for data_file in tqdm(find_files(input_path, "OUTCAR"), desc="Processing files", disable= not verbose):
        data = parse_ase(os.path.join(data_file["path"], data_file["filename"]), "vasp-out")
        record_metadata = {
        "mdf": {
            "title": "Oxygen Interstitials and Deformation Twins - " + data["chemical_formula"],
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
            "composition": data["chemical_formula"],
#            "raw": ,

            "links": {
#                "landing_page": ,

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                "outcar": {
 
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    "http_host": "https://data.materialsdatafacility.org",

                    "path": "/collections/" + data_file["no_root_path"] + "/" + data_file["filename"],
                    },
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
