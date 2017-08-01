import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_refinery.parsers.ase_parser import parse_ase
from mdf_forge.toolbox import find_files

# VERSION 0.3.0

# This is the converter for the Ti-O EAM Model dataset.
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
            "title": "A Modified Embedded Atom Method Potential for the Titanium-Oxygen System",
            "acl": ["public"],
            "source_name": "ti_o_meam_model",
            "citation": ['W.J. Joost, S. Ankem, M.M. Kuklja "A modified embedded atom method potential for the titanium-oxygen system" Modelling and Simulation in Materials Science and Engineering Vol. 23, pp. 015006 (2015) doi:10.1088/0965-0393/23/1/015006'],
            "data_contact": {

                "given_name": "William",
                "family_name": "Joost",

                "email": "wjoost@umd.edu",
                "institution": "University of Maryland",

                },

            "author": [{

                "given_name": "William",
                "family_name": "Joost",

                "email": "wjoost@umd.edu",
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

#            "license": ,

            "collection": "Ti-O MEAM Model",
            "tags": ["dft", "atom-scale simulation"],

            "description": '''The files included here are:
             1) LAMMPS and VASP input files describing the structures specified in the article.
              2) LAMMPS and VASP output files describing the calculation results and the output structures.
               3) A Python script used in this study to perform a brute force search of the parameter space for the Ti-O MEAM potential. Further details are provided in the article, and in the script.
                4) The Ti, O and Ti-O potential files in LAMMPS MEAM format''',
            "year": 2014,

            "links": {

                "landing_page": "https://materialsdata.nist.gov/dspace/xmlui/handle/11115/244",

                "publication": "https:/dx.doi.org/10.1088/0965-0393/23/1/015006",
                "data_doi": "http://hdl.handle.net/11115/244",

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
    for file_data in tqdm(find_files(input_path, "OUTCAR"), desc="Processing files", disable= not verbose):
        record = parse_ase(os.path.join(file_data["path"], file_data["filename"]), "vasp-out")
        record_metadata = {
        "mdf": {
            "title": "Ti-O MEAM Model - " + record["chemical_formula"],
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
            "composition": record["chemical_formula"],
#            "raw": ,

            "links": {
#                "landing_page": ,

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                "outcar": {
 
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    "http_host": "https://data.materialsdatafacility.org",

                    "path": "/collections/" + file_data["no_root_path"] + "/" + file_data["filename"],
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
