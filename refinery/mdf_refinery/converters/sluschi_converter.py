import json
import sys
import os

from tqdm import tqdm

from mdf_refinery.validator import Validator
from mdf_forge.toolbox import find_files
from mdf_refinery.parsers.ase_parser import parse_ase

# VERSION 0.3.0

# This is the converter for
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
            "title": "Solid and Liquid in Ultra Small Coexistence with Hovering Interfaces",
            "acl": ["public"],
            "source_name": "sluschi",
            "citation": ["Qi-Jun Hong, Axel van de Walle, A user guide for SLUSCHI: Solid and Liquid in Ultra Small Coexistence with Hovering Interfaces, Calphad, Volume 52, March 2016, Pages 88-97, ISSN 0364-5916, http://doi.org/10.1016/j.calphad.2015.12.003."],
            "data_contact": {

                "given_name": "Qi-Jun",
                "family_name": "Hong",

                "email": "qhong@alumni.caltech.edu",
                "institution": "Brown University"

                # IDs
                },

            "author": [{

                "given_name": "Qi-Jun",
                "family_name": "Hong",

                "email": "qhong@alumni.caltech.edu",
                "institution": "Brown University"

                # IDs
                },
                {

                "given_name": "Axel",
                "family_name": "van de Walle",

                "email": "avdw@alum.mit.edu",
                "institution": "Brown University"

                # IDs
                }],

#            "license": ,

            "collection": "SLUSCHI",
            "tags": ["Melting temperature calculation", "Density functional theory", "Automated code"],

            "description": "Although various approaches for melting point calculations from first principles have been proposed and employed for years, their practical implementation has hitherto remained a complex and time-consuming process. The SLUSCHI code (Solid and Liquid in Ultra Small Coexistence with Hovering Interfaces) drastically simplifies this procedure into an automated package, by implementing the recently-developed small-size coexistence method and putting together a series of steps that lead to final melting point evaluation. Based on density functional theory, SLUSCHI employs Born–Oppenheimer molecular dynamics techniques under the isobaric–isothermal (NPT) ensemble, with interface to the first-principles code VASP.",
            "year": 2015,

            "links": {

                "landing_page": "http://blogs.brown.edu/qhong/?page_id=102",

                "publication": "https://doi.org/10.1016/j.calphad.2015.12.003",
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
    for dir_data in tqdm(find_files(root=input_path, file_pattern="^OUTCAR$"), desc="Processing data files", disable= not verbose):
        try:
            file_data = parse_ase(file_path=os.path.join(dir_data["path"], dir_data["filename"]), data_format="vasp-out", verbose=False)
            # If no data, skip record
            if not file_data:
                raise ValueError("No Data")
        except Exception as e:
            if "No Data" in repr(e):
                print("No data in record")
            # On an exception, skip the record
            continue

        uri = "/collections/sluschi/" + dir_data["no_root_path"] + "/" + dir_data["filename"]
        record_metadata = {
        "mdf": {
            "title": "SLUSCHI - " + file_data["chemical_formula"],
            "acl": ["public"],

#            "tags": ,
#            "description": ,
            
            "composition": file_data["chemical_formula"],
#            "raw": ,

            "links": {
#                "landing_page": uri,

#                "publication": ,
#                "dataset_doi": ,

#                "related_id": ,

                "outcar": {
 
                    "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                    "http_host": "https://data.materialsdatafacility.org",

                    "path": uri,
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
